//! Core download orchestration: probing, single-thread fallback, and the
//! multi-thread worker fan-out.
//!
//! ## Design notes
//!
//! * Each worker opens its **own** [`tokio::fs::File`] handle. The kernel keeps
//!   per-FD positions, so multiple workers seeking and writing to disjoint
//!   ranges in the same file do not race. There is no shared `Mutex<File>`.
//! * Progress is tracked via a single `Arc<AtomicU64>` of bytes-downloaded.
//!   Workers do `fetch_add`; a background task reads the atomic on a ticker
//!   and emits [`DownloadEvent::Progress`]. No `O(chunks)` channel traffic.
//! * Per-worker positions live in `Vec<Arc<AtomicU64>>`. A background state
//!   task snapshots them on a ticker and writes the resume file atomically.
//! * Disk flushes are byte-counted, not modulo-positioned. The flush trigger
//!   actually fires.
//! * Single-thread downloads also respect resume: if a partial file exists,
//!   we issue a `Range: bytes=N-` request and either resume (206) or restart
//!   (200, with explicit truncation).

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use reqwest::{Client, StatusCode, Url};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::checksum::{self, Checksum};
use crate::error::{Error, Result};
use crate::http::{self, FileInfo, HttpConfig};
use crate::progress::{DownloadEvent, ProgressReporter, SpeedMeter};
use crate::state::{self, DownloadState, WorkerRange};

/// User-facing configuration.
#[derive(Debug, Clone)]
pub struct DownloaderConfig {
    pub http: HttpConfig,
    /// Number of threads when the caller doesn't override.
    pub default_threads: u16,
    /// Hard upper bound on threads per task.
    pub max_threads: u16,
    /// Don't slice ranges smaller than this. Prevents 16 workers fighting over
    /// a 1 KiB file.
    pub min_chunk_size: u64,
    /// How often to emit `Progress` events.
    pub progress_interval: Duration,
    /// Sliding window for speed smoothing.
    pub speed_window: Duration,
    /// How often the resume-state file is written.
    pub state_save_interval: Duration,
    /// Per-worker: how many bytes between `flush()` calls.
    pub flush_interval_bytes: u64,
    /// Verify the finished file against a digest the server advertised
    /// (`Repr-Digest` / `Digest` / `Content-MD5`) when the caller didn't supply
    /// one. A caller-supplied checksum is always verified regardless. Costs one
    /// sequential read+hash of the file, only when a digest is available.
    pub verify_server_digest: bool,
    /// Make resume crash-safe against *power loss* (not just process kill):
    /// `fdatasync`/`fsync` the data file before each resume checkpoint, so the
    /// saved progress never names bytes the OS hasn't durably written. Off by
    /// default because the extra syncs cost throughput — the default already
    /// survives a process kill (the page cache outlives the process); only a
    /// hard power loss / kernel panic needs this.
    pub durable_resume: bool,
    /// Per-worker: maximum number of retry attempts before giving up.
    pub max_retries: u32,
    /// Initial backoff between retries; doubles each time up to
    /// `max_retry_delay`.
    pub initial_retry_delay: Duration,
    pub max_retry_delay: Duration,
}

impl Default for DownloaderConfig {
    fn default() -> Self {
        Self {
            http: HttpConfig::default(),
            default_threads: 16,
            max_threads: 32,
            min_chunk_size: 1 << 20, // 1 MiB
            progress_interval: Duration::from_millis(250),
            speed_window: Duration::from_secs(2),
            state_save_interval: Duration::from_secs(2),
            flush_interval_bytes: 4 * 1024 * 1024, // 4 MiB
            verify_server_digest: true,
            durable_resume: false,
            max_retries: 8,
            initial_retry_delay: Duration::from_millis(500),
            max_retry_delay: Duration::from_secs(30),
        }
    }
}

/// One unit of work for the downloader.
#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub id: u64,
    pub url: String,
    /// Exact path the file should be written to. The caller is responsible for
    /// choosing this; we never derive a path component from server-supplied
    /// metadata (that would be a directory-traversal hazard).
    pub save_path: PathBuf,
    /// Override the downloader's default thread count for this task.
    pub thread_count: Option<u16>,
    /// Expected content digest. When set, the finished file is hashed and
    /// verified against it; a mismatch fails the download and removes the file.
    /// This is the strongest integrity guard and catches corruption that the
    /// size / ETag / If-Range checks cannot (e.g. a power-loss resume gap).
    pub expected_checksum: Option<Checksum>,
}

impl DownloadTask {
    pub fn new(url: impl Into<String>, save_path: impl Into<PathBuf>) -> Self {
        Self {
            id: 0,
            url: url.into(),
            save_path: save_path.into(),
            thread_count: None,
            expected_checksum: None,
        }
    }
    pub fn with_id(mut self, id: u64) -> Self {
        self.id = id;
        self
    }
    pub fn with_threads(mut self, n: u16) -> Self {
        self.thread_count = Some(n);
        self
    }
    /// Verify the finished file against this digest.
    pub fn with_checksum(mut self, checksum: Checksum) -> Self {
        self.expected_checksum = Some(checksum);
        self
    }
}

/// What the downloader returns on success.
#[derive(Debug, Clone)]
pub struct DownloadOutcome {
    pub id: u64,
    pub path: PathBuf,
    pub size: u64,
    pub elapsed: Duration,
}

/// The downloader itself. Cheap to clone (only an `Arc` is duplicated).
#[derive(Clone)]
pub struct Downloader {
    inner: Arc<DownloaderInner>,
}

struct DownloaderInner {
    client: Client,
    config: DownloaderConfig,
    reporter: Arc<dyn ProgressReporter>,
}

impl Downloader {
    pub fn new(reporter: Arc<dyn ProgressReporter>) -> Result<Self> {
        Self::with_config(DownloaderConfig::default(), reporter)
    }

    pub fn with_config(
        config: DownloaderConfig,
        reporter: Arc<dyn ProgressReporter>,
    ) -> Result<Self> {
        let client = http::build_client(&config.http)?;
        Ok(Self {
            inner: Arc::new(DownloaderInner {
                client,
                config,
                reporter,
            }),
        })
    }

    pub fn config(&self) -> &DownloaderConfig {
        &self.inner.config
    }

    pub fn reporter(&self) -> &Arc<dyn ProgressReporter> {
        &self.inner.reporter
    }

    /// Download a single task. Cancellable.
    pub async fn download(
        &self,
        task: DownloadTask,
        cancel: CancellationToken,
    ) -> Result<DownloadOutcome> {
        let started = Instant::now();
        let id = task.id;
        let reporter = &self.inner.reporter;

        reporter.on_event(DownloadEvent::Probing { id });

        // Run the actual work and capture errors so we can emit Failed / Cancelled.
        let result = self.download_inner(&task, &cancel, started).await;
        match &result {
            Ok(out) => {
                reporter.on_event(DownloadEvent::Completed {
                    id,
                    path: out.path.clone(),
                    size: out.size,
                    elapsed: out.elapsed,
                });
            }
            Err(e) if e.is_cancelled() => {
                reporter.on_event(DownloadEvent::Cancelled { id });
            }
            Err(e) => {
                reporter.on_event(DownloadEvent::Failed {
                    id,
                    error: e.to_string(),
                });
            }
        }
        result
    }

    async fn download_inner(
        &self,
        task: &DownloadTask,
        cancel: &CancellationToken,
        started: Instant,
    ) -> Result<DownloadOutcome> {
        let cfg = &self.inner.config;
        let id = task.id;

        let url = Url::parse(&task.url).map_err(|_| Error::InvalidUrl(task.url.clone()))?;

        // Make parent directory.
        if let Some(parent) = task.save_path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }

        // Probe (with cancellation).
        let info = tokio::select! {
            r = http::probe(&self.inner.client, &url, &cfg.http) => r?,
            _ = cancel.cancelled() => return Err(Error::Cancelled),
        };

        let requested_threads = task
            .thread_count
            .unwrap_or(cfg.default_threads)
            .clamp(1, cfg.max_threads);

        let can_multi = info.supports_range
            && info.size.map(|s| s > 0).unwrap_or(false)
            && requested_threads > 1;

        let chosen_threads = if can_multi { requested_threads } else { 1 };

        self.inner.reporter.on_event(DownloadEvent::Started {
            id,
            total_size: info.size,
            supports_range: info.supports_range,
            thread_count: chosen_threads,
            path: task.save_path.clone(),
        });

        let final_path = if can_multi {
            self.multi_thread(id, &info, &task.save_path, requested_threads, cancel)
                .await?
        } else {
            self.single_thread(id, &info, &task.save_path, cancel)
                .await?
        };

        let size = tokio::fs::metadata(&final_path).await?.len();
        if let Some(expected) = info.size {
            if size != expected {
                return Err(Error::SizeMismatch {
                    expected,
                    actual: size,
                });
            }
        }

        // End-to-end content verification. The caller's checksum wins; otherwise
        // fall back to a server-advertised digest (if enabled). This is the only
        // check that catches corruption the byte-count/validator checks can't —
        // e.g. a power-loss resume that skipped un-fsynced bytes.
        let expected_checksum = task.expected_checksum.clone().or_else(|| {
            if cfg.verify_server_digest {
                info.digest.clone()
            } else {
                None
            }
        });
        if let Some(expected) = expected_checksum {
            if let Err(e) = checksum::verify_file(&final_path, &expected).await {
                // The finished file is corrupt: remove it and any resume state so
                // a rerun starts clean instead of "completing" a bad file again.
                let _ = tokio::fs::remove_file(&final_path).await;
                state::delete(&state::state_path_for(&final_path)).await;
                return Err(e);
            }
        }

        Ok(DownloadOutcome {
            id,
            path: final_path,
            size,
            elapsed: started.elapsed(),
        })
    }

    async fn multi_thread(
        &self,
        id: u64,
        info: &FileInfo,
        path: &Path,
        thread_count: u16,
        cancel: &CancellationToken,
    ) -> Result<PathBuf> {
        let cfg = &self.inner.config;
        let file_size = info.size.ok_or(Error::UnknownLength)?;
        let state_path = state::state_path_for(path);

        // Try to load resume state. Throw it away unless the server still
        // reports the same resource: same length *and* a matching validator
        // (ETag / Last-Modified). A same-size content change with stale state
        // would otherwise splice two versions together and corrupt the file.
        let workers: Vec<WorkerRange> = match state::load(&state_path).await {
            Ok(s)
                if s.file_size == file_size
                    && !s.workers.is_empty()
                    && s.matches_resource(info.etag.as_deref(), info.last_modified.as_deref()) =>
            {
                s.workers
            }
            _ => state::split_workers(file_size, thread_count, cfg.min_chunk_size),
        };

        // Ensure the destination file exists at the right length. We use
        // `truncate(false)` because if a partial download from a previous run
        // exists, we want to keep its bytes – the resume state tells the
        // workers which bytes are already valid.
        {
            let f = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(path)
                .await?;
            f.set_len(file_size).await?;
            // Best-effort fsync so the metadata change survives a power loss.
            let _ = f.sync_all().await;
        }

        let starts: Vec<u64> = workers.iter().map(|w| w.start).collect();
        let ends: Vec<u64> = workers.iter().map(|w| w.end).collect();
        let currents: Vec<Arc<AtomicU64>> = workers
            .iter()
            .map(|w| Arc::new(AtomicU64::new(w.current)))
            .collect();

        let initial_downloaded: u64 = workers.iter().map(|w| w.current - w.start).sum();
        let downloaded = Arc::new(AtomicU64::new(initial_downloaded));

        let stop_bg = CancellationToken::new();

        // ---- progress task ----
        let prog_handle = {
            let reporter = self.inner.reporter.clone();
            let downloaded = downloaded.clone();
            let stop = stop_bg.clone();
            let interval = cfg.progress_interval;
            let window = cfg.speed_window;
            tokio::spawn(progress_task(
                id,
                Some(file_size),
                downloaded,
                reporter,
                interval,
                window,
                stop,
            ))
        };

        // ---- state-save task ----
        let state_handle = {
            let starts = starts.clone();
            let ends = ends.clone();
            let currents = currents.clone();
            let url = info.final_url.to_string();
            let etag = info.etag.clone();
            let last_modified = info.last_modified.clone();
            let state_path_cl = state_path.clone();
            let data_path = path.to_path_buf();
            let durable = cfg.durable_resume;
            let stop = stop_bg.clone();
            let interval = cfg.state_save_interval;
            tokio::spawn(state_save_task(
                starts,
                ends,
                currents,
                url,
                etag,
                last_modified,
                file_size,
                state_path_cl,
                data_path,
                durable,
                interval,
                stop,
            ))
        };

        // Validator sent as `If-Range` on every worker request, so the server
        // returns 200 (not 206) the instant the resource changes under us —
        // turning a would-be silent splice into a clean, detectable failure.
        let if_range = info.if_range_validator().map(|s| s.to_owned());

        // ---- workers ----
        let mut handles = Vec::new();
        for i in 0..currents.len() {
            let start = starts[i];
            let end = ends[i];
            if start >= end {
                continue;
            }
            let current = currents[i].clone();
            let downloaded = downloaded.clone();
            let client = self.inner.client.clone();
            let url = info.final_url.clone();
            let path = path.to_path_buf();
            let cfg = cfg.clone();
            let cancel = cancel.clone();
            let if_range = if_range.clone();
            handles.push(tokio::spawn(async move {
                worker_loop(
                    client, url, path, end, current, downloaded, if_range, cfg, cancel,
                )
                .await
            }));
        }

        // Collect worker results.
        let mut first_error: Option<Error> = None;
        let mut cancelled = false;
        for h in handles {
            match h.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) if e.is_cancelled() => cancelled = true,
                Ok(Err(e)) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
                Err(join_err) => {
                    if first_error.is_none() {
                        first_error = Some(Error::Io(std::io::Error::other(format!(
                            "worker join: {join_err}"
                        ))));
                    }
                }
            }
        }

        // Stop background tasks (they save their final state on the way out).
        stop_bg.cancel();
        let _ = prog_handle.await;
        let _ = state_handle.await;

        if cancelled {
            return Err(Error::Cancelled);
        }
        if let Some(e) = first_error {
            // The resource changed mid-flight: the bytes on disk are a mix of
            // versions, so the saved progress is worthless. Drop it so the next
            // run re-probes and downloads the new version cleanly.
            if e.is_resource_changed() {
                state::delete(&state_path).await;
            }
            return Err(e);
        }

        // Final integrity check: did we actually transfer the expected number
        // of bytes? `metadata.len()` alone is not enough because `set_len`
        // already set the length up-front.
        let total = downloaded.load(Ordering::Relaxed);
        if total != file_size {
            return Err(Error::SizeMismatch {
                expected: file_size,
                actual: total,
            });
        }

        // Final fsync so the user-visible "complete" state is durable.
        {
            let f = tokio::fs::OpenOptions::new().write(true).open(path).await?;
            let _ = f.sync_all().await;
        }

        state::delete(&state_path).await;
        Ok(path.to_path_buf())
    }

    async fn single_thread(
        &self,
        id: u64,
        info: &FileInfo,
        path: &Path,
        cancel: &CancellationToken,
    ) -> Result<PathBuf> {
        let cfg = &self.inner.config;

        // Drive retries at this level so a fresh attempt re-reads on-disk
        // state (the previous attempt may have made partial progress).
        let mut attempt: u32 = 0;
        loop {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            match single_thread_attempt(
                &self.inner.client,
                &info.final_url,
                path,
                info.size,
                info.supports_range,
                info.if_range_validator(),
                cfg.durable_resume,
                id,
                &self.inner.reporter,
                cfg,
                cancel,
            )
            .await
            {
                Ok(()) => return Ok(path.to_path_buf()),
                Err(e) if e.is_cancelled() => return Err(Error::Cancelled),
                Err(e) => {
                    attempt += 1;
                    if attempt >= cfg.max_retries {
                        return Err(Error::RetryExhausted {
                            attempts: attempt,
                            source: Box::new(e),
                        });
                    }
                    let delay = backoff(attempt, cfg.initial_retry_delay, cfg.max_retry_delay);
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = cancel.cancelled() => return Err(Error::Cancelled),
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

async fn progress_task(
    id: u64,
    total: Option<u64>,
    downloaded: Arc<AtomicU64>,
    reporter: Arc<dyn ProgressReporter>,
    interval: Duration,
    window: Duration,
    stop: CancellationToken,
) {
    let mut meter = SpeedMeter::new(window);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // First tick fires immediately; throw it away so we don't emit a spurious
    // 0-bps event before any data has been seen.
    ticker.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            _ = ticker.tick() => {
                emit(id, total, &downloaded, &mut meter, &*reporter);
            }
        }
    }
    // Final emit so the consumer sees the last position before Completed.
    emit(id, total, &downloaded, &mut meter, &*reporter);
}

fn emit(
    id: u64,
    total: Option<u64>,
    downloaded: &AtomicU64,
    meter: &mut SpeedMeter,
    reporter: &dyn ProgressReporter,
) {
    let now = Instant::now();
    let cur = downloaded.load(Ordering::Relaxed);
    meter.record(now, cur);
    let speed = meter.speed_bps(now);
    let eta = total.and_then(|t| {
        if speed == 0 || cur >= t {
            None
        } else {
            Some(t.saturating_sub(cur) / speed)
        }
    });
    reporter.on_event(DownloadEvent::Progress {
        id,
        downloaded: cur,
        total,
        speed_bps: speed,
        eta_secs: eta,
    });
}

#[allow(clippy::too_many_arguments)]
async fn state_save_task(
    starts: Vec<u64>,
    ends: Vec<u64>,
    currents: Vec<Arc<AtomicU64>>,
    url: String,
    etag: Option<String>,
    last_modified: Option<String>,
    file_size: u64,
    path: PathBuf,
    data_path: PathBuf,
    durable_resume: bool,
    interval: Duration,
    stop: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await; // throw away the immediate first tick

    // When durability is requested, keep a handle to the data file so we can
    // `fdatasync` it before each checkpoint. The file is pre-allocated to its
    // full length up front, so only data blocks (not size metadata) need
    // syncing here — `sync_data` is enough and cheaper than `sync_all`.
    let sync_file = if durable_resume {
        tokio::fs::OpenOptions::new()
            .write(true)
            .open(&data_path)
            .await
            .ok()
    } else {
        None
    };

    let snapshot = |starts: &[u64], ends: &[u64], currents: &[Arc<AtomicU64>]| {
        DownloadState::new(
            url.clone(),
            file_size,
            starts
                .iter()
                .zip(ends.iter())
                .zip(currents.iter())
                .map(|((&s, &e), c)| WorkerRange {
                    start: s,
                    current: c.load(Ordering::Relaxed).min(e),
                    end: e,
                })
                .collect(),
        )
        .with_resource_id(etag.clone(), last_modified.clone())
    };

    // Snapshot first, then sync, then persist: every byte named by the snapshot
    // had its `write_all` complete before we read `current`, so it is in the
    // page cache before the `sync_data` call and therefore durable once it
    // returns. The saved state can never get ahead of durable bytes.
    let checkpoint = |starts: &[u64], ends: &[u64], currents: &[Arc<AtomicU64>]| {
        let s = snapshot(starts, ends, currents);
        let sync_file = &sync_file;
        let path = &path;
        async move {
            if let Some(f) = sync_file.as_ref() {
                let _ = f.sync_data().await;
            }
            let _ = state::save(path, &s).await;
        }
    };

    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            _ = ticker.tick() => {
                checkpoint(&starts, &ends, &currents).await;
            }
        }
    }
    // Always checkpoint once on shutdown so a cancellation or error preserves
    // resume state.
    checkpoint(&starts, &ends, &currents).await;
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn worker_loop(
    client: Client,
    url: Url,
    path: PathBuf,
    end: u64,
    current: Arc<AtomicU64>,
    downloaded: Arc<AtomicU64>,
    if_range: Option<String>,
    cfg: DownloaderConfig,
    cancel: CancellationToken,
) -> Result<()> {
    let mut attempt: u32 = 0;

    while current.load(Ordering::Relaxed) < end {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let pos = current.load(Ordering::Relaxed);
        let r = download_segment(
            &client,
            &url,
            &path,
            pos,
            end,
            &current,
            &downloaded,
            if_range.as_deref(),
            &cfg,
            &cancel,
        )
        .await;
        match r {
            Ok(()) => {
                attempt = 0;
            }
            Err(Error::Cancelled) => return Err(Error::Cancelled),
            // `NoRangeSupport` from inside a worker means the server returned
            // 200 to a Range request mid-download. We can't recover within
            // multi-thread mode; surface it to the orchestrator.
            Err(Error::NoRangeSupport) => return Err(Error::NoRangeSupport),
            // The resource changed under us (If-Range rejected): retrying can't
            // help and the bytes on disk are now a mix of versions. Surface it
            // so the orchestrator discards the state instead of corrupting.
            Err(Error::ResourceChanged) => return Err(Error::ResourceChanged),
            Err(e) => {
                attempt += 1;
                if attempt >= cfg.max_retries {
                    return Err(Error::RetryExhausted {
                        attempts: attempt,
                        source: Box::new(e),
                    });
                }
                let delay = backoff(attempt, cfg.initial_retry_delay, cfg.max_retry_delay);
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = cancel.cancelled() => return Err(Error::Cancelled),
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn download_segment(
    client: &Client,
    url: &Url,
    path: &Path,
    start: u64,
    end: u64,
    current: &AtomicU64,
    downloaded: &AtomicU64,
    if_range: Option<&str>,
    cfg: &DownloaderConfig,
    cancel: &CancellationToken,
) -> Result<()> {
    if start >= end {
        return Ok(());
    }
    let range = format!("bytes={}-{}", start, end - 1);
    let mut req = client
        .get(url.as_str())
        .header(reqwest::header::RANGE, &range)
        .timeout(cfg.http.chunk_request_timeout);
    if let Some(v) = if_range {
        req = req.header(reqwest::header::IF_RANGE, v);
    }
    let resp = req.send().await?;

    match resp.status() {
        StatusCode::PARTIAL_CONTENT => {}
        // A 200 to a Range request means the server served the whole entity.
        // With `If-Range` set that specifically means the validator no longer
        // matches (the resource changed); without it, the server just ignores
        // ranges. Distinguish so the orchestrator can react correctly.
        StatusCode::OK if if_range.is_some() => return Err(Error::ResourceChanged),
        StatusCode::OK => return Err(Error::NoRangeSupport),
        StatusCode::RANGE_NOT_SATISFIABLE => {
            // The state file thinks this worker still has bytes to fetch, but
            // the server says the range is unsatisfiable. Mark the worker done
            // so its loop can't spin re-issuing the same doomed request; the
            // orchestrator's byte-count check still catches real corruption.
            current.store(end, Ordering::Relaxed);
            return Ok(());
        }
        s => return Err(Error::Status(s)),
    }

    // Open our own file handle for this worker. No shared mutex.
    let mut file = tokio::fs::OpenOptions::new().write(true).open(path).await?;
    file.seek(SeekFrom::Start(start)).await?;

    let mut pos = start;
    let mut bytes_since_flush: u64 = 0;
    let mut stream = resp.bytes_stream();
    while let Some(chunk_result) = stream.next().await {
        if cancel.is_cancelled() {
            let _ = file.flush().await;
            return Err(Error::Cancelled);
        }
        let chunk = chunk_result?;
        let chunk_len = chunk.len() as u64;
        if chunk_len == 0 {
            continue;
        }

        // Cap at the worker's end so we never trample the next worker's range.
        let write_len = if pos + chunk_len > end {
            (end - pos) as usize
        } else {
            chunk_len as usize
        };
        if write_len == 0 {
            break;
        }
        file.write_all(&chunk[..write_len]).await?;
        pos += write_len as u64;
        bytes_since_flush += write_len as u64;

        current.store(pos, Ordering::Relaxed);
        downloaded.fetch_add(write_len as u64, Ordering::Relaxed);

        if bytes_since_flush >= cfg.flush_interval_bytes {
            file.flush().await?;
            bytes_since_flush = 0;
        }
        if pos >= end {
            break;
        }
    }
    file.flush().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Single-thread path (also used as a fallback when the server lacks Range)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn single_thread_attempt(
    client: &Client,
    url: &Url,
    path: &Path,
    expected_size: Option<u64>,
    server_supports_range: bool,
    if_range: Option<&str>,
    durable_resume: bool,
    id: u64,
    reporter: &Arc<dyn ProgressReporter>,
    cfg: &DownloaderConfig,
    cancel: &CancellationToken,
) -> Result<()> {
    // How many bytes are already on disk?
    let existing = match tokio::fs::metadata(path).await {
        Ok(m) => m.len(),
        Err(_) => 0,
    };

    if let Some(total) = expected_size {
        if existing >= total && existing > 0 {
            // Already complete from a previous run.
            return Ok(());
        }
    }

    let attempt_resume = existing > 0 && server_supports_range;
    let mut request = client
        .get(url.as_str())
        .timeout(cfg.http.chunk_request_timeout);
    if attempt_resume {
        request = request.header(reqwest::header::RANGE, format!("bytes={existing}-"));
        // If the resource changed, `If-Range` makes the server reply 200 (full
        // body) instead of 206. The `(true, OK)` arm below then truncates and
        // re-downloads from zero — so a changed resource self-heals here rather
        // than appending new bytes onto stale ones.
        if let Some(v) = if_range {
            request = request.header(reqwest::header::IF_RANGE, v);
        }
    }

    let resp = tokio::select! {
        r = request.send() => r?,
        _ = cancel.cancelled() => return Err(Error::Cancelled),
    };
    let status = resp.status();

    // Decide write position and total based on response.
    let (mut pos, total) = match (attempt_resume, status) {
        (true, StatusCode::PARTIAL_CONTENT) => {
            // Successful resume. Trust the server's total when it gives one.
            let server_total = resp
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.rsplit('/').next().and_then(|n| n.parse::<u64>().ok()))
                .or(expected_size);
            (existing, server_total)
        }
        (true, StatusCode::OK) => {
            // Server ignored Range. Start over from byte 0; truncate file.
            tokio::fs::File::create(path).await?;
            let server_total = resp
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .or(expected_size);
            (0u64, server_total)
        }
        (false, StatusCode::OK) => {
            // Fresh download, no resume.
            tokio::fs::File::create(path).await?;
            let server_total = resp
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .or(expected_size);
            (0u64, server_total)
        }
        (false, StatusCode::PARTIAL_CONTENT) => {
            // Unusual: server volunteered a 206 to a non-Range request.
            // Still works as a full download.
            tokio::fs::File::create(path).await?;
            (0u64, expected_size)
        }
        (_, s) if !s.is_success() => return Err(Error::Status(s)),
        _ => unreachable!(),
    };

    // `truncate(false)`: when resuming we want to keep the bytes already on
    // disk. When `(pos == 0)` was forced above we already truncated via
    // `tokio::fs::File::create`, so opening without truncating is safe.
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)
        .await?;
    file.seek(SeekFrom::Start(pos)).await?;

    // Per-attempt progress tracking. Each retry restarts its own progress
    // task, sharing the AtomicU64.
    let downloaded = Arc::new(AtomicU64::new(pos));
    let stop_bg = CancellationToken::new();
    let prog_handle = {
        let reporter = reporter.clone();
        let downloaded = downloaded.clone();
        let stop = stop_bg.clone();
        let interval = cfg.progress_interval;
        let window = cfg.speed_window;
        tokio::spawn(progress_task(
            id, total, downloaded, reporter, interval, window, stop,
        ))
    };

    let mut bytes_since_flush: u64 = 0;
    let mut stream = resp.bytes_stream();
    let stream_result = async {
        while let Some(chunk_result) = stream.next().await {
            if cancel.is_cancelled() {
                let _ = file.flush().await;
                return Err(Error::Cancelled);
            }
            let chunk = chunk_result?;
            if chunk.is_empty() {
                continue;
            }
            // Bound the write by `total` so a server that lies about
            // Content-Length doesn't make us write forever.
            let write_len = if let Some(t) = total {
                if pos >= t {
                    break;
                }
                ((t - pos) as usize).min(chunk.len())
            } else {
                chunk.len()
            };
            file.write_all(&chunk[..write_len]).await?;
            pos += write_len as u64;
            downloaded.store(pos, Ordering::Relaxed);
            bytes_since_flush += write_len as u64;
            if bytes_since_flush >= cfg.flush_interval_bytes {
                file.flush().await?;
                if durable_resume {
                    // This file grows as we write, so a length-based resume needs
                    // the size metadata durable too — full fsync, not fdatasync.
                    file.sync_all().await?;
                }
                bytes_since_flush = 0;
            }
        }
        Ok::<(), Error>(())
    }
    .await;

    let _ = file.flush().await;
    let _ = file.sync_all().await;
    stop_bg.cancel();
    let _ = prog_handle.await;
    stream_result?;

    // For a known total, verify we got exactly that many bytes on disk.
    if let Some(t) = total {
        let on_disk = tokio::fs::metadata(path).await?.len();
        if on_disk != t {
            return Err(Error::SizeMismatch {
                expected: t,
                actual: on_disk,
            });
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Exponential backoff with cap. `attempt` is 1-based.
fn backoff(attempt: u32, initial: Duration, cap: Duration) -> Duration {
    // 2^(attempt-1) * initial, saturating at cap.
    let factor = 1u64
        .checked_shl(attempt.saturating_sub(1).min(30))
        .unwrap_or(1);
    let nanos = initial.as_nanos().saturating_mul(factor as u128);
    let capped = nanos.min(cap.as_nanos());
    Duration::from_nanos(capped.min(u128::from(u64::MAX)) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_growth() {
        let init = Duration::from_millis(500);
        let cap = Duration::from_secs(30);
        assert_eq!(backoff(1, init, cap), Duration::from_millis(500));
        assert_eq!(backoff(2, init, cap), Duration::from_millis(1000));
        assert_eq!(backoff(3, init, cap), Duration::from_millis(2000));
        // Eventually caps.
        assert_eq!(backoff(50, init, cap), cap);
    }
}
