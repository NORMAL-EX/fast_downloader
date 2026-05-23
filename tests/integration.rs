//! End-to-end tests against a controllable HTTP test server.

#![allow(clippy::field_reassign_with_default)]

mod common;

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use common::{make_content, sha256, ServerBehavior, TestServer};
use fast_downloader::{
    CancellationToken, DownloadEvent, DownloadQueue, DownloadTask, Downloader, DownloaderConfig,
    NoopReporter, ProgressReporter,
};

/// Capture all events emitted during a test so we can assert on them later.
#[derive(Default)]
struct CapturingReporter(Mutex<Vec<DownloadEvent>>);

impl ProgressReporter for CapturingReporter {
    fn on_event(&self, event: DownloadEvent) {
        self.0.lock().unwrap().push(event);
    }
}

impl CapturingReporter {
    fn events(&self) -> Vec<DownloadEvent> {
        self.0.lock().unwrap().clone()
    }
    fn has_progress(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .iter()
            .any(|e| matches!(e, DownloadEvent::Progress { .. }))
    }
    fn has_started(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .iter()
            .any(|e| matches!(e, DownloadEvent::Started { .. }))
    }
    fn has_completed(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .iter()
            .any(|e| matches!(e, DownloadEvent::Completed { .. }))
    }
}

fn test_config() -> DownloaderConfig {
    let mut cfg = DownloaderConfig::default();
    // Speed up retries for tests.
    cfg.initial_retry_delay = Duration::from_millis(20);
    cfg.max_retry_delay = Duration::from_millis(200);
    cfg.progress_interval = Duration::from_millis(50);
    cfg.state_save_interval = Duration::from_millis(200);
    cfg.flush_interval_bytes = 64 * 1024;
    cfg.http.probe_timeout = Duration::from_secs(5);
    cfg.http.chunk_request_timeout = Duration::from_secs(15);
    cfg
}

#[tokio::test]
async fn multi_thread_downloads_full_content() {
    let content = make_content(1024 * 1024 * 3 + 137, 1); // ~3 MiB + tail
    let want_hash = sha256(&content);
    let server = TestServer::start(
        content.clone(),
        ServerBehavior {
            support_range: true,
            ..Default::default()
        },
    )
    .await;

    let reporter = Arc::new(CapturingReporter::default());
    let dl = Downloader::with_config(test_config(), reporter.clone()).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("blob.bin");
    let task = DownloadTask::new(server.url("/blob.bin"), dest.clone()).with_threads(16);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();

    assert_eq!(outcome.size, content.len() as u64);
    let on_disk = std::fs::read(&dest).unwrap();
    assert_eq!(sha256(&on_disk), want_hash, "downloaded content differs");
    assert!(reporter.has_started());
    assert!(reporter.has_progress());
    assert!(reporter.has_completed());
    // State file should be cleaned up on success.
    assert!(!dest.with_extension("bin.dlstate").exists());
}

#[tokio::test]
async fn small_file_uses_single_thread_path() {
    let content = make_content(2048, 42);
    let want = sha256(&content);
    let server = TestServer::start(
        content.clone(),
        ServerBehavior {
            support_range: true,
            ..Default::default()
        },
    )
    .await;

    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("small.bin");
    let task = DownloadTask::new(server.url("/small.bin"), dest.clone()).with_threads(16);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();

    assert_eq!(outcome.size, 2048);
    assert_eq!(sha256(&std::fs::read(&dest).unwrap()), want);
}

#[tokio::test]
async fn falls_back_to_single_thread_without_range_support() {
    let content = make_content(256 * 1024, 7);
    let want = sha256(&content);
    let server = TestServer::start(
        content.clone(),
        ServerBehavior {
            support_range: false,
            ..Default::default()
        },
    )
    .await;

    let reporter = Arc::new(CapturingReporter::default());
    let dl = Downloader::with_config(test_config(), reporter.clone()).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("nr.bin");
    let task = DownloadTask::new(server.url("/nr.bin"), dest.clone()).with_threads(16);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();

    assert_eq!(outcome.size, content.len() as u64);
    assert_eq!(sha256(&std::fs::read(&dest).unwrap()), want);

    // Ensure the Started event reported thread_count = 1.
    let events = reporter.events();
    let started = events
        .iter()
        .find_map(|e| match e {
            DownloadEvent::Started { thread_count, .. } => Some(*thread_count),
            _ => None,
        })
        .unwrap();
    assert_eq!(started, 1);
}

#[tokio::test]
async fn resumes_from_state_file_after_partial_download() {
    // Server drops the first request mid-stream; on retry it serves normally.
    // The state file persists between attempts; the second `download` call
    // should resume rather than redownloading.
    let content = make_content(512 * 1024, 99);
    let want = sha256(&content);
    let server = TestServer::start(
        content.clone(),
        ServerBehavior {
            support_range: true,
            // First two responses drop part-way through to force resume.
            drop_first_n_responses: 4,
            chunk_size: 8 * 1024,
            ..Default::default()
        },
    )
    .await;

    let reporter = Arc::new(CapturingReporter::default());
    let dl = Downloader::with_config(test_config(), reporter.clone()).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("r.bin");
    let task = DownloadTask::new(server.url("/r.bin"), dest.clone()).with_threads(8);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();
    assert_eq!(outcome.size, content.len() as u64);
    assert_eq!(sha256(&std::fs::read(&dest).unwrap()), want);
    assert!(server.stats.dropped_responses.load(Ordering::Relaxed) > 0);
}

#[tokio::test]
async fn manual_resume_picks_up_state_file() {
    // We simulate: download is interrupted (cancelled) partway through, then
    // a fresh `download()` call resumes from the saved state. Final output
    // must match the original content.
    let content = make_content(2 * 1024 * 1024, 17); // 2 MiB
    let want = sha256(&content);
    let server = TestServer::start(
        content.clone(),
        ServerBehavior {
            support_range: true,
            chunk_size: 16 * 1024,
            chunk_delay_ms: 1, // slow the stream so we have time to cancel
            ..Default::default()
        },
    )
    .await;

    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("res.bin");

    // First attempt: cancel after a short delay.
    let cancel = CancellationToken::new();
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel.cancel();
        });
    }
    let task = DownloadTask::new(server.url("/res.bin"), dest.clone()).with_threads(4);
    let err = dl.download(task, cancel).await.unwrap_err();
    assert!(err.is_cancelled(), "expected Cancelled, got {err}");

    // The destination file should exist (set_len) and the state file should
    // be present so we can resume.
    let mut state_path = dest.clone().into_os_string();
    state_path.push(".dlstate");
    let state_path = PathBuf::from(state_path);
    assert!(
        state_path.exists(),
        "state file should be present after cancel"
    );

    // Second attempt: full download.
    let task = DownloadTask::new(server.url("/res.bin"), dest.clone()).with_threads(4);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();
    assert_eq!(outcome.size, content.len() as u64);
    assert_eq!(sha256(&std::fs::read(&dest).unwrap()), want);
    // State file should be cleaned up.
    assert!(!state_path.exists());
}

#[tokio::test]
async fn cancel_in_flight_returns_cancelled_error() {
    let content = make_content(4 * 1024 * 1024, 31);
    let server = TestServer::start(
        content,
        ServerBehavior {
            support_range: true,
            chunk_delay_ms: 5,
            chunk_size: 8 * 1024,
            ..Default::default()
        },
    )
    .await;
    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("c.bin");
    let cancel = CancellationToken::new();
    {
        let c = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            c.cancel();
        });
    }
    let task = DownloadTask::new(server.url("/c.bin"), dest).with_threads(8);
    let err = dl.download(task, cancel).await.unwrap_err();
    assert!(err.is_cancelled());
}

#[tokio::test]
async fn queue_runs_multiple_downloads_concurrently() {
    let server = TestServer::start(
        make_content(128 * 1024, 5),
        ServerBehavior {
            support_range: true,
            ..Default::default()
        },
    )
    .await;
    let want = sha256(&server.content);

    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let queue = DownloadQueue::new(dl, 3, 32);

    let tmp = tempfile::tempdir().unwrap();
    let mut handles = Vec::new();
    for i in 0..6 {
        let dest = tmp.path().join(format!("q{i}.bin"));
        let task = DownloadTask::new(server.url(&format!("/q{i}.bin")), dest).with_threads(8);
        let h = queue.submit(task).await.unwrap();
        handles.push(h);
    }
    for h in handles {
        let out = h.result.await.unwrap().unwrap();
        let bytes = std::fs::read(&out.path).unwrap();
        assert_eq!(sha256(&bytes), want);
    }
    queue.shutdown().await;
}

#[tokio::test]
async fn queue_cancels_one_task_others_finish() {
    let server = TestServer::start(
        make_content(512 * 1024, 11),
        ServerBehavior {
            support_range: true,
            chunk_delay_ms: 3,
            chunk_size: 16 * 1024,
            ..Default::default()
        },
    )
    .await;

    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let queue = DownloadQueue::new(dl, 4, 32);
    let tmp = tempfile::tempdir().unwrap();
    let mut handles = Vec::new();
    for i in 0..4 {
        let dest = tmp.path().join(format!("file-{i}.bin"));
        let task = DownloadTask::new(server.url(&format!("/file-{i}.bin")), dest).with_threads(4);
        let h = queue.submit(task).await.unwrap();
        handles.push(h);
    }
    // Cancel #1.
    let target_id = handles[1].id;
    tokio::time::sleep(Duration::from_millis(15)).await;
    queue.cancel(target_id).await.unwrap();

    let mut successes = 0;
    let mut cancelled = 0;
    for h in handles {
        match h.result.await.unwrap() {
            Ok(_) => successes += 1,
            Err(e) if e.is_cancelled() => cancelled += 1,
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
    assert_eq!(cancelled, 1);
    assert_eq!(successes, 3);
    queue.shutdown().await;
}

#[tokio::test]
async fn server_breaks_range_mid_download_returns_error() {
    // The server claims Range support but then answers Range with 200.
    // Multi-thread downloader should fail (NoRangeSupport surfaces from a worker).
    let content = make_content(256 * 1024, 13);
    let server = TestServer::start(
        content,
        ServerBehavior {
            support_range: true,
            answer_range_with_200: true,
            ..Default::default()
        },
    )
    .await;
    let mut cfg = test_config();
    // Lower max_retries so we don't churn forever.
    cfg.max_retries = 2;
    let dl = Downloader::with_config(cfg, Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("bad.bin");
    let task = DownloadTask::new(server.url("/bad.bin"), dest).with_threads(8);
    let err = dl
        .download(task, CancellationToken::new())
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("range") || msg.contains("Range"),
        "expected range-related error, got: {msg}"
    );
}

#[tokio::test]
async fn etag_unaware_resume_invalidates_when_size_changes() {
    // First download some bytes (interrupted). Then the server's content size
    // changes. A second download should *not* corrupt the file by reusing
    // stale state.
    let content_v1 = make_content(512 * 1024, 21);
    let server = TestServer::start(
        content_v1.clone(),
        ServerBehavior {
            support_range: true,
            chunk_size: 8 * 1024,
            chunk_delay_ms: 2,
            ..Default::default()
        },
    )
    .await;
    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("ver.bin");

    // Interrupt first download.
    let cancel = CancellationToken::new();
    {
        let c = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            c.cancel();
        });
    }
    let task = DownloadTask::new(server.url("/ver.bin"), dest.clone()).with_threads(4);
    let _ = dl.download(task, cancel).await;

    // Now change the server's content to a different size.
    let content_v2 = make_content(384 * 1024, 22);
    let want_v2 = sha256(&content_v2);
    // Replace by stopping and starting a new server on the same port? Easier:
    // mutate the Arc<Vec<u8>>. We can't (Arc is immutable). Use a second
    // server and a fresh destination to simulate. But the dest is the same
    // path; the test is about state-vs-fresh-probe size mismatch.
    drop(server);
    let server2 = TestServer::start(
        content_v2.clone(),
        ServerBehavior {
            support_range: true,
            ..Default::default()
        },
    )
    .await;
    // Same dest path, different URL with same content_v2.
    let task = DownloadTask::new(server2.url("/ver.bin"), dest.clone()).with_threads(4);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();
    assert_eq!(outcome.size, content_v2.len() as u64);
    assert_eq!(sha256(&std::fs::read(&dest).unwrap()), want_v2);
}

#[tokio::test]
async fn empty_file_downloads_cleanly() {
    let server = TestServer::start(
        Vec::new(),
        ServerBehavior {
            support_range: true,
            ..Default::default()
        },
    )
    .await;
    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("empty.bin");
    let task = DownloadTask::new(server.url("/empty.bin"), dest.clone()).with_threads(16);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();
    assert_eq!(outcome.size, 0);
    assert!(dest.exists());
    assert_eq!(std::fs::read(&dest).unwrap(), Vec::<u8>::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_file_with_many_threads_on_multi_thread_runtime() {
    // Larger payload, full 16 threads, multi-thread tokio runtime.
    // Verifies there's no contention bug between worker tasks scheduled on
    // different runtime threads.
    let content = make_content(20 * 1024 * 1024, 1234); // 20 MiB
    let want = sha256(&content);
    let server = TestServer::start(
        content,
        ServerBehavior {
            support_range: true,
            chunk_size: 32 * 1024,
            ..Default::default()
        },
    )
    .await;
    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("big.bin");
    let task = DownloadTask::new(server.url("/big.bin"), dest.clone()).with_threads(16);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();
    assert_eq!(outcome.size, server.content.len() as u64);
    assert_eq!(sha256(&std::fs::read(&dest).unwrap()), want);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_queue_downloads_under_multi_thread() {
    let server = TestServer::start(
        make_content(512 * 1024, 88),
        ServerBehavior {
            support_range: true,
            ..Default::default()
        },
    )
    .await;
    let want = sha256(&server.content);
    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let queue = DownloadQueue::new(dl, 8, 256);
    let tmp = tempfile::tempdir().unwrap();
    let n = 30usize;
    let mut handles = Vec::new();
    for i in 0..n {
        let dest = tmp.path().join(format!("c{i}.bin"));
        let task = DownloadTask::new(server.url(&format!("/c{i}.bin")), dest).with_threads(8);
        let h = queue.submit(task).await.unwrap();
        handles.push(h);
    }
    let mut ok = 0;
    for h in handles {
        let out = h.result.await.unwrap().unwrap();
        let bytes = std::fs::read(&out.path).unwrap();
        assert_eq!(sha256(&bytes), want);
        ok += 1;
    }
    assert_eq!(ok, n);
    queue.shutdown().await;
}

#[tokio::test]
async fn drops_during_stream_eventually_recover() {
    let content = make_content(256 * 1024, 51);
    let want = sha256(&content);
    let server = TestServer::start(
        content,
        ServerBehavior {
            support_range: true,
            drop_first_n_responses: 6,
            chunk_size: 4 * 1024,
            ..Default::default()
        },
    )
    .await;
    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("flaky.bin");
    let task = DownloadTask::new(server.url("/flaky.bin"), dest.clone()).with_threads(4);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();
    assert_eq!(outcome.size, server.content.len() as u64);
    assert_eq!(sha256(&std::fs::read(&dest).unwrap()), want);
}
