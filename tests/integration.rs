//! End-to-end tests against a controllable HTTP test server.

#![allow(clippy::field_reassign_with_default)]

mod common;

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use base64::prelude::*;
use common::{make_content, sha256, ServerBehavior, TestServer};
use fast_downloader::{
    CancellationToken, Checksum, DownloadEvent, DownloadQueue, DownloadTask, Downloader,
    DownloaderConfig, NoopReporter, ProgressReporter,
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
async fn etag_change_invalidates_resume_same_size() {
    // The corruption case ETag validation closes: a prior run left partial bytes
    // of one version on disk, then the server's content changed to a *different
    // file of the same size* with a new ETag. A size-only resume check would
    // splice the old partial bytes with new ones (corruption); with ETag
    // validation the stale state must be discarded and the new content fetched.
    //
    // The prior-run state is constructed deterministically (a half-written file
    // plus a matching `.dlstate`) so the test exercises the resume *decision*
    // directly, with no dependence on interrupt timing or how a server's
    // mid-stream drop is surfaced.
    let size = 512 * 1024usize;
    let half = size / 2;
    let content_v1 = make_content(size, 21);
    let content_v2 = make_content(size, 22);
    let want_v2 = sha256(&content_v2);

    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("etag.bin");

    // Half-written file: v1's first half on disk, the rest still zero-filled.
    let mut partial = content_v1[..half].to_vec();
    partial.resize(size, 0);
    std::fs::write(&dest, &partial).unwrap();

    // A resume state tagged with the OLD validator, recording that single
    // [0, size) worker is half-done. (Hand-written to match the on-disk format.)
    let mut state_path = dest.clone().into_os_string();
    state_path.push(".dlstate");
    let state_path = PathBuf::from(state_path);
    let state_json = format!(
        r#"{{"version":2,"url":"http://old/etag.bin","file_size":{size},"etag":"\"v1\"","last_modified":null,"workers":[{{"start":0,"current":{half},"end":{size}}}]}}"#
    );
    std::fs::write(&state_path, state_json).unwrap();

    // Server now serves different bytes of the same size, with a new ETag.
    let server = TestServer::start(
        content_v2.clone(),
        ServerBehavior {
            support_range: true,
            etag: Some("\"v2\"".into()),
            ..Default::default()
        },
    )
    .await;

    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    // threads > 1 so we take the multi-thread path that consults the state file.
    let task = DownloadTask::new(server.url("/etag.bin"), dest.clone()).with_threads(4);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();
    assert_eq!(outcome.size, size as u64);
    assert_eq!(
        sha256(&std::fs::read(&dest).unwrap()),
        want_v2,
        "a changed ETag must not be resumed into — the file would be corrupt"
    );
}

#[tokio::test]
async fn matching_etag_still_resumes() {
    // The flip side: when the ETag is unchanged, resume must still work so the
    // safety check doesn't cost us efficiency on the happy path.
    let content = make_content(512 * 1024, 99);
    let want = sha256(&content);
    let server = TestServer::start(
        content.clone(),
        ServerBehavior {
            support_range: true,
            drop_first_n_responses: 4, // force at least one resume
            chunk_size: 8 * 1024,
            etag: Some("\"stable\"".into()),
            ..Default::default()
        },
    )
    .await;

    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("stable.bin");
    let task = DownloadTask::new(server.url("/stable.bin"), dest.clone()).with_threads(8);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();
    assert_eq!(outcome.size, content.len() as u64);
    assert_eq!(sha256(&std::fs::read(&dest).unwrap()), want);
    assert!(server.stats.dropped_responses.load(Ordering::Relaxed) > 0);
}

#[tokio::test]
async fn if_range_detects_change_multithread() {
    // The server reports the resource changed since it was probed (it answers
    // any If-Range request with a full 200). A multi-thread worker must surface
    // this as ResourceChanged — failing safe — instead of splicing versions.
    let content = make_content(512 * 1024, 5);
    let server = TestServer::start(
        content,
        ServerBehavior {
            support_range: true,
            etag: Some("\"v1\"".into()),
            if_range_stale: true,
            ..Default::default()
        },
    )
    .await;

    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("changed.bin");
    let task = DownloadTask::new(server.url("/changed.bin"), dest.clone()).with_threads(4);
    let err = dl
        .download(task, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(
        err.is_resource_changed(),
        "expected ResourceChanged, got {err}"
    );

    // The now-worthless state must be cleared so a rerun starts fresh.
    let mut state_path = dest.into_os_string();
    state_path.push(".dlstate");
    assert!(
        !PathBuf::from(state_path).exists(),
        "stale state not cleared"
    );
}

#[tokio::test]
async fn if_range_single_thread_restarts_on_change() {
    // A half-written file from an old version exists; the server then reports a
    // change on the If-Range resume request. The single-thread path must
    // truncate and re-fetch rather than append new bytes onto stale ones.
    let size = 256 * 1024usize;
    let content_v1 = make_content(size, 1);
    let content_v2 = make_content(size, 2);
    let want_v2 = sha256(&content_v2);

    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("s.bin");
    std::fs::write(&dest, &content_v1[..size / 2]).unwrap(); // stale partial

    let server = TestServer::start(
        content_v2.clone(),
        ServerBehavior {
            support_range: true,
            etag: Some("\"v2\"".into()),
            if_range_stale: true,
            ..Default::default()
        },
    )
    .await;

    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    // threads = 1 forces the single-thread path.
    let task = DownloadTask::new(server.url("/s.bin"), dest.clone()).with_threads(1);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();
    assert_eq!(outcome.size, size as u64);
    assert_eq!(
        sha256(&std::fs::read(&dest).unwrap()),
        want_v2,
        "single-thread resume must heal a changed resource, not corrupt it"
    );
}

#[tokio::test]
async fn caller_checksum_accepts_correct_file() {
    let content = make_content(512 * 1024, 3);
    let want = sha256(&content);
    let server = TestServer::start(
        content,
        ServerBehavior {
            support_range: true,
            ..Default::default()
        },
    )
    .await;

    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("ck.bin");
    let task = DownloadTask::new(server.url("/ck.bin"), dest.clone())
        .with_threads(8)
        .with_checksum(Checksum::Sha256(want));
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();
    assert_eq!(outcome.size, 512 * 1024);
    assert!(dest.exists());
}

#[tokio::test]
async fn caller_checksum_rejects_and_removes_bad_file() {
    let content = make_content(512 * 1024, 3);
    let server = TestServer::start(
        content,
        ServerBehavior {
            support_range: true,
            ..Default::default()
        },
    )
    .await;

    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("ck.bin");
    // Wrong expected hash -> verification must fail and remove the file.
    let task = DownloadTask::new(server.url("/ck.bin"), dest.clone())
        .with_threads(8)
        .with_checksum(Checksum::Sha256([0u8; 32]));
    let err = dl
        .download(task, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(err.is_checksum_mismatch(), "got {err}");
    assert!(
        !dest.exists(),
        "a file that fails verification must be removed"
    );
}

#[tokio::test]
async fn server_repr_digest_is_verified() {
    let content = make_content(300 * 1024, 8);
    let good = format!("sha-256=:{}:", BASE64_STANDARD.encode(sha256(&content)));

    // Correct server digest, no caller checksum -> auto-verified, succeeds.
    let server = TestServer::start(
        content.clone(),
        ServerBehavior {
            support_range: true,
            repr_digest: Some(good),
            ..Default::default()
        },
    )
    .await;
    let dl = Downloader::with_config(test_config(), Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("d.bin");
    let task = DownloadTask::new(server.url("/d.bin"), dest.clone()).with_threads(8);
    dl.download(task, CancellationToken::new()).await.unwrap();
    assert!(dest.exists());

    // Wrong server digest -> auto-verification fails and removes the file.
    let bad = format!("sha-256=:{}:", BASE64_STANDARD.encode([0u8; 32]));
    let server2 = TestServer::start(
        content,
        ServerBehavior {
            support_range: true,
            repr_digest: Some(bad),
            ..Default::default()
        },
    )
    .await;
    let dest2 = tmp.path().join("d2.bin");
    let task = DownloadTask::new(server2.url("/d2.bin"), dest2.clone()).with_threads(8);
    let err = dl
        .download(task, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(err.is_checksum_mismatch(), "got {err}");
    assert!(!dest2.exists());
}

#[tokio::test]
async fn server_digest_verification_can_be_disabled() {
    let content = make_content(128 * 1024, 9);
    let bad = format!("sha-256=:{}:", BASE64_STANDARD.encode([0u8; 32]));
    let server = TestServer::start(
        content,
        ServerBehavior {
            support_range: true,
            repr_digest: Some(bad), // would fail if honored
            ..Default::default()
        },
    )
    .await;

    let mut cfg = test_config();
    cfg.verify_server_digest = false;
    let dl = Downloader::with_config(cfg, Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("nd.bin");
    let task = DownloadTask::new(server.url("/nd.bin"), dest.clone()).with_threads(8);
    // With server-digest verification off, the bogus digest is ignored.
    dl.download(task, CancellationToken::new()).await.unwrap();
    assert!(dest.exists());
}

#[tokio::test]
async fn durable_resume_multithread_recovers() {
    // `durable_resume` fdatasyncs the data file before each checkpoint. We can't
    // crash-test power loss here, but this drives that path under a real resume
    // and confirms it still yields the correct file (i.e. the extra syncs and
    // the second data-file handle don't break the download).
    let content = make_content(512 * 1024, 99);
    let want = sha256(&content);
    let server = TestServer::start(
        content,
        ServerBehavior {
            support_range: true,
            drop_first_n_responses: 4,
            chunk_size: 8 * 1024,
            ..Default::default()
        },
    )
    .await;

    let mut cfg = test_config();
    cfg.durable_resume = true;
    let dl = Downloader::with_config(cfg, Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("dur.bin");
    let task = DownloadTask::new(server.url("/dur.bin"), dest.clone()).with_threads(8);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();
    assert_eq!(outcome.size, 512 * 1024);
    assert_eq!(sha256(&std::fs::read(&dest).unwrap()), want);
}

#[tokio::test]
async fn durable_resume_single_thread_ok() {
    let content = make_content(96 * 1024, 7);
    let want = sha256(&content);
    let server = TestServer::start(
        content,
        ServerBehavior {
            support_range: true,
            ..Default::default()
        },
    )
    .await;

    let mut cfg = test_config();
    cfg.durable_resume = true;
    cfg.flush_interval_bytes = 16 * 1024; // force several fsyncs across the file
    let dl = Downloader::with_config(cfg, Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("durs.bin");
    let task = DownloadTask::new(server.url("/durs.bin"), dest.clone()).with_threads(1);
    let outcome = dl.download(task, CancellationToken::new()).await.unwrap();
    assert_eq!(outcome.size, 96 * 1024);
    assert_eq!(sha256(&std::fs::read(&dest).unwrap()), want);
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

/// Throughput benchmark. The server runs on its **own** runtime in a dedicated
/// OS thread so it never steals the client's runtime threads — the timing then
/// reflects the client's download path, not in-process server contention. Run:
///   cargo test --release --test integration -- --ignored --nocapture bench
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark; run with --ignored --nocapture"]
async fn bench_multi_thread_throughput() {
    let size = 128 * 1024 * 1024; // 128 MiB
    let content = make_content(size, 7);

    // Spin the test server up on an isolated 2-thread runtime in its own OS
    // thread. We only need its address back; the thread parks forever.
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let server = TestServer::start(
                content,
                ServerBehavior {
                    support_range: true,
                    chunk_size: 16 * 1024, // many small chunks: stress the path
                    ..Default::default()
                },
            )
            .await;
            addr_tx.send(server.addr).unwrap();
            std::future::pending::<()>().await; // keep server alive
        });
    });
    let addr = addr_rx.recv().unwrap();

    let dl = Downloader::with_config(DownloaderConfig::default(), Arc::new(NoopReporter)).unwrap();
    let tmp = tempfile::tempdir().unwrap();

    let runs = 5;
    let mut best = f64::MAX;
    for i in 0..runs {
        let dest = tmp.path().join(format!("bench{i}.bin"));
        let url = format!("http://{addr}/bench{i}.bin");
        let task = DownloadTask::new(url, dest).with_threads(16);
        let t = std::time::Instant::now();
        let out = dl.download(task, CancellationToken::new()).await.unwrap();
        let secs = t.elapsed().as_secs_f64();
        let mibps = (out.size as f64 / (1024.0 * 1024.0)) / secs;
        best = best.min(secs);
        eprintln!("run {i}: {secs:.3}s  {mibps:.1} MiB/s");
    }
    eprintln!(
        "best: {best:.3}s  ({:.1} MiB/s) over {} MiB",
        (size as f64 / (1024.0 * 1024.0)) / best,
        size / (1024 * 1024)
    );
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
