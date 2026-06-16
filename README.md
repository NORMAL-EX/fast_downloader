# fast_downloader

Multi-threaded HTTP downloader written in Rust. Supports resumable downloads,
a concurrent task queue with per-task cancellation, and per-task progress and
speed reporting.

## Highlights

- **16 worker threads by default** per task, configurable per-task and globally.
- **Resumable**: state is persisted to `<file>.dlstate` and atomically rewritten
  on a ticker; interrupted downloads continue where they left off.
- **Validated resume**: the resume state records the resource's `ETag` /
  `Last-Modified`, and every range request carries `If-Range`. A server that
  has changed the resource then answers `200` instead of `206`, so a same-size
  change can never splice two versions together — the multi-thread path fails
  cleanly (and clears the stale state) while the single-thread path truncates
  and re-fetches. Unchanged (or validator-less) resources resume exactly as
  before, at no extra cost.
- **Single-thread fallback** when the server does not support Range, with
  Range-based resume for that path too.
- **No shared file mutex**: each worker holds its own file descriptor and
  writes to a disjoint byte range, so multi-thread downloads do not serialize
  on a `Mutex<File>`.
- **No `danger_accept_invalid_certs`**: TLS validation is on by default.
- **Filename sanitization** prevents path-traversal from
  `Content-Disposition` headers; the public API takes an exact `save_path`.
- **Task queue** with bounded concurrency, per-task and global cancel.
- **Progress events** include downloaded bytes, total bytes, smoothed speed
  (2-second sliding window by default), and ETA.

## Quick start

```rust
use std::sync::Arc;
use std::path::PathBuf;
use fast_downloader::{
    CancellationToken, Downloader, DownloadTask, NoopReporter,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dl = Downloader::new(Arc::new(NoopReporter))?;
    let task = DownloadTask::new(
        "https://example.com/big.iso",
        PathBuf::from("/tmp/big.iso"),
    )
    .with_threads(16);

    let outcome = dl.download(task, CancellationToken::new()).await?;
    println!("got {} bytes -> {}", outcome.size, outcome.path.display());
    Ok(())
}
```

Run the examples:

```text
cargo run --release --example basic -- https://example.com/file /tmp/file 16
cargo run --release --example queue -- /tmp/dest URL1 URL2 URL3 ...
```

## Architecture (one-paragraph version)

`Downloader::download` probes the URL via HEAD (with a `Range: bytes=0-0` GET
fallback), splits `[0, file_size)` into `N` ranges, and `tokio::spawn`s one
worker per range. Each worker opens its own `tokio::fs::File` handle and
streams bytes with `Range:` requests, retrying with exponential backoff on
errors. Two background tasks share the workers' atomics: a progress ticker
emits `DownloadEvent::Progress`, and a state ticker writes the resume file
atomically. On success the state file is removed; on cancellation or failure
it is left in place so the next call resumes.

## Tests

```text
cargo test            # unit + integration tests
cargo clippy --all-targets -- -D warnings
```

The integration suite (`tests/integration.rs`) spins up an axum-based HTTP
server with configurable misbehaviour (no Range support, mid-stream drops,
Range-to-200 corruption, slow streams, etc.) and exercises the downloader
against each case.
