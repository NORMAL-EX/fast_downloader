//! Minimal one-off download with a printing progress reporter.
//!
//! ```text
//! cargo run --release --example basic -- <URL> <SAVE_PATH> [threads]
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fast_downloader::{
    CancellationToken, DownloadEvent, DownloadTask, Downloader, ProgressReporter,
};

struct PrintReporter;

impl ProgressReporter for PrintReporter {
    fn on_event(&self, event: DownloadEvent) {
        match event {
            DownloadEvent::Probing { id } => {
                eprintln!("[{id}] probing...");
            }
            DownloadEvent::Started {
                id,
                total_size,
                supports_range,
                thread_count,
                path,
            } => {
                eprintln!(
                    "[{id}] started: {} bytes, range={}, threads={}, path={}",
                    total_size
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "unknown".into()),
                    supports_range,
                    thread_count,
                    path.display()
                );
            }
            DownloadEvent::Progress {
                id,
                downloaded,
                total,
                speed_bps,
                eta_secs,
            } => {
                let percent = total
                    .map(|t| {
                        if t == 0 {
                            0.0
                        } else {
                            downloaded as f64 / t as f64 * 100.0
                        }
                    })
                    .unwrap_or(0.0);
                let mb_s = speed_bps as f64 / (1024.0 * 1024.0);
                let eta = eta_secs
                    .map(|s| format!("{}s", s))
                    .unwrap_or_else(|| "?".into());
                eprintln!(
                    "[{id}] {:.1}% ({} / {}) @ {:.2} MB/s eta {}",
                    percent,
                    downloaded,
                    total.map(|t| t.to_string()).unwrap_or_else(|| "?".into()),
                    mb_s,
                    eta
                );
            }
            DownloadEvent::Completed {
                id,
                path,
                size,
                elapsed,
            } => {
                let mb = size as f64 / (1024.0 * 1024.0);
                let secs = elapsed.as_secs_f64();
                let avg = if secs > 0.0 { mb / secs } else { 0.0 };
                eprintln!(
                    "[{id}] done: {} ({:.2} MB) in {:.2}s ({:.2} MB/s avg)",
                    path.display(),
                    mb,
                    secs,
                    avg
                );
            }
            DownloadEvent::Failed { id, error } => {
                eprintln!("[{id}] FAILED: {error}");
            }
            DownloadEvent::Cancelled { id } => {
                eprintln!("[{id}] cancelled");
            }
            DownloadEvent::Queued { .. } => {}
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let url = args
        .next()
        .expect("usage: basic <URL> <SAVE_PATH> [threads]");
    let save = PathBuf::from(
        args.next()
            .expect("usage: basic <URL> <SAVE_PATH> [threads]"),
    );
    let threads = args
        .next()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(16);

    let reporter = Arc::new(PrintReporter);
    let downloader = Downloader::new(reporter)?;
    let cancel = CancellationToken::new();

    // Ctrl-C → cancel.
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("\nctrl-c received, cancelling...");
            cancel.cancel();
        });
    }

    let task = DownloadTask::new(url, save)
        .with_threads(threads)
        .with_id(1);
    match downloader.download(task, cancel).await {
        Ok(out) => {
            // give the reporter a moment to print
            tokio::time::sleep(Duration::from_millis(50)).await;
            println!("ok: {}", out.path.display());
            Ok(())
        }
        Err(e) => {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Err(e.into())
        }
    }
}
