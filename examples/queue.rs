//! Demonstrates the task-queue API: enqueue many URLs at once, watch progress,
//! cancel a specific task by id, and gather final results.
//!
//! ```text
//! cargo run --release --example queue -- <SAVE_DIR> <URL>...
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use fast_downloader::{DownloadEvent, DownloadQueue, DownloadTask, Downloader, ProgressReporter};

struct LogReporter;
impl ProgressReporter for LogReporter {
    fn on_event(&self, event: DownloadEvent) {
        match event {
            DownloadEvent::Queued { id, url } => println!("[{id}] queued: {url}"),
            DownloadEvent::Started {
                id,
                total_size,
                thread_count,
                ..
            } => println!(
                "[{id}] started ({} bytes, {} threads)",
                total_size
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "?".into()),
                thread_count
            ),
            DownloadEvent::Progress {
                id,
                downloaded,
                total,
                speed_bps,
                ..
            } => {
                let mb_s = speed_bps as f64 / (1024.0 * 1024.0);
                let pct = total
                    .map(|t| {
                        if t == 0 {
                            0.0
                        } else {
                            downloaded as f64 / t as f64 * 100.0
                        }
                    })
                    .unwrap_or(0.0);
                println!("[{id}] {pct:5.1}%  {mb_s:6.2} MB/s");
            }
            DownloadEvent::Completed { id, path, size, .. } => {
                println!("[{id}] done: {} ({} bytes)", path.display(), size);
            }
            DownloadEvent::Failed { id, error } => println!("[{id}] failed: {error}"),
            DownloadEvent::Cancelled { id } => println!("[{id}] cancelled"),
            DownloadEvent::Probing { .. } => {}
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let dir = PathBuf::from(args.next().expect("usage: queue <SAVE_DIR> <URL>..."));
    let urls: Vec<String> = args.collect();
    if urls.is_empty() {
        eprintln!("usage: queue <SAVE_DIR> <URL>...");
        std::process::exit(2);
    }
    tokio::fs::create_dir_all(&dir).await?;

    let downloader = Downloader::new(Arc::new(LogReporter))?;
    // 3 concurrent downloads, queue depth of 64.
    let queue = DownloadQueue::new(downloader, 3, 64);

    let mut handles = Vec::new();
    for (i, url) in urls.into_iter().enumerate() {
        let filename = url
            .rsplit('/')
            .find(|s| !s.is_empty())
            .map(fast_downloader::sanitize_filename)
            .unwrap_or_else(|| format!("file-{i}"));
        let save = dir.join(filename);
        let task = DownloadTask::new(url, save).with_threads(16);
        let handle = queue.submit(task).await?;
        handles.push(handle);
    }

    // Wait for all results.
    let mut ok = 0usize;
    let mut fail = 0usize;
    for h in handles {
        match h.result.await {
            Ok(Ok(out)) => {
                ok += 1;
                println!("OK [{}] {}", out.id, out.path.display());
            }
            Ok(Err(e)) => {
                fail += 1;
                println!("ERR: {e}");
            }
            Err(_) => {
                fail += 1;
                println!("ERR: result channel dropped");
            }
        }
    }
    queue.shutdown().await;
    println!("summary: {ok} ok, {fail} failed");
    Ok(())
}
