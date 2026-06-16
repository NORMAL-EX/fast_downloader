//! `fast_downloader` — a multi-threaded HTTP downloader with resume support
//! and a concurrent task queue.
//!
//! Quick start:
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::path::PathBuf;
//! use fast_downloader::{Downloader, DownloadTask, NoopReporter};
//! use tokio_util::sync::CancellationToken;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let dl = Downloader::new(Arc::new(NoopReporter))?;
//! let task = DownloadTask::new(
//!     "https://example.com/big.iso",
//!     PathBuf::from("/tmp/big.iso"),
//! );
//! let outcome = dl.download(task, CancellationToken::new()).await?;
//! println!("downloaded {} bytes to {}", outcome.size, outcome.path.display());
//! # Ok(()) }
//! ```

#![forbid(unsafe_code)]

mod checksum;
mod downloader;
mod error;
mod filename;
mod http;
mod progress;
mod queue;
mod state;

pub use checksum::Checksum;
pub use downloader::{DownloadOutcome, DownloadTask, Downloader, DownloaderConfig};
pub use error::{Error, Result};
pub use filename::{derive_filename, parse_content_disposition, sanitize as sanitize_filename};
pub use http::{FileInfo, HttpConfig};
pub use progress::{DownloadEvent, NoopReporter, ProgressReporter};
pub use queue::{DownloadQueue, TaskHandle};

// Re-export for callers that need to construct cancellation tokens.
pub use tokio_util::sync::CancellationToken;
