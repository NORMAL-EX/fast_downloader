//! On-disk download state for resumable transfers.
//!
//! State files are written atomically (write-to-temp then rename) so a crash
//! in the middle of a save never leaves a partially-written file on disk.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::error::{Error, Result};

pub const STATE_FILE_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRange {
    pub start: u64,
    pub current: u64,
    pub end: u64,
}

impl WorkerRange {
    #[allow(dead_code)]
    pub fn remaining(&self) -> u64 {
        self.end.saturating_sub(self.current)
    }
    #[allow(dead_code)]
    pub fn is_done(&self) -> bool {
        self.current >= self.end
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadState {
    pub version: u32,
    pub url: String,
    pub file_size: u64,
    pub workers: Vec<WorkerRange>,
}

impl DownloadState {
    pub fn new(url: String, file_size: u64, workers: Vec<WorkerRange>) -> Self {
        Self {
            version: STATE_FILE_VERSION,
            url,
            file_size,
            workers,
        }
    }

    /// Validate internal consistency. Returns Err if the state is unusable.
    pub fn validate(&self) -> Result<()> {
        if self.version != STATE_FILE_VERSION {
            return Err(Error::StateCorrupted(format!(
                "version mismatch: {} vs {}",
                self.version, STATE_FILE_VERSION
            )));
        }
        if self.workers.is_empty() {
            return Err(Error::StateCorrupted("no workers".into()));
        }
        // Workers must be sorted, contiguous, cover [0, file_size) exactly,
        // and have current within [start, end].
        let mut expected_start = 0u64;
        for (i, w) in self.workers.iter().enumerate() {
            if w.start != expected_start {
                return Err(Error::StateCorrupted(format!(
                    "worker {} start={} expected {}",
                    i, w.start, expected_start
                )));
            }
            if w.end < w.start {
                return Err(Error::StateCorrupted(format!(
                    "worker {} end < start ({} < {})",
                    i, w.end, w.start
                )));
            }
            if w.current < w.start || w.current > w.end {
                return Err(Error::StateCorrupted(format!(
                    "worker {} current={} out of [{}, {}]",
                    i, w.current, w.start, w.end
                )));
            }
            expected_start = w.end;
        }
        if expected_start != self.file_size {
            return Err(Error::StateCorrupted(format!(
                "workers cover {} bytes but file_size={}",
                expected_start, self.file_size
            )));
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn total_downloaded(&self) -> u64 {
        self.workers.iter().map(|w| w.current - w.start).sum()
    }
}

pub fn state_path_for(file_path: &Path) -> PathBuf {
    let mut s = file_path.as_os_str().to_owned();
    s.push(".dlstate");
    PathBuf::from(s)
}

/// Atomically write the state to disk.
pub async fn save(path: &Path, state: &DownloadState) -> Result<()> {
    // Compact (not pretty) JSON: this file is rewritten on a ticker, so the
    // smaller payload means less to serialize and less to write on each save.
    let bytes =
        serde_json::to_vec(state).map_err(|e| Error::StateCorrupted(format!("serialize: {e}")))?;

    // Write to a sibling temp file with a stable name, then rename.
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp_path = PathBuf::from(tmp);

    {
        let mut f = tokio::fs::File::create(&tmp_path).await?;
        f.write_all(&bytes).await?;
        f.flush().await?;
        // Best-effort fsync; some platforms don't support it on all FS, ignore error.
        let _ = f.sync_all().await;
    }
    tokio::fs::rename(&tmp_path, path).await?;
    Ok(())
}

/// Load state from disk. Returns Err for any deserialization or validation
/// failure; the caller should treat that as "no resumable state".
pub async fn load(path: &Path) -> Result<DownloadState> {
    let bytes = tokio::fs::read(path).await?;
    let state: DownloadState = serde_json::from_slice(&bytes)
        .map_err(|e| Error::StateCorrupted(format!("deserialize: {e}")))?;
    state.validate()?;
    Ok(state)
}

pub async fn delete(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}

/// Build an even split of [0, file_size) into `thread_count` ranges.
/// Ensures every chunk is at least `min_chunk` bytes; if the file is smaller
/// than `min_chunk * thread_count`, the thread count is reduced accordingly.
pub fn split_workers(file_size: u64, thread_count: u16, min_chunk: u64) -> Vec<WorkerRange> {
    assert!(file_size > 0, "split_workers requires file_size > 0");
    let thread_count = thread_count.max(1) as u64;
    let effective = if min_chunk > 0 {
        thread_count.min(file_size.div_ceil(min_chunk)).max(1)
    } else {
        thread_count
    };
    let chunk = file_size / effective;
    let mut workers = Vec::with_capacity(effective as usize);
    let mut pos = 0u64;
    for i in 0..effective {
        let start = pos;
        let end = if i == effective - 1 {
            file_size
        } else {
            start + chunk
        };
        workers.push(WorkerRange {
            start,
            current: start,
            end,
        });
        pos = end;
    }
    workers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_basic() {
        let w = split_workers(1000, 4, 0);
        assert_eq!(w.len(), 4);
        assert_eq!(w[0].start, 0);
        assert_eq!(w[3].end, 1000);
        for pair in w.windows(2) {
            assert_eq!(pair[0].end, pair[1].start);
        }
    }

    #[test]
    fn split_with_min_chunk_reduces_threads() {
        let w = split_workers(1000, 16, 500);
        assert_eq!(w.len(), 2);
        assert_eq!(w[0].start, 0);
        assert_eq!(w[1].end, 1000);
    }

    #[test]
    fn split_single_byte_files() {
        let w = split_workers(1, 16, 0);
        assert_eq!(w.len(), 16);
        // Last worker holds the byte.
        assert_eq!(w.last().unwrap().end - w.last().unwrap().start, 1);
        for entry in w.iter().take(15) {
            assert_eq!(entry.start, entry.end);
        }
    }

    #[test]
    fn split_with_min_chunk_single_byte() {
        let w = split_workers(1, 16, 100);
        assert_eq!(w.len(), 1);
        assert_eq!(w[0].start, 0);
        assert_eq!(w[0].end, 1);
    }

    #[test]
    fn validate_good() {
        let s = DownloadState::new(
            "u".into(),
            100,
            vec![
                WorkerRange {
                    start: 0,
                    current: 0,
                    end: 50,
                },
                WorkerRange {
                    start: 50,
                    current: 70,
                    end: 100,
                },
            ],
        );
        s.validate().unwrap();
    }

    #[test]
    fn validate_gap() {
        let s = DownloadState::new(
            "u".into(),
            100,
            vec![
                WorkerRange {
                    start: 0,
                    current: 0,
                    end: 40,
                },
                WorkerRange {
                    start: 50,
                    current: 50,
                    end: 100,
                },
            ],
        );
        assert!(s.validate().is_err());
    }

    #[test]
    fn validate_short() {
        let s = DownloadState::new(
            "u".into(),
            100,
            vec![WorkerRange {
                start: 0,
                current: 0,
                end: 90,
            }],
        );
        assert!(s.validate().is_err());
    }

    #[test]
    fn validate_current_out_of_range() {
        let s = DownloadState::new(
            "u".into(),
            100,
            vec![WorkerRange {
                start: 0,
                current: 200,
                end: 100,
            }],
        );
        assert!(s.validate().is_err());
    }
}
