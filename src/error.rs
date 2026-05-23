use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid URL: {0}")]
    InvalidUrl(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("server returned unexpected status {0}")]
    Status(reqwest::StatusCode),

    #[error("server does not support range requests")]
    NoRangeSupport,

    #[error("byte total mismatch: expected {expected}, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },

    #[error("download cancelled")]
    Cancelled,

    #[error("download failed after {attempts} attempts: {source}")]
    RetryExhausted { attempts: u32, source: Box<Error> },

    #[error("invalid save path: {0:?}")]
    InvalidSavePath(PathBuf),

    #[error("state file corrupted: {0}")]
    StateCorrupted(String),

    #[error("queue closed")]
    QueueClosed,

    #[error("task not found: {0}")]
    TaskNotFound(u64),

    #[error("unknown content length and server does not support range")]
    UnknownLength,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Error::Cancelled)
    }
}
