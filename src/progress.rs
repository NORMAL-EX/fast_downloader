//! Progress reporting types and helpers.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A single event in the lifecycle of a download task.
#[derive(Debug, Clone)]
pub enum DownloadEvent {
    /// The task is queued and waiting for an execution slot.
    Queued { id: u64, url: String },
    /// The downloader is probing the server for file metadata.
    Probing { id: u64 },
    /// The download has started.
    Started {
        id: u64,
        total_size: Option<u64>,
        supports_range: bool,
        thread_count: u16,
        path: PathBuf,
    },
    /// Periodic progress update.
    Progress {
        id: u64,
        downloaded: u64,
        total: Option<u64>,
        /// Smoothed speed over the configured window, in bytes per second.
        speed_bps: u64,
        /// Estimated seconds remaining, if total is known and speed > 0.
        eta_secs: Option<u64>,
    },
    /// The download completed successfully.
    Completed {
        id: u64,
        path: PathBuf,
        size: u64,
        elapsed: Duration,
    },
    /// The download failed.
    Failed { id: u64, error: String },
    /// The download was cancelled.
    Cancelled { id: u64 },
}

impl DownloadEvent {
    pub fn id(&self) -> u64 {
        match self {
            DownloadEvent::Queued { id, .. }
            | DownloadEvent::Probing { id }
            | DownloadEvent::Started { id, .. }
            | DownloadEvent::Progress { id, .. }
            | DownloadEvent::Completed { id, .. }
            | DownloadEvent::Failed { id, .. }
            | DownloadEvent::Cancelled { id } => *id,
        }
    }
}

/// Trait implemented by anything that wants to receive download events.
pub trait ProgressReporter: Send + Sync + 'static {
    fn on_event(&self, event: DownloadEvent);
}

/// A reporter that throws events away.
pub struct NoopReporter;

impl ProgressReporter for NoopReporter {
    fn on_event(&self, _: DownloadEvent) {}
}

impl<F> ProgressReporter for F
where
    F: Fn(DownloadEvent) + Send + Sync + 'static,
{
    fn on_event(&self, event: DownloadEvent) {
        (self)(event)
    }
}

impl<R: ProgressReporter + ?Sized> ProgressReporter for Arc<R> {
    fn on_event(&self, event: DownloadEvent) {
        (**self).on_event(event)
    }
}

/// Sliding-window speed meter. Tracks (timestamp, cumulative_bytes) samples
/// and computes bytes-per-second over the configured window.
#[derive(Debug)]
pub(crate) struct SpeedMeter {
    window: Duration,
    samples: VecDeque<(Instant, u64)>,
    /// Cap on number of samples; protects against unbounded growth in case of
    /// extremely high update frequency.
    capacity: usize,
}

impl SpeedMeter {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            samples: VecDeque::with_capacity(64),
            capacity: 1024,
        }
    }

    pub fn record(&mut self, now: Instant, cumulative_bytes: u64) {
        // Always drop samples older than the window, even when no new bytes
        // have arrived – otherwise a stalled download would keep reporting a
        // stale "speed" forever.
        while let Some(&(t, _)) = self.samples.front() {
            if now.duration_since(t) > self.window {
                self.samples.pop_front();
            } else {
                break;
            }
        }
        self.samples.push_back((now, cumulative_bytes));
        while self.samples.len() > self.capacity {
            self.samples.pop_front();
        }
    }

    pub fn speed_bps(&self, now: Instant) -> u64 {
        // Recompute with a fresh "now" so a stalled stream returns 0.
        if self.samples.len() < 2 {
            return 0;
        }
        let (t_old, b_old) = *self.samples.front().unwrap();
        let (_, b_new) = *self.samples.back().unwrap();
        // Use `now` as the reference if the most recent sample is older than the window
        // (so the speed naturally decays to zero on stall).
        let window_secs = if now.duration_since(t_old).as_secs_f64() > self.window.as_secs_f64() {
            self.window.as_secs_f64()
        } else {
            now.duration_since(t_old).as_secs_f64().max(0.001)
        };
        let delta_bytes = b_new.saturating_sub(b_old) as f64;
        (delta_bytes / window_secs).max(0.0) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn speed_meter_empty() {
        let m = SpeedMeter::new(Duration::from_secs(2));
        assert_eq!(m.speed_bps(Instant::now()), 0);
    }

    #[test]
    fn speed_meter_basic() {
        let mut m = SpeedMeter::new(Duration::from_secs(2));
        let t0 = Instant::now();
        m.record(t0, 0);
        m.record(t0 + Duration::from_secs(1), 1_000_000);
        // ~1 MB over 1s
        let s = m.speed_bps(t0 + Duration::from_secs(1));
        assert!(s > 900_000 && s < 1_100_000, "speed = {}", s);
    }

    #[test]
    fn speed_meter_window_drops_old() {
        let mut m = SpeedMeter::new(Duration::from_secs(2));
        let t0 = Instant::now();
        m.record(t0, 0);
        m.record(t0 + Duration::from_secs(5), 5_000_000);
        // First sample is dropped; only one sample remains, speed = 0.
        let s = m.speed_bps(t0 + Duration::from_secs(5));
        assert_eq!(s, 0);
    }
}
