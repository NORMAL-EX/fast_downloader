//! Concurrent download task queue.
//!
//! The queue accepts [`DownloadTask`]s through [`DownloadQueue::submit`], runs
//! up to `max_concurrent` of them in parallel, and exposes per-task and
//! global cancellation. Each submission returns a [`TaskHandle`] whose
//! `result` future resolves when the task is finished, cancelled, or fails.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::downloader::{DownloadOutcome, DownloadTask, Downloader};
use crate::error::{Error, Result};
use crate::progress::DownloadEvent;

/// Public handle returned by [`DownloadQueue::submit`].
pub struct TaskHandle {
    pub id: u64,
    pub result: oneshot::Receiver<Result<DownloadOutcome>>,
    pub cancel: CancellationToken,
}

impl TaskHandle {
    /// Convenience: cancel this specific task.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

struct QueueState {
    cancels: Mutex<HashMap<u64, CancellationToken>>,
    next_id: AtomicU64,
}

/// The queue itself. Cheap to clone (only an Arc duplicates).
#[derive(Clone)]
pub struct DownloadQueue {
    tx: mpsc::Sender<QueueMsg>,
    state: Arc<QueueState>,
    /// Kept so the queue can be `join()`ed on shutdown.
    dispatcher: Arc<Mutex<Option<JoinHandle<()>>>>,
}

enum QueueMsg {
    Submit {
        task: DownloadTask,
        cancel: CancellationToken,
        reply: oneshot::Sender<Result<DownloadOutcome>>,
    },
    Shutdown,
}

impl DownloadQueue {
    /// Create a queue. `max_concurrent` is the maximum number of tasks running
    /// at once. `queue_capacity` is the backpressure bound on `submit()`.
    pub fn new(downloader: Downloader, max_concurrent: usize, queue_capacity: usize) -> Self {
        let max_concurrent = max_concurrent.max(1);
        let queue_capacity = queue_capacity.max(1);
        let (tx, mut rx) = mpsc::channel::<QueueMsg>(queue_capacity);
        let state = Arc::new(QueueState {
            cancels: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        });
        let dispatcher = {
            let state = state.clone();
            tokio::spawn(async move {
                let sem = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
                while let Some(msg) = rx.recv().await {
                    match msg {
                        QueueMsg::Submit {
                            task,
                            cancel,
                            reply,
                        } => {
                            let id = task.id;
                            let dl = downloader.clone();
                            let sem = sem.clone();
                            let state = state.clone();
                            // Emit a Queued event so subscribers see the
                            // arrival even before the slot is available.
                            dl.reporter().on_event(DownloadEvent::Queued {
                                id,
                                url: task.url.clone(),
                            });
                            tokio::spawn(async move {
                                // Acquire concurrency slot. If the queue is
                                // shutting down or the task is cancelled
                                // before the slot is available, bail.
                                let permit = tokio::select! {
                                    p = sem.acquire_owned() => p,
                                    _ = cancel.cancelled() => {
                                        let _ = reply.send(Err(Error::Cancelled));
                                        state.cancels.lock().await.remove(&id);
                                        return;
                                    }
                                };
                                // semaphore can only fail to acquire if it is
                                // closed; treat that as queue closed.
                                let _permit = match permit {
                                    Ok(p) => p,
                                    Err(_) => {
                                        let _ = reply.send(Err(Error::QueueClosed));
                                        state.cancels.lock().await.remove(&id);
                                        return;
                                    }
                                };
                                let outcome = dl.download(task, cancel).await;
                                let _ = reply.send(outcome);
                                state.cancels.lock().await.remove(&id);
                            });
                        }
                        QueueMsg::Shutdown => {
                            // Drop the receiver: pending tokio::spawn'd tasks
                            // continue, but no more can be submitted.
                            break;
                        }
                    }
                }
            })
        };
        Self {
            tx,
            state,
            dispatcher: Arc::new(Mutex::new(Some(dispatcher))),
        }
    }

    /// Assign a fresh task id and submit. Returns a handle whose `result`
    /// future completes when the task finishes.
    pub async fn submit(&self, mut task: DownloadTask) -> Result<TaskHandle> {
        if task.id == 0 {
            task.id = self.state.next_id.fetch_add(1, Ordering::Relaxed);
        }
        let id = task.id;
        let cancel = CancellationToken::new();
        self.state.cancels.lock().await.insert(id, cancel.clone());
        let (reply_tx, reply_rx) = oneshot::channel();
        let cancel_clone = cancel.clone();
        self.tx
            .send(QueueMsg::Submit {
                task,
                cancel: cancel_clone,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Error::QueueClosed)?;
        Ok(TaskHandle {
            id,
            result: reply_rx,
            cancel,
        })
    }

    /// Cancel a previously submitted task by id.
    pub async fn cancel(&self, id: u64) -> Result<()> {
        let cancels = self.state.cancels.lock().await;
        let token = cancels.get(&id).ok_or(Error::TaskNotFound(id))?;
        token.cancel();
        Ok(())
    }

    /// Cancel every in-flight task.
    pub async fn cancel_all(&self) {
        let cancels = self.state.cancels.lock().await;
        for (_, t) in cancels.iter() {
            t.cancel();
        }
    }

    /// Stop accepting new tasks and join the dispatcher. In-flight tasks
    /// continue to completion; call [`Self::cancel_all`] first if you want
    /// them aborted. Safe to call from any clone; subsequent calls are no-ops.
    pub async fn shutdown(&self) {
        let _ = self.tx.send(QueueMsg::Shutdown).await;
        let handle = self.dispatcher.lock().await.take();
        if let Some(h) = handle {
            let _ = h.await;
        }
    }
}
