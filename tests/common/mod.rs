//! Configurable HTTP test server used by the integration tests.
//!
//! Each `TestServer` is a tokio task listening on an ephemeral port that
//! serves a single fixed payload at any path, honouring Range requests
//! (or deliberately not, per configuration).

#![allow(dead_code, clippy::manual_clamp)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use futures_util::stream;
use tokio::sync::oneshot;

#[derive(Clone, Default)]
pub struct ServerBehavior {
    pub support_range: bool,
    /// Return 200 (full body) to Range requests. Multi-thread downloader
    /// should detect this and bail.
    pub answer_range_with_200: bool,
    /// Drop the connection after this many bytes have been sent on each
    /// response. 0 means never drop. Counter resets per request.
    pub drop_after_bytes: u64,
    /// Number of times the server should drop the connection mid-stream
    /// before sending complete responses normally.
    pub drop_first_n_responses: u64,
    /// Override Content-Disposition header.
    pub content_disposition: Option<String>,
    /// Sleep before responding (simulates slow network).
    pub response_delay_ms: u64,
    /// Per-chunk delay used when streaming (slows the stream).
    pub chunk_delay_ms: u64,
    /// Optional fixed chunk size for streaming. 0 = let axum decide.
    pub chunk_size: usize,
    /// Optional `ETag` to advertise on HEAD and GET responses.
    pub etag: Option<String>,
    /// When true, any request carrying an `If-Range` header is answered with a
    /// full `200` (as a real server does when the validator no longer matches),
    /// simulating a resource that changed after it was probed.
    pub if_range_stale: bool,
    /// Optional `Repr-Digest` header value (e.g. `sha-256=:<base64>:`) advertised
    /// on the HEAD response for end-to-end verification tests.
    pub repr_digest: Option<String>,
}

pub struct TestServer {
    pub addr: SocketAddr,
    pub content: Arc<Vec<u8>>,
    pub behavior: Arc<std::sync::RwLock<ServerBehavior>>,
    pub stats: Arc<ServerStats>,
    shutdown: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
pub struct ServerStats {
    pub head_requests: AtomicU64,
    pub get_requests: AtomicU64,
    pub range_requests: AtomicU64,
    pub bytes_served: AtomicU64,
    pub dropped_responses: AtomicU64,
}

#[derive(Clone)]
struct AppState {
    content: Arc<Vec<u8>>,
    behavior: Arc<std::sync::RwLock<ServerBehavior>>,
    stats: Arc<ServerStats>,
}

impl TestServer {
    pub async fn start(content: Vec<u8>, behavior: ServerBehavior) -> Self {
        let content = Arc::new(content);
        let behavior = Arc::new(std::sync::RwLock::new(behavior));
        let stats = Arc::new(ServerStats::default());
        let state = AppState {
            content: content.clone(),
            behavior: behavior.clone(),
            stats: stats.clone(),
        };

        let app = Router::new()
            .route("/*path", any(handle))
            .route("/", any(handle))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = rx.await;
                })
                .await;
        });

        Self {
            addr,
            content,
            behavior,
            stats,
            shutdown: Some(tx),
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    pub fn set_behavior(&self, b: ServerBehavior) {
        *self.behavior.write().unwrap() = b;
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

async fn handle(State(state): State<AppState>, req: Request) -> Response {
    let method = req.method().clone();
    let headers = req.headers().clone();
    let behavior = state.behavior.read().unwrap().clone();
    let total = state.content.len() as u64;

    if behavior.response_delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(behavior.response_delay_ms)).await;
    }

    if method == axum::http::Method::HEAD {
        state.stats.head_requests.fetch_add(1, Ordering::Relaxed);
        let mut resp = Response::builder().status(StatusCode::OK);
        let h = resp.headers_mut().unwrap();
        h.insert(header::CONTENT_LENGTH, HeaderValue::from(total));
        if behavior.support_range {
            h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        } else {
            h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("none"));
        }
        if let Some(cd) = &behavior.content_disposition {
            h.insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_str(cd).unwrap(),
            );
        }
        if let Some(etag) = &behavior.etag {
            h.insert(header::ETAG, HeaderValue::from_str(etag).unwrap());
        }
        if let Some(d) = &behavior.repr_digest {
            h.insert("repr-digest", HeaderValue::from_str(d).unwrap());
        }
        return resp.body(Body::empty()).unwrap();
    }

    state.stats.get_requests.fetch_add(1, Ordering::Relaxed);
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let range_parsed = range.and_then(parse_range_single);
    // A real server answers a stale If-Range with the full 200 body.
    let if_range_stale = behavior.if_range_stale && headers.contains_key(header::IF_RANGE);

    // Decide if we honour Range.
    let (status, start, end) = match range_parsed {
        Some((s, e))
            if behavior.support_range && !behavior.answer_range_with_200 && !if_range_stale =>
        {
            state.stats.range_requests.fetch_add(1, Ordering::Relaxed);
            (StatusCode::PARTIAL_CONTENT, s, e)
        }
        Some(_) => {
            // Range ignored (server doesn't support it, or If-Range went stale).
            (StatusCode::OK, 0, total.saturating_sub(1))
        }
        None => (StatusCode::OK, 0, total.saturating_sub(1)),
    };

    let body_start = start.min(total);
    let body_end_inclusive = end.min(total.saturating_sub(1));
    if total == 0 {
        let resp = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, 0)
            .body(Body::empty())
            .unwrap();
        return resp;
    }

    let body_bytes = if body_end_inclusive >= body_start {
        state.content[body_start as usize..=(body_end_inclusive as usize)].to_vec()
    } else {
        Vec::new()
    };
    let body_len = body_bytes.len() as u64;

    // Should we drop this connection mid-stream?
    let should_drop = {
        let mut bh = state.behavior.write().unwrap();
        if bh.drop_first_n_responses > 0 {
            bh.drop_first_n_responses -= 1;
            state
                .stats
                .dropped_responses
                .fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    };

    let drop_after = if should_drop {
        // Send some bytes then drop. Use half the body, but at most 256 bytes.
        (body_len / 2).min(256).max(1)
    } else if behavior.drop_after_bytes > 0 {
        behavior.drop_after_bytes
    } else {
        u64::MAX
    };

    let chunk_size = if behavior.chunk_size > 0 {
        behavior.chunk_size
    } else {
        16 * 1024
    };
    let chunk_delay = Duration::from_millis(behavior.chunk_delay_ms);
    let stats = state.stats.clone();

    let stream = stream::unfold(
        (
            body_bytes,
            0usize,
            drop_after,
            0u64,
            chunk_size,
            chunk_delay,
            stats,
            should_drop,
        ),
        |(buf, mut pos, drop_after, mut emitted, chunk_size, chunk_delay, stats, should_drop)| async move {
            if pos >= buf.len() {
                return None;
            }
            if emitted >= drop_after {
                // Simulate connection drop by emitting an explicit error.
                return Some((
                    Err::<bytes::Bytes, std::io::Error>(std::io::Error::other("simulated drop")),
                    (
                        buf,
                        pos,
                        drop_after,
                        emitted,
                        chunk_size,
                        chunk_delay,
                        stats,
                        should_drop,
                    ),
                ));
            }
            if chunk_delay > Duration::ZERO {
                tokio::time::sleep(chunk_delay).await;
            }
            let take = chunk_size.min(buf.len() - pos);
            let allowed = if emitted + take as u64 > drop_after {
                (drop_after - emitted) as usize
            } else {
                take
            };
            if allowed == 0 {
                return None;
            }
            let chunk = bytes::Bytes::copy_from_slice(&buf[pos..pos + allowed]);
            pos += allowed;
            emitted += allowed as u64;
            stats
                .bytes_served
                .fetch_add(allowed as u64, Ordering::Relaxed);
            Some((
                Ok(chunk),
                (
                    buf,
                    pos,
                    drop_after,
                    emitted,
                    chunk_size,
                    chunk_delay,
                    stats,
                    should_drop,
                ),
            ))
        },
    );

    let mut builder = Response::builder().status(status);
    let mut hmap = HeaderMap::new();
    hmap.insert(header::CONTENT_LENGTH, HeaderValue::from(body_len));
    if status == StatusCode::PARTIAL_CONTENT {
        hmap.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!(
                "bytes {}-{}/{}",
                body_start, body_end_inclusive, total
            ))
            .unwrap(),
        );
    }
    if behavior.support_range {
        hmap.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    }
    if let Some(cd) = &behavior.content_disposition {
        hmap.insert(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_str(cd).unwrap(),
        );
    }
    if let Some(etag) = &behavior.etag {
        hmap.insert(header::ETAG, HeaderValue::from_str(etag).unwrap());
    }
    {
        let h = builder.headers_mut().unwrap();
        for (k, v) in hmap {
            if let Some(k) = k {
                h.insert(k, v);
            }
        }
    }
    builder
        .body(Body::from_stream(stream))
        .unwrap()
        .into_response()
}

fn parse_range_single(s: &str) -> Option<(u64, u64)> {
    let s = s.strip_prefix("bytes=")?;
    let (a, b) = s.split_once('-')?;
    let start: u64 = a.trim().parse().ok()?;
    let end: u64 = if b.trim().is_empty() {
        u64::MAX
    } else {
        b.trim().parse().ok()?
    };
    Some((start, end))
}

/// Generate deterministic but non-trivial test content.
pub fn make_content(size: usize, seed: u64) -> Vec<u8> {
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut v = vec![0u8; size];
    rng.fill(&mut v[..]);
    v
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(data);
    let r = h.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&r);
    out
}
