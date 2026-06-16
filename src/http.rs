//! HTTP client construction and metadata probing.

use std::time::Duration;

use reqwest::{Client, StatusCode, Url};

use crate::error::{Error, Result};
use crate::filename;

pub const DEFAULT_USER_AGENT: &str = concat!(
    "fast_downloader/",
    env!("CARGO_PKG_VERSION"),
    " (+https://github.com/normal-ex/download)"
);

/// HTTP-level configuration applied to every request issued by the downloader.
#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub user_agent: String,
    pub connect_timeout: Duration,
    /// Per-request timeout for short metadata probes (HEAD / Range probe).
    pub probe_timeout: Duration,
    /// Read timeout while streaming a chunk. Applied per chunk request, not
    /// globally to the whole download.
    pub chunk_request_timeout: Duration,
    pub pool_max_idle_per_host: usize,
    pub accept_invalid_certs: bool,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            user_agent: DEFAULT_USER_AGENT.to_string(),
            connect_timeout: Duration::from_secs(15),
            probe_timeout: Duration::from_secs(15),
            chunk_request_timeout: Duration::from_secs(120),
            pool_max_idle_per_host: 32,
            accept_invalid_certs: false,
        }
    }
}

pub fn build_client(cfg: &HttpConfig) -> Result<Client> {
    let mut builder = Client::builder()
        .user_agent(&cfg.user_agent)
        .connect_timeout(cfg.connect_timeout)
        .pool_max_idle_per_host(cfg.pool_max_idle_per_host)
        // reqwest already defaults this on; set it explicitly so a future
        // default change can't silently let Nagle delay our small range
        // requests behind unacked bulk data.
        .tcp_nodelay(true)
        // We do not set a global `timeout()` because that would also kill
        // long-running streamed chunk transfers. Per-request timeouts are
        // attached individually.
        .gzip(true)
        .deflate(true);
    if cfg.accept_invalid_certs {
        // Opt-in only. Off by default.
        builder = builder.danger_accept_invalid_certs(true);
    }
    Ok(builder.build()?)
}

/// What we learned about a remote file by probing.
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub final_url: Url,
    pub filename: String,
    /// `None` means the server did not disclose a content length (HTTP/1.1
    /// chunked transfer encoding, or omitted Content-Length).
    pub size: Option<u64>,
    pub supports_range: bool,
    pub etag: Option<String>,
}

/// Probe a URL using HEAD and, when necessary, a tiny Range GET, to discover
/// total size and range support.
///
/// Strategy:
/// 1. Issue HEAD.
///    - If it returns a success status with a Content-Length:
///      - If `Accept-Ranges: bytes` is present, range support is confirmed.
///      - Otherwise we fall through to a Range probe to confirm.
/// 2. Issue `GET Range: bytes=0-0`.
///    - 206 → range support confirmed; size from Content-Range.
///    - 200 → server ignored Range; size from Content-Length (if any).
///    - Any other status → bubble up as an error.
///
/// The Range probe response body is dropped without being read, but we
/// explicitly drop the `Response` *before* returning so reqwest can close the
/// connection (otherwise the server might keep streaming an entire file we
/// don't want).
pub async fn probe(client: &Client, url: &Url, cfg: &HttpConfig) -> Result<FileInfo> {
    // ---- HEAD ----
    let head = client
        .head(url.as_str())
        .timeout(cfg.probe_timeout)
        .send()
        .await;

    #[allow(unused_assignments)]
    let mut final_url: Option<Url> = None;
    let mut filename: Option<String> = None;
    let mut size: Option<u64> = None;
    let mut supports_range: Option<bool> = None;
    let mut etag: Option<String> = None;

    if let Ok(resp) = head {
        if resp.status().is_success() {
            let u = resp.url().clone();
            filename = Some(filename::derive_filename(&resp, &u));
            final_url = Some(u);
            size = resp
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            etag = resp
                .headers()
                .get(reqwest::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            let ar = resp
                .headers()
                .get(reqwest::header::ACCEPT_RANGES)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_ascii_lowercase());
            supports_range = match ar.as_deref() {
                Some("bytes") => Some(true),
                Some("none") => Some(false),
                _ => None,
            };
            // HEAD told us everything we need.
            if supports_range == Some(true) && size.is_some() {
                return Ok(FileInfo {
                    final_url: final_url.unwrap_or_else(|| url.clone()),
                    filename: filename.unwrap_or_else(|| "download.bin".into()),
                    size,
                    supports_range: true,
                    etag,
                });
            }
        }
    }

    // ---- Range probe ----
    let resp = client
        .get(url.as_str())
        .header(reqwest::header::RANGE, "bytes=0-0")
        .timeout(cfg.probe_timeout)
        .send()
        .await?;
    let status = resp.status();
    if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
        return Err(Error::Status(status));
    }
    let probe_url = resp.url().clone();
    if filename.is_none() {
        filename = Some(filename::derive_filename(&resp, &probe_url));
    }
    final_url = Some(probe_url);
    if etag.is_none() {
        etag = resp
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
    }

    let confirmed_range = status == StatusCode::PARTIAL_CONTENT;
    if confirmed_range {
        // Content-Range: bytes 0-0/12345
        let total = resp
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_content_range_total);
        if let Some(t) = total {
            size = Some(t);
        }
    } else {
        // Server ignored Range. Use Content-Length if any.
        if size.is_none() {
            size = resp
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
        }
    }

    // Drop the response *now*, before returning. This causes reqwest to close
    // the connection rather than continue receiving the rest of the body.
    drop(resp);

    Ok(FileInfo {
        final_url: final_url.unwrap_or_else(|| url.clone()),
        filename: filename.unwrap_or_else(|| "download.bin".into()),
        size,
        supports_range: confirmed_range || supports_range == Some(true),
        etag,
    })
}

fn parse_content_range_total(value: &str) -> Option<u64> {
    // bytes 0-0/12345    or   bytes */12345
    let after_slash = value.rsplit('/').next()?.trim();
    if after_slash == "*" {
        return None;
    }
    after_slash.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_content_range_ok() {
        assert_eq!(parse_content_range_total("bytes 0-0/12345"), Some(12345));
        assert_eq!(parse_content_range_total("bytes 100-200/9999"), Some(9999));
    }

    #[test]
    fn parse_content_range_unknown() {
        assert_eq!(parse_content_range_total("bytes 0-0/*"), None);
        assert_eq!(parse_content_range_total("garbage"), None);
    }
}
