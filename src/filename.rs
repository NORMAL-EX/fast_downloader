//! Content-Disposition parsing and filename sanitization.
//!
//! Sanitization here is the *only* defense against malicious server-supplied
//! filenames; never use the raw value from the network as part of a filesystem path.

use percent_encoding::percent_decode_str;
use reqwest::Response;
use url::Url;

const MAX_FILENAME_LEN: usize = 200;
const FALLBACK_FILENAME: &str = "download.bin";

/// Returns a filename derived from the response or URL, sanitized for safe use
/// as the last component of a filesystem path. Never returns an empty string,
/// never returns a string containing path separators or NUL.
pub fn derive_filename(response: &Response, url: &Url) -> String {
    let raw = extract_from_content_disposition(response)
        .or_else(|| extract_from_url(url))
        .unwrap_or_default();
    sanitize(&raw)
}

pub fn extract_from_content_disposition(response: &Response) -> Option<String> {
    let value = response
        .headers()
        .get(reqwest::header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())?;
    parse_content_disposition(value)
}

pub fn extract_from_url(url: &Url) -> Option<String> {
    let last = url.path_segments()?.next_back()?;
    if last.is_empty() {
        return None;
    }
    let decoded = percent_decode_str(last).decode_utf8_lossy().into_owned();
    if decoded.is_empty() {
        None
    } else {
        Some(decoded)
    }
}

/// Parses a Content-Disposition header value. Tries the RFC 5987 extended form
/// (`filename*=charset'lang'value`) first, then the regular `filename=` form.
pub fn parse_content_disposition(value: &str) -> Option<String> {
    let params = parse_params(value);
    // Prefer filename* over filename.
    if let Some(v) = params.iter().find(|p| eq_ascii(&p.name, "filename*")) {
        if let Some(name) = decode_ext_value(&v.value) {
            return Some(name);
        }
    }
    if let Some(v) = params.iter().find(|p| eq_ascii(&p.name, "filename")) {
        if !v.value.is_empty() {
            // Older browsers sometimes percent-encode the regular value. We
            // try to decode but fall back to the raw text.
            let decoded = percent_decode_str(&v.value)
                .decode_utf8_lossy()
                .into_owned();
            return Some(decoded);
        }
    }
    None
}

#[derive(Debug)]
struct Param {
    name: String,
    value: String,
}

/// Split a Content-Disposition header into its parameter list. The first
/// disposition-type token is discarded.
fn parse_params(input: &str) -> Vec<Param> {
    let mut out = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0usize;

    // Skip leading whitespace.
    while i < bytes.len() && is_ws(bytes[i]) {
        i += 1;
    }
    // Skip disposition-type up to ';'.
    while i < bytes.len() && bytes[i] != b';' {
        i += 1;
    }

    while i < bytes.len() {
        if bytes[i] != b';' {
            i += 1;
            continue;
        }
        i += 1; // skip ';'
        while i < bytes.len() && is_ws(bytes[i]) {
            i += 1;
        }
        // Parameter name.
        let name_start = i;
        while i < bytes.len() && bytes[i] != b'=' && bytes[i] != b';' && !is_ws(bytes[i]) {
            i += 1;
        }
        let name_end = i;
        if name_start == name_end {
            continue;
        }
        let name = input[name_start..name_end].to_string();
        // Skip whitespace before '='.
        while i < bytes.len() && is_ws(bytes[i]) {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'=' {
            // Valueless parameter; ignore.
            continue;
        }
        i += 1; // skip '='
        while i < bytes.len() && is_ws(bytes[i]) {
            i += 1;
        }
        // Parameter value: quoted-string or token.
        let value = if i < bytes.len() && bytes[i] == b'"' {
            i += 1;
            let mut buf = String::new();
            while i < bytes.len() && bytes[i] != b'"' {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    buf.push(bytes[i + 1] as char);
                    i += 2;
                } else {
                    buf.push(bytes[i] as char);
                    i += 1;
                }
            }
            if i < bytes.len() {
                i += 1; // closing quote
            }
            buf
        } else {
            let start = i;
            while i < bytes.len() && bytes[i] != b';' {
                i += 1;
            }
            input[start..i].trim().to_string()
        };
        out.push(Param { name, value });
    }
    out
}

fn is_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t')
}

fn eq_ascii(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// Decode an RFC 5987 ext-value: `charset'lang'percent-encoded`.
fn decode_ext_value(value: &str) -> Option<String> {
    let q1 = value.find('\'')?;
    let after_q1 = &value[q1 + 1..];
    let q2 = after_q1.find('\'')?;
    let charset = value[..q1].to_ascii_lowercase();
    let encoded = &after_q1[q2 + 1..];
    if encoded.is_empty() {
        return None;
    }
    let bytes = percent_decode_str(encoded).collect::<Vec<u8>>();
    let decoded = match charset.as_str() {
        "" | "utf-8" | "utf8" => String::from_utf8_lossy(&bytes).into_owned(),
        "iso-8859-1" | "latin-1" => bytes.iter().map(|&b| b as char).collect(),
        _ => String::from_utf8_lossy(&bytes).into_owned(),
    };
    if decoded.is_empty() {
        None
    } else {
        Some(decoded)
    }
}

/// Sanitizes a filename so it is safe to use as a single path component.
///
/// Rules:
/// - strip path separators and NUL
/// - replace Windows-reserved chars (`< > : " | ? *`) with `_`
/// - replace control characters with `_`
/// - trim leading/trailing dots and whitespace
/// - if the result is empty, `.`, or `..`, use a fallback name
/// - cap length to `MAX_FILENAME_LEN` bytes, keeping a short extension if possible
pub fn sanitize(name: &str) -> String {
    let mut s: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | '\0' => '_',
            '<' | '>' | ':' | '"' | '|' | '?' | '*' => '_',
            c if (c as u32) < 0x20 => '_',
            c => c,
        })
        .collect();

    // Strip Windows reserved trailing/leading dots and spaces. We also strip a
    // leading `_` because path-separator replacements often produce them.
    while s
        .chars()
        .next()
        .map(|c| c == '.' || c == ' ' || c == '_')
        .unwrap_or(false)
    {
        s.remove(0);
        if s.is_empty() {
            break;
        }
    }
    while s
        .chars()
        .next_back()
        .map(|c| c == '.' || c == ' ')
        .unwrap_or(false)
    {
        s.pop();
    }

    if s.is_empty() || s == "." || s == ".." {
        return FALLBACK_FILENAME.to_string();
    }

    if s.len() > MAX_FILENAME_LEN {
        if let Some(dot) = s.rfind('.') {
            let ext = &s[dot..];
            if ext.len() <= 16 && dot > 0 {
                let stem_budget = MAX_FILENAME_LEN.saturating_sub(ext.len());
                let mut stem = s[..dot].to_string();
                while !stem.is_char_boundary(stem_budget.min(stem.len())) {
                    stem.pop();
                }
                stem.truncate(stem_budget.min(stem.len()));
                return format!("{stem}{ext}");
            }
        }
        // Truncate at a UTF-8 boundary.
        let mut cut = MAX_FILENAME_LEN;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        s.truncate(cut);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_drops_traversal() {
        assert_eq!(sanitize("../etc/passwd"), "etc_passwd");
        assert_eq!(sanitize("..\\..\\windows\\system32"), "windows_system32");
        assert_eq!(sanitize(".."), FALLBACK_FILENAME);
        assert_eq!(sanitize("."), FALLBACK_FILENAME);
        assert_eq!(sanitize(""), FALLBACK_FILENAME);
    }

    #[test]
    fn sanitize_replaces_windows_reserved() {
        assert_eq!(sanitize("foo:bar?baz*.txt"), "foo_bar_baz_.txt");
    }

    #[test]
    fn sanitize_keeps_unicode() {
        assert_eq!(sanitize("文件名.zip"), "文件名.zip");
    }

    #[test]
    fn sanitize_strips_control() {
        assert_eq!(sanitize("a\nb.txt"), "a_b.txt");
        assert_eq!(sanitize("a\0b"), "a_b");
    }

    #[test]
    fn sanitize_truncates_keeping_extension() {
        let long = "a".repeat(300) + ".zip";
        let s = sanitize(&long);
        assert!(s.ends_with(".zip"));
        assert!(s.len() <= MAX_FILENAME_LEN);
    }

    #[test]
    fn parse_cd_quoted() {
        assert_eq!(
            parse_content_disposition(r#"attachment; filename="hello world.txt""#).as_deref(),
            Some("hello world.txt")
        );
    }

    #[test]
    fn parse_cd_quoted_escaped() {
        assert_eq!(
            parse_content_disposition(r#"attachment; filename="a\"b.txt""#).as_deref(),
            Some(r#"a"b.txt"#)
        );
    }

    #[test]
    fn parse_cd_unquoted() {
        assert_eq!(
            parse_content_disposition("inline; filename=foo.bin; size=42").as_deref(),
            Some("foo.bin")
        );
    }

    #[test]
    fn parse_cd_extended_utf8() {
        assert_eq!(
            parse_content_disposition("attachment; filename*=UTF-8''%E4%BD%A0%E5%A5%BD.txt")
                .as_deref(),
            Some("你好.txt")
        );
    }

    #[test]
    fn parse_cd_extended_prefers_over_regular() {
        let v = "attachment; filename=fallback.txt; filename*=UTF-8''real.txt";
        assert_eq!(parse_content_disposition(v).as_deref(), Some("real.txt"));
    }

    #[test]
    fn parse_cd_case_insensitive() {
        assert_eq!(
            parse_content_disposition("Attachment; Filename=Foo.bin").as_deref(),
            Some("Foo.bin")
        );
    }

    #[test]
    fn parse_cd_no_filename() {
        assert_eq!(parse_content_disposition("attachment"), None);
    }

    #[test]
    fn parse_cd_doesnt_match_lookalike_name() {
        // `myfilename=` should NOT be parsed as `filename=`.
        // Our tokenizer treats the parameter name as the whole token, so it sees
        // `myfilename`, not `filename`.
        assert_eq!(
            parse_content_disposition("attachment; myfilename=foo.txt"),
            None
        );
    }

    #[test]
    fn parse_cd_skips_filename_star_when_finding_regular() {
        let v = "attachment; filename*=garbage";
        // extended fails (no quotes inside the value), regular must not falsely
        // pick up the `filename` part of `filename*`.
        assert_eq!(parse_content_disposition(v), None);
    }
}
