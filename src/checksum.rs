//! Optional end-to-end content verification.
//!
//! This is the strongest integrity guarantee the downloader offers: after the
//! bytes land on disk, the whole file is hashed and compared to an expected
//! digest. Unlike the size / ETag / If-Range checks — which guard against a
//! *changing* resource — a content hash catches *every* corruption source,
//! including a power-loss resume that skipped un-fsynced bytes, a buggy server,
//! or on-disk bit-rot.
//!
//! The expected digest comes either from the caller ([`crate::DownloadTask`])
//! or, when the caller supplies none, from a server `Repr-Digest` / `Digest` /
//! `Content-MD5` header captured while probing. Verification only runs when a
//! digest is available, so it costs nothing on the common path.

use std::io::Read;
use std::path::Path;

use base64::prelude::*;

use crate::error::{Error, Result};

/// An expected content digest. MD5 is supported only because servers still
/// advertise it; it is fine for detecting *accidental* corruption (the point of
/// this module), not for adversarial integrity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Checksum {
    Md5([u8; 16]),
    Sha256([u8; 32]),
    Sha512([u8; 64]),
}

impl Checksum {
    /// Parse a hex-encoded SHA-256 (64 hex chars).
    pub fn sha256_hex(s: &str) -> Result<Self> {
        Ok(Checksum::Sha256(from_hex::<32>(s)?))
    }
    /// Parse a hex-encoded SHA-512 (128 hex chars).
    pub fn sha512_hex(s: &str) -> Result<Self> {
        Ok(Checksum::Sha512(from_hex::<64>(s)?))
    }
    /// Parse a hex-encoded MD5 (32 hex chars).
    pub fn md5_hex(s: &str) -> Result<Self> {
        Ok(Checksum::Md5(from_hex::<16>(s)?))
    }

    pub fn algorithm(&self) -> &'static str {
        match self {
            Checksum::Md5(_) => "md5",
            Checksum::Sha256(_) => "sha-256",
            Checksum::Sha512(_) => "sha-512",
        }
    }

    fn bytes(&self) -> &[u8] {
        match self {
            Checksum::Md5(a) => a,
            Checksum::Sha256(a) => a,
            Checksum::Sha512(a) => a,
        }
    }

    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(self.bytes().len() * 2);
        for b in self.bytes() {
            s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
            s.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
        }
        s
    }
}

impl std::fmt::Display for Checksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.algorithm(), self.to_hex())
    }
}

/// Hash `path` and compare against `expected`. Runs the (blocking) read+hash on
/// a blocking thread so it never stalls the async runtime.
pub(crate) async fn verify_file(path: &Path, expected: &Checksum) -> Result<()> {
    let p = path.to_owned();
    let exp = expected.clone();
    let actual = tokio::task::spawn_blocking(move || compute_blocking(&p, &exp))
        .await
        .map_err(|e| Error::Io(std::io::Error::other(format!("hash task: {e}"))))??;
    if actual == *expected {
        Ok(())
    } else {
        Err(Error::ChecksumMismatch {
            expected: expected.to_string(),
            actual: actual.to_string(),
        })
    }
}

fn compute_blocking(path: &Path, like: &Checksum) -> Result<Checksum> {
    let f = std::fs::File::open(path)?;
    let mut r = std::io::BufReader::with_capacity(1 << 20, f);
    Ok(match like {
        Checksum::Md5(_) => Checksum::Md5(
            hash_with::<md5::Md5>(&mut r)?
                .try_into()
                .expect("md5 is 16 bytes"),
        ),
        Checksum::Sha256(_) => Checksum::Sha256(
            hash_with::<sha2::Sha256>(&mut r)?
                .try_into()
                .expect("sha-256 is 32 bytes"),
        ),
        Checksum::Sha512(_) => Checksum::Sha512(
            hash_with::<sha2::Sha512>(&mut r)?
                .try_into()
                .expect("sha-512 is 64 bytes"),
        ),
    })
}

fn hash_with<D: sha2::Digest>(r: &mut impl Read) -> Result<Vec<u8>> {
    let mut hasher = D::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = r.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_vec())
}

// ---------------------------------------------------------------------------
// Server-advertised digests
// ---------------------------------------------------------------------------

/// Pick the strongest digest a server advertised across the `Repr-Digest`,
/// `Digest`, and `Content-MD5` headers. Parsing is deliberately conservative:
/// anything we can't decode cleanly is ignored (returns `None`) rather than
/// risking a false rejection of a correct download.
pub(crate) fn digest_from_parts(
    repr_digest: Option<&str>,
    digest: Option<&str>,
    content_md5: Option<&str>,
) -> Option<Checksum> {
    let mut best: Option<Checksum> = None;
    let mut best_rank = 0u8;
    let mut consider = |c: Option<Checksum>| {
        if let Some(c) = c {
            let rank = rank(&c);
            if rank > best_rank {
                best_rank = rank;
                best = Some(c);
            }
        }
    };
    if let Some(v) = repr_digest {
        consider(parse_digest_list(v));
    }
    if let Some(v) = digest {
        consider(parse_digest_list(v));
    }
    if let Some(v) = content_md5 {
        consider(parse_content_md5(v));
    }
    best
}

fn rank(c: &Checksum) -> u8 {
    match c {
        Checksum::Sha512(_) => 3,
        Checksum::Sha256(_) => 2,
        Checksum::Md5(_) => 1,
    }
}

/// Parse an RFC 9530 `Repr-Digest`/`Digest` (sf-dictionary, `name=:base64:`) or
/// the older RFC 3230 `Digest` (`name=base64`) into the strongest supported
/// algorithm present.
fn parse_digest_list(value: &str) -> Option<Checksum> {
    let mut best: Option<Checksum> = None;
    let mut best_rank = 0u8;
    for entry in value.split(',') {
        let entry = entry.trim();
        let Some((name, val)) = entry.split_once('=') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        // RFC 9530 wraps the base64 in `:` (structured-field byte sequence);
        // RFC 3230 does not. Trimming `:` handles both.
        let val = val.trim().trim_matches(':').trim();
        let parsed = match name.as_str() {
            "sha-512" | "sha512" => decode_n::<64>(val).map(Checksum::Sha512),
            "sha-256" | "sha256" => decode_n::<32>(val).map(Checksum::Sha256),
            "md5" => decode_n::<16>(val).map(Checksum::Md5),
            _ => None,
        };
        if let Some(c) = parsed {
            let r = rank(&c);
            if r > best_rank {
                best_rank = r;
                best = Some(c);
            }
        }
    }
    best
}

fn parse_content_md5(value: &str) -> Option<Checksum> {
    decode_n::<16>(value.trim()).map(Checksum::Md5)
}

fn decode_n<const N: usize>(s: &str) -> Option<[u8; N]> {
    let raw = BASE64_STANDARD.decode(s).ok()?;
    raw.try_into().ok()
}

fn from_hex<const N: usize>(s: &str) -> Result<[u8; N]> {
    let s = s.trim();
    if s.len() != 2 * N {
        return Err(Error::InvalidChecksum(format!(
            "expected {} hex chars, got {}",
            2 * N,
            s.len()
        )));
    }
    let bytes = s.as_bytes();
    let mut out = [0u8; N];
    for (i, slot) in out.iter_mut().enumerate() {
        let hi = hex_val(bytes[2 * i])?;
        let lo = hex_val(bytes[2 * i + 1])?;
        *slot = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_val(b: u8) -> Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(Error::InvalidChecksum("non-hex character".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let c = Checksum::sha256_hex(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        assert_eq!(
            c.to_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(c.algorithm(), "sha-256");
    }

    #[test]
    fn hex_rejects_bad_input() {
        assert!(Checksum::sha256_hex("abc").is_err()); // wrong length
        assert!(Checksum::sha256_hex(&"zz".repeat(32)).is_err()); // non-hex
    }

    #[test]
    fn repr_digest_sf_binary_sha256() {
        // RFC 9530 form: sha-256=:<base64>:
        let bytes = [0xABu8; 32];
        let b64 = BASE64_STANDARD.encode(bytes);
        let header = format!("sha-256=:{b64}:");
        assert_eq!(parse_digest_list(&header), Some(Checksum::Sha256(bytes)));
    }

    #[test]
    fn digest_rfc3230_and_strength_preference() {
        let s256 = BASE64_STANDARD.encode([1u8; 32]);
        let s512 = BASE64_STANDARD.encode([2u8; 64]);
        // Both present (no colons, RFC 3230 style) -> strongest (sha-512) wins.
        let header = format!("sha-256={s256}, sha-512={s512}");
        assert_eq!(
            parse_digest_list(&header),
            Some(Checksum::Sha512([2u8; 64]))
        );
    }

    #[test]
    fn digest_ignores_unknown_and_malformed() {
        assert_eq!(parse_digest_list("unixsum=12345"), None);
        assert_eq!(parse_digest_list("sha-256=not-base64!!"), None);
        // Right algorithm, wrong decoded length -> ignored, not a false match.
        let short = BASE64_STANDARD.encode([0u8; 8]);
        assert_eq!(parse_digest_list(&format!("sha-256={short}")), None);
    }

    #[test]
    fn content_md5_parsed() {
        let b64 = BASE64_STANDARD.encode([7u8; 16]);
        assert_eq!(parse_content_md5(&b64), Some(Checksum::Md5([7u8; 16])));
        assert_eq!(parse_content_md5("garbage="), None);
    }

    #[test]
    fn digest_from_parts_prefers_strength() {
        let md5 = BASE64_STANDARD.encode([9u8; 16]);
        let s256 = BASE64_STANDARD.encode([3u8; 32]);
        let got = digest_from_parts(Some(&format!("sha-256=:{s256}:")), None, Some(&md5));
        assert_eq!(got, Some(Checksum::Sha256([3u8; 32])));
    }
}
