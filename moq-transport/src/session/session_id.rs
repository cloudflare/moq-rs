// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{fmt, sync::Arc};

/// Stable correlation identity for one MoQ session.
///
/// Native callers can supply the initial QUIC connection ID in lowercase hex
/// to correlate qlog/mlog filenames. Otherwise a local UUID is generated; it
/// identifies the session but need not match a transport connection ID.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SessionId(Arc<str>);

impl SessionId {
    /// Adopt an application-supplied correlation ID. Use a non-secret value.
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }

    /// Generate a local identity as 32 lowercase hexadecimal characters.
    pub fn generate() -> Self {
        Self::new(uuid::Uuid::new_v4().simple().to_string())
    }

    /// Borrow the identity without allocating.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Create an explicit root so work on a shared upstream cannot inherit a
    /// downstream session's identity. Enable `moq_transport::session` alongside
    /// target-specific event filters to retain span context.
    ///
    /// The most verbose enabled level is used, including WARN/ERROR under
    /// restrictive filters. Span levels describe filtering, not session health.
    pub fn span(&self) -> tracing::Span {
        macro_rules! span {
            ($level:ident) => {
                tracing::span!(target: "moq_transport::session", parent: None,
                    tracing::Level::$level, "moq_session", session_id = %self)
            };
        }
        if tracing::enabled!(target: "moq_transport::session", tracing::Level::TRACE) {
            span!(TRACE)
        } else if tracing::enabled!(target: "moq_transport::session", tracing::Level::DEBUG) {
            span!(DEBUG)
        } else if tracing::enabled!(target: "moq_transport::session", tracing::Level::INFO) {
            span!(INFO)
        } else if tracing::enabled!(target: "moq_transport::session", tracing::Level::WARN) {
            span!(WARN)
        } else if tracing::enabled!(target: "moq_transport::session", tracing::Level::ERROR) {
            span!(ERROR)
        } else {
            tracing::Span::none()
        }
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for SessionId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for SessionId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// A quoted, single-line rendering of untrusted diagnostic bytes.
///
/// Invalid UTF-8 is replaced, then control characters and Unicode separators
/// are escaped by Debug formatting. Tokens resembling URLs, paths, queries or JWTs are
/// hidden entirely, including JWTs presented as the first path segment.
pub struct LogSafe<'a>(pub &'a [u8]);

impl fmt::Display for LogSafe<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = String::from_utf8_lossy(self.0);
        let mut redacted = String::new();
        for word in text.split_inclusive(char::is_whitespace) {
            if word.contains('/') || word.contains('?') || word.contains("eyJ") {
                redacted.push_str("[redacted]");
                // Keep separators, including hostile control characters, so
                // Debug formatting below escapes them consistently.
                redacted.push_str(word.trim_start_matches(|c: char| !c.is_whitespace()));
            } else {
                redacted.push_str(word);
            }
        }
        write!(f, "{redacted:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_stable_and_cheap_to_clone() {
        let id = SessionId::generate();
        assert_eq!(id.as_str().len(), 32);
        assert!(id
            .as_str()
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
        let clone = id.clone();
        assert!(std::ptr::eq(id.as_str().as_ptr(), clone.as_str().as_ptr()));
        assert_ne!(id, SessionId::generate());
        assert_eq!(SessionId::new("001122").to_string(), "001122");
    }

    #[test]
    fn untrusted_reasons_are_single_line_and_redact_credentials() {
        let rendered = LogSafe(b"a\r\nb\0\x1b\xff level=ERROR").to_string();
        assert!(rendered.starts_with('"') && rendered.ends_with('"'));
        assert!(!rendered.chars().any(char::is_control));
        assert!(rendered.contains("\\r\\n"));
        assert!(rendered.contains("\\0") && rendered.contains("\\u{1b}"));
        for input in [
            "bad /eyJhbGci.payload.signature",
            "https://peer.test/path?secret",
            "/scope/secret",
            "?token=secret",
        ] {
            let out = LogSafe(input.as_bytes()).to_string();
            assert!(out.contains("[redacted]"));
            assert!(!out.contains("secret") && !out.contains("eyJ"));
        }
        for separator in ['\u{2028}', '\u{2029}'] {
            assert!(!LogSafe(separator.to_string().as_bytes())
                .to_string()
                .contains(separator));
        }
    }
}
