// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError, VarInt};

use std::fmt;
use std::ops::Deref;

/// A version number negotiated during the setup.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version(pub u32);

impl Version {
    // Note: older draft versions are NOT included here, as we will no longer
    //       handle the old SETUP message type numbers of (0x40 and 0x41)

    /// First version we might see in CLIENT_SETUP (0x20) or SERVER_SETUP (0x21)
    /// https://www.ietf.org/archive/id/draft-ietf-moq-transport-11.html
    pub const DRAFT_11: Version = Version(0xff00000b);

    /// https://www.ietf.org/archive/id/draft-ietf-moq-transport-12.html
    pub const DRAFT_12: Version = Version(0xff00000c);

    /// https://www.ietf.org/archive/id/draft-ietf-moq-transport-13.html
    pub const DRAFT_13: Version = Version(0xff00000d);

    /// https://www.ietf.org/archive/id/draft-ietf-moq-transport-14.html
    pub const DRAFT_14: Version = Version(0xff00000e);

    /// https://www.ietf.org/archive/id/draft-ietf-moq-transport-16.html
    pub const DRAFT_16: Version = Version(0xff000010);
    pub const DRAFT_18: Version = Version(0xff000012);
}

impl From<u32> for Version {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl From<Version> for u32 {
    fn from(v: Version) -> Self {
        v.0
    }
}

impl Decode for Version {
    /// Decode the version number.
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let v = VarInt::decode(r)?;
        Ok(Self(u32::try_from(v).map_err(DecodeError::BoundsExceeded)?))
    }
}

impl Encode for Version {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        VarInt::from_u32(self.0).encode(w)?;
        Ok(())
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Just reuse the Display formatting
        write!(f, "{self}")
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 > 0xff000000 {
            write!(f, "DRAFT_{:02}", self.0 & 0x00ffffff)
        } else {
            self.0.fmt(f)
        }
    }
}

/// A list of versions in arbitrary order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Versions(pub Vec<Version>);

impl Decode for Versions {
    /// Decode the version list.
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let count = usize::decode(r)?;
        let mut vs = Vec::new();

        for _ in 0..count {
            let v = Version::decode(r)?;
            vs.push(v);
        }

        Ok(Self(vs))
    }
}

impl Encode for Versions {
    /// Encode the version list.
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.0.len().encode(w)?;

        for v in &self.0 {
            v.encode(w)?;
        }

        Ok(())
    }
}

impl Deref for Versions {
    type Target = Vec<Version>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<Version>> for Versions {
    fn from(vs: Vec<Version>) -> Self {
        Self(vs)
    }
}

impl<const N: usize> From<[Version; N]> for Versions {
    fn from(vs: [Version; N]) -> Self {
        Self(vs.to_vec())
    }
}

/// An IETF MoQT draft identified by its ALPN negotiation string.
///
/// Draft-18 is the confirmed production version. Draft-20 and Draft-22 carry
/// provisional ALPN identifiers — do not add them to
/// [`SUPPORTED_ALPNS`][crate::setup::SUPPORTED_ALPNS] until the identifiers
/// are confirmed at the Seattle interop event (October 2026).
///
/// This enum is `#[non_exhaustive]`: external crates that match on it need a
/// `_ =>` catch-all, so that adding a new draft variant is not a breaking change.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DraftVersion {
    /// `draft-ietf-moq-transport-18` — ALPN `"moqt-18"`. In production.
    Draft18,
    /// `draft-ietf-moq-transport-20` — ALPN `"moqt-20"` (provisional).
    ///
    /// Not yet in [`SUPPORTED_ALPNS`][crate::setup::SUPPORTED_ALPNS].
    /// Enable only after the IETF WG confirms the identifier.
    Draft20,
    /// `draft-ietf-moq-transport-22` — ALPN `"moqt-22"` (provisional).
    ///
    /// Not yet in [`SUPPORTED_ALPNS`][crate::setup::SUPPORTED_ALPNS].
    /// Enable only after the IETF WG confirms the identifier.
    Draft22,
}

impl DraftVersion {
    /// The ALPN protocol identifier string for this draft.
    ///
    /// Used for TLS ALPN negotiation (raw QUIC) and
    /// `WT-Available-Protocols` / `WT-Protocol` header negotiation (WebTransport).
    pub const fn alpn_str(&self) -> &'static str {
        match self {
            Self::Draft18 => "moqt-18",
            // TODO(Seattle, Oct 2026): verify these identifiers before enabling in SUPPORTED_ALPNS.
            Self::Draft20 => "moqt-20",
            Self::Draft22 => "moqt-22",
        }
    }

    /// The ALPN protocol identifier as a byte slice, for TLS configuration.
    pub const fn alpn_bytes(&self) -> &'static [u8] {
        self.alpn_str().as_bytes()
    }

    /// Convert an ALPN string to the matching `DraftVersion`.
    ///
    /// Returns `None` for unrecognized strings. Known-but-gated variants
    /// (e.g. `Draft20`, `Draft22`) return `Some` even if not yet in
    /// [`SUPPORTED_ALPNS`][crate::setup::SUPPORTED_ALPNS]. For
    /// connection-time version gating use [`negotiate_version`][crate::setup::negotiate_version].
    pub fn from_alpn(s: &str) -> Option<Self> {
        match s {
            "moqt-18" => Some(Self::Draft18),
            "moqt-20" => Some(Self::Draft20),
            "moqt-22" => Some(Self::Draft22),
            _ => None,
        }
    }

    /// A short, stable label for metrics and structured log fields.
    ///
    /// Stable across binary restarts; safe to use as a Prometheus label value.
    pub const fn metric_label(&self) -> &'static str {
        match self {
            Self::Draft18 => "d18",
            Self::Draft20 => "d20",
            Self::Draft22 => "d22",
        }
    }
}

impl fmt::Display for DraftVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.alpn_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    // ── Version / Versions ────────────────────────────────────────────────────

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();
        let versions = Versions(vec![Version(1), Version::DRAFT_12, Version::DRAFT_13]);

        versions.encode(&mut buf).unwrap();
        #[rustfmt::skip]
        assert_eq!(
            buf.to_vec(),
            vec![
                0x03, // 3 Versions
                // Version 1
                0x01,
                // Version DRAFT_12 (0xff00000c)
                0xF0, 0xFF, 0x00, 0x00, 0x0C,
                // Version DRAFT_13 (0xff00000d)
                0xF0, 0xFF, 0x00, 0x00, 0x0D,
            ]
        );

        let decoded = Versions::decode(&mut buf).unwrap();
        assert_eq!(decoded, versions);
    }

    // ── DraftVersion ──────────────────────────────────────────────────────────

    #[test]
    fn draft_version_alpn_str_values() {
        assert_eq!(DraftVersion::Draft18.alpn_str(), "moqt-18");
        assert_eq!(DraftVersion::Draft20.alpn_str(), "moqt-20");
        assert_eq!(DraftVersion::Draft22.alpn_str(), "moqt-22");
    }

    #[test]
    fn draft_version_alpn_bytes_matches_str() {
        assert_eq!(DraftVersion::Draft18.alpn_bytes(), b"moqt-18");
        assert_eq!(DraftVersion::Draft20.alpn_bytes(), b"moqt-20");
        assert_eq!(DraftVersion::Draft22.alpn_bytes(), b"moqt-22");
    }

    #[test]
    fn draft_version_from_alpn_round_trip() {
        for d in [
            DraftVersion::Draft18,
            DraftVersion::Draft20,
            DraftVersion::Draft22,
        ] {
            assert_eq!(
                DraftVersion::from_alpn(d.alpn_str()),
                Some(d),
                "from_alpn({:?}) should round-trip",
                d.alpn_str()
            );
        }
    }

    #[test]
    fn draft_version_from_alpn_unknown() {
        assert_eq!(DraftVersion::from_alpn("moqt-99"), None);
        assert_eq!(DraftVersion::from_alpn("moqt-16"), None);
        assert_eq!(DraftVersion::from_alpn(""), None);
        assert_eq!(DraftVersion::from_alpn("MOQT-18"), None); // case-sensitive
    }

    #[test]
    fn draft_version_metric_label_values() {
        assert_eq!(DraftVersion::Draft18.metric_label(), "d18");
        assert_eq!(DraftVersion::Draft20.metric_label(), "d20");
        assert_eq!(DraftVersion::Draft22.metric_label(), "d22");
    }

    #[test]
    fn draft_version_display() {
        assert_eq!(DraftVersion::Draft18.to_string(), "moqt-18");
        assert_eq!(DraftVersion::Draft20.to_string(), "moqt-20");
        assert_eq!(DraftVersion::Draft22.to_string(), "moqt-22");
    }

    #[test]
    fn draft_version_ordering() {
        // Declaration order: Draft18 < Draft20 < Draft22.
        assert!(DraftVersion::Draft18 < DraftVersion::Draft20);
        assert!(DraftVersion::Draft20 < DraftVersion::Draft22);
    }

    #[test]
    fn draft_version_debug_contains_name() {
        let s = format!("{:?}", DraftVersion::Draft18);
        assert!(s.contains("Draft18"), "Debug output: {s}");
    }
}
