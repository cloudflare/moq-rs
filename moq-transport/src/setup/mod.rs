// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Messages used for the MoQ Transport handshake.
//!
//! In draft-18, both peers open a unidirectional control stream and send a
//! unified SETUP message. Version negotiation is handled entirely by ALPN.

mod auth_token;
mod authorization_token;
mod param_types;
#[allow(clippy::module_inception)]
mod setup;
mod version;

pub use auth_token::{
    decode_setup_tokens, AliasType, SetupToken, TokenError, CAT_TOKEN_TYPE, MAX_SETUP_TOKENS,
};
pub(crate) use authorization_token::encode_setup_options;
pub use authorization_token::AuthorizationToken;
pub use param_types::*;
pub use setup::*;
pub use version::*;

/// Supported MoQT ALPN protocol identifiers, in preference order (most preferred first).
///
/// The server picks the first entry that the client also offers. This list controls both
/// what is advertised and the preference ordering when multiple drafts overlap.
///
/// For native QUIC, entries are offered as TLS ALPN values.
/// For WebTransport, entries are offered via `WT-Available-Protocols` and selected via
/// `WT-Protocol`.
///
/// **To enable draft-20 or draft-22:** add `DraftVersion::Draft20.alpn_str()` and/or
/// `DraftVersion::Draft22.alpn_str()` to this array only after the IETF WG confirms the
/// ALPN identifiers (expected at Seattle, October 2026).
pub const SUPPORTED_ALPNS: &[&str] = &[
    DraftVersion::Draft18.alpn_str(),
    // DraftVersion::Draft20.alpn_str(),  // TODO(Seattle, Oct 2026): enable after confirmation
    // DraftVersion::Draft22.alpn_str(),  // TODO(Seattle, Oct 2026): enable after confirmation
];

/// The draft-18 ALPN byte string, kept for backward compatibility.
///
/// Prefer [`DraftVersion::Draft18`]`.alpn_bytes()` in new code; this constant will be
/// removed in a future cleanup once all call sites are updated.
#[deprecated(
    since = "0.16.0",
    note = "Use `DraftVersion::Draft18.alpn_bytes()` instead."
)]
pub const ALPN: &[u8] = DraftVersion::Draft18.alpn_bytes();

/// Select the best mutually-supported MoQT version from a peer's offered ALPN list.
///
/// Iterates [`SUPPORTED_ALPNS`] in preference order (most preferred first) and returns
/// the first [`DraftVersion`] that the peer also offers. Returns `None` when there is no
/// overlap; the caller must reject the connection in that case.
pub fn negotiate_version(offered: &[impl AsRef<str>]) -> Option<DraftVersion> {
    SUPPORTED_ALPNS
        .iter()
        .copied()
        .find(|ours| offered.iter().any(|theirs| theirs.as_ref() == *ours))
        .and_then(DraftVersion::from_alpn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiate_exact_match() {
        let offered = vec!["moqt-18".to_string()];
        assert_eq!(negotiate_version(&offered), Some(DraftVersion::Draft18));
    }

    #[test]
    fn negotiate_skips_unsupported() {
        // moqt-16 is not in SUPPORTED_ALPNS; moqt-18 is selected as the only match.
        let offered = vec!["moqt-16".to_string(), "moqt-18".to_string()];
        assert_eq!(negotiate_version(&offered), Some(DraftVersion::Draft18));
    }

    #[test]
    fn negotiate_no_overlap() {
        let offered = vec!["moqt-99".to_string()];
        assert_eq!(negotiate_version(&offered), None);
    }

    #[test]
    fn negotiate_empty_offer() {
        let offered: Vec<String> = vec![];
        assert_eq!(negotiate_version(&offered), None);
    }

    #[test]
    fn negotiate_str_slices() {
        // Accepts &[&str] without allocating, not just &[String].
        let offered: &[&str] = &["moqt-18"];
        assert_eq!(negotiate_version(offered), Some(DraftVersion::Draft18));
    }

    #[test]
    fn supported_alpns_all_parse_to_known_draft_version() {
        // Every entry in SUPPORTED_ALPNS must map to a known DraftVersion.
        // This catches any future sync issues between SUPPORTED_ALPNS and DraftVersion::from_alpn.
        for alpn in SUPPORTED_ALPNS {
            assert!(
                DraftVersion::from_alpn(alpn).is_some(),
                "SUPPORTED_ALPNS entry {alpn:?} has no corresponding DraftVersion"
            );
        }
    }

    #[test]
    #[allow(deprecated)]
    fn alpn_compat_constant_matches_draft18() {
        // The deprecated ALPN constant must stay byte-identical to Draft18's ALPN.
        assert_eq!(ALPN, DraftVersion::Draft18.alpn_bytes());
    }
}
