// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Session and request authorization foundations for the draft-18 relay.
//!
//! # C1 behavior — two paths, one invariant
//!
//! **`auth=None`:** The scope has no policy. [`ScopeAuthorizer::resolve`]
//! returns [`AllowAllAuthHook`]; every session is admitted freely.
//!
//! **`auth=Some(cfg)`:** The scope has a CAT policy. [`ScopeAuthorizer::resolve`]
//! returns [`DenyAllAuthHook`] in C1 because `CatAuthHook` (C2) is not yet
//! implemented. The relay session path does not yet call `resolve` (that
//! wiring arrives in C2), but the hook contract is correct: **`DenyAllAuthHook`
//! never admits a session** — so once C2 wires in `resolve`, no configured
//! policy can accidentally admit sessions.
//!
//! This contract is machine-tested in
//! [`scope::tests::auth_none_allows_auth_some_denies`].
//!
//! # C2 behavior (planned — do not implement here)
//!
//! C2 replaces the `Some(_) → DenyAll` arm in `ScopeAuthorizer::resolve` with
//! `CatAuthHook::new(cfg)`, adds session-token decoding, relay gate wiring,
//! and metrics. No C1 type needs to change for that migration.
//!
//! # CAT action mapping (draft-ietf-moq-c4m)
//!
//! | Wire Message | [`AuthzOperation`] | CAT `MoqtAction` | Value |
//! |---|---|---|---|
//! | SETUP | *(inline in `on_setup`)* | `ClientSetup` | 0 |
//! | SUBSCRIBE | `Subscribe` | `Subscribe` | 4 |
//! | SUBSCRIBE_NAMESPACE | `SubscribeNamespace` | `SubscribeNamespace` | 3 |
//! | PUBLISH | `Publish` | `Publish` | 6 |
//! | PUBLISH_NAMESPACE | `PublishNamespace` | `PublishNamespace` | 2 |
//! | FETCH | **`Fetch`** | **`Fetch`** | **7** |
//! | TRACK_STATUS | `TrackStatus` | `TrackStatus` | 8 |
//!
//! ## Draft-18 note: Fetch uses action 7
//!
//! Unlike the draft-16 relay (which mapped FETCH to `Subscribe` action 4),
//! this draft-18 implementation correctly assigns FETCH its own variant.
//! Token issuers must include action 7 for clients that issue FETCH requests.

pub mod hook;
pub mod scope;
pub mod token;
pub mod types;

pub use hook::{AllowAllAuthHook, AuthHook, DenyAllAuthHook};
pub use scope::{ScopeAuthConfig, ScopeAuthorizer};
pub use token::{
    decode_setup_tokens, AliasType, AuthToken, SetupTokens, CAT_TOKEN_TYPE, MAX_SETUP_TOKENS,
};
pub use types::{
    AuthDecision, AuthError, AuthRequest, AuthzOperation, DenyReason, Principal, ResourceShape,
};
