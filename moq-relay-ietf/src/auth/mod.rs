// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Session and request authorization for the relay.
//!
//! # Model
//!
//! Authorization is a per-scope policy, not a relay-wide switch. After
//! [`Coordinator::resolve_scope`] identifies which scope a connection belongs
//! to, [`Coordinator::get_scope_config`] supplies that scope's
//! [`ScopeAuthConfig`], including the public keys used to verify tokens. A
//! scope that returns no policy carries no token enforcement; AUTHORIZATION
//! TOKEN options are still validated for wire-format conformance
//! (draft-18 §10.2.2) but do not gate admission.
//!
//! Internal relay peers bypass token authorization, relying on the embedder
//! to authenticate them before tagging them internal. For public sessions
//! where a policy is present, enforcement happens at two points:
//!
//! * **Setup.** The AUTHORIZATION TOKEN options from the client's SETUP
//!   (draft-18 §10.3.1.4) are decoded (see [`decode_setup_tokens`]) and handed
//!   to [`AuthHook::on_setup`]. A denial terminates the session with
//!   UNAUTHORIZED (§3.5) before either half of it is constructed.
//! * **Request.** Authorization-relevant requests are checked against the
//!   [`Principal`] established at setup, through [`AuthHook::on_request`],
//!   before the relay acts on them. A denial rejects that request and leaves
//!   the session running.
//!
//! Cryptography is confined to setup. The identity established there is
//! carried in a [`Principal`], so per-request checks are pure claim
//! evaluation. Expiry is re-checked on every request, so no *new* request is
//! authorized more than [`ScopeAuthConfig::clock_skew`] past the token's
//! `exp`. That tolerance exists to absorb clock differences between issuer
//! and relay, and it applies to the per-request check exactly as it does at
//! setup — so "expired" in practice means `exp + clock_skew`, not `exp`.
//!
//! # CAT action mapping (draft-ietf-moq-c4m)
//!
//! | Wire message (draft-18) | [`AuthzOperation`] | CAT `moqt` action |
//! |---|---|---|
//! | SETUP (§10.3) | *(inside `on_setup`)* | `ClientSetup` (0) |
//! | PUBLISH_NAMESPACE (§10.15) | `PublishNamespace` | 2 |
//! | SUBSCRIBE_NAMESPACE (§10.18) | `SubscribeNamespace` | 3 |
//! | SUBSCRIBE (§10.7) | `Subscribe` | 4 |
//! | PUBLISH (§10.10) | `Publish` | 6 |
//! | FETCH (§10.12) | `Fetch` | 7 |
//! | TRACK_STATUS (§10.14) | `TrackStatus` | 8 |
//! | SUBSCRIBE_TRACKS (§10.19) | `SubscribeNamespace` *(interim)* | 3 |
//!
//! FETCH requires `Fetch(7)`; a token that grants only `Subscribe(4)` cannot
//! fetch. This relay does not serve FETCH yet (the session layer answers it
//! as not supported), so no FETCH gate exists; the mapping is in place for
//! when relay FETCH serving lands.
//!
//! SUBSCRIBE_TRACKS is new in draft-18 and has no CAT action yet, so the
//! request is gated as `SubscribeNamespace` on its prefix. That grants
//! discovery only: every PUBLISH it generates is separately authorized as a
//! `Subscribe` for that exact track, as is every NAMESPACE a
//! SUBSCRIBE_NAMESPACE would disclose (as `SubscribeNamespace` on the exact
//! namespace). Tracks and namespaces outside the grant are withheld rather
//! than failing the request.
//!
//! # Revocation, and what it does not cover
//!
//! While the coordinator is reachable, rotating a scope's keys stops new
//! sessions being established with the old key within one
//! [`ScopeAuthConfig::ttl`]. By default, during an outage, new sessions use the
//! last fetched policy for at most 12 effective TTLs from the last successful
//! fetch (one hour at the default TTL). A failed refresh past that cap refuses
//! new sessions until a refresh succeeds. Scopes without a policy have no reuse
//! limit. A coordinator can turn reuse off with
//! [`Coordinator::reuse_scope_config_on_error`]; a failed refresh then refuses
//! new sessions on any scope as soon as its cached entry expires.
//! Rotation does **not** terminate sessions already running: a session holds
//! the hook it was admitted with.
//! Token expiry is therefore what bounds a compromised credential, which is
//! why the CAT hook requires `exp` and caps how far ahead it may be.
//!
//! Expiry bounds *admission*, not *delivery*. A SUBSCRIBE authorized before
//! the token expired keeps streaming afterwards, because nothing revisits an
//! established subscription — so a leaked token buys media for as long as the
//! session survives, not merely until `exp`. Tearing down in-flight
//! subscriptions at expiry, and re-resolving the hook when a scope's
//! configuration changes, are the two pieces that would close this; neither is
//! implemented.
//!
//! # Failing closed
//!
//! For public sessions on scopes with a policy, anything short of an explicit
//! allow denies:
//!
//! * a coordinator that cannot be reached before this scope's policy has been
//!   fetched, after the enforced policy's 12-TTL reuse cap, or whenever a fetch
//!   fails with reuse turned off,
//! * a configuration with no keys, too many keys, or unparseable key material,
//! * a relay built without the scheme's feature,
//! * a hook that returns an error,
//! * a missing, malformed, expired or out-of-scope token.
//!
//! By default, a failed refresh reuses a fetched policy within the cap, with
//! token checks enforced. A cached configuration with no policy has no reuse
//! limit because it does not require token authorization. A coordinator can
//! turn this exception off with [`Coordinator::reuse_scope_config_on_error`],
//! so every failed fetch denies admission even on scopes without a policy.
//!
//! Scopes without a policy and authenticated internal relay peers bypass
//! token authorization. Token wire-format validation applies to every session.
//!
//! [`Coordinator::resolve_scope`]: crate::Coordinator::resolve_scope
//! [`Coordinator::get_scope_config`]: crate::Coordinator::get_scope_config
//! [`Coordinator::reuse_scope_config_on_error`]: crate::Coordinator::reuse_scope_config_on_error
//! [`ScopeAuthConfig`]: crate::ScopeAuthConfig
//! [`ScopeAuthConfig::clock_skew`]: crate::ScopeAuthConfig::clock_skew
//! [`ScopeAuthConfig::ttl`]: crate::ScopeAuthConfig::ttl

mod hook;
mod scope;
mod session;
mod token;
mod types;

#[cfg(feature = "auth-cat")]
mod cat;

pub use hook::{AllowAllAuthHook, AuthHook};
pub use token::{decode_setup_tokens, TokenError, CAT_TOKEN_TYPE, MAX_SETUP_TOKENS};
pub use types::{
    AuthDecision, AuthError, AuthRequest, AuthToken, AuthzOperation, DenyReason, Principal, Verdict,
};

#[cfg(feature = "auth-cat")]
pub use cat::CatAuthHook;

pub(crate) use hook::DenyAllAuthHook;
pub(crate) use scope::ScopeAuthorizer;
pub(crate) use session::{authorize, SessionAuth};

use crate::DEFAULT_SCOPE_AUTH_TTL;

/// Session termination code for UNAUTHORIZED (draft-18 §3.5, §15.10.1;
/// unchanged from draft-16).
pub(crate) const SESSION_ERROR_UNAUTHORIZED: u32 = 0x2;
