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
//! TOKEN parameters are still validated for wire-format conformance (§9.2.2.1)
//! but do not gate admission.
//!
//! Internal relay peers bypass token authorization, relying on the embedder
//! to authenticate them before tagging them internal. For public sessions
//! where a policy is present, enforcement happens at two points:
//!
//! * **Setup.** The AUTHORIZATION TOKEN parameters from CLIENT_SETUP are
//!   decoded (see [`decode_setup_tokens`]) and handed to
//!   [`AuthHook::on_setup`]. A denial terminates the session before either
//!   half of it is constructed.
//! * **Request.** Every SUBSCRIBE, SUBSCRIBE_NAMESPACE, TRACK_STATUS,
//!   PUBLISH_NAMESPACE, PUBLISH and FETCH (standalone and joining) are checked
//!   before the relay acts on them. The first five use [`AuthHook::on_request`];
//!   FETCH is checked via `may_fetch_track` under the `Fetch(7)` grant. A
//!   denial rejects that request and leaves the session running.
//!
//! Cryptography is confined to setup. The identity established there is
//! carried in a [`Principal`], so per-request checks are pure claim
//! evaluation. Expiry is re-checked on every request, so no *new* request is
//! authorized more than [`ScopeAuthConfig::clock_skew`] past the token's
//! `exp`. That tolerance exists to absorb clock differences between issuer
//! and relay, and it applies to the per-request check exactly as it does at
//! setup — so "expired" in practice means `exp + clock_skew`, not `exp`. The
//! CAT hook clamps the configured value so a scope cannot widen the window
//! indefinitely.
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
//! Scopes without a policy and authenticated internal relay peers bypass
//! token authorization. Token wire-format validation applies to every session.
//!
//! By default, a failed refresh reuses a fetched policy within the cap, with
//! token checks enforced. A cached configuration with no policy has no reuse
//! limit because it does not require token authorization. A coordinator can
//! turn this exception off with [`Coordinator::reuse_scope_config_on_error`],
//! so every failed fetch denies admission of public sessions, even on scopes
//! without a policy.
//!
//! [`Coordinator::resolve_scope`]: crate::Coordinator::resolve_scope
//! [`Coordinator::get_scope_config`]: crate::Coordinator::get_scope_config
//! [`Coordinator::reuse_scope_config_on_error`]: crate::Coordinator::reuse_scope_config_on_error
//! [`ScopeAuthConfig`]: crate::ScopeAuthConfig

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

pub(crate) const UNSCOPED: &str = "<unscoped>";

/// Session termination code for UNAUTHORIZED (draft-16 §13.4.1).
pub(crate) const SESSION_ERROR_UNAUTHORIZED: u32 = 0x2;
