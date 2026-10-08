// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Authorization operation and decision types.
//!
//! Every relay enforcement point names its operation via [`AuthzOperation`],
//! which [`AuthHook::on_request`] uses to decide whether to allow or deny.
//! The hook receives a fully decoded [`Principal`] (established at setup) so
//! it never repeats cryptographic work on the request path.
//!
//! # CAT action mapping (draft-ietf-moq-c4m)
//!
//! Each [`AuthzOperation`] variant maps to a specific `MoqtAction` integer
//! that must appear in the token's `moqt` claim scope. The mapping is:
//!
//! | Operation | CAT `MoqtAction` | Value |
//! |---|---|---|
//! | [`AuthzOperation::PublishNamespace`] | `PublishNamespace` | 2 |
//! | [`AuthzOperation::SubscribeNamespace`] | `SubscribeNamespace` | 3 |
//! | [`AuthzOperation::Subscribe`] | `Subscribe` | 4 |
//! | [`AuthzOperation::Publish`] | `Publish` | 6 |
//! | [`AuthzOperation::Fetch`] | **`Fetch`** | **7** |
//! | [`AuthzOperation::TrackStatus`] | `TrackStatus` | 8 |
//!
//! `ClientSetup` (action 0) is checked inside [`AuthHook::on_setup`] and has
//! no corresponding `AuthzOperation` variant.
//!
//! ## Draft-18 note: Fetch uses action 7
//!
//! Unlike the draft-16 relay (which mapped FETCH to the `Subscribe` (4) action
//! as a deliberate simplification), this draft-18 implementation assigns
//! [`AuthzOperation::Fetch`] its own variant that maps to `MoqtAction::Fetch`
//! (value 7). Token issuers must include action 7 alongside action 4 (or use
//! the `.subscriber()` builder which adds both) for subscribers that will
//! issue FETCH requests.

use moq_transport::coding::{TrackName, TrackNamespace, TrackNamespacePrefix};

/// The authorization operation being requested.
///
/// Passed to [`AuthHook::on_request`] so the hook can evaluate the operation
/// against the caller's [`Principal`] without reconstructing protocol context.
///
/// See the module documentation for the CAT `MoqtAction` mapping.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AuthzOperation<'a> {
    /// Inbound PUBLISH_NAMESPACE (draft-18 §10.15) — publisher announces
    /// availability of tracks under a namespace.
    ///
    /// Maps to CAT `MoqtAction::PublishNamespace` (value 2).
    PublishNamespace { namespace: &'a TrackNamespace },

    /// Inbound SUBSCRIBE_NAMESPACE (draft-18 §10.18) — subscriber requests
    /// namespace discovery for a prefix.
    ///
    /// Maps to CAT `MoqtAction::SubscribeNamespace` (value 3).
    SubscribeNamespace { prefix: &'a TrackNamespacePrefix },

    /// Inbound SUBSCRIBE (draft-18 §10.7) — subscriber requests a specific
    /// track.
    ///
    /// Maps to CAT `MoqtAction::Subscribe` (value 4).
    Subscribe {
        namespace: &'a TrackNamespace,
        track: &'a TrackName,
    },

    /// Inbound PUBLISH (draft-18 §10.11) — publisher sends a track.
    ///
    /// Maps to CAT `MoqtAction::Publish` (value 6).
    Publish {
        namespace: &'a TrackNamespace,
        track: &'a TrackName,
    },

    /// Inbound FETCH (draft-18 §10.13) — subscriber requests a historical
    /// object range from a named track.
    ///
    /// Maps to CAT `MoqtAction::Fetch` (value **7**).
    ///
    /// In this draft-18 implementation, FETCH uses its own action value (7)
    /// rather than reusing `Subscribe` (4). Token issuers must include
    /// action 7 for clients that issue FETCH requests.
    Fetch {
        namespace: &'a TrackNamespace,
        track: &'a TrackName,
    },

    /// Inbound TRACK_STATUS (draft-18 §10.15) — subscriber queries delivery
    /// status for a specific track.
    ///
    /// Maps to CAT `MoqtAction::TrackStatus` (value 8).
    TrackStatus {
        namespace: &'a TrackNamespace,
        track: &'a TrackName,
    },
}

impl<'a> AuthzOperation<'a> {
    /// Short human-readable label, used in log messages.
    pub fn label(&self) -> &'static str {
        match self {
            Self::PublishNamespace { .. } => "publish_namespace",
            Self::SubscribeNamespace { .. } => "subscribe_namespace",
            Self::Subscribe { .. } => "subscribe",
            Self::Publish { .. } => "publish",
            Self::Fetch { .. } => "fetch",
            Self::TrackStatus { .. } => "track_status",
        }
    }

    /// The resource shape this operation targets.
    ///
    /// Namespace-level operations carry only a namespace; track-level
    /// operations carry both a namespace and a track name.
    pub fn resource_shape(&self) -> ResourceShape {
        match self {
            Self::PublishNamespace { .. } | Self::SubscribeNamespace { .. } => {
                ResourceShape::Namespace
            }
            Self::Subscribe { .. }
            | Self::Publish { .. }
            | Self::Fetch { .. }
            | Self::TrackStatus { .. } => ResourceShape::Track,
        }
    }
}

/// The resource shape targeted by an authorization operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceShape {
    /// Operation targets a namespace or namespace prefix (no track name).
    Namespace,
    /// Operation targets a specific track within a namespace.
    Track,
}

/// The outcome of an authorization check.
#[derive(Debug, Clone)]
pub enum AuthDecision {
    /// The operation is permitted. Carries the [`Principal`] established at
    /// setup (for `on_setup`), or the original principal (for `on_request`).
    Allow(Principal),
    /// The operation is denied for the given reason.
    Deny(DenyReason),
}

impl AuthDecision {
    /// Construct an allow decision carrying `principal`.
    pub fn allow(principal: Principal) -> Self {
        Self::Allow(principal)
    }

    /// Construct a deny decision with `reason`.
    pub fn deny(reason: DenyReason) -> Self {
        Self::Deny(reason)
    }

    /// Returns `true` if this is an allow.
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow(_))
    }

    /// Return the principal from an allow decision, or `None` for deny.
    ///
    /// For use in tests only.
    #[cfg(test)]
    pub fn into_principal(self) -> Option<Principal> {
        match self {
            Self::Allow(p) => Some(p),
            Self::Deny(_) => None,
        }
    }

    /// Extract the deny reason if this is a deny.
    pub fn deny_reason(&self) -> Option<&DenyReason> {
        match self {
            Self::Allow(_) => None,
            Self::Deny(r) => Some(r),
        }
    }
}

/// Why an authorization check was denied.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DenyReason {
    /// The token is missing from CLIENT_SETUP.
    TokenMissing,
    /// The token bytes could not be decoded as a valid credential.
    TokenMalformed,
    /// The token's signature did not verify.
    TokenInvalid,
    /// The token has passed its `exp` claim.
    TokenExpired,
    /// The token's issuer is not in the configured allow-list.
    IssuerUnknown,
    /// The token's audience does not match the relay's identity.
    AudienceMismatch,
    /// The token's scope does not cover the requested operation or resource.
    ScopeMismatch,
    /// The token was replayed.
    TokenReplayed,
    /// A policy constraint in the token (e.g. `catreplay`, `catu`) could not
    /// be enforced by this relay.
    PolicyDenied { message: String },
    /// The hook itself faulted (configuration error, coordinator down, etc.).
    /// Treated as a denial but counted separately in metrics.
    HookFault { message: String },
}

/// The verified identity established at session setup.
///
/// Carried through the session so per-request checks only do claim
/// evaluation — no repeated cryptographic work.
///
/// # Clone cost
///
/// All fields are `Arc`-backed so `Clone` is O(1) — two refcount increments.
/// `AllowAllAuthHook::on_request` clones the principal and re-wraps it in a
/// new `AuthDecision::Allow`; this costs two `Arc::clone` calls with no heap
/// allocation.
///
/// # Stability
///
/// `#[non_exhaustive]` allows fields to be added without breaking callers.
/// Construct via [`Principal::new`]; the `data` field is for hook-internal use.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Principal {
    /// The `sub` claim from the token (opaque subject identifier).
    ///
    /// `Arc<str>` rather than `String` so `Clone` is a single refcount
    /// increment. C2 sets this from the decoded CAT `sub` claim.
    pub subject: Option<std::sync::Arc<str>>,

    /// Opaque per-implementation data carried through the session.
    ///
    /// `CatAuthHook` stores the decoded, verified token here so `on_request`
    /// can evaluate claims without re-parsing. Other hooks leave this `None`.
    /// `Arc<dyn Any>` makes `Clone` cheap (refcount only, no deep copy).
    pub data: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
}

impl Principal {
    /// Construct a principal with an optional subject string.
    pub fn new(subject: Option<impl Into<std::sync::Arc<str>>>) -> Self {
        Self {
            subject: subject.map(Into::into),
            data: None,
        }
    }

    /// Construct a principal with no subject or data (anonymous session).
    pub fn anonymous() -> Self {
        Self {
            subject: None,
            data: None,
        }
    }

    /// Attach opaque session data (consumed once, shared cheaply thereafter).
    pub fn with_data(mut self, data: impl std::any::Any + Send + Sync + 'static) -> Self {
        self.data = Some(std::sync::Arc::new(data));
        self
    }
}

/// Context passed to [`AuthHook::on_request`].
pub struct AuthRequest<'a> {
    /// The principal established at session setup.
    pub principal: &'a Principal,
    /// The operation being authorized.
    pub operation: &'a AuthzOperation<'a>,
    /// The request ID from the wire message (used in metrics and logs).
    pub request_id: Option<u64>,
}

/// Errors from the authorization layer.
///
/// Returned by:
/// - [`ScopeAuthConfig::validate`] — invalid configuration
/// - [`AuthHook::on_setup`] — hook fault during session establishment
/// - [`AuthHook::on_request`] — hook fault during per-request authorization
///
/// A hook that returns `Err` is treated as a denial and counted separately
/// in metrics (fault vs. intentional denial).
///
/// [`AuthHook::on_setup`]: crate::auth::hook::AuthHook::on_setup
/// [`AuthHook::on_request`]: crate::auth::hook::AuthHook::on_request
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AuthError {
    /// The scope configuration is invalid or incompatible with this hook.
    #[error("configuration error: {0}")]
    Configuration(String),
    /// The coordinator returned an error while fetching scope config.
    #[error("coordinator error: {0}")]
    Backend(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use moq_transport::coding::{TrackName, TrackNamespace, TrackNamespacePrefix};

    fn ns() -> TrackNamespace {
        TrackNamespace::from_utf8_path("sports/football")
    }

    fn track() -> TrackName {
        TrackName::from("video")
    }

    fn prefix() -> TrackNamespacePrefix {
        TrackNamespacePrefix::from_utf8_path("sports")
    }

    #[test]
    fn fetch_operation_has_track_resource_shape() {
        let ns = ns();
        let track = track();
        let op = AuthzOperation::Fetch {
            namespace: &ns,
            track: &track,
        };
        // Draft-18 FETCH uses MoqtAction::Fetch (7), a Track-level operation.
        // Resource shape must be Track so scope matching checks both namespace
        // and track name — consistent with Subscribe and TrackStatus.
        assert_eq!(
            op.resource_shape(),
            ResourceShape::Track,
            "Fetch must be Track-shaped so CAT Fetch(7) scope matching includes track"
        );
        assert_eq!(op.label(), "fetch");
    }

    #[test]
    fn subscribe_namespace_has_namespace_resource_shape() {
        let pfx = prefix();
        let op = AuthzOperation::SubscribeNamespace { prefix: &pfx };
        assert_eq!(op.resource_shape(), ResourceShape::Namespace);
    }

    #[test]
    fn subscribe_has_track_resource_shape() {
        let ns = ns();
        let track = track();
        let op = AuthzOperation::Subscribe {
            namespace: &ns,
            track: &track,
        };
        assert_eq!(op.resource_shape(), ResourceShape::Track);
    }

    #[test]
    fn track_status_has_track_resource_shape() {
        let ns = ns();
        let track = track();
        let op = AuthzOperation::TrackStatus {
            namespace: &ns,
            track: &track,
        };
        assert_eq!(op.resource_shape(), ResourceShape::Track);
    }

    #[test]
    fn auth_decision_allow_is_allowed() {
        let d = AuthDecision::allow(Principal::new(Some(std::sync::Arc::<str>::from("alice"))));
        assert!(d.is_allowed());
        assert!(d.deny_reason().is_none());
    }

    #[test]
    fn auth_decision_deny_is_not_allowed() {
        let d = AuthDecision::deny(DenyReason::ScopeMismatch);
        assert!(!d.is_allowed());
        assert_eq!(d.deny_reason(), Some(&DenyReason::ScopeMismatch));
    }

    #[test]
    fn fetch_and_subscribe_are_distinct_operations() {
        let ns = ns();
        let track = track();
        let fetch = AuthzOperation::Fetch {
            namespace: &ns,
            track: &track,
        };
        let subscribe = AuthzOperation::Subscribe {
            namespace: &ns,
            track: &track,
        };
        // Both are Track-shaped, but their labels are distinct — they map to
        // different CAT MoqtAction values (Fetch=7, Subscribe=4).
        assert_eq!(fetch.label(), "fetch");
        assert_eq!(subscribe.label(), "subscribe");
        assert_ne!(fetch.label(), subscribe.label());
    }
}
