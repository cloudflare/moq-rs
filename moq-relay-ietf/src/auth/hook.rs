// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Authorization hook trait and built-in implementations.
//!
//! Implement [`AuthHook`] to provide custom session and request authorization.
//!
//! # Built-in hooks
//!
//! - [`AllowAllAuthHook`] — admits every session and request. Returned by
//!   [`ScopeAuthorizer::resolve`] when `ScopeConfig::auth = None`.
//! - [`DenyAllAuthHook`] — denies every session. Returned by
//!   [`ScopeAuthorizer::resolve`] when `ScopeConfig::auth = Some(...)` in C1,
//!   because `CatAuthHook` is not yet implemented.
//!
//! # C1 note: hook is not yet wired into the relay session path
//!
//! [`ScopeAuthorizer::resolve`] produces hooks but the relay session code
//! (`relay.rs`) does not yet call them. Integration is deferred to C2.
//!
//! [`ScopeAuthorizer`]: crate::auth::scope::ScopeAuthorizer
//! [`ScopeAuthorizer::resolve`]: crate::auth::scope::ScopeAuthorizer::resolve

use std::sync::Arc;

use async_trait::async_trait;
use moq_transport::setup::Setup;

use super::types::{AuthDecision, AuthError, AuthRequest, DenyReason, Principal};

/// Pluggable authorization for a MoQT session.
///
/// A hook is built per scope from [`ScopeConfig::auth`] and shared by every
/// session in that scope via [`Arc`]. Both methods are required rather than
/// defaulted: a default that allows would silently authorize everything in
/// any implementation that forgot to override it. Use [`AllowAllAuthHook`]
/// to opt into permissive behaviour explicitly.
///
/// # Contract
///
/// * [`on_setup`] runs **once per session**, before either session half is
///   constructed. A denial terminates the session.
/// * [`on_request`] runs before **every** authorization-relevant message and
///   is given the [`Principal`] from setup. It is a hot path; avoid
///   cryptographic work here — verify at setup and carry the result in the
///   principal's `data` field.
/// * Returning `Err` is **never an allow**. The relay treats it as a denial
///   and counts it separately as a hook fault.
///
/// [`on_setup`]: Self::on_setup
/// [`on_request`]: Self::on_request
/// [`ScopeConfig::auth`]: crate::coordinator::ScopeConfig::auth
// `clippy::double_must_use` fires when a function carries an explicit
// `#[must_use]` annotation AND its return type is also `#[must_use]`.
// Both conditions hold for every async method here:
//   1. `async_trait` injects `#[must_use]` on each generated method signature
//      so that callers must `.await` the returned future.
//   2. `std::future::Future` is `#[must_use]`, so the generated return type
//      `Pin<Box<dyn Future<...>>>` is also `#[must_use]`.
// This became a hard error in CI under Rust 1.99 stable with
// `RUSTFLAGS=-D warnings` (injected by `actions-rust-lang/setup-rust-toolchain`).
// Remove this allow when async-trait drops its `#[must_use]` injection or
// when rustc/clippy gains an exemption for proc-macro-generated annotations.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait AuthHook: Send + Sync {
    /// Authorize session establishment and establish the peer's identity.
    ///
    /// `setup` contains the decoded SETUP message, including any
    /// AUTHORIZATION TOKEN parameters. The hook should decode and verify
    /// the token(s) and return a [`Principal`] on success.
    ///
    /// If multiple AUTHORIZATION TOKEN parameters are present (draft-18
    /// §10.3.1.4 allows repetition), the hook decides which to use.
    async fn on_setup(&self, setup: &Setup) -> Result<AuthDecision, AuthError>;

    /// Authorize a single request against the identity from setup.
    ///
    /// Called before every SUBSCRIBE, SUBSCRIBE_NAMESPACE, TRACK_STATUS,
    /// PUBLISH_NAMESPACE, PUBLISH, and FETCH. Returns `Ok(AuthDecision::Allow)`
    /// to permit or `Ok(AuthDecision::Deny)` to reject; `Err` is a hook fault.
    async fn on_request(&self, request: &AuthRequest<'_>) -> Result<AuthDecision, AuthError>;
}

/// An [`AuthHook`] that admits every session and request.
///
/// Use this for scopes with no authorization policy. [`ScopeAuthorizer::resolve`]
/// returns this when `ScopeConfig::auth = None`. The relay session path does
/// not yet call `resolve` in C1; that wiring arrives in C2.
#[derive(Clone, Debug, Default)]
pub struct AllowAllAuthHook;

#[async_trait]
impl AuthHook for AllowAllAuthHook {
    async fn on_setup(&self, _setup: &Setup) -> Result<AuthDecision, AuthError> {
        Ok(AuthDecision::allow(Principal::anonymous()))
    }

    /// Admits every request, cloning the principal in O(1) via `Arc` refcounts.
    async fn on_request(&self, request: &AuthRequest<'_>) -> Result<AuthDecision, AuthError> {
        Ok(AuthDecision::allow(request.principal.clone()))
    }
}

/// An [`AuthHook`] that denies every session with a fixed reason.
///
/// In C1, [`ScopeAuthorizer::resolve`] returns this for **every**
/// `ScopeConfig::auth = Some(...)`, regardless of whether the config is valid,
/// because `CatAuthHook` is not yet implemented. This ensures no configured
/// policy can accidentally admit sessions.
///
/// In C2, `DenyAllAuthHook` will additionally be used as a fallback when
/// `CatAuthHook::new()` fails (bad PEM, empty key list, coordinator error),
/// replacing the generic error with an explicit fail-closed hook.
///
/// [`ScopeAuthorizer`]: crate::auth::scope::ScopeAuthorizer
/// [`ScopeAuthorizer::resolve`]: crate::auth::scope::ScopeAuthorizer::resolve
#[derive(Clone, Debug)]
pub struct DenyAllAuthHook {
    reason: DenyReason,
}

impl DenyAllAuthHook {
    /// Create a hook that denies with `reason`.
    pub fn new(reason: DenyReason) -> Arc<Self> {
        Arc::new(Self { reason })
    }
}

#[async_trait]
impl AuthHook for DenyAllAuthHook {
    async fn on_setup(&self, _setup: &Setup) -> Result<AuthDecision, AuthError> {
        Ok(AuthDecision::deny(self.reason.clone()))
    }

    async fn on_request(&self, _request: &AuthRequest<'_>) -> Result<AuthDecision, AuthError> {
        Ok(AuthDecision::deny(self.reason.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::types::{AuthzOperation, DenyReason, Principal};
    use moq_transport::coding::{TrackName, TrackNamespace};

    fn dummy_setup() -> Setup {
        use moq_transport::coding::KeyValuePairs;
        Setup {
            params: KeyValuePairs::default(),
        }
    }

    fn dummy_principal() -> Principal {
        Principal::new(Some(std::sync::Arc::<str>::from("test-subject")))
    }

    #[tokio::test]
    async fn allow_all_hook_permits_setup() {
        let hook = AllowAllAuthHook;
        let setup = dummy_setup();
        let result = hook.on_setup(&setup).await.unwrap();
        assert!(result.is_allowed());
    }

    #[tokio::test]
    async fn allow_all_hook_permits_request() {
        let hook = AllowAllAuthHook;
        let ns = TrackNamespace::from_utf8_path("sports");
        let track = TrackName::from("video");
        let principal = dummy_principal();
        let operation = AuthzOperation::Subscribe {
            namespace: &ns,
            track: &track,
        };
        let request = AuthRequest {
            principal: &principal,
            operation: &operation,
            request_id: Some(1),
        };
        let result = hook.on_request(&request).await.unwrap();
        assert!(result.is_allowed());
    }

    #[tokio::test]
    async fn deny_all_hook_denies_setup() {
        let hook = DenyAllAuthHook::new(DenyReason::HookFault {
            message: "broken config".into(),
        });
        let setup = dummy_setup();
        let result = hook.on_setup(&setup).await.unwrap();
        assert!(!result.is_allowed());
    }

    #[tokio::test]
    async fn deny_all_hook_denies_fetch_request() {
        let hook = DenyAllAuthHook::new(DenyReason::ScopeMismatch);
        let ns = TrackNamespace::from_utf8_path("sports");
        let track = TrackName::from("video");
        let principal = dummy_principal();
        // Specifically test the Fetch operation — must be denied by DenyAllAuthHook.
        let operation = AuthzOperation::Fetch {
            namespace: &ns,
            track: &track,
        };
        let request = AuthRequest {
            principal: &principal,
            operation: &operation,
            request_id: Some(7),
        };
        let result = hook.on_request(&request).await.unwrap();
        assert!(!result.is_allowed());
        assert_eq!(result.deny_reason(), Some(&DenyReason::ScopeMismatch));
    }
}
