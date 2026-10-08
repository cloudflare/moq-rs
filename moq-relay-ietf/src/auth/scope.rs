// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Per-scope authorization configuration and hook resolution.
//!
//! [`ScopeAuthConfig`] holds the policy parameters that will be used by
//! `CatAuthHook` (implemented in C2). [`ScopeAuthorizer`] builds and caches
//! the per-scope hook instance.
//!
//! # C1 behavior — auth=None permits, auth=Some fails closed
//!
//! `ScopeAuthorizer::resolve` is the **only** correct path to obtain a hook:
//!
//! | `ScopeConfig::auth` | Hook returned | Sessions admitted? |
//! |---|---|---|
//! | `None` | [`AllowAllAuthHook`] | Yes |
//! | `Some(cfg)` with invalid cfg | [`DenyAllAuthHook`] (`HookFault`) | **No** |
//! | `Some(cfg)` with valid cfg | [`DenyAllAuthHook`] (`HookFault`) | **No** |
//!
//! Setting `auth=Some(...)` causes `resolve` to return `DenyAllAuthHook`,
//! which **never** admits sessions when called. Note: the relay session code
//! does not yet call `resolve` (wiring is deferred to C2), so the deny hook
//! is not yet reached at runtime — but the contract is correct for C2.
//!
//! # C2 behavior (planned)
//!
//! C2 replaces the `Some(_) => DenyAll` arm with actual `CatAuthHook`
//! construction. The [`ScopeAuthConfig`] fields are sized to hold exactly
//! the inputs `CatAuthHook::new()` requires so no migration is needed.

use std::sync::Arc;

use super::hook::{AllowAllAuthHook, AuthHook, DenyAllAuthHook};
use super::types::{AuthError, DenyReason};

/// Configuration for per-scope CAT bearer-token authorization.
///
/// Set as `Some(cfg)` on [`ScopeConfig::auth`] to declare that this scope
/// requires token enforcement.
///
/// In C1, [`ScopeAuthorizer::resolve`] returns [`DenyAllAuthHook`] for any
/// `Some(cfg)`. The relay session path does not yet call `resolve` (that
/// wiring is C2), so there is no runtime enforcement yet. The contract is
/// established now so C2 can wire it in without changing the type.
///
/// # Empty-field semantics
///
/// Field emptiness is validated by [`ScopeAuthConfig::validate`] before any
/// hook is constructed:
///
/// - **`keys`**: must be non-empty (at least one public key required).
/// - **`audience`**: must be non-empty (the relay UID, server-derived).
/// - **`issuers`**: may be empty to accept any issuer (unsafe; prefer an
///   explicit list in production). An empty list is NOT an error in C1 because
///   the list is only enforced by `CatAuthHook` (C2).
///
/// [`ScopeConfig::auth`]: crate::coordinator::ScopeConfig::auth
#[derive(Debug, Clone, Default)]
pub struct ScopeAuthConfig {
    /// ES256 public keys in rotation order. Each entry is a PEM-encoded
    /// SubjectPublicKeyInfo (-----BEGIN PUBLIC KEY-----).
    ///
    /// Must be non-empty. An empty list is a configuration error that maps
    /// to [`DenyAllAuthHook`] when validate() is called.
    pub keys: Vec<String>,

    /// Expected token issuers (`iss` claim). An empty list means any issuer
    /// is accepted — unsafe for production; prefer an explicit allowlist.
    ///
    /// In C1 this field is not enforced; it is validated and stored for C2.
    pub issuers: Vec<String>,

    /// Expected token audience (`aud` claim). Should be set to the relay UID.
    ///
    /// Must be non-empty. An empty string is a configuration error.
    pub audience: String,
}

impl ScopeAuthConfig {
    /// Validate the configuration before building a hook.
    ///
    /// Returns `Err` if any required field is missing or invalid. C1 callers
    /// use this to produce a `DenyAllAuthHook` with an informative message
    /// rather than silently admitting sessions.
    pub fn validate(&self) -> Result<(), AuthError> {
        if self.keys.is_empty() {
            return Err(AuthError::Configuration(
                "ScopeAuthConfig.keys is empty — at least one public key is required".into(),
            ));
        }
        if self.audience.is_empty() {
            return Err(AuthError::Configuration(
                "ScopeAuthConfig.audience is empty — set it to the relay UID (server-derived)"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Whether the issuer list is unrestricted (empty = accept any issuer).
    ///
    /// An unrestricted issuer list is legal but unsafe in production.
    pub fn any_issuer(&self) -> bool {
        self.issuers.is_empty()
    }
}

/// Builds the per-scope [`AuthHook`] from an optional [`ScopeAuthConfig`].
///
/// # C1 fail-closed contract
///
/// The guarantee that every caller MUST preserve:
///
/// * `auth = None` → [`AllowAllAuthHook`] — scope has no policy, sessions
///   admitted freely. This is the only path that admits sessions in C1.
/// * `auth = Some(_)` → [`DenyAllAuthHook`] — **always fail closed** in C1,
///   regardless of whether the config is valid or not. No session is ever
///   admitted when a policy is configured but the `CatAuthHook` implementation
///   (C2) is absent.
///
/// This contract is tested by [`tests::auth_none_allows_auth_some_denies`].
///
/// # C2 change
///
/// C2 replaces the `Some(_) → DenyAll` arm with `CatAuthHook::new(cfg)`.
/// The `None → AllowAll` arm is unchanged.
pub struct ScopeAuthorizer;

impl ScopeAuthorizer {
    /// Return the hook for a scope given its optional auth config.
    ///
    /// Pass `ScopeConfig::auth.as_ref()` here.
    ///
    /// See [`ScopeAuthorizer`] for the exact fail-closed contract.
    pub fn resolve(auth: Option<&ScopeAuthConfig>) -> Arc<dyn AuthHook> {
        match auth {
            // No policy configured — admit sessions without enforcement.
            None => Arc::new(AllowAllAuthHook),

            // Policy configured but CatAuthHook (C2) not yet available.
            // Fail closed: deny every session with an explicit HookFault so
            // the reason is visible in logs and metrics. This prevents any
            // auth=Some(...) configuration from silently admitting sessions.
            Some(cfg) => {
                let message = match cfg.validate() {
                    Ok(()) => "CAT policy is configured but CatAuthHook is not implemented in C1; \
                         upgrade to a C2 release to enable enforcement"
                        .into(),
                    Err(e) => format!("invalid ScopeAuthConfig ({e}); upgrade to C2"),
                };
                DenyAllAuthHook::new(DenyReason::HookFault { message })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::types::DenyReason;
    use moq_transport::coding::{KeyValuePairs, TrackName, TrackNamespace};
    use moq_transport::setup::Setup;

    use crate::auth::hook::AuthHook;
    use crate::auth::types::{AuthRequest, AuthzOperation, Principal};

    fn dummy_setup() -> Setup {
        Setup {
            params: KeyValuePairs::default(),
        }
    }

    fn dummy_principal() -> Principal {
        Principal::new(None::<std::sync::Arc<str>>)
    }

    // =========================================================================
    // The core C1 security invariant.
    // =========================================================================

    /// **Security contract**: `auth=None` → AllowAll; `auth=Some(...)` → DenyAll.
    ///
    /// This test must never be removed or weakened. It is the machine-readable
    /// proof that auth=Some cannot accidentally admit sessions in C1.
    #[tokio::test]
    async fn auth_none_allows_auth_some_denies() {
        let setup = dummy_setup();

        // auth=None: all sessions admitted.
        let allow_hook = ScopeAuthorizer::resolve(None);
        let result = allow_hook.on_setup(&setup).await.unwrap();
        assert!(
            result.is_allowed(),
            "auth=None must admit sessions (AllowAllAuthHook)"
        );

        // auth=Some(valid): all sessions denied — fail closed.
        let valid_cfg = ScopeAuthConfig {
            keys: vec!["-----BEGIN PUBLIC KEY-----\nfake\n-----END PUBLIC KEY-----".into()],
            issuers: vec!["https://issuer.example".into()],
            audience: "relay-uid-hex".into(),
        };
        let deny_hook = ScopeAuthorizer::resolve(Some(&valid_cfg));
        let result = deny_hook.on_setup(&setup).await.unwrap();
        assert!(
            !result.is_allowed(),
            "auth=Some(valid_cfg) must deny sessions in C1 (fail closed)"
        );
        assert!(
            matches!(result.deny_reason(), Some(DenyReason::HookFault { .. })),
            "auth=Some(valid_cfg) must produce HookFault deny reason in C1"
        );

        // auth=Some(invalid): also denied, not panicked.
        let invalid_cfg = ScopeAuthConfig::default(); // empty keys + empty audience
        let deny_hook2 = ScopeAuthorizer::resolve(Some(&invalid_cfg));
        let result2 = deny_hook2.on_setup(&setup).await.unwrap();
        assert!(
            !result2.is_allowed(),
            "auth=Some(invalid_cfg) must also deny — invalid config cannot fail open"
        );

        // on_request with auth=Some also denies.
        let ns = TrackNamespace::from_utf8_path("sports");
        let track = TrackName::from("video");
        let principal = dummy_principal();
        let op = AuthzOperation::Fetch {
            namespace: &ns,
            track: &track,
        };
        let req = AuthRequest {
            principal: &principal,
            operation: &op,
            request_id: Some(1),
        };
        let req_result = deny_hook.on_request(&req).await.unwrap();
        assert!(
            !req_result.is_allowed(),
            "on_request with auth=Some must also deny"
        );
    }

    // =========================================================================
    // ScopeAuthConfig validation.
    // =========================================================================

    #[test]
    fn validate_rejects_empty_keys() {
        let cfg = ScopeAuthConfig {
            keys: vec![],
            issuers: vec![],
            audience: "relay-uid".into(),
        };
        assert!(cfg.validate().is_err(), "empty keys must be invalid");
    }

    #[test]
    fn validate_rejects_empty_audience() {
        let cfg = ScopeAuthConfig {
            keys: vec!["key".into()],
            issuers: vec![],
            audience: String::new(),
        };
        assert!(cfg.validate().is_err(), "empty audience must be invalid");
    }

    #[test]
    fn validate_accepts_empty_issuers() {
        // Empty issuers = any issuer accepted. Not ideal but not invalid.
        let cfg = ScopeAuthConfig {
            keys: vec!["key".into()],
            issuers: vec![],
            audience: "relay-uid".into(),
        };
        assert!(cfg.validate().is_ok());
        assert!(cfg.any_issuer());
    }

    #[test]
    fn validate_accepts_full_config() {
        let cfg = ScopeAuthConfig {
            keys: vec!["key".into()],
            issuers: vec!["https://issuer.example".into()],
            audience: "relay-uid".into(),
        };
        assert!(cfg.validate().is_ok());
        assert!(!cfg.any_issuer());
    }

    #[test]
    fn default_config_is_invalid() {
        // ScopeAuthConfig::default() must fail validation — it must never
        // accidentally pass as a working config.
        let cfg = ScopeAuthConfig::default();
        assert!(
            cfg.validate().is_err(),
            "default (all-empty) config must be invalid; C1 cannot silently accept it"
        );
    }
}
