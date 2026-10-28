// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Resolves a scope's authorization policy into a hook, and caches it.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use super::{AuthError, AuthHook, DenyAllAuthHook, UNSCOPED};
use crate::{Coordinator, ScopeAuthConfig};

/// Retry interval for an unusable configuration, or for a failed refresh that
/// reuses the cached state.
///
/// Short enough that fixing a misconfiguration takes effect promptly, long
/// enough that a broken scope under load does not turn into a retry storm
/// against the coordinator.
const BROKEN_CONFIG_TTL: Duration = Duration::from_secs(30);

/// Maximum age of an enforced policy, in TTLs from the last successful fetch:
/// one hour at the default TTL.
///
/// The cap trades availability for revocation. Without it, a key removed during
/// an outage, for example a compromised one, would keep admitting new sessions
/// until the coordinator came back. Token expiry does not bound that case,
/// because whoever holds the key can sign fresh tokens.
const MAX_STALE_TTLS: u32 = 12;

/// Upper bound on cached scopes.
///
/// The cache key is the resolved scope ID, and with the default
/// [`Coordinator::resolve_scope`] that is the client-chosen connection path, so
/// without a bound a peer could grow the cache by connecting with fresh paths.
/// When the cache is full, expired entries are pruned; if it is still full the
/// policy is resolved normally but not cached. That costs a coordinator call
/// for that connection but never changes the decision. An expired entry pruned
/// under cache pressure loses its fallback, so that scope fails closed if the
/// coordinator cannot be reached.
const MAX_CACHED_SCOPES: usize = 1024;

/// Builds and caches the authorization hook for each scope.
///
/// [`Coordinator::get_scope_config`] is the source of truth for a scope's
/// keys, but calling it per connection would put a coordinator round-trip and
/// a set of PEM parses on the accept path. Results are therefore cached for
/// [`ScopeAuthConfig::ttl`], which bounds how long a key rotation takes to
/// reach new sessions while the coordinator is reachable.
///
/// By default, during an outage, an enforced policy applies for at most 12 of
/// its TTLs from the last successful fetch. After that, new sessions are refused
/// until a refresh succeeds. Scopes without a policy can reuse their cached
/// state without a limit. A coordinator can turn reuse off with
/// [`Coordinator::reuse_scope_config_on_error`], so a failed refresh refuses new
/// sessions as soon as the cached entry expires.
pub(crate) struct ScopeAuthorizer {
    coordinator: Arc<dyn Coordinator>,
    reuse_scope_config_on_error: bool,
    cache: RwLock<HashMap<Option<String>, CacheEntry>>,
}

#[derive(Clone)]
struct CacheEntry {
    state: CacheState,
    expires_at: Instant,
    last_success_at: Instant,
    // The policy's effective TTL. Kept separately because a failed refresh
    // moves expires_at by the retry interval instead.
    policy_ttl: Duration,
}

#[derive(Clone)]
enum CacheState {
    /// The scope requires token authorization.
    Enforced(Arc<dyn AuthHook>),
    /// The scope did not opt in; sessions skip authorization entirely.
    Disabled,
}

impl ScopeAuthorizer {
    pub(crate) fn new(coordinator: Arc<dyn Coordinator>) -> Self {
        let reuse_scope_config_on_error = coordinator.reuse_scope_config_on_error();
        Self {
            coordinator,
            reuse_scope_config_on_error,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// The hook for a scope, or `None` if the scope does not require
    /// authorization.
    ///
    /// Returns `Err` when a fetch fails and either no cached entry exists or
    /// the coordinator has turned reuse off with
    /// [`Coordinator::reuse_scope_config_on_error`]. The caller must fail closed
    /// because the scope's current token requirements cannot be determined.
    pub(crate) async fn resolve(
        &self,
        scope: Option<&str>,
    ) -> Result<Option<Arc<dyn AuthHook>>, AuthError> {
        if let Some(entry) = self.cached(scope) {
            return Ok(entry.state.into_hook());
        }

        let config = match self.coordinator.get_scope_config(scope).await {
            Ok(config) => config,
            Err(err) if !self.reuse_scope_config_on_error => {
                return Err(AuthError::Backend(format!(
                    "get_scope_config failed: {err}"
                )));
            }
            Err(err) => {
                let mut cache = self.cache.write().unwrap_or_else(|err| err.into_inner());
                let Some(entry) = cache.get_mut(&scope.map(str::to_string)) else {
                    return Err(AuthError::Backend(format!(
                        "get_scope_config failed: {err}"
                    )));
                };

                // A concurrent refresh may have renewed this entry. Extend and
                // report only if it is still expired, once per retry interval.
                let now = Instant::now();
                if entry.expires_at <= now {
                    let enforced = matches!(entry.state, CacheState::Enforced(_));
                    // None when the cap is beyond what Instant can represent.
                    let reuse_until = entry
                        .last_success_at
                        .checked_add(entry.policy_ttl.saturating_mul(MAX_STALE_TTLS));
                    let retry_at = now + BROKEN_CONFIG_TTL;
                    if enforced && reuse_until.is_some_and(|until| now >= until) {
                        entry.expires_at = retry_at;
                        entry.state = CacheState::Enforced(Arc::new(DenyAllAuthHook::new(
                            "scope authorization policy is too old",
                        )));
                        tracing::error!(
                            scope = scope.unwrap_or(UNSCOPED),
                            error = %err,
                            "could not refresh the scope's authorization policy; reuse cap reached, \
                             refusing new sessions in this scope"
                        );
                    } else {
                        // Cache hits do not check the cap, so the retry
                        // interval must not carry the policy past it.
                        entry.expires_at = match reuse_until {
                            Some(until) if enforced => retry_at.min(until),
                            _ => retry_at,
                        };
                        tracing::warn!(
                            scope = scope.unwrap_or(UNSCOPED),
                            error = %err,
                            "could not refresh the scope's authorization policy; retaining the cached state"
                        );
                    }
                    metrics::counter!("moq_relay_auth_errors_total", "stage" => "scope_config")
                        .increment(1);
                }
                return Ok(entry.state.clone().into_hook());
            }
        };

        let policy_ttl = config.auth.as_ref().map_or(
            super::DEFAULT_SCOPE_AUTH_TTL,
            ScopeAuthConfig::effective_ttl,
        );
        let (state, ttl) = match config.auth {
            None => {
                // Logged once per TTL rather than per connection. "No policy"
                // is a legitimate configuration, but it is indistinguishable
                // from a coordinator that forgot to implement
                // `get_scope_config`, so it should be visible in logs rather
                // than inferred from the absence of anything.
                tracing::info!(
                    scope = scope.unwrap_or(UNSCOPED),
                    "scope has no token authorization policy; admitting sessions on \
                     scope permissions alone"
                );
                (CacheState::Disabled, super::DEFAULT_SCOPE_AUTH_TTL)
            }
            Some(auth) => {
                match build_hook(&auth, scope) {
                    Ok(hook) => (CacheState::Enforced(hook), policy_ttl),
                    // A scope that asked for authorization but cannot have it
                    // must refuse every session, not run unauthenticated.
                    Err(err) => {
                        tracing::error!(
                            scope = scope.unwrap_or(UNSCOPED),
                            error = %err,
                            "scope requires token authorization but its configuration is unusable; \
                             refusing all sessions in this scope"
                        );
                        metrics::counter!(
                            "moq_relay_auth_errors_total",
                            "stage" => "scope_config"
                        )
                        .increment(1);
                        (
                            CacheState::Enforced(Arc::new(DenyAllAuthHook::new(err.to_string()))),
                            BROKEN_CONFIG_TTL,
                        )
                    }
                }
            }
        };

        self.store(scope, state.clone(), ttl, policy_ttl);

        Ok(state.into_hook())
    }

    fn cached(&self, scope: Option<&str>) -> Option<CacheEntry> {
        let cache = self.cache.read().unwrap_or_else(|err| err.into_inner());
        // `Option<&str>` cannot look up an `Option<String>` key directly, so
        // the key is allocated. A hashed lookup keeps this O(1) however many
        // scopes are cached, which a linear scan would not.
        cache
            .get(&scope.map(str::to_string))
            .filter(|entry| entry.expires_at > Instant::now())
            .cloned()
    }

    fn store(&self, scope: Option<&str>, state: CacheState, ttl: Duration, policy_ttl: Duration) {
        let mut cache = self.cache.write().unwrap_or_else(|err| err.into_inner());
        let key = scope.map(str::to_string);
        if cache.len() >= MAX_CACHED_SCOPES && !cache.contains_key(&key) {
            let now = Instant::now();
            cache.retain(|_, entry| entry.expires_at > now);
            if cache.len() >= MAX_CACHED_SCOPES {
                // Still full of live entries: resolve without caching rather
                // than evict a scope that is in use.
                return;
            }
        }
        let now = Instant::now();
        cache.insert(
            key,
            CacheEntry {
                state,
                expires_at: now + ttl,
                last_success_at: now,
                policy_ttl,
            },
        );
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.cache
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .len()
    }

    /// Drop a scope's cached policy, forcing the next session to re-fetch it.
    #[cfg(test)]
    pub(crate) fn invalidate(&self, scope: Option<&str>) {
        let mut cache = self.cache.write().unwrap_or_else(|err| err.into_inner());
        cache.retain(|key, _| key.as_deref() != scope);
    }
}

impl CacheState {
    fn into_hook(self) -> Option<Arc<dyn AuthHook>> {
        match self {
            CacheState::Enforced(hook) => Some(hook),
            CacheState::Disabled => None,
        }
    }
}

/// Build the hook a scope's configuration calls for.
///
/// Separated from [`ScopeAuthorizer::resolve`] so that the feature boundary is
/// a single function rather than a `cfg` scattered through the cache logic.
fn build_hook(
    config: &ScopeAuthConfig,
    scope: Option<&str>,
) -> Result<Arc<dyn AuthHook>, AuthError> {
    #[cfg(feature = "auth-cat")]
    {
        Ok(Arc::new(super::CatAuthHook::new(config, scope)?))
    }

    #[cfg(not(feature = "auth-cat"))]
    {
        // Silence unused-parameter warnings without changing the signature.
        let _ = (config, scope);
        Err(AuthError::Configuration(
            "relay was built without the `auth-cat` feature, so it cannot verify \
             the tokens this scope requires"
                .to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use moq_native_ietf::Client;
    use moq_transport::coding::TrackNamespace;

    use crate::{
        AuthPublicKey, CoordinatorContext, CoordinatorError, CoordinatorResult, NamespaceOrigin,
        NamespaceRegistration, ScopeConfig,
    };

    /// A coordinator that returns a scripted config and counts calls, so the
    /// tests can assert on caching rather than on wall-clock behaviour.
    struct ScriptedCoordinator {
        config: std::sync::Mutex<ScopeConfig>,
        calls: AtomicUsize,
        fail: std::sync::atomic::AtomicBool,
    }

    impl ScriptedCoordinator {
        fn new(config: ScopeConfig) -> Self {
            Self {
                config: std::sync::Mutex::new(config),
                calls: AtomicUsize::new(0),
                fail: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn set_config(&self, config: ScopeConfig) {
            *self.config.lock().unwrap() = config;
        }

        fn set_failing(&self, failing: bool) {
            self.fail.store(failing, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Coordinator for ScriptedCoordinator {
        async fn register_namespace(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
            _context: &CoordinatorContext,
        ) -> CoordinatorResult<NamespaceRegistration> {
            Ok(NamespaceRegistration::new(()))
        }

        async fn unregister_namespace(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
        ) -> CoordinatorResult<()> {
            Ok(())
        }

        async fn lookup(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
        ) -> CoordinatorResult<(NamespaceOrigin, Option<Client>)> {
            Err(CoordinatorError::NamespaceNotFound)
        }

        async fn get_scope_config(&self, _scope: Option<&str>) -> CoordinatorResult<ScopeConfig> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                // Let concurrent resolves reach the coordinator before failing.
                tokio::task::yield_now().await;
                return Err(CoordinatorError::Other(anyhow::anyhow!("backend down")));
            }
            Ok(self.config.lock().unwrap().clone())
        }
    }

    struct NonReusingCoordinator(Arc<ScriptedCoordinator>);

    #[async_trait]
    impl Coordinator for NonReusingCoordinator {
        async fn register_namespace(
            &self,
            scope: Option<&str>,
            namespace: &TrackNamespace,
            context: &CoordinatorContext,
        ) -> CoordinatorResult<NamespaceRegistration> {
            self.0.register_namespace(scope, namespace, context).await
        }

        async fn unregister_namespace(
            &self,
            scope: Option<&str>,
            namespace: &TrackNamespace,
        ) -> CoordinatorResult<()> {
            self.0.unregister_namespace(scope, namespace).await
        }

        async fn lookup(
            &self,
            scope: Option<&str>,
            namespace: &TrackNamespace,
        ) -> CoordinatorResult<(NamespaceOrigin, Option<Client>)> {
            self.0.lookup(scope, namespace).await
        }

        async fn get_scope_config(&self, scope: Option<&str>) -> CoordinatorResult<ScopeConfig> {
            self.0.get_scope_config(scope).await
        }

        fn reuse_scope_config_on_error(&self) -> bool {
            false
        }
    }

    /// A syntactically valid ES256 public key, so configuration parses.
    fn valid_pem() -> String {
        use p256::ecdsa::SigningKey;
        use p256::pkcs8::EncodePublicKey;

        let signing = SigningKey::random(&mut rand_core_compat());
        p256::ecdsa::VerifyingKey::from(&signing)
            .to_public_key_pem(Default::default())
            .expect("pem")
    }

    /// `p256` re-exports the RNG its own API expects; using it avoids pinning a
    /// separate `rand` version in dev-dependencies.
    fn rand_core_compat() -> impl p256::elliptic_curve::rand_core::CryptoRngCore {
        p256::elliptic_curve::rand_core::OsRng
    }

    fn enforced_config() -> ScopeConfig {
        ScopeConfig {
            auth: Some(ScopeAuthConfig::new(vec![
                AuthPublicKey::es256(valid_pem()),
            ])),
            ..Default::default()
        }
    }

    #[cfg(feature = "auth-cat")]
    fn enforced_config_and_setup_token(ttl: Duration) -> (ScopeConfig, crate::auth::AuthToken) {
        use cat_token::{encode_token, CatToken, Es256Algorithm, MoqtAction, MoqtScopeBuilder};
        use p256::pkcs8::EncodePublicKey;

        use crate::auth::{AuthToken, CAT_TOKEN_TYPE};

        let signer = Es256Algorithm::new_with_key_pair().unwrap();
        let pem = signer
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        let config = ScopeConfig {
            auth: Some(ScopeAuthConfig::new(vec![AuthPublicKey::es256(pem)]).with_ttl(ttl)),
            ..Default::default()
        };
        let token = AuthToken::new(
            CAT_TOKEN_TYPE,
            encode_token(
                &CatToken::new().with_expires_in(3600).with_moqt_scope(
                    MoqtScopeBuilder::new()
                        .action(MoqtAction::ClientSetup)
                        .build(),
                ),
                &signer,
            )
            .unwrap(),
        );
        (config, token)
    }

    // The cache uses std::time::Instant, which a paused tokio clock cannot
    // advance. Age entries directly to exercise expiry without real waits.
    fn advance_cache_age(authorizer: &ScopeAuthorizer, scope: Option<&str>, elapsed: Duration) {
        let mut cache = authorizer.cache.write().unwrap();
        let entry = cache.get_mut(&scope.map(str::to_string)).unwrap();
        entry.expires_at -= elapsed;
        entry.last_success_at -= elapsed;
    }

    #[tokio::test]
    async fn a_scope_without_auth_config_needs_no_hook() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig::default()));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());

        assert!(authorizer.resolve(Some("open")).await.unwrap().is_none());
        assert_eq!(coordinator.calls(), 1);
    }

    #[tokio::test]
    async fn results_are_cached_per_scope() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig::default()));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());

        for _ in 0..5 {
            authorizer.resolve(Some("tenant")).await.unwrap();
        }
        assert_eq!(coordinator.calls(), 1, "repeat resolves must hit the cache");

        // A different scope is a separate cache entry.
        authorizer.resolve(Some("other")).await.unwrap();
        assert_eq!(coordinator.calls(), 2);

        // The unscoped session is its own entry, distinct from any named scope.
        authorizer.resolve(None).await.unwrap();
        assert_eq!(coordinator.calls(), 3);
        authorizer.resolve(None).await.unwrap();
        assert_eq!(coordinator.calls(), 3);
    }

    /// An unusable policy is cached too, on its own shorter TTL, so a broken
    /// scope under load does not turn into a retry storm against the
    /// coordinator. It must still deny throughout.
    #[tokio::test]
    async fn a_broken_config_is_cached_and_keeps_denying() {
        // Unbuildable in both configurations: with the feature because the key
        // list is empty, without it because the scheme is unavailable.
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig {
            auth: Some(
                ScopeAuthConfig::new(vec![])
                    // Ignored in favour of BROKEN_CONFIG_TTL.
                    .with_ttl(Duration::from_nanos(1)),
            ),
            ..Default::default()
        }));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());
        let session = crate::SessionContext::public(Some("tenant".to_string()));

        for _ in 0..3 {
            let hook = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
            assert!(
                !hook.on_setup(&session, &[]).await.unwrap().is_allowed(),
                "a broken scope must deny on every resolve"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        assert_eq!(
            coordinator.calls(),
            1,
            "a broken config must be cached, not refetched per connection"
        );
    }

    #[tokio::test]
    async fn expired_scope_config_fails_closed_when_reuse_is_off() {
        for config in [ScopeConfig::default(), enforced_config()] {
            let requires_auth = config.auth.is_some();
            let coordinator = Arc::new(ScriptedCoordinator::new(config));
            let authorizer =
                ScopeAuthorizer::new(Arc::new(NonReusingCoordinator(coordinator.clone())));
            assert_eq!(
                authorizer.resolve(Some("tenant")).await.unwrap().is_some(),
                requires_auth,
            );

            coordinator.set_failing(true);
            assert_eq!(
                authorizer.resolve(Some("tenant")).await.unwrap().is_some(),
                requires_auth,
                "a backend failure must not affect an entry within its TTL",
            );
            assert_eq!(coordinator.calls(), 1);

            advance_cache_age(&authorizer, Some("tenant"), crate::DEFAULT_SCOPE_AUTH_TTL);
            for expected_calls in 2..=3 {
                assert!(
                    matches!(
                        authorizer.resolve(Some("tenant")).await,
                        Err(AuthError::Backend(_))
                    ),
                    "a failed refresh must surface, requires_auth={requires_auth}",
                );
                assert_eq!(coordinator.calls(), expected_calls, "retry on every lookup");
            }
            assert!(matches!(
                authorizer.resolve(Some("cold")).await,
                Err(AuthError::Backend(_))
            ));
            assert_eq!(coordinator.calls(), 4);

            coordinator.set_config(if requires_auth {
                ScopeConfig::default()
            } else {
                enforced_config()
            });
            coordinator.set_failing(false);
            for _ in 0..2 {
                assert_eq!(
                    authorizer.resolve(Some("tenant")).await.unwrap().is_some(),
                    !requires_auth,
                    "a successful refresh must replace the expired state immediately",
                );
            }
            assert_eq!(
                coordinator.calls(),
                5,
                "cache the successfully fetched state"
            );
        }
    }

    #[tokio::test]
    async fn a_disabled_scope_past_twelve_ttls_survives_a_coordinator_failure() {
        // No override in ScriptedCoordinator: this guards the trait's default.
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig::default()));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());
        assert!(authorizer.resolve(Some("open")).await.unwrap().is_none());

        advance_cache_age(
            &authorizer,
            Some("open"),
            crate::DEFAULT_SCOPE_AUTH_TTL * 12 + Duration::from_secs(1),
        );
        coordinator.set_failing(true);

        for _ in 0..3 {
            assert!(authorizer.resolve(Some("open")).await.unwrap().is_none());
        }
        assert_eq!(
            coordinator.calls(),
            2,
            "retry must be cached between sessions"
        );
    }

    #[cfg(feature = "auth-cat")]
    #[tokio::test]
    async fn an_enforced_scope_is_reused_until_the_cap_after_a_coordinator_failure() {
        use crate::auth::DenyReason;

        let ttl = Duration::from_secs(60);
        let (config, token) = enforced_config_and_setup_token(ttl);
        let coordinator = Arc::new(ScriptedCoordinator::new(config));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());
        let original = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
        let session = crate::SessionContext::public(Some("tenant".to_string()));
        assert!(original
            .on_setup(&session, std::slice::from_ref(&token))
            .await
            .unwrap()
            .is_allowed());

        advance_cache_age(
            &authorizer,
            Some("tenant"),
            ttl * 12 - Duration::from_secs(1),
        );
        coordinator.set_failing(true);

        for _ in 0..3 {
            let hook = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
            assert!(Arc::ptr_eq(&hook, &original), "reuse the last fetched hook");
            assert!(matches!(
                hook.on_setup(&session, &[]).await.unwrap().deny_reason(),
                Some(DenyReason::TokenMissing)
            ));
            assert!(hook
                .on_setup(&session, std::slice::from_ref(&token))
                .await
                .unwrap()
                .is_allowed());
        }
        assert_eq!(
            coordinator.calls(),
            2,
            "retry must be cached between sessions"
        );

        // The retry interval must not carry the policy past the cap.
        advance_cache_age(&authorizer, Some("tenant"), Duration::from_secs(2));
        for _ in 0..3 {
            let hook = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
            assert!(matches!(
                hook.on_setup(&session, std::slice::from_ref(&token))
                    .await
                    .unwrap()
                    .deny_reason(),
                Some(DenyReason::PolicyDenied { .. })
            ));
        }
        assert_eq!(coordinator.calls(), 3, "one retry at the cap");
    }

    #[cfg(feature = "auth-cat")]
    #[tokio::test]
    async fn an_enforced_scope_past_the_reuse_cap_denies_until_refresh_succeeds() {
        use crate::auth::DenyReason;

        let ttl = Duration::from_secs(60);
        let (config, token) = enforced_config_and_setup_token(ttl);
        let coordinator = Arc::new(ScriptedCoordinator::new(config));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());
        let original = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
        let session = crate::SessionContext::public(Some("tenant".to_string()));
        assert!(original
            .on_setup(&session, std::slice::from_ref(&token))
            .await
            .unwrap()
            .is_allowed());

        advance_cache_age(&authorizer, Some("tenant"), ttl * 11);
        coordinator.set_failing(true);
        let reused = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
        assert!(reused
            .on_setup(&session, std::slice::from_ref(&token))
            .await
            .unwrap()
            .is_allowed());
        assert_eq!(coordinator.calls(), 2);

        // Failed refreshes must not reset the age measured from a successful fetch.
        advance_cache_age(&authorizer, Some("tenant"), ttl);
        for _ in 0..3 {
            let hook = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
            let decision = hook
                .on_setup(&session, std::slice::from_ref(&token))
                .await
                .unwrap();
            assert!(matches!(
                decision.deny_reason(),
                Some(DenyReason::PolicyDenied { .. })
            ));
        }
        assert_eq!(coordinator.calls(), 3, "cache the denial between retries");

        advance_cache_age(&authorizer, Some("tenant"), BROKEN_CONFIG_TTL);
        let denied = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
        assert!(!denied
            .on_setup(&session, std::slice::from_ref(&token))
            .await
            .unwrap()
            .is_allowed());
        assert_eq!(coordinator.calls(), 4, "retry while past the cap");

        // Admitted sessions hold their original hook even after the reuse cap.
        assert!(original
            .on_setup(&session, std::slice::from_ref(&token))
            .await
            .unwrap()
            .is_allowed());

        coordinator.set_failing(false);
        let hook = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
        assert!(!hook
            .on_setup(&session, std::slice::from_ref(&token))
            .await
            .unwrap()
            .is_allowed());
        assert_eq!(coordinator.calls(), 4, "wait for the retry interval");

        advance_cache_age(&authorizer, Some("tenant"), BROKEN_CONFIG_TTL);
        for _ in 0..3 {
            let recovered = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
            assert!(recovered
                .on_setup(&session, std::slice::from_ref(&token))
                .await
                .unwrap()
                .is_allowed());
        }
        assert_eq!(
            coordinator.calls(),
            5,
            "cache the successfully fetched policy"
        );

        advance_cache_age(&authorizer, Some("tenant"), ttl);
        coordinator.set_failing(true);
        let hook = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
        assert!(hook
            .on_setup(&session, std::slice::from_ref(&token))
            .await
            .unwrap()
            .is_allowed());
        assert_eq!(
            coordinator.calls(),
            6,
            "successful refresh resets policy age"
        );
    }

    #[tokio::test]
    async fn a_failed_refresh_does_not_overwrite_a_concurrent_success() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig {
            auth: Some(ScopeAuthConfig::new(vec![])),
            ..Default::default()
        }));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());
        assert!(authorizer.resolve(Some("tenant")).await.unwrap().is_some());
        advance_cache_age(
            &authorizer,
            Some("tenant"),
            crate::DEFAULT_SCOPE_AUTH_TTL * 12,
        );
        coordinator.set_failing(true);

        let (failed_refresh, successful_refresh) =
            tokio::join!(authorizer.resolve(Some("tenant")), async {
                coordinator.set_config(ScopeConfig::default());
                coordinator.set_failing(false);
                authorizer.resolve(Some("tenant")).await
            },);
        assert!(successful_refresh.unwrap().is_none());
        assert!(failed_refresh.unwrap().is_none());
        assert!(authorizer.resolve(Some("tenant")).await.unwrap().is_none());
        assert_eq!(coordinator.calls(), 3, "both refreshes must overlap");
    }

    #[tokio::test]
    async fn an_expired_deny_all_scope_survives_a_coordinator_failure() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig {
            auth: Some(ScopeAuthConfig::new(vec![])),
            ..Default::default()
        }));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());
        let original = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
        let session = crate::SessionContext::public(Some("tenant".to_string()));

        advance_cache_age(&authorizer, Some("tenant"), BROKEN_CONFIG_TTL);
        coordinator.set_failing(true);

        for _ in 0..3 {
            let hook = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
            assert!(
                Arc::ptr_eq(&hook, &original),
                "reuse even an unusable policy's hook"
            );
            assert!(!hook.on_setup(&session, &[]).await.unwrap().is_allowed());
        }
        assert_eq!(
            coordinator.calls(),
            2,
            "retry must be cached between sessions"
        );
    }

    #[tokio::test]
    async fn a_recovered_coordinator_replaces_the_policy_after_the_retry_interval() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig::default()));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());
        assert!(authorizer.resolve(Some("tenant")).await.unwrap().is_none());

        advance_cache_age(&authorizer, Some("tenant"), crate::DEFAULT_SCOPE_AUTH_TTL);
        coordinator.set_failing(true);
        assert!(authorizer.resolve(Some("tenant")).await.unwrap().is_none());
        assert_eq!(coordinator.calls(), 2);

        coordinator.set_config(ScopeConfig {
            auth: Some(ScopeAuthConfig::new(vec![])),
            ..Default::default()
        });
        coordinator.set_failing(false);
        assert!(authorizer.resolve(Some("tenant")).await.unwrap().is_none());
        assert_eq!(
            coordinator.calls(),
            2,
            "wait for the retry interval before refetching"
        );

        advance_cache_age(&authorizer, Some("tenant"), BROKEN_CONFIG_TTL);
        let hook = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
        let session = crate::SessionContext::public(Some("tenant".to_string()));
        assert!(!hook.on_setup(&session, &[]).await.unwrap().is_allowed());
        assert_eq!(coordinator.calls(), 3);
        assert!(authorizer.resolve(Some("tenant")).await.unwrap().is_some());
        assert_eq!(coordinator.calls(), 3, "cache the recovered policy");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_refresh_failures_are_counted_once_per_retry_interval() {
        use std::sync::atomic::AtomicU64;

        use metrics::{
            Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
        };

        struct CountScopeErrors(Arc<AtomicU64>);
        impl Recorder for CountScopeErrors {
            fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

            fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
                assert_eq!(key.name(), "moq_relay_auth_errors_total");
                assert_eq!(
                    key.labels().collect::<Vec<_>>(),
                    vec![&metrics::Label::new("stage", "scope_config")]
                );
                Counter::from_arc(self.0.clone())
            }

            fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
                Gauge::noop()
            }

            fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
                Histogram::noop()
            }
        }

        let errors = Arc::new(AtomicU64::new(0));
        let recorder = CountScopeErrors(errors.clone());
        // The local recorder is thread-bound, so this test uses one runtime thread.
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig::default()));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());
        assert!(authorizer.resolve(Some("tenant")).await.unwrap().is_none());
        advance_cache_age(&authorizer, Some("tenant"), crate::DEFAULT_SCOPE_AUTH_TTL);
        coordinator.set_failing(true);

        let (first, second) = tokio::join!(
            authorizer.resolve(Some("tenant")),
            authorizer.resolve(Some("tenant")),
        );
        assert!(first.unwrap().is_none());
        assert!(second.unwrap().is_none());
        assert_eq!(coordinator.calls(), 3, "both refreshes must overlap");
        assert!(authorizer.resolve(Some("tenant")).await.unwrap().is_none());
        assert_eq!(coordinator.calls(), 3);
        assert_eq!(errors.load(Ordering::SeqCst), 1);

        advance_cache_age(&authorizer, Some("tenant"), BROKEN_CONFIG_TTL);
        assert!(authorizer.resolve(Some("tenant")).await.unwrap().is_none());
        assert_eq!(coordinator.calls(), 4);
        assert_eq!(errors.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_cold_scope_coordinator_failure_fails_closed() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig::default()));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());
        assert!(authorizer.resolve(Some("open")).await.unwrap().is_none());
        coordinator.set_failing(true);

        // Another scope's cached policy cannot establish this scope's policy.
        let err = authorizer
            .resolve(Some("tenant"))
            .await
            .err()
            .expect("a backend failure must surface");
        assert!(matches!(err, AuthError::Backend(_)), "{err:?}");
        assert_eq!(coordinator.calls(), 2);
    }

    #[tokio::test]
    async fn a_failure_is_not_cached_as_permissive() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig::default()));
        coordinator.set_failing(true);
        let authorizer = ScopeAuthorizer::new(coordinator.clone());

        assert!(authorizer.resolve(Some("tenant")).await.is_err());

        // Once the backend recovers the scope resolves normally: the failure
        // left no poisoned entry behind.
        coordinator.set_failing(false);
        assert!(authorizer.resolve(Some("tenant")).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn an_unusable_config_denies_rather_than_disables() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig {
            // Enabled but with no keys: unusable.
            auth: Some(ScopeAuthConfig::new(vec![])),
            ..Default::default()
        }));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());

        let hook = authorizer
            .resolve(Some("tenant"))
            .await
            .unwrap()
            .expect("a broken scope must still get a hook, not be waved through");

        let session = crate::SessionContext::public(Some("tenant".to_string()));
        assert!(!hook.on_setup(&session, &[]).await.unwrap().is_allowed());
    }

    #[tokio::test]
    async fn too_many_keys_denies_rather_than_disables() {
        let keys = (0..crate::MAX_SCOPE_AUTH_KEYS + 1)
            .map(|_| AuthPublicKey::es256(valid_pem()))
            .collect();
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig {
            auth: Some(ScopeAuthConfig::new(keys)),
            ..Default::default()
        }));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());

        let hook = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
        let session = crate::SessionContext::public(Some("tenant".to_string()));
        assert!(!hook.on_setup(&session, &[]).await.unwrap().is_allowed());
    }

    /// Without the feature, a scope that requires tokens cannot be served, so
    /// it must refuse sessions rather than run unauthenticated.
    #[cfg(not(feature = "auth-cat"))]
    #[tokio::test]
    async fn a_scope_requiring_tokens_is_refused_without_the_feature() {
        let coordinator = Arc::new(ScriptedCoordinator::new(enforced_config()));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());

        let hook = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
        let session = crate::SessionContext::public(Some("tenant".to_string()));
        let decision = hook.on_setup(&session, &[]).await.unwrap();

        assert!(!decision.is_allowed());
        assert!(
            decision
                .deny_reason()
                .unwrap()
                .to_string()
                .contains("auth-cat"),
            "the denial should name the missing feature"
        );

        // The request path must deny too. A hook that refused setup but waved
        // requests through would be no protection at all for any session that
        // reached the request stage by another route.
        let namespace = TrackNamespace::from_utf8_path("sports/football");
        let track = moq_transport::coding::TrackName::from("video");
        let auth = crate::auth::SessionAuth::new(hook, crate::auth::Principal::anonymous());

        for operation in [
            crate::auth::AuthzOperation::PublishNamespace {
                namespace: &namespace,
            },
            crate::auth::AuthzOperation::Publish {
                namespace: &namespace,
                track: &track,
            },
            crate::auth::AuthzOperation::Subscribe {
                namespace: &namespace,
                track: &track,
            },
        ] {
            let label = operation.label();
            assert!(
                crate::auth::authorize(Some(&auth), &session, operation, None)
                    .await
                    .is_err(),
                "{label} must be denied without the feature"
            );
        }
    }

    #[tokio::test]
    async fn concurrent_resolves_are_consistent() {
        // The cache is shared across connection tasks; this exercises the lock
        // discipline under contention rather than asserting a call count, since
        // concurrent misses may legitimately each fetch.
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig::default()));
        let authorizer = Arc::new(ScopeAuthorizer::new(coordinator.clone()));

        let mut tasks = Vec::new();
        for _ in 0..32 {
            let authorizer = authorizer.clone();
            tasks.push(tokio::spawn(async move {
                authorizer.resolve(Some("tenant")).await.unwrap().is_none()
            }));
        }

        for task in tasks {
            assert!(task.await.unwrap(), "every resolve must agree");
        }
    }

    #[tokio::test]
    async fn invalidate_forces_a_refetch() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig::default()));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());

        authorizer.resolve(Some("tenant")).await.unwrap();
        authorizer.resolve(Some("tenant")).await.unwrap();
        assert_eq!(coordinator.calls(), 1);

        authorizer.invalidate(Some("tenant"));
        authorizer.resolve(Some("tenant")).await.unwrap();
        assert_eq!(coordinator.calls(), 2);
    }

    /// The key is peer-influenced, so the cache must not grow without bound,
    /// and a full cache must still produce correct decisions.
    #[tokio::test]
    async fn the_cache_is_bounded_and_still_decides_when_full() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig::default()));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());

        for i in 0..MAX_CACHED_SCOPES + 10 {
            let scope = format!("/path-{i}");
            assert!(authorizer.resolve(Some(&scope)).await.unwrap().is_none());
        }
        assert_eq!(authorizer.len(), MAX_CACHED_SCOPES);

        // An uncached scope is resolved on every call, not wrongly answered.
        let calls = coordinator.calls();
        authorizer.resolve(Some("/overflow")).await.unwrap();
        authorizer.resolve(Some("/overflow")).await.unwrap();
        assert_eq!(coordinator.calls(), calls + 2);

        // A cached scope still hits.
        let calls = coordinator.calls();
        authorizer.resolve(Some("/path-0")).await.unwrap();
        assert_eq!(coordinator.calls(), calls);
    }

    #[tokio::test]
    async fn failed_refreshes_respect_the_cache_cap_and_pruned_scopes_fail_closed() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig::default()));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());
        for i in 0..MAX_CACHED_SCOPES {
            let scope = format!("/path-{i}");
            assert!(authorizer.resolve(Some(&scope)).await.unwrap().is_none());
        }
        advance_cache_age(&authorizer, Some("/path-0"), crate::DEFAULT_SCOPE_AUTH_TTL);
        advance_cache_age(&authorizer, Some("/path-1"), crate::DEFAULT_SCOPE_AUTH_TTL);
        coordinator.set_failing(true);

        // Reuse an entry that is present even when the cache is full.
        let calls = coordinator.calls();
        for _ in 0..2 {
            assert!(authorizer.resolve(Some("/path-0")).await.unwrap().is_none());
        }
        assert_eq!(coordinator.calls(), calls + 1, "cache the retry interval");
        assert_eq!(authorizer.len(), MAX_CACHED_SCOPES);

        // A failed fetch for an uncached scope cannot insert or prune entries.
        let calls = coordinator.calls();
        for _ in 0..2 {
            assert!(matches!(
                authorizer.resolve(Some("/overflow")).await,
                Err(AuthError::Backend(_))
            ));
            assert_eq!(authorizer.len(), MAX_CACHED_SCOPES);
        }
        assert_eq!(coordinator.calls(), calls + 2);

        // A successful fetch under pressure prunes the other expired entry.
        coordinator.set_failing(false);
        assert!(authorizer
            .resolve(Some("/overflow"))
            .await
            .unwrap()
            .is_none());
        assert_eq!(authorizer.len(), MAX_CACHED_SCOPES);
        coordinator.set_failing(true);
        let calls = coordinator.calls();
        assert!(matches!(
            authorizer.resolve(Some("/path-1")).await,
            Err(AuthError::Backend(_))
        ));
        assert_eq!(
            coordinator.calls(),
            calls + 1,
            "pruning removes the fallback"
        );
        assert_eq!(authorizer.len(), MAX_CACHED_SCOPES);
        assert!(authorizer.resolve(Some("/path-0")).await.unwrap().is_none());
        assert_eq!(
            coordinator.calls(),
            calls + 1,
            "the reused entry was retained"
        );
    }

    /// A full cache whose entries have expired makes room instead of refusing
    /// to cache.
    #[tokio::test]
    async fn expired_entries_are_pruned_when_the_cache_is_full() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig::default()));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());

        for i in 0..MAX_CACHED_SCOPES {
            authorizer.store(
                Some(&format!("/stale-{i}")),
                CacheState::Disabled,
                Duration::from_nanos(1),
                crate::DEFAULT_SCOPE_AUTH_TTL,
            );
        }
        tokio::time::sleep(Duration::from_millis(2)).await;

        authorizer.resolve(Some("/fresh")).await.unwrap();
        assert_eq!(authorizer.len(), 1, "expired entries must be pruned");

        let calls = coordinator.calls();
        authorizer.resolve(Some("/fresh")).await.unwrap();
        assert_eq!(coordinator.calls(), calls, "the new entry must be cached");
    }

    /// A usable policy is cached for the TTL the coordinator asked for, so an
    /// already-elapsed TTL forces a refetch. Needs the feature, since without
    /// it the policy is unbuildable and takes the broken-config path below.
    #[cfg(feature = "auth-cat")]
    #[tokio::test]
    async fn an_expired_entry_is_refetched() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig {
            auth: Some(
                ScopeAuthConfig::new(vec![AuthPublicKey::es256(valid_pem())])
                    // Already elapsed by the time the next resolve runs.
                    .with_ttl(Duration::from_nanos(1)),
            ),
            ..Default::default()
        }));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());

        authorizer.resolve(Some("tenant")).await.ok();
        tokio::time::sleep(Duration::from_millis(5)).await;
        authorizer.resolve(Some("tenant")).await.ok();

        assert_eq!(coordinator.calls(), 2, "an expired entry must be refetched");
    }

    #[cfg(feature = "auth-cat")]
    #[tokio::test]
    async fn a_well_formed_scope_gets_a_working_hook() {
        let coordinator = Arc::new(ScriptedCoordinator::new(enforced_config()));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());

        let hook = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
        let session = crate::SessionContext::public(Some("tenant".to_string()));

        // No token presented, so denied - but by the CAT hook, on the merits.
        let decision = hook.on_setup(&session, &[]).await.unwrap();
        assert!(matches!(
            decision.deny_reason(),
            Some(crate::auth::DenyReason::TokenMissing)
        ));
    }

    #[cfg(feature = "auth-cat")]
    #[tokio::test]
    async fn rotating_keys_reaches_new_sessions_after_the_ttl() {
        let coordinator = Arc::new(ScriptedCoordinator::new(ScopeConfig {
            auth: Some(
                ScopeAuthConfig::new(vec![AuthPublicKey::es256(valid_pem())])
                    .with_ttl(Duration::from_nanos(1)),
            ),
            ..Default::default()
        }));
        let authorizer = ScopeAuthorizer::new(coordinator.clone());

        authorizer.resolve(Some("tenant")).await.unwrap();

        // Rotate to a configuration the relay cannot use; after the TTL the
        // scope must pick it up and start refusing.
        coordinator.set_config(ScopeConfig {
            auth: Some(ScopeAuthConfig::new(vec![])),
            ..Default::default()
        });
        tokio::time::sleep(Duration::from_millis(5)).await;

        let hook = authorizer.resolve(Some("tenant")).await.unwrap().unwrap();
        let session = crate::SessionContext::public(Some("tenant".to_string()));
        assert!(!hook.on_setup(&session, &[]).await.unwrap().is_allowed());
        assert_eq!(coordinator.calls(), 2);
    }
}
