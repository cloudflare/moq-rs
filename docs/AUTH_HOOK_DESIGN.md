# MoQ Relay AuthHook Design Proposal

**Status**: Design sketch — implementation in progress  
**Target**: `cloudflare/moq-rs`  
**Last updated**: 2026-05-27

---

## What this doc is for

This document describes the shape of an `AuthHook` trait we plan to add to `moq-rs` to support pluggable intra-scope authorization. It is shared with external contributors to coordinate implementation work across multiple auth schemes — specifically PrivacyPass and CAT for MoQ (C4M) — so that parallel implementation efforts can target a common interface.

The trait is a design sketch, not finished code. The shape is concrete enough to start implementing against, but some details may shift during implementation. Feedback on the trait surface is welcome before we land the first commits.

---

## Overview

MoQT's AUTHORIZATION TOKEN parameters (§9.2 of the MoQT spec) provide a general-purpose mechanism for carrying auth material in SETUP and per-message key-value pairs. The `AuthHook` trait is the relay's extension point for deciding what to _do_ with those tokens — validating them and rendering allow/deny verdicts for relay operations.

### What the trait does

`AuthHook` is called at two points in the relay's request pipeline:

1. **At session establishment** — after AUTHORIZATION TOKEN parameters from CLIENT_SETUP have been decoded. Used to validate session-level credentials.
2. **At each authorization-relevant request** — before the relay performs the underlying action (subscribe, publish, fetch, etc.). Used to validate per-operation credentials.

A third method, `shutdown`, gives implementations a chance to flush state gracefully.

### What the trait deliberately does not do

- **No request mutation.** The hook returns allow or deny; it cannot rewrite namespaces or substitute auth scopes.
- **No outbound credential management.** If the relay needs to present credentials to an upstream origin, that is a separate concern (a future `UpstreamAuthProvider` trait or similar). `AuthHook` is strictly for incoming authorization.
- **No relay-side verdict cache.** Implementations that want per-session caching (e.g., parse a token once at SETUP and apply cached claims on subsequent operations) manage that state internally.
- **No revocation channel.** If a credential is revoked mid-session, the hook will deny the next operation's call. There is no relay-side "kill this session" API.
- **No alias bookkeeping.** The relay's wire layer resolves AUTHORIZATION TOKEN aliases before invoking the hook. The hook always sees fully-resolved `(token_type, token_value)` pairs.

---

## The trait

```rust
#[async_trait]
pub trait AuthHook: Send + Sync {
    /// Called once per session at SETUP time, after AUTHORIZATION TOKEN
    /// parameters have been decoded and any aliases resolved.
    ///
    /// Returning `Verdict::Deny` causes the relay to terminate the session
    /// with UNAUTHORIZED (0x2). Scope resolution is skipped.
    async fn on_setup(
        &self,
        ctx: &SessionContext,
        tokens: &[AuthBlob],
    ) -> Result<AuthDecision> {
        Ok(AuthDecision::allow())
    }

    /// Called once per request before the relay performs any
    /// authorization-relevant action. The relay maps a deny verdict to a
    /// REQUEST_ERROR code appropriate for the auth scheme in use.
    async fn on_request(
        &self,
        ctx: &RequestContext<'_>,
        tokens: &[AuthBlob],
    ) -> Result<AuthDecision> {
        Ok(AuthDecision::allow())
    }

    /// Graceful shutdown hook. Implementations may use this to flush state
    /// (replay-window logs, background task teardown, etc.).
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}
```

### Design choices

**One method per category, not one per operation type.** `on_setup` is its own method because the failure mode is session termination rather than a per-request error. Everything else — PUBLISH, SUBSCRIBE, FETCH, SUBSCRIBE_NAMESPACE, TRACK_STATUS, REQUEST_UPDATE — is a request with the same shape: an operation kind, a namespace, maybe a track name, and AUTH blobs. New MoQT operations are additive: add an enum variant to `AuthzOperation`, not a new method on the trait.

**Default impls return `Ok(AuthDecision::allow())`.** A relay built without an `AuthHook`, or with `AllowAllAuthHook`, behaves identically to a relay with no auth at all. Backwards-compatible by construction.

**Hooks are async.** Implementations may call out to external services. The relay must be prepared for per-invocation async latency. For in-process validation (PP, C4M), the await resolves immediately.

---

## Supporting types

```rust
/// Information about the QUIC/MoQT session. Stable for the session's lifetime.
pub struct SessionContext {
    /// Relay-assigned session handle. Stable across the session lifetime.
    /// This is a transport handle, not an auth principal.
    pub session_id: u64,

    /// The connection path from the WebTransport URL or the PATH setup
    /// parameter (when using raw QUIC).
    pub connection_path: Option<String>,

    /// Peer socket address. Useful for IP-based policies or logging.
    pub peer: SocketAddr,
}

/// Information about a specific request. Built fresh per call.
pub struct RequestContext<'a> {
    pub session: &'a SessionContext,
    pub operation: AuthzOperation<'a>,
    pub request_id: Option<u64>,
}

/// The operation being authorized. Kept narrow and message-shaped so new
/// operations are additive (one new enum variant, not a new trait method).
pub enum AuthzOperation<'a> {
    PublishNamespace { namespace: &'a TrackNamespace },
    PublishNamespaceDone { namespace: &'a TrackNamespace },
    Publish { namespace: &'a TrackNamespace, track: &'a str },
    Subscribe { namespace: &'a TrackNamespace, track: &'a str },
    SubscribeNamespace { prefix: &'a TrackNamespace },
    Fetch { namespace: &'a TrackNamespace, track: &'a str },
    TrackStatus { namespace: &'a TrackNamespace, track: &'a str },
    RequestUpdate { request_id: u64 },
}

/// A fully-resolved AUTHORIZATION TOKEN parameter. Alias bookkeeping is
/// handled by the relay's transport layer; the hook always sees Type+Value
/// pairs, never raw alias directives.
pub struct AuthBlob {
    /// Token type from the IANA "MOQT Auth Token Type" registry.
    pub token_type: u64,
    pub token_value: Bytes,
}

/// The hook's decision.
pub struct AuthDecision {
    pub verdict: Verdict,
    /// Optional opaque principal identifier for logging and metrics.
    /// The relay does not interpret this value.
    pub principal: Option<String>,
}

pub enum Verdict {
    Allow,
    Deny(DenyReason),
}

/// Abstract deny reasons. The relay maps these to wire codes appropriate
/// for the auth scheme in use (see DenyReason → wire codes section below).
pub enum DenyReason {
    TokenMissing,
    TokenInvalid,
    TokenExpired,
    TokenReplayed,
    TokenMalformed,
    ScopeMismatch,
    IssuerUnknown,
    /// Free-form deny with a human-readable reason.
    Other { message: String },
}
```

---

## Where the hook plugs in

The relay's request pipeline, with `AuthHook` invocation points:

| Stage | Hook call | On `Deny` |
|---|---|---|
| After SETUP decode, before scope resolution | `on_setup(session_ctx, setup_tokens)` | Terminate session with `UNAUTHORIZED (0x2)` |
| SUBSCRIBE arrives, before coordinator lookup | `on_request(req_ctx, msg_tokens)` with `Subscribe` | `REQUEST_ERROR` with scheme-mapped code |
| PUBLISH_NAMESPACE arrives, before coordinator registration | `on_request(req_ctx, msg_tokens)` with `PublishNamespace` | `REQUEST_ERROR` |
| SUBSCRIBE_NAMESPACE arrives | `on_request(req_ctx, msg_tokens)` with `SubscribeNamespace` | `REQUEST_ERROR` |
| FETCH arrives | `on_request(req_ctx, msg_tokens)` with `Fetch` | `REQUEST_ERROR` |
| TRACK_STATUS, REQUEST_UPDATE, PUBLISH | Same pattern | Same pattern |

The hook is always invoked **before** the corresponding coordinator action. The coordinator stays focused on directory/routing work; `AuthHook` is a separate concern composed alongside it.

### Composition at the relay-construction site

```rust
let coordinator = Arc::new(MyCoordinator::new(...));
let auth_hook = Arc::new(PrivacyPassAuthHook::new(issuer_pubkeys));

let relay = Relay::new(RelayConfig {
    coordinator,
    auth_hook,  // new field; defaults to AllowAllAuthHook
    // ...
})?;
```

The `auth_hook` field on `RelayConfig` is mandatory-with-default: if not set, it defaults to `AllowAllAuthHook`. This avoids `if let Some(hook) = ...` boilerplate at every invocation point.

---

## Token decoding contract

The relay's MoQ wire layer is responsible for:

1. Parsing AUTHORIZATION TOKEN parameters from SETUP and per-message KVPs.
2. Resolving aliases (REGISTER → store, USE_ALIAS → look up, DELETE → retire) using an `AuthTokenCache`.
3. Surfacing the resolved set as `Vec<AuthBlob>` — `(token_type, token_value)` pairs — to the hook.

The hook **never sees** wire-level alias bookkeeping. By the time `on_setup` or `on_request` is called, every blob is fully resolved.

If the wire layer encounters a malformed token, an unknown alias, or a cache overflow, those are handled per spec (`MALFORMED_AUTH_TOKEN`, `UNKNOWN_AUTH_TOKEN_ALIAS`, `AUTH_TOKEN_CACHE_OVERFLOW`) without invoking the hook. The hook only sees well-formed blobs.

A single session may carry blobs with different token types — e.g., a PrivacyPass token and a C4M token in the same SETUP. Hook implementations select the token type(s) they understand and ignore others.

---

## Reference implementations

### `AllowAllAuthHook`: the default

```rust
pub struct AllowAllAuthHook;

#[async_trait]
impl AuthHook for AllowAllAuthHook {
    // Both methods use the trait's default impls (allow all).
}
```

Backwards-compatibility shim. What `Default` returns. A relay without explicit auth configuration gets this.

---

### `DenyAllAuthHook`: testing scaffold

```rust
pub struct DenyAllAuthHook;

#[async_trait]
impl AuthHook for DenyAllAuthHook {
    async fn on_setup(&self, _: &SessionContext, _: &[AuthBlob]) -> Result<AuthDecision> {
        Ok(AuthDecision::deny(DenyReason::TokenMissing))
    }

    async fn on_request(&self, _: &RequestContext<'_>, _: &[AuthBlob]) -> Result<AuthDecision> {
        Ok(AuthDecision::deny(DenyReason::TokenMissing))
    }
}
```

Useful as a sanity check that the hook is wired up correctly at all invocation points.

---

### `LoggingAuthHook<H>`: composable observability wrapper

```rust
pub struct LoggingAuthHook<H>(pub H);

#[async_trait]
impl<H: AuthHook> AuthHook for LoggingAuthHook<H> {
    async fn on_setup(
        &self,
        ctx: &SessionContext,
        tokens: &[AuthBlob],
    ) -> Result<AuthDecision> {
        let res = self.0.on_setup(ctx, tokens).await;
        log::info!(
            "on_setup session={:x} token_count={} result={:?}",
            ctx.session_id, tokens.len(), res
        );
        res
    }

    async fn on_request(
        &self,
        ctx: &RequestContext<'_>,
        tokens: &[AuthBlob],
    ) -> Result<AuthDecision> {
        let res = self.0.on_request(ctx, tokens).await;
        log::info!(
            "on_request session={:x} op={:?} token_count={} result={:?}",
            ctx.session.session_id, ctx.operation, tokens.len(), res
        );
        res
    }
}
```

Demonstrates that the trait composes well via wrapping. A `LoggingAuthHook<PrivacyPassAuthHook>` logs every invocation while delegating the actual decision to the inner hook.

---

### `KeyValueAuthHook`: shared-secret interop scaffold

```rust
/// Accepts any token whose Token Type is 0 (the spec's "negotiated
/// out-of-band" type) and whose Token Value matches a configured shared
/// secret. Useful for early end-to-end integration testing without standing
/// up issuer infrastructure.
pub struct KeyValueAuthHook {
    expected_secret: Bytes,
}

#[async_trait]
impl AuthHook for KeyValueAuthHook {
    async fn on_setup(
        &self,
        _: &SessionContext,
        tokens: &[AuthBlob],
    ) -> Result<AuthDecision> {
        if tokens
            .iter()
            .any(|t| t.token_type == 0 && t.token_value == self.expected_secret)
        {
            Ok(AuthDecision::allow())
        } else {
            Ok(AuthDecision::deny(DenyReason::TokenInvalid))
        }
    }

    async fn on_request(
        &self,
        _: &RequestContext<'_>,
        tokens: &[AuthBlob],
    ) -> Result<AuthDecision> {
        // Same logic as on_setup; the shared secret is valid for all operations.
        if tokens
            .iter()
            .any(|t| t.token_type == 0 && t.token_value == self.expected_secret)
        {
            Ok(AuthDecision::allow())
        } else {
            Ok(AuthDecision::deny(DenyReason::TokenInvalid))
        }
    }
}
```

The simplest non-trivial hook. A good starting point for shaking out the wire-format plumbing before committing to a full auth scheme implementation.

---

### `PrivacyPassAuthHook`: PP auth scheme

Implements [draft-ietf-moq-privacy-pass-auth](https://datatracker.ietf.org/doc/draft-ietf-moq-privacy-pass-auth/). Tokens carry a `MoQAuthorizationInfo` structure that encodes allowed actions, namespace patterns, and track name patterns for the session.

```rust
pub struct PrivacyPassAuthHook {
    /// Issuer public keys, keyed by token_key_id.
    issuer_keys: HashMap<TokenKeyId, IssuerPublicKey>,
    /// Sliding-window replay protection.
    replay_window: ReplayWindow,
}

#[async_trait]
impl AuthHook for PrivacyPassAuthHook {
    async fn on_setup(
        &self,
        ctx: &SessionContext,
        tokens: &[AuthBlob],
    ) -> Result<AuthDecision> {
        // 1. Find a blob with the PP token type.
        let pp = tokens.iter().find(|t| is_pp_token_type(t.token_type));
        let Some(pp) = pp else {
            return Ok(AuthDecision::deny(DenyReason::TokenMissing));
        };

        // 2. Decode the PP token structure.
        // 3. Verify the token signature against the issuer key for this token's kid.
        // 4. Check the replay window.
        // 5. Decode the inline MoQAuthorizationInfo (namespace/track ACL).
        // 6. Verify the SETUP context is permitted by the token's scope.
        match validate_pp_setup(pp, ctx, &self.issuer_keys, &self.replay_window) {
            Ok(principal) => Ok(AuthDecision::allow().with_principal(principal)),
            Err(e) => Ok(AuthDecision::deny(e.into())),
        }
    }

    async fn on_request(
        &self,
        ctx: &RequestContext<'_>,
        tokens: &[AuthBlob],
    ) -> Result<AuthDecision> {
        // Same validation as on_setup, plus: check that the token's
        // MoQAuthorizationInfo permits this specific action against this
        // specific namespace and track name.
        //
        // The PP draft's MoQAuthScope has: actions, namespace_match,
        // track_name_match. Authorization is OR across scopes: any matching
        // scope authorizes the operation.
        match validate_pp_request(tokens, ctx, &self.issuer_keys, &self.replay_window) {
            Ok(principal) => Ok(AuthDecision::allow().with_principal(principal)),
            Err(e) => Ok(AuthDecision::deny(e.into())),
        }
    }

    async fn shutdown(&self) -> Result<()> {
        self.replay_window.flush().await
    }
}
```

The heavy lifting is in `validate_pp_setup` / `validate_pp_request`, not in the trait surface itself. The hook is a thin dispatch layer; the PP-specific cryptography is encapsulated in those helpers.

**References**:

- [draft-ietf-moq-privacy-pass-auth](https://datatracker.ietf.org/doc/draft-ietf-moq-privacy-pass-auth/) — the spec this hook implements.
- [`raphaelrobert/privacypass`](https://github.com/raphaelrobert/privacypass) — Rust crate implementing the underlying PrivacyPass Blind RSA / VOPRF token protocol. This provides the core cryptographic primitives (blind token issuance, redemption, verification) so the hook implementation can focus on MoQ-specific scope matching rather than building from raw RustCrypto primitives.

---

### `C4MAuthHook`: CAT for MoQ auth scheme

Implements [draft-ietf-moq-c4m](https://datatracker.ietf.org/doc/draft-ietf-moq-c4m/). Tokens are CWT (CBOR Web Token) carrying MOQT-specific claims: an array of `MoqtScope` entries, each specifying allowed actions and namespace/track match patterns. Optionally, tokens carry a DPoP (Demonstration of Proof-of-Possession) binding requiring per-request proofs.

The Cisco [`cat-token`](https://github.com/Quicr/cat-token) Rust library implements the full C4M token lifecycle and is the expected implementation vehicle.

```rust
use cat_token::prelude::*;

pub struct C4MAuthHook {
    /// Verifying algorithm (public key only). One per trusted issuer key.
    algorithm: Arc<dyn CryptographicAlgorithm + Send + Sync>,
    /// Validates standard CWT claims: exp, nbf, iss, aud.
    token_validator: CatTokenValidator,
    /// Validates MOQT-specific claims and performs scope authorization.
    moqt_validator: MoqtValidator,
}

impl C4MAuthHook {
    pub fn new(
        public_key: VerifyingKey,
        expected_issuers: Vec<String>,
        expected_audiences: Vec<String>,
    ) -> Self {
        let algorithm = Arc::new(Es256Algorithm::new_from_verifying_key(public_key)
            .expect("valid verifying key"));

        let token_validator = CatTokenValidator::new()
            .with_expected_issuers(expected_issuers)
            .with_expected_audiences(expected_audiences)
            .with_clock_skew_tolerance(60);

        let moqt_validator = MoqtValidator::new()
            .with_min_revalidation_interval(60.0);

        Self { algorithm, token_validator, moqt_validator }
    }
}

#[async_trait]
impl AuthHook for C4MAuthHook {
    async fn on_setup(
        &self,
        _ctx: &SessionContext,
        tokens: &[AuthBlob],
    ) -> Result<AuthDecision> {
        // Find a blob with the C4M token type (IANA-registered for C4M).
        let blob = tokens.iter().find(|t| is_c4m_token_type(t.token_type));
        let Some(blob) = blob else {
            return Ok(AuthDecision::deny(DenyReason::TokenMissing));
        };

        // The cat-token 4-step validation pattern:
        //
        // Step 1: Decode and verify the cryptographic signature.
        let token_str = std::str::from_utf8(&blob.token_value)
            .map_err(|_| anyhow::anyhow!("token is not valid UTF-8"))?;
        let token = decode_token(token_str, self.algorithm.as_ref())
            .map_err(|e| map_cat_error(e))?;

        // Step 2: Validate standard CWT claims (exp, nbf, iss, aud).
        self.token_validator.validate(&token)
            .map_err(|e| map_cat_error(e))?;

        // Step 3: Validate MOQT-specific claims (revalidation constraints).
        self.moqt_validator.validate_moqt_claims(&token)
            .map_err(|e| map_cat_error(e))?;

        // Step 4 for SETUP: check that the token authorizes ClientSetup.
        // Note: the C4M MoqtAction::ClientSetup maps to the on_setup path;
        // all other actions are checked in on_request.
        let setup_req = MoqtAuthRequest::new(
            MoqtAction::ClientSetup,
            vec![],  // namespace not applicable at SETUP
            vec![],
        );
        let result = self.moqt_validator.authorize(&token, &setup_req);
        if !result.authorized {
            return Ok(AuthDecision::deny(DenyReason::ScopeMismatch));
        }

        // TODO: If token.moqt.moqt_reval is set, schedule revalidation.
        // The current trait has no revalidation callback; see Open Questions.

        let principal = token.informational.sub.clone();
        Ok(AuthDecision::allow().with_principal(principal))
    }

    async fn on_request(
        &self,
        ctx: &RequestContext<'_>,
        tokens: &[AuthBlob],
    ) -> Result<AuthDecision> {
        let blob = tokens.iter().find(|t| is_c4m_token_type(t.token_type));
        let Some(blob) = blob else {
            return Ok(AuthDecision::deny(DenyReason::TokenMissing));
        };

        let token_str = std::str::from_utf8(&blob.token_value)
            .map_err(|_| anyhow::anyhow!("token is not valid UTF-8"))?;
        let token = decode_token(token_str, self.algorithm.as_ref())
            .map_err(|e| map_cat_error(e))?;

        self.token_validator.validate(&token)
            .map_err(|e| map_cat_error(e))?;

        // Map the relay's AuthzOperation to cat-token's MoqtAction + namespace + track.
        let (action, namespace, track) = map_operation_to_cat(&ctx.operation);

        // If the token carries a DPoP binding (cnf.jkt is set), validate the
        // DPoP proof from the tokens alongside the scope check.
        let result = if token.dpop.cnf.is_some() {
            let dpop_proof = extract_dpop_proof(tokens)?;
            let req = MoqtAuthRequest::new(action, namespace, track)
                .with_dpop_proof(dpop_proof);
            self.moqt_validator.authorize_with_dpop(&token, &req, self.algorithm.as_ref())
                .map_err(|e| map_cat_error(e))?
        } else {
            let req = MoqtAuthRequest::new(action, namespace, track);
            self.moqt_validator.authorize(&token, &req)
        };

        if result.authorized {
            Ok(AuthDecision::allow())
        } else {
            Ok(AuthDecision::deny(DenyReason::ScopeMismatch))
        }
    }
}

/// Maps an AuthzOperation to the (MoqtAction, namespace tuple, track name)
/// triple expected by cat-token's MoqtAuthRequest.
fn map_operation_to_cat(op: &AuthzOperation<'_>) -> (MoqtAction, Vec<Vec<u8>>, Vec<u8>) {
    match op {
        AuthzOperation::Publish { namespace, track } =>
            (MoqtAction::Publish, namespace_to_tuple(namespace), track.as_bytes().to_vec()),
        AuthzOperation::PublishNamespace { namespace } =>
            (MoqtAction::PublishNamespace, namespace_to_tuple(namespace), vec![]),
        AuthzOperation::Subscribe { namespace, track } =>
            (MoqtAction::Subscribe, namespace_to_tuple(namespace), track.as_bytes().to_vec()),
        AuthzOperation::SubscribeNamespace { prefix } =>
            (MoqtAction::SubscribeNamespace, namespace_to_tuple(prefix), vec![]),
        AuthzOperation::Fetch { namespace, track } =>
            (MoqtAction::Fetch, namespace_to_tuple(namespace), track.as_bytes().to_vec()),
        AuthzOperation::TrackStatus { namespace, track } =>
            (MoqtAction::TrackStatus, namespace_to_tuple(namespace), track.as_bytes().to_vec()),
        AuthzOperation::RequestUpdate { .. } =>
            (MoqtAction::RequestUpdate, vec![], vec![]),
        AuthzOperation::PublishNamespaceDone { .. } =>
            // No corresponding MoqtAction; treat as allow-by-default at teardown.
            (MoqtAction::PublishNamespace, vec![], vec![]),
    }
}
```

**Namespace tuple conversion**: In `moq-rs`, `TrackNamespace` is defined as a struct wrapping `pub fields: Vec<TupleField>` where `TupleField { pub value: Vec<u8> }`. Both fields are public. Converting to the `Vec<Vec<u8>>` shape that `cat-token`'s `MoqtAuthRequest` expects is:

```rust
fn namespace_to_tuple(ns: &TrackNamespace) -> Vec<Vec<u8>> {
    ns.fields.iter().map(|f| f.value.clone()).collect()
}
```

`cat-token`'s `MoqtScope` matches namespace elements positionally, so a `TrackNamespace` with two fields needs a scope with two `namespace_exact` / `namespace_prefix` / `namespace_suffix` calls (plus an optional `namespace_nil` if the token should refuse namespaces with additional tuple elements).

**Note on token caching**: The implementation above re-decodes and re-validates the token on every `on_request` call. A production implementation would likely cache the parsed `CatToken` at session establishment (in a `DashMap<session_id, CatToken>` keyed off `SessionContext::session_id`) and skip steps 1–3 on subsequent requests. This is an internal optimization; it doesn't affect the trait surface.

---

## Coexistence of multiple auth schemes

### Multiple token types in one session

Because `tokens: &[AuthBlob]` is a flat list keyed by `token_type`, a single session can carry tokens of multiple types simultaneously. Each hook implementation selects the blob(s) it understands by `token_type` and ignores others. This is load-bearing for a relay that needs to accept both PP and C4M sessions participating in the same scope at the same time — for example, a conference call where one participant authenticates via PrivacyPass and another via C4M.

Each session is independently authorized by its own hook invocation; the hook doesn't need to know what auth schemes other sessions in the same scope used.

### The multi-scheme hook pattern

A relay that needs to accept either auth scheme on any given session can use a composite hook that tries each inner hook against the available blobs and accepts if any inner hook accepts:

```rust
/// Accepts a session if any of the inner hooks accepts it.
/// Each hook is tried in order; the first Allow verdict wins.
/// If all hooks deny, returns the last deny reason.
///
/// This is the right shape for a relay that accepts PP *or* C4M tokens
/// on the same port/scope, without requiring the client to know in
/// advance which the relay expects.
pub struct AnySchemeAuthHook(Vec<Arc<dyn AuthHook>>);

#[async_trait]
impl AuthHook for AnySchemeAuthHook {
    async fn on_setup(
        &self,
        ctx: &SessionContext,
        tokens: &[AuthBlob],
    ) -> Result<AuthDecision> {
        let mut last_deny = AuthDecision::deny(DenyReason::TokenMissing);
        for hook in &self.0 {
            let decision = hook.on_setup(ctx, tokens).await?;
            match decision.verdict {
                Verdict::Allow => return Ok(decision),
                Verdict::Deny(_) => last_deny = decision,
            }
        }
        Ok(last_deny)
    }

    async fn on_request(
        &self,
        ctx: &RequestContext<'_>,
        tokens: &[AuthBlob],
    ) -> Result<AuthDecision> {
        let mut last_deny = AuthDecision::deny(DenyReason::TokenMissing);
        for hook in &self.0 {
            let decision = hook.on_request(ctx, tokens).await?;
            match decision.verdict {
                Verdict::Allow => return Ok(decision),
                Verdict::Deny(_) => last_deny = decision,
            }
        }
        Ok(last_deny)
    }
}
```

A relay configured with `AnySchemeAuthHook(vec![pp_hook, c4m_hook])` will accept any session that can satisfy either scheme, independently authorizing each operation against whichever scheme the session's blobs belong to.

### Per-scope configuration

At deployment scale, a relay operator will want to control auth behavior per scope — which schemes are accepted, which issuer public keys are trusted, etc. The `AuthHook` trait is deliberately scope-unaware: it validates tokens using whatever configuration it was given at construction time, and it doesn't know or care what scope the session belongs to.

This follows the same pattern as the existing `ScopeConfig` / `Coordinator::get_scope_config()` API in moq-rs. The relay already resolves scopes at session establishment (via `Coordinator::resolve_scope()`) and retrieves per-scope operational parameters (origin fallback URLs, lingering subscribe settings) via `get_scope_config()`. Auth configuration — accepted schemes, trusted issuer keys — fits naturally alongside those existing per-scope settings.

The relay is responsible for reading per-scope auth config and constructing the appropriate `AuthHook` (or composite `AnySchemeAuthHook`) with the right parameters for each session. Concrete hook impls accept their configuration at construction time — `PrivacyPassAuthHook::new(issuer_keys)`, `C4MAuthHook::new(public_key, issuers, audiences)` — but they don't need to determine the scope themselves. The relay feeds in the right config; the hook does its job.

---

## DenyReason → wire codes

On the SETUP path, any `Verdict::Deny(_)` produces session termination with `UNAUTHORIZED (0x2)`. The reason phrase carries the deny reason for diagnostic purposes.

On request paths, the relay maps abstract deny reasons to scheme-appropriate `REQUEST_ERROR` codes:

| `DenyReason` | PP draft code | C4M / generic |
|---|---|---|
| `TokenMissing` | `TOKEN_MISSING (0x0100)` | generic auth-failure |
| `TokenInvalid` | `TOKEN_INVALID (0x0101)` | same |
| `TokenExpired` | `TOKEN_EXPIRED (0x0102)` | same |
| `TokenReplayed` | `TOKEN_REPLAYED (0x0103)` | same |
| `ScopeMismatch` | `SCOPE_MISMATCH (0x0104)` | same |
| `IssuerUnknown` | `ISSUER_UNKNOWN (0x0105)` | same |
| `TokenMalformed` | `TOKEN_MALFORMED (0x0106)` | same |
| `Other { msg }` | scheme-specific or generic | generic, msg in reason phrase |

draft-ietf-moq-c4m-00 does not define scheme-specific `REQUEST_ERROR` codes — auth error signaling is deferred to the base MoQT Transport spec. This may change in future revisions. The abstract `DenyReason` type insulates implementations from needing to know which scheme is active at the wire layer, so the mapping table above can be extended without changing the trait or existing hook implementations.

---

## Forward-portability across MoQT draft versions

The initial PP and C4M PoC implementations will target **draft-16**, which is the version currently deployed in the reference relay. The next planned implementation target after the PoCs is **draft-18** (draft-17 will be skipped). Supporting multiple MoQT draft versions simultaneously in the relay is not a current goal, but it is something we expect to revisit after draft-18 lands.

The `AuthHook` trait is designed to be stable across this transition. Known flexion points:

- **Operation enum**: add variants as MoQ adds operations. Don't remove or renumber existing variants. Prefer deprecating old variants over backwards-incompatible changes — implementing `on_request` requires matching on `AuthzOperation`, and adding variants is a breaking change for exhaustive matches if not handled carefully (use `#[non_exhaustive]` or a catch-all arm).
- **AUTHORIZATION TOKEN parameter parsing**: the wire shape has been consistent across draft-14, -16, and the editor's copy as of 2026-05. No changes expected here.
- **Setup message shape**: draft-16 has `CLIENT_SETUP` and `SERVER_SETUP`; draft-17 introduced unified `SETUP`. `on_setup` is invoked on whichever exists — a wire-layer concern that doesn't surface in the trait.
- **REQUEST_ERROR codes**: PP draft codes are stable so far. C4M draft-00 defers error codes to the base Transport spec.

The trait itself should be stable across draft-16 → draft-18. Change is more likely in concrete implementations (PP/C4M wire format evolution, claim key assignments still marked TBD in C4M) and in supporting machinery (alias resolution, token cache semantics).

---

## Design rationale

**Two methods, not one per operation.** Each MoQT operation could have its own trait method (`on_subscribe`, `on_publish_namespace`, etc.). Folding them into `on_request` with `AuthzOperation` keeps the trait surface small and makes adding new operations additive rather than breaking. Implementations match on the operation kind internally; the cost is mild, the benefit to trait stability is significant.

**Mandatory-with-default, not `Option<Arc<dyn AuthHook>>`.** The relay config carries a hook unconditionally. If the operator doesn't configure one, they get `AllowAllAuthHook`. This avoids `if let Some(hook) = ...` boilerplate at every invocation point and makes the "no auth" case an explicit named thing rather than an absence.

**No relay-side verdict cache.** Per-operation caching at the relay level has limited value: sessions are independent and cache state would not be shared across them in a meaningful way. Implementations that want per-session caching — for example, parsing a token once at SETUP and applying cached claims locally on every operation — manage that state internally. A `CachingAuthHook<H>` wrapper could be added later as a layered opt-in.

**No translation or mutation hooks.** The hook can deny or allow; it cannot rewrite a SUBSCRIBE's namespace or substitute auth scopes. Translation conflates auth with routing and has a much larger blast radius. Keeping the trait narrow makes it easier to reason about safety and correctness.

**Hook is for incoming authorization only.** If the relay needs to present credentials to an upstream origin (relay-to-origin pull, relay-to-relay forwarding), that is a separate concern outside this trait. Mixing the two would make the trait harder to implement and harder to test in isolation.

**`shutdown` mirrors the coordinator pattern.** Stateful implementations (replay-window logs, background key-rotation tasks) may want to clean up on graceful shutdown. Default no-op for stateless hooks.

---

## Open questions

These are not blocking but are relevant to C4M and PP implementors:

1. **Revalidation support.** The C4M spec (`moqt_reval` claim) allows a token to declare a required revalidation interval — the relay is expected to re-check the token's validity periodically, not just on each operation. The current trait is purely request-driven and has no timer callback. Options: (a) add an `on_revalidation_tick` method, (b) expose a revalidation interval hint in `AuthDecision` and let the relay schedule re-calls to `on_setup`, (c) leave revalidation entirely to the implementation and document that `on_request` may be used as a proxy. This needs resolution before `C4MAuthHook` is production-quality.

2. **`peer_identity: Option<PeerIdentity>` on `SessionContext`.** When mTLS support lands (relay-to-relay and relay-to-upstream-publisher authentication), the session context could carry the peer's verified TLS identity. This would enable hook implementations to make auth decisions based on the peer's certificate, complementing or replacing token-based auth for relay-to-relay scenarios. Add when there is a concrete use case driving it; not needed for the PoC implementations.

3. **`cache_for: Option<Duration>` hint on `AuthDecision`.** If relay-side verdict caching proves useful in practice, an implementation could hint a TTL. Currently absent; add if the need arises from profiling.

4. **DPoP integration shape for C4M.** The `cat-token` library's DPoP support requires a per-request DPoP proof alongside the CAT token. The current `AuthBlob` type is a flat list — it doesn't distinguish between the "main" auth token and a DPoP proof. Options: (a) carry the DPoP proof as a second `AuthBlob` with a distinct token type, (b) add a dedicated `dpop_proof: Option<Bytes>` field to `RequestContext`. The `C4MAuthHook` sketch above assumes option (a).

5. **Composite/multi-issuer key management.** When multiple issuers are in play (e.g., a PP issuer and a C4M issuer in the same deployment), the hook is responsible for routing blobs to the right validator. A future `KeyRegistry` abstraction could make this cleaner.

---

## Implementation notes

The work naturally divides into three layers, each independently reviewable:

**Foundation** — spec-correct `AuthToken` / `AuthTokenType` structs with `Decode`/`Encode` impls; `AuthTokenCache` with the four alias operations (REGISTER, USE_ALIAS, USE_VALUE, DELETE) and spec-aligned error handling; plumbing AUTHORIZATION TOKEN parameters through the session layer so that resolved `Vec<AuthBlob>` is available at each invocation point.

**Trait integration** — adding `auth_hook: Arc<dyn AuthHook>` to `RelayConfig` (defaulting to `AllowAllAuthHook`); invoking `on_setup` after SETUP decode; invoking `on_request` at each operation entry point before the corresponding coordinator call.

**Concrete implementations** — `DenyAllAuthHook` and `LoggingAuthHook` as scaffolds; `KeyValueAuthHook` for early end-to-end interop testing; `PrivacyPassAuthHook` and `C4MAuthHook` as the substantive PoC implementations.

The foundation and integration work needs to land once and is shared by both PoC implementations. Whichever of PP or C4M gets there first will prove out that layer; the second implementation builds on it. The two concrete hook implementations are otherwise independent and can be developed in parallel.
