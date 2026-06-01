// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;

use crate::*;

fn test_session_ctx() -> SessionContext {
    SessionContext {
        session_id: 1,
        connection_path: Some("/test/scope".to_string()),
        peer: "127.0.0.1:4443".parse::<SocketAddr>().unwrap(),
    }
}

fn test_tokens(secret: &[u8]) -> Vec<AuthBlob> {
    vec![AuthBlob {
        token_type: 0,
        token_value: Bytes::copy_from_slice(secret),
    }]
}

#[tokio::test]
async fn allow_all_hook_allows_setup() {
    let hook = AllowAllAuthHook;
    let ctx = test_session_ctx();
    let decision = hook.on_setup(&ctx, &[]).await.unwrap();
    assert!(decision.is_allowed());
}

#[tokio::test]
async fn allow_all_hook_allows_request() {
    let hook = AllowAllAuthHook;
    let ctx = test_session_ctx();
    let ns = moq_transport::coding::TrackNamespace::from_utf8_path("test/ns");
    let req_ctx = RequestContext {
        session: &ctx,
        operation: AuthzOperation::Subscribe {
            namespace: &ns,
            track: b"video",
        },
        request_id: Some(1),
    };
    let decision = hook.on_request(&req_ctx, &[]).await.unwrap();
    assert!(decision.is_allowed());
}

#[tokio::test]
async fn deny_all_hook_denies_setup() {
    let hook = DenyAllAuthHook;
    let ctx = test_session_ctx();
    let decision = hook.on_setup(&ctx, &[]).await.unwrap();
    assert!(!decision.is_allowed());
    assert!(matches!(decision.verdict, Verdict::Deny(DenyReason::TokenMissing)));
}

#[tokio::test]
async fn deny_all_hook_denies_request() {
    let hook = DenyAllAuthHook;
    let ctx = test_session_ctx();
    let ns = moq_transport::coding::TrackNamespace::from_utf8_path("test/ns");
    let req_ctx = RequestContext {
        session: &ctx,
        operation: AuthzOperation::PublishNamespace { namespace: &ns },
        request_id: None,
    };
    let decision = hook.on_request(&req_ctx, &[]).await.unwrap();
    assert!(!decision.is_allowed());
}

#[tokio::test]
async fn key_value_hook_accepts_matching_secret() {
    let hook = KeyValueAuthHook::new(b"my-secret".as_slice());
    let ctx = test_session_ctx();
    let tokens = test_tokens(b"my-secret");
    let decision = hook.on_setup(&ctx, &tokens).await.unwrap();
    assert!(decision.is_allowed());
}

#[tokio::test]
async fn key_value_hook_rejects_wrong_secret() {
    let hook = KeyValueAuthHook::new(b"my-secret".as_slice());
    let ctx = test_session_ctx();
    let tokens = test_tokens(b"wrong-secret");
    let decision = hook.on_setup(&ctx, &tokens).await.unwrap();
    assert!(!decision.is_allowed());
    assert!(matches!(decision.verdict, Verdict::Deny(DenyReason::TokenInvalid)));
}

#[tokio::test]
async fn key_value_hook_rejects_empty_tokens() {
    let hook = KeyValueAuthHook::new(b"my-secret".as_slice());
    let ctx = test_session_ctx();
    let decision = hook.on_setup(&ctx, &[]).await.unwrap();
    assert!(!decision.is_allowed());
}

#[tokio::test]
async fn key_value_hook_rejects_wrong_token_type() {
    let hook = KeyValueAuthHook::new(b"my-secret".as_slice());
    let ctx = test_session_ctx();
    let tokens = vec![AuthBlob {
        token_type: 1, // wrong type, expects 0
        token_value: Bytes::from_static(b"my-secret"),
    }];
    let decision = hook.on_setup(&ctx, &tokens).await.unwrap();
    assert!(!decision.is_allowed());
}

#[tokio::test]
async fn any_scheme_hook_first_allow_wins() {
    let hooks: Vec<Arc<dyn AuthHook>> = vec![
        Arc::new(DenyAllAuthHook),
        Arc::new(AllowAllAuthHook),
    ];
    let hook = AnySchemeAuthHook::new(hooks);
    let ctx = test_session_ctx();
    let decision = hook.on_setup(&ctx, &[]).await.unwrap();
    assert!(decision.is_allowed());
}

#[tokio::test]
async fn any_scheme_hook_all_deny_returns_last() {
    let hooks: Vec<Arc<dyn AuthHook>> = vec![
        Arc::new(DenyAllAuthHook),
        Arc::new(DenyAllAuthHook),
    ];
    let hook = AnySchemeAuthHook::new(hooks);
    let ctx = test_session_ctx();
    let decision = hook.on_setup(&ctx, &[]).await.unwrap();
    assert!(!decision.is_allowed());
}

#[tokio::test]
async fn any_scheme_hook_empty_hooks_denies() {
    let hook = AnySchemeAuthHook::new(vec![]);
    let ctx = test_session_ctx();
    let decision = hook.on_setup(&ctx, &[]).await.unwrap();
    assert!(!decision.is_allowed());
}

#[tokio::test]
async fn auth_decision_builder() {
    let d = AuthDecision::allow().with_principal(Some("user@example.com".to_string()));
    assert!(d.is_allowed());
    assert_eq!(d.principal.as_deref(), Some("user@example.com"));

    let d = AuthDecision::deny(DenyReason::TokenExpired);
    assert!(!d.is_allowed());
    assert!(d.principal.is_none());
}

#[tokio::test]
async fn logging_hook_delegates_to_inner() {
    let inner = KeyValueAuthHook::new(b"secret".as_slice());
    let hook = LoggingAuthHook::new(inner);
    let ctx = test_session_ctx();

    let tokens = test_tokens(b"secret");
    let decision = hook.on_setup(&ctx, &tokens).await.unwrap();
    assert!(decision.is_allowed());

    let tokens = test_tokens(b"wrong");
    let decision = hook.on_setup(&ctx, &tokens).await.unwrap();
    assert!(!decision.is_allowed());
}
