// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::net::SocketAddr;

use bytes::Bytes;
use cat_token::{
    CatTokenBuilder, Es256Algorithm, MoqtAction, MoqtScopeBuilder, encode_token,
};
use moq_auth::{
    AuthBlob, AuthHook, AuthzOperation, RequestContext, SessionContext,
};
use moq_transport::coding::TrackNamespace;

use crate::{C4MAuthHook, C4MConfig, C4M_TOKEN_TYPE};

fn test_session_ctx() -> SessionContext {
    SessionContext {
        session_id: 42,
        connection_path: Some("/test/scope".to_string()),
        peer: "127.0.0.1:4443".parse::<SocketAddr>().unwrap(),
    }
}

fn make_hook_and_key() -> (C4MAuthHook, Es256Algorithm) {
    let signing_key = Es256Algorithm::new_with_key_pair().unwrap();
    let verifying_key = Es256Algorithm::new_verifier(
        signing_key.verifying_key().clone(),
    );

    let config = C4MConfig::new(verifying_key)
        .with_expected_issuers(vec!["test-issuer".to_string()])
        .with_expected_audiences(vec!["test-relay".to_string()])
        .with_clock_skew_tolerance(60);

    (C4MAuthHook::new(config), signing_key)
}

fn make_publisher_token(signing_key: &Es256Algorithm, namespace_parts: &[&[u8]]) -> String {
    let mut builder = MoqtScopeBuilder::new().publisher();
    for part in namespace_parts {
        builder = builder.namespace_prefix(part);
    }
    let scope = builder.track_prefix(b"").build();

    let setup_scope = MoqtScopeBuilder::new()
        .action(MoqtAction::ClientSetup)
        .build();

    let token = CatTokenBuilder::new()
        .issuer("test-issuer")
        .single_audience("test-relay")
        .subject("publisher-1")
        .expires_in(3600)
        .moqt_scope(scope)
        .moqt_scope(setup_scope)
        .build();

    encode_token(&token, signing_key).unwrap()
}

fn make_subscriber_token(signing_key: &Es256Algorithm, namespace_parts: &[&[u8]]) -> String {
    let mut builder = MoqtScopeBuilder::new().subscriber();
    for part in namespace_parts {
        builder = builder.namespace_prefix(part);
    }
    let scope = builder.track_prefix(b"").build();

    let setup_scope = MoqtScopeBuilder::new()
        .action(MoqtAction::ClientSetup)
        .build();

    let token = CatTokenBuilder::new()
        .issuer("test-issuer")
        .single_audience("test-relay")
        .subject("subscriber-1")
        .expires_in(3600)
        .moqt_scope(scope)
        .moqt_scope(setup_scope)
        .build();

    encode_token(&token, signing_key).unwrap()
}

fn token_to_blobs(token_str: &str) -> Vec<AuthBlob> {
    vec![AuthBlob {
        token_type: C4M_TOKEN_TYPE,
        token_value: Bytes::from(token_str.to_string()),
    }]
}

#[tokio::test]
async fn valid_publisher_token_allows_setup() {
    let (hook, key) = make_hook_and_key();
    let token = make_publisher_token(&key, &[b"sports", b"football"]);
    let blobs = token_to_blobs(&token);
    let ctx = test_session_ctx();

    let decision = hook.on_setup(&ctx, &blobs).await.unwrap();
    assert!(decision.is_allowed());
    assert_eq!(decision.principal.as_deref(), Some("publisher-1"));
}

#[tokio::test]
async fn valid_publisher_token_allows_publish_namespace() {
    let (hook, key) = make_hook_and_key();
    let token = make_publisher_token(&key, &[b"sports", b"football"]);
    let blobs = token_to_blobs(&token);
    let ctx = test_session_ctx();
    let ns = TrackNamespace::from_utf8_path("sports/football/match-42");

    let req_ctx = RequestContext {
        session: &ctx,
        operation: AuthzOperation::PublishNamespace { namespace: &ns },
        request_id: None,
    };
    let decision = hook.on_request(&req_ctx, &blobs).await.unwrap();
    assert!(decision.is_allowed());
}

#[tokio::test]
async fn valid_publisher_token_allows_publish() {
    let (hook, key) = make_hook_and_key();
    let token = make_publisher_token(&key, &[b"sports", b"football"]);
    let blobs = token_to_blobs(&token);
    let ctx = test_session_ctx();
    let ns = TrackNamespace::from_utf8_path("sports/football/match-42");

    let req_ctx = RequestContext {
        session: &ctx,
        operation: AuthzOperation::Publish {
            namespace: &ns,
            track: b"video-1080p",
        },
        request_id: None,
    };
    let decision = hook.on_request(&req_ctx, &blobs).await.unwrap();
    assert!(decision.is_allowed());
}

#[tokio::test]
async fn subscriber_token_denies_publish() {
    let (hook, key) = make_hook_and_key();
    let token = make_subscriber_token(&key, &[b"sports", b"football"]);
    let blobs = token_to_blobs(&token);
    let ctx = test_session_ctx();
    let ns = TrackNamespace::from_utf8_path("sports/football/match-42");

    let req_ctx = RequestContext {
        session: &ctx,
        operation: AuthzOperation::PublishNamespace { namespace: &ns },
        request_id: None,
    };
    let decision = hook.on_request(&req_ctx, &blobs).await.unwrap();
    assert!(!decision.is_allowed());
}

#[tokio::test]
async fn valid_subscriber_token_allows_subscribe() {
    let (hook, key) = make_hook_and_key();
    let token = make_subscriber_token(&key, &[b"sports", b"football"]);
    let blobs = token_to_blobs(&token);
    let ctx = test_session_ctx();
    let ns = TrackNamespace::from_utf8_path("sports/football/match-42");

    let req_ctx = RequestContext {
        session: &ctx,
        operation: AuthzOperation::Subscribe {
            namespace: &ns,
            track: b"video-1080p",
        },
        request_id: None,
    };
    let decision = hook.on_request(&req_ctx, &blobs).await.unwrap();
    assert!(decision.is_allowed());
}

#[tokio::test]
async fn valid_subscriber_token_allows_fetch() {
    let (hook, key) = make_hook_and_key();
    let token = make_subscriber_token(&key, &[b"sports", b"football"]);
    let blobs = token_to_blobs(&token);
    let ctx = test_session_ctx();
    let ns = TrackNamespace::from_utf8_path("sports/football/match-42");

    let req_ctx = RequestContext {
        session: &ctx,
        operation: AuthzOperation::Fetch {
            namespace: &ns,
            track: b"video-1080p",
        },
        request_id: None,
    };
    let decision = hook.on_request(&req_ctx, &blobs).await.unwrap();
    assert!(decision.is_allowed());
}

#[tokio::test]
async fn wrong_namespace_denies() {
    let (hook, key) = make_hook_and_key();
    let token = make_publisher_token(&key, &[b"sports", b"football"]);
    let blobs = token_to_blobs(&token);
    let ctx = test_session_ctx();
    let ns = TrackNamespace::from_utf8_path("music/concert/live");

    let req_ctx = RequestContext {
        session: &ctx,
        operation: AuthzOperation::PublishNamespace { namespace: &ns },
        request_id: None,
    };
    let decision = hook.on_request(&req_ctx, &blobs).await.unwrap();
    assert!(!decision.is_allowed());
}

#[tokio::test]
async fn missing_token_denies() {
    let (hook, _) = make_hook_and_key();
    let ctx = test_session_ctx();

    let decision = hook.on_setup(&ctx, &[]).await.unwrap();
    assert!(!decision.is_allowed());
}

#[tokio::test]
async fn malformed_token_denies() {
    let (hook, _) = make_hook_and_key();
    let ctx = test_session_ctx();
    let blobs = vec![AuthBlob {
        token_type: C4M_TOKEN_TYPE,
        token_value: Bytes::from_static(b"not.a.valid.token"),
    }];

    let decision = hook.on_setup(&ctx, &blobs).await.unwrap();
    assert!(!decision.is_allowed());
}

#[tokio::test]
async fn wrong_issuer_denies() {
    let signing_key = Es256Algorithm::new_with_key_pair().unwrap();
    let verifying_key = Es256Algorithm::new_verifier(
        signing_key.verifying_key().clone(),
    );

    let config = C4MConfig::new(verifying_key)
        .with_expected_issuers(vec!["expected-issuer".to_string()])
        .with_expected_audiences(vec!["test-relay".to_string()]);

    let hook = C4MAuthHook::new(config);

    let scope = MoqtScopeBuilder::new()
        .action(MoqtAction::ClientSetup)
        .build();

    let token = CatTokenBuilder::new()
        .issuer("wrong-issuer")
        .single_audience("test-relay")
        .expires_in(3600)
        .moqt_scope(scope)
        .build();

    let token_str = encode_token(&token, &signing_key).unwrap();
    let blobs = token_to_blobs(&token_str);
    let ctx = test_session_ctx();

    let decision = hook.on_setup(&ctx, &blobs).await.unwrap();
    assert!(!decision.is_allowed());
}

#[tokio::test]
async fn wrong_audience_denies() {
    let signing_key = Es256Algorithm::new_with_key_pair().unwrap();
    let verifying_key = Es256Algorithm::new_verifier(
        signing_key.verifying_key().clone(),
    );

    let config = C4MConfig::new(verifying_key)
        .with_expected_issuers(vec!["test-issuer".to_string()])
        .with_expected_audiences(vec!["expected-relay".to_string()]);

    let hook = C4MAuthHook::new(config);

    let scope = MoqtScopeBuilder::new()
        .action(MoqtAction::ClientSetup)
        .build();

    let token = CatTokenBuilder::new()
        .issuer("test-issuer")
        .single_audience("wrong-relay")
        .expires_in(3600)
        .moqt_scope(scope)
        .build();

    let token_str = encode_token(&token, &signing_key).unwrap();
    let blobs = token_to_blobs(&token_str);
    let ctx = test_session_ctx();

    let decision = hook.on_setup(&ctx, &blobs).await.unwrap();
    assert!(!decision.is_allowed());
}

#[tokio::test]
async fn token_without_client_setup_scope_denies_setup() {
    let (hook, key) = make_hook_and_key();

    let scope = MoqtScopeBuilder::new()
        .publisher()
        .namespace_prefix(b"sports")
        .track_prefix(b"/")
        .build();

    let token = CatTokenBuilder::new()
        .issuer("test-issuer")
        .single_audience("test-relay")
        .expires_in(3600)
        .moqt_scope(scope)
        .build();

    let token_str = encode_token(&token, &key).unwrap();
    let blobs = token_to_blobs(&token_str);
    let ctx = test_session_ctx();

    let decision = hook.on_setup(&ctx, &blobs).await.unwrap();
    assert!(!decision.is_allowed());
}

#[tokio::test]
async fn invalid_signature_denies() {
    let signing_key = Es256Algorithm::new_with_key_pair().unwrap();
    let different_key = Es256Algorithm::new_with_key_pair().unwrap();
    let verifying_key = Es256Algorithm::new_verifier(
        different_key.verifying_key().clone(),
    );

    let config = C4MConfig::new(verifying_key)
        .with_expected_issuers(vec!["test-issuer".to_string()])
        .with_expected_audiences(vec!["test-relay".to_string()]);

    let hook = C4MAuthHook::new(config);

    let scope = MoqtScopeBuilder::new()
        .action(MoqtAction::ClientSetup)
        .build();

    let token = CatTokenBuilder::new()
        .issuer("test-issuer")
        .single_audience("test-relay")
        .expires_in(3600)
        .moqt_scope(scope)
        .build();

    let token_str = encode_token(&token, &signing_key).unwrap();
    let blobs = token_to_blobs(&token_str);
    let ctx = test_session_ctx();

    let decision = hook.on_setup(&ctx, &blobs).await.unwrap();
    assert!(!decision.is_allowed());
}
