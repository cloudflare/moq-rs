// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;
use std::io::Write;

#[derive(Clone)]
struct Output(Arc<Mutex<Vec<u8>>>);

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn capture(filter: &str, f: impl FnOnce()) -> String {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let output = Output(bytes.clone());
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_env_filter(filter)
        .with_writer(move || output.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, f);
    let out = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    out
}

#[test]
fn restrictive_filters_preserve_identity_and_span_level() {
    for (filter, level) in [
        ("trace", tracing::Level::TRACE),
        ("debug", tracing::Level::DEBUG),
        ("info", tracing::Level::INFO),
        ("warn", tracing::Level::WARN),
        ("error", tracing::Level::ERROR),
        (
            "off,moq_transport::session=error,moq_transport::control=error",
            tracing::Level::ERROR,
        ),
    ] {
        let out = capture(filter, || {
            let span = SessionId::new("filtered-identity").span();
            assert_eq!(*span.metadata().unwrap().level(), level);
            let _guard = span.enter();
            tracing::error!(target: "moq_transport::control", "filtered event");
        });
        assert!(out.contains("session_id=filtered-identity"), "{out}");
    }
}

#[test]
fn an_upstream_root_does_not_inherit_downstream_identity() {
    let out = capture("warn", || {
        let downstream = SessionId::new("downstream-id").span();
        let _downstream = downstream.enter();
        let upstream = SessionId::new("upstream-id").span();
        let _upstream = upstream.enter();
        tracing::warn!("upstream event");
    });
    assert!(out.contains("upstream-id"));
    assert!(!out.contains("downstream-id"), "{out}");
}

#[test]
fn all_reason_bearing_control_messages_are_quoted_and_safe() {
    let reason = crate::coding::ReasonPhrase::new("peer\r\nforged\0\x1b level=ERROR");
    let messages = [
        message::RequestError {
            id: 7,
            error_code: 1,
            retry_interval: 0,
            reason: reason.clone(),
        }
        .into(),
        message::PublishDone {
            id: 7,
            status_code: 1,
            stream_count: 0,
            reason: reason.clone(),
        }
        .into(),
        message::PublishNamespaceCancel {
            id: 7,
            error_code: 1,
            reason_phrase: reason,
        }
        .into(),
    ];
    let out = capture("debug", || {
        for msg in &messages {
            Session::log_control_message(&SessionId::new("reason-session"), msg, "recv");
        }
    });
    assert_eq!(out.lines().count(), 3, "{out}");
    for line in out.lines() {
        assert!(line.contains("session_id=reason-session"));
        assert!(
            line.contains(r#"reason="peer\r\nforged\0\u{1b} level=ERROR""#),
            "{line}"
        );
        assert!(!line.contains('\r') && !line.contains('\0') && !line.contains('\x1b'));
    }
    let out = capture("debug", || {
        let msg = message::RequestError::new(
            7,
            message::RequestErrorCode::InternalError,
            0,
            "bad /eyJhbGci.payload.signature?token=secret",
        )
        .into();
        Session::log_control_message(&SessionId::new("redacted-session"), &msg, "recv");
    });
    assert!(out.contains("[redacted]"));
    assert!(!out.contains("eyJ") && !out.contains("secret"));
}

#[tokio::test]
async fn setup_shares_one_identity_across_both_roles() {
    let (client, server) = test_support::loopback_session_pair().await;
    let (client, server) = tokio::join!(
        Session::connect_with_config_and_session_id(
            client,
            None,
            Transport::RawQuic,
            SessionConfig::default(),
            SessionId::new("client-id")
        ),
        Session::accept_with_config_and_session_id(
            server,
            None,
            Transport::RawQuic,
            SessionConfig::default(),
            SessionId::new("server-id")
        ),
    );
    let (session, publisher, subscriber) = client.unwrap();
    assert_eq!(session.session_id(), publisher.session_id());
    assert_eq!(session.session_id(), subscriber.session_id());
    assert_eq!(session.session_id().as_str(), "client-id");
    let (session, publisher, subscriber) = server.unwrap();
    assert_eq!(session.session_id(), publisher.unwrap().session_id());
    assert_eq!(session.session_id(), subscriber.unwrap().session_id());
    assert_eq!(session.session_id().as_str(), "server-id");
}

#[tokio::test]
async fn bidi_error_id_joins_wire_reason_to_owning_session() {
    let (client, server) = test_support::loopback_session_pair().await;
    let (send, recv) = client.open_bi().await.unwrap();
    // Quinn only makes a stream visible to the peer once bytes are written.
    let mut opening = Writer::with_session_id(send, SessionId::new("client-error-session"));
    opening.write(&[0]).await.unwrap();
    let (send, _) = server.accept_bi().await.unwrap();
    let mut writer = Writer::with_session_id(send, SessionId::new("server-error-session"));
    let err = crate::serve::ServeError::internal_ctx("test context");
    let error_id = err.error_id().unwrap().to_string();
    let msg = message::RequestError::new(
        7,
        message::RequestErrorCode::InternalError,
        0,
        &err.to_string(),
    )
    .into();
    let output = capture("debug", || {
        futures::executor::block_on(Session::encode_bidi_response(&mut writer, &msg)).unwrap();
    });
    writer.finish().unwrap();
    let mut reader = Reader::with_session_id(recv, SessionId::new("client-error-session"));
    let Message::RequestError(decoded) =
        Session::decode_bidi_response(&mut reader, 7).await.unwrap()
    else {
        panic!("expected REQUEST_ERROR");
    };
    assert_eq!(decoded.id, 7);
    assert!(decoded.reason.0.contains(&error_id));
    assert!(reader.done().await.unwrap());
    assert!(
        output.contains("server-error-session") && output.contains(&error_id),
        "{output}"
    );
}
