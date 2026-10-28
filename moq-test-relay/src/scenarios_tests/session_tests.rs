// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

#[tokio::test]
async fn setup_only_comments_on_fetch_without_changing_verdict() -> Result<()> {
    let mut relay = Relay::start("setup-only", "https").await?;
    let mut drivers = JoinSet::new();
    let (session, _, _, _) = relay.connect(&mut drivers).await?;
    let (_writer, mut reader) = send_request(
        &session,
        message::Fetch {
            id: 0,
            fetch_type: message::FetchType::Standalone,
            standalone_fetch: Some(message::StandaloneFetch {
                track_namespace: TrackNamespace::from_utf8_path(TEST_NAMESPACE),
                track_name: TEST_TRACK.into(),
                start_location: Location::new(0, 0),
                end_location: Location::new(1, 0),
            }),
            joining_fetch: None,
            params: KeyValuePairs::default(),
        }
        .into(),
    )
    .await?;
    let (kind, payload) = timeout(Duration::from_secs(1), read_frame(&mut reader)).await??;
    assert_eq!(kind, message::wire_id::RequestError);
    assert_eq!(
        u64::decode(&mut payload.as_slice())?,
        message::RequestErrorCode::NotSupported as u64
    );
    session.close(0, "test complete");
    let result = relay.verdict().await?;
    assert_eq!(result.message, None);
    let output = crate::tap::render(
        registry::find("setup-only", None).context("entry missing")?,
        &result,
    )?;
    assert!(output.ends_with("  ...\n# unexpected request: FETCH on session 1\n"));
    Ok(())
}

#[tokio::test]
async fn requests_outside_procedure_are_answered_and_commented_without_failure() -> Result<()> {
    let mut relay = Relay::start("announce-only", "https").await?;
    let mut drivers = JoinSet::new();
    let (session, _, _, _) = relay.connect(&mut drivers).await?;
    let _namespace = publish_namespace(&session, TEST_NAMESPACE).await?;
    let (_writer, mut reader) = send_request(
        &session,
        message::TrackStatus {
            id: 2,
            track_namespace: TrackNamespace::from_utf8_path(TEST_NAMESPACE),
            track_name: TEST_TRACK.into(),
            params: KeyValuePairs::default(),
        }
        .into(),
    )
    .await?;
    let (kind, payload) = timeout(Duration::from_secs(1), read_frame(&mut reader)).await??;
    assert_eq!(kind, message::wire_id::RequestError);
    assert_eq!(
        u64::decode(&mut payload.as_slice())?,
        message::RequestErrorCode::NotSupported as u64
    );
    session.close(0, "test complete");
    let result = relay.verdict().await?;
    assert_eq!(result.message, None);
    let output = crate::tap::render(
        registry::find("announce-only", None).context("entry missing")?,
        &result,
    )?;
    assert!(output.ends_with("  ...\n# unexpected request: TRACK_STATUS on session 1\n"));
    Ok(())
}

#[tokio::test]
async fn setup_only_requires_explicit_graceful_transport_close() -> Result<()> {
    for scheme in ["https", "moqt"] {
        for code in [0, 42] {
            let mut relay = Relay::start("setup-only", scheme).await?;
            let mut drivers = JoinSet::new();
            let (session, _, _, _) = relay.connect(&mut drivers).await?;
            session.close(code, "test close");
            let result = relay.verdict().await?;
            if code == 0 {
                assert_eq!(result.message, None);
            } else {
                assert!(result
                    .message
                    .is_some_and(|message| message.contains("error or reset")));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn setup_only_answers_requests_outside_procedure_and_safety_caps_open_session() -> Result<()>
{
    let mut relay = Relay::start("setup-only", "moqt").await?;
    let mut drivers = JoinSet::new();
    let (session, _, _, _) = relay.connect(&mut drivers).await?;
    let namespace = TrackNamespace::from_utf8_path(TEST_NAMESPACE);
    let prefix = TrackNamespacePrefix::from_utf8_path(TEST_NAMESPACE);
    let requests: Vec<message::Message> = vec![
        message::PublishNamespace {
            id: 0,
            track_namespace: namespace.clone(),
            params: KeyValuePairs::default(),
        }
        .into(),
        message::SubscribeNamespace {
            id: 2,
            track_namespace_prefix: prefix.clone(),
            params: KeyValuePairs::default(),
        }
        .into(),
        message::SubscribeTracks {
            id: 4,
            track_namespace_prefix: prefix,
            params: KeyValuePairs::default(),
        }
        .into(),
        message::TrackStatus {
            id: 6,
            track_namespace: namespace.clone(),
            track_name: TEST_TRACK.into(),
            params: KeyValuePairs::default(),
        }
        .into(),
        message::Subscribe {
            id: 8,
            track_namespace: namespace.clone(),
            track_name: TEST_TRACK.into(),
            params: KeyValuePairs::default(),
        }
        .into(),
        message::Publish {
            id: 10,
            track_namespace: namespace,
            track_name: TEST_TRACK.into(),
            track_alias: 0,
            params: KeyValuePairs::default(),
            track_extensions: Default::default(),
        }
        .into(),
    ];
    for request in requests {
        let name = request.name();
        let (_writer, mut reader) = send_request(&session, request).await?;
        let (kind, _) = timeout(Duration::from_secs(1), read_frame(&mut reader)).await??;
        assert_eq!(
            kind,
            message::wire_id::RequestError,
            "unexpected {name} must be answered"
        );
    }
    // Leave the session open to exercise the cap rather than peer shutdown.
    let result = relay.verdict().await?;
    assert!(result.message.is_some_and(|message| message
        .contains("did not close the session gracefully")
        && !message.contains("unexpected request")));
    assert_eq!(
        result
            .comments
            .iter()
            .filter(|comment| comment.starts_with("unexpected request:"))
            .count(),
        6
    );
    assert!(result
        .comments
        .iter()
        .any(|comment| comment.contains("post-verdict safety cap reached")));
    assert!(
        result.duration >= Duration::from_secs(2) && result.duration < Duration::from_millis(2250)
    );
    Ok(())
}

#[tokio::test]
async fn setup_only_fails_control_fin_before_complete_setup() -> Result<()> {
    let mut relay = Relay::start("setup-only", "moqt").await?;
    let (session, _, _) = relay.client.connect(&relay.url, None).await?;
    let mut control = session.open_uni().await?;
    let mut prefix = Vec::new();
    moq_transport::setup::SETUP_TYPE.encode(&mut prefix)?;
    write_bytes(&mut control, &prefix).await?;
    control.finish()?;
    let result = relay.verdict().await?;
    assert!(result.message.is_some_and(|message| message
        .contains("client closed the control stream before completing SETUP")
        && message.contains("Section 3.3")));
    Ok(())
}

#[tokio::test]
async fn setup_only_tolerates_control_fin_after_setup_and_a_graceful_close() -> Result<()> {
    for scheme in ["https", "moqt"] {
        let mut relay = Relay::start("setup-only", scheme).await?;
        let (session, mut control, _peer_control) = connect_raw(&relay).await?;
        control.finish()?;
        tokio::time::sleep(Duration::from_millis(60)).await;
        session.close(0, "test complete");
        let result = relay.verdict().await?;
        assert_eq!(result.message, None);
        assert!(result.comments.iter().any(|comment| comment
            .contains("control-stream FIN on session 1 (client)")
            && comment.contains("Section 3.3")));
    }
    Ok(())
}
