// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

#[tokio::test]
async fn announce_only_comments_on_later_publication_without_judging_its_name() -> Result<()> {
    let mut relay = Relay::start("announce-only", "https").await?;
    let mut drivers = JoinSet::new();
    let (session, _, _, _) = relay.connect(&mut drivers).await?;
    let _namespace = publish_namespace(&session, TEST_NAMESPACE).await?;
    let mut extra = send_request(
        &session,
        message::PublishNamespace {
            id: 2,
            track_namespace: TrackNamespace::from_utf8_path("other/namespace"),
            params: KeyValuePairs::default(),
        }
        .into(),
    )
    .await?;
    assert_eq!(
        read_frame(&mut extra.1).await?.0,
        message::wire_id::RequestOk
    );
    cancel_namespace(&mut extra).await?;
    session.close(0, "test complete");
    let result = relay.verdict().await?;
    assert_eq!(result.message, None);
    assert!(result
        .comments
        .iter()
        .any(|comment| comment == "unexpected request: PUBLISH_NAMESPACE on session 1"));
    Ok(())
}

#[tokio::test]
async fn subscribe_namespace_lifecycle_withdraws_extra_publication_from_routing() -> Result<()> {
    let mut relay = Relay::start("subscribe-namespace-lifecycle", "https").await?;
    let mut drivers = JoinSet::new();
    let (sub_session, _, _, _) = relay.connect(&mut drivers).await?;
    let mut subscription = namespace_subscription(&sub_session, "test").await?;
    let (pub_session, _, _, _) = relay.connect(&mut drivers).await?;
    let mut required = publish_namespace(&pub_session, "test/required").await?;
    namespace_update(&mut subscription.1, false, "required").await?;
    let mut extra = send_request(
        &pub_session,
        message::PublishNamespace {
            id: 2,
            track_namespace: TrackNamespace::from_utf8_path("test/extra"),
            params: KeyValuePairs::default(),
        }
        .into(),
    )
    .await?;
    assert_eq!(
        read_frame(&mut extra.1).await?.0,
        message::wire_id::RequestOk
    );
    namespace_update(&mut subscription.1, false, "extra").await?;
    cancel_namespace(&mut extra).await?;
    namespace_update(&mut subscription.1, true, "extra").await?;
    // A new subscriber must not receive a withdrawn extra in its snapshot.
    let (later_session, _, _, _) = relay.connect(&mut drivers).await?;
    let mut later = namespace_subscription(&later_session, "test/extra").await?;
    assert!(timeout(Duration::from_millis(60), read_frame(&mut later.1))
        .await
        .is_err());
    cancel_namespace(&mut required).await?;
    namespace_update(&mut subscription.1, true, "required").await?;
    pub_session.close(0, "test complete");
    sub_session.close(0, "test complete");
    later_session.close(0, "test complete");
    let result = relay.verdict().await?;
    assert_eq!(result.message, None);
    assert!(result
        .comments
        .iter()
        .any(|comment| comment == "unexpected request: PUBLISH_NAMESPACE on session 2"));
    Ok(())
}

#[tokio::test]
async fn announce_only_accepts_namespace_and_requires_graceful_close() -> Result<()> {
    for scheme in ["https", "moqt"] {
        for code in [0, 42] {
            let mut relay = Relay::start("announce-only", scheme).await?;
            let mut drivers = JoinSet::new();
            let (session, _, _, _) = relay.connect(&mut drivers).await?;
            let _namespace = publish_namespace(&session, TEST_NAMESPACE).await?;
            session.close(code, "test complete");
            let result = relay.verdict().await?;
            if code == 0 {
                assert_eq!(result.message, None);
            } else {
                assert!(result
                    .message
                    .is_some_and(|message| message.contains("error or reset")));
            }
            assert_eq!(result.sessions[0].role, Some("publisher"));
        }
    }
    Ok(())
}

#[tokio::test]
async fn announce_only_names_wrong_namespace_and_missing_close() -> Result<()> {
    for wrong_name in [false, true] {
        let mut relay = Relay::start("announce-only", "https").await?;
        let mut drivers = JoinSet::new();
        let (session, _, _, _) = relay.connect(&mut drivers).await?;
        let _namespace = publish_namespace(
            &session,
            if wrong_name {
                "wrong/namespace"
            } else {
                TEST_NAMESPACE
            },
        )
        .await?;
        if wrong_name {
            session.close(0, "test complete");
        }
        let result = relay.verdict().await?;
        assert!(result
            .message
            .is_some_and(|message| message.contains(if wrong_name {
                "PUBLISH_NAMESPACE named /wrong/namespace"
            } else {
                "did not close the session gracefully"
            })));
    }
    Ok(())
}

#[tokio::test]
async fn announce_only_deadline_starts_at_publish_namespace() -> Result<()> {
    let mut relay = Relay::start("announce-only", "https").await?;
    let mut drivers = JoinSet::new();
    let (session, _, _, _) = relay.connect(&mut drivers).await?;
    tokio::time::sleep(Duration::from_millis(1400)).await;
    let _namespace = publish_namespace(&session, TEST_NAMESPACE).await?;
    tokio::time::sleep(Duration::from_millis(800)).await;
    session.close(0, "test complete");
    let result = relay.verdict().await?;
    assert_eq!(result.message, None);
    assert!(result.duration > Duration::from_secs(2));
    Ok(())
}

#[tokio::test]
async fn publish_namespace_done_requires_peer_cancel_not_session_teardown() -> Result<()> {
    for scheme in ["https", "moqt"] {
        for cancel in [true, false] {
            let mut relay = Relay::start("publish-namespace-done", scheme).await?;
            let mut drivers = JoinSet::new();
            let (session, _, _, _) = relay.connect(&mut drivers).await?;
            let mut namespace = publish_namespace(&session, TEST_NAMESPACE).await?;
            if cancel {
                cancel_namespace(&mut namespace).await?;
            }
            session.close(0, "test complete");
            let result = relay.verdict().await?;
            if cancel {
                assert_eq!(result.message, None);
            } else {
                assert!(result.message.is_some_and(
                    |message| message.contains("did not cancel the PUBLISH_NAMESPACE request")
                ));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn publish_namespace_done_judges_delivered_cancel_before_immediate_session_close(
) -> Result<()> {
    for scheme in ["https", "moqt"] {
        let mut relay = Relay::start("publish-namespace-done", scheme).await?;
        let mut drivers = JoinSet::new();
        let (session, _, _, _) = relay.connect(&mut drivers).await?;
        let mut namespace = publish_namespace(&session, TEST_NAMESPACE).await?;
        namespace.0.reset(1);
        namespace.1.stop(1);
        session.close(0, "test complete");
        let result = relay.verdict().await?;
        if scheme == "https" {
            assert_eq!(
                result.message, None,
                "WebTransport delivers cancellation before close"
            );
        } else {
            // Quinn's immediate raw-QUIC close discards the queued stream reset.
            // Parsed-frame traces confirm only CONNECTION_CLOSE reaches the relay.
            assert!(result.message.is_some_and(|message| message
                .contains("no withdrawal reached the relay before the session closed")));
        }
    }
    Ok(())
}

#[tokio::test]
async fn publish_namespace_done_reports_draft16_message_as_session_error() -> Result<()> {
    for scheme in ["https", "moqt"] {
        let mut relay = Relay::start("publish-namespace-done", scheme).await?;
        let (session, mut control, _peer_control) = connect_raw(&relay).await?;
        let _namespace = publish_namespace(&session, TEST_NAMESPACE).await?;
        let mut legacy = Vec::new();
        message::Message::PublishNamespaceDone(message::PublishNamespaceDone { id: 0 })
            .encode(&mut legacy)?;
        write_bytes(&mut control, &legacy).await?;
        let result = relay.verdict().await?;
        let message = result
            .message
            .context("draft-16 withdrawal incorrectly passed")?;
        assert!(
            message.contains("session 1 ended with error or reset"),
            "{message}"
        );
        assert!(
            message.contains("PUBLISH_NAMESPACE_DONE is not part of draft-18"),
            "{message}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn subscribe_namespace_lifecycle_sends_snapshot_and_withdrawal_updates() -> Result<()> {
    for scheme in ["https", "moqt"] {
        for publisher_first in [false, true] {
            let mut relay = Relay::start("subscribe-namespace-lifecycle", scheme).await?;
            let mut drivers = JoinSet::new();
            let (sub_session, _, _, _) = relay.connect(&mut drivers).await?;
            let (pub_session, _, _, _) = relay.connect(&mut drivers).await?;
            let mut publication = if publisher_first {
                Some(publish_namespace(&pub_session, "test/discovery/unique").await?)
            } else {
                None
            };
            let mut subscription = namespace_subscription(&sub_session, "test/discovery").await?;
            if publication.is_none() {
                publication = Some(publish_namespace(&pub_session, "test/discovery/unique").await?);
            }
            namespace_update(&mut subscription.1, false, "unique").await?;
            cancel_namespace(publication.as_mut().context("publication missing")?).await?;
            namespace_update(&mut subscription.1, true, "unique").await?;
            pub_session.close(0, "test complete");
            sub_session.close(0, "test complete");
            let result = relay.verdict().await?;
            assert_eq!(result.message, None);
            assert_eq!(result.sessions[0].role, Some("subscriber"));
            assert_eq!(result.sessions[1].role, Some("publisher"));
        }
    }
    Ok(())
}

#[tokio::test]
async fn subscribe_namespace_lifecycle_requires_matching_namespace_and_explicit_cancel(
) -> Result<()> {
    for wrong_namespace in [false, true] {
        let mut relay = Relay::start("subscribe-namespace-lifecycle", "https").await?;
        let mut drivers = JoinSet::new();
        let (sub_session, _, _, _) = relay.connect(&mut drivers).await?;
        let mut subscription = namespace_subscription(&sub_session, "test/discovery").await?;
        let (pub_session, _, _, _) = relay.connect(&mut drivers).await?;
        let _publication = publish_namespace(
            &pub_session,
            if wrong_namespace {
                "wrong/namespace"
            } else {
                "test/discovery/unique"
            },
        )
        .await?;
        if !wrong_namespace {
            namespace_update(&mut subscription.1, false, "unique").await?;
        }
        pub_session.close(0, "test complete");
        if !wrong_namespace {
            namespace_update(&mut subscription.1, true, "unique").await?;
        }
        sub_session.close(0, "test complete");
        let result = timeout(Duration::from_secs(6), &mut relay.task).await???;
        assert!(result
            .message
            .is_some_and(|message| message.contains(if wrong_namespace {
                "does not match SUBSCRIBE_NAMESPACE prefix"
            } else {
                "did not cancel the PUBLISH_NAMESPACE request"
            })));
    }
    Ok(())
}

#[tokio::test]
async fn publish_namespace_done_fails_control_fin_before_required_withdrawal() -> Result<()> {
    for after_deadline in [false, true] {
        let mut relay = Relay::start("publish-namespace-done", "https").await?;
        let (session, mut control, _peer_control) = connect_raw(&relay).await?;
        let _namespace = publish_namespace(&session, TEST_NAMESPACE).await?;
        if after_deadline {
            tokio::time::sleep(Duration::from_millis(2050)).await;
        }
        control.finish()?;
        tokio::time::sleep(Duration::from_millis(60)).await;
        session.close(0, "test complete");
        let result = relay.verdict().await?;
        assert!(result.message.is_some_and(|message| message.contains("publisher session 1 closed the control stream at the transport layer before withdrawing the namespace") && message.contains("Section 3.3") && !message.contains("fill buffer")), "FIN after deadline: {after_deadline}");
    }
    Ok(())
}
