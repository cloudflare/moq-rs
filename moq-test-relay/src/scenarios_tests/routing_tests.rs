// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

#[tokio::test]
async fn announce_subscribe_fails_downstream_cancellation_before_publisher_answer() -> Result<()> {
    for (scheme, fin) in [("https", false), ("moqt", false), ("https", true)] {
        let mut relay = Relay::start("announce-subscribe", scheme).await?;
        let (pub_session, _pub_control, _pub_peer_control) = connect_raw(&relay).await?;
        let _namespace = publish_namespace(&pub_session, TEST_NAMESPACE).await?;
        let (sub_session, mut sub_control, _sub_peer_control) = connect_raw(&relay).await?;
        let mut downstream = send_request(
            &sub_session,
            message::Subscribe {
                id: 0,
                track_namespace: TrackNamespace::from_utf8_path(TEST_NAMESPACE),
                track_name: TEST_TRACK.into(),
                params: KeyValuePairs::default(),
            }
            .into(),
        )
        .await?;
        let (mut upstream_send, mut upstream_recv) =
            timeout(Duration::from_secs(1), pub_session.accept_bi()).await??;
        let (kind, payload) = read_frame(&mut upstream_recv).await?;
        assert_eq!(kind, message::wire_id::Subscribe);
        let request = message::Subscribe::decode(&mut payload.as_slice())?;
        // Wait for the relay to process cancellation, keeping both sessions open.
        downstream.0.reset(1);
        let closed = timeout(Duration::from_secs(1), downstream.1.read(1024)).await?;
        assert!(matches!(closed, Err(_) | Ok(None)));
        let mut payload = Vec::new();
        request.id.encode(&mut payload)?;
        KeyValuePairs::default().encode_message_params(&mut payload)?;
        message::TrackExtensions::default().encode(&mut payload)?;
        let mut response = Vec::new();
        message::wire_id::SubscribeOk.encode(&mut response)?;
        (payload.len() as u16).encode(&mut response)?;
        response.extend_from_slice(&payload);
        // A cancelled upstream request may reject this delayed acceptance.
        let _ = write_bytes(&mut upstream_send, &response).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        if fin {
            // The cancelled SUBSCRIBE was never answered, so this FIN comes
            // before the subscriber's response step (Section 3.3).
            sub_control.finish()?;
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        pub_session.close(0, "test complete");
        sub_session.close(0, "test complete");
        let result = relay.verdict().await?;
        let message = result.message.unwrap_or_default();
        assert!(
            message.contains(
                "subscriber cancelled SUBSCRIBE before receiving SUBSCRIBE_OK or REQUEST_ERROR"
            ),
            "{scheme}, FIN {fin}: {message}"
        );
        if fin {
            assert!(
                message.contains("subscriber session 2 closed the control stream at the transport layer before the SUBSCRIBE response was sent"),
                "{scheme}: {message}"
            );
            assert!(!result
                .comments
                .iter()
                .any(|comment| comment.starts_with("control-stream FIN on session 2")));
        }
    }
    Ok(())
}

#[tokio::test]
async fn announce_subscribe_routes_after_request_ok_and_forwards_parameters() -> Result<()> {
    for scheme in ["https", "moqt"] {
        timeout(Duration::from_secs(6), async {
            let mut relay = Relay::start("announce-subscribe", scheme).await?;
            let mut drivers = JoinSet::new();
            let (pub_session, mut publisher, _, pub_cid) = relay.connect(&mut drivers).await?;
            let _namespace = publish_namespace(&pub_session, TEST_NAMESPACE).await?;
            let (sub_session, _, mut subscriber, sub_cid) = relay.connect(&mut drivers).await?;
            let (writer, _reader) =
                Track::new(TrackNamespace::from_utf8_path(TEST_NAMESPACE), TEST_TRACK).produce();
            let (pub_writer, pub_reader) =
                Track::new(TrackNamespace::from_utf8_path(TEST_NAMESPACE), TEST_TRACK).produce();
            let mut params = KeyValuePairs::default();
            params.set_new_group_request(12);
            params.set_subscriber_priority(31);
            params.set_group_order(GroupOrder::Descending);
            params.set_forward(false);
            let largest = Location::new(7, 8);
            let upstream = async {
                let request = publisher
                    .subscribed()
                    .await
                    .context("publisher never received forwarded SUBSCRIBE")?;
                assert_eq!(
                    request.track_namespace,
                    TrackNamespace::from_utf8_path(TEST_NAMESPACE)
                );
                assert_eq!(request.track_name.as_ref(), TEST_TRACK.as_bytes());
                assert_eq!(request.params.new_group_request()?, Some(12));
                assert_eq!(request.subscriber_priority, 31);
                assert_eq!(request.group_order, GroupOrder::Descending);
                assert!(!request.forward);
                drivers.spawn(request.serve_with_largest_location(pub_reader, Some(largest)));
                Ok::<_, anyhow::Error>(())
            };
            let (accepted, subscribed) = tokio::join!(
                upstream,
                subscriber.subscribe_open_with_params(writer, params)
            );
            accepted?;
            let subscribed = subscribed?;
            assert_eq!(
                subscribed
                    .subscribe_ok()
                    .context("subscriber did not get SUBSCRIBE_OK")?
                    .params
                    .largest_object()?,
                Some(largest)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                !relay.task.is_finished(),
                "relay must keep serving while sessions remain open"
            );
            pub_session.close(0, "test complete");
            sub_session.close(0, "test complete");
            let result = relay.verdict().await?;
            assert_eq!(result.message, None);
            assert_eq!(result.sessions.len(), 2);
            assert_eq!(result.sessions[0].role, Some("publisher"));
            assert_eq!(result.sessions[1].role, Some("subscriber"));
            assert_eq!(result.sessions[0].connection_id, pub_cid);
            assert_eq!(result.sessions[1].connection_id, sub_cid);
            println!(
                "# ordered routing over {scheme}\n{}",
                crate::tap::render(
                    registry::find("announce-subscribe", None).context("entry missing")?,
                    &result
                )?
            );
            drop((subscribed, pub_writer));
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    }
    Ok(())
}

#[tokio::test]
async fn announce_subscribe_rejects_before_namespace_without_holding() -> Result<()> {
    for scheme in ["https", "moqt"] {
        let mut relay = Relay::start("announce-subscribe", scheme).await?;
        let mut drivers = JoinSet::new();
        let (sub_session, _, mut subscriber, _) = relay.connect(&mut drivers).await?;
        let (writer, _reader) =
            Track::new(TrackNamespace::from_utf8_path(TEST_NAMESPACE), TEST_TRACK).produce();
        let answer = timeout(Duration::from_secs(1), subscriber.subscribe_open(writer)).await?;
        assert!(
            matches!(
                answer,
                Err(ServeError::NotFound | ServeError::NotFoundWithId(_, _))
            ),
            "zero-timeout request must get DOES_NOT_EXIST before any namespace arrives"
        );
        let (pub_session, _, _, _) = relay.connect(&mut drivers).await?;
        let _namespace = publish_namespace(&pub_session, TEST_NAMESPACE).await?;
        pub_session.close(0, "test complete");
        sub_session.close(0, "test complete");
        let result = relay.verdict().await?;
        println!(
            "# reverse ordering over {scheme}\n{}",
            crate::tap::render(
                registry::find("announce-subscribe", None).context("entry missing")?,
                &result
            )?
        );
        let message = result
            .message
            .context("early subscription incorrectly passed")?;
        assert!(
            message.contains("sent DOES_NOT_EXIST (draft-18 Section 10.2.6)"),
            "{message}"
        );
        assert!(
            message.contains("PUBLISH_NAMESPACE received at")
                && message.contains("SUBSCRIBE received at"),
            "{message}"
        );
        assert!(
            !message.contains("not received"),
            "both messages arrived: {message}"
        );
        assert_eq!(result.sessions[0].role, Some("subscriber"));
        assert_eq!(result.sessions[1].role, Some("publisher"));
    }
    Ok(())
}

#[tokio::test]
async fn announce_subscribe_keeps_serving_after_deadline_and_counts_late_session_errors(
) -> Result<()> {
    for code in [0, 42] {
        let mut relay =
            Relay::start_with_wait("announce-subscribe", "https", Duration::from_secs(5)).await?;
        let mut drivers = JoinSet::new();
        let (pub_session, mut publisher, _, _) = relay.connect(&mut drivers).await?;
        let _namespace = publish_namespace(&pub_session, TEST_NAMESPACE).await?;
        let (sub_session, _, mut subscriber, _) = relay.connect(&mut drivers).await?;
        let (writer, _reader) = track(TEST_NAMESPACE, TEST_TRACK);
        let (pub_writer, pub_reader) = track(TEST_NAMESPACE, TEST_TRACK);
        let upstream = async {
            drivers.spawn(
                publisher
                    .subscribed()
                    .await
                    .context("upstream missing")?
                    .serve(pub_reader),
            );
            Ok::<_, anyhow::Error>(())
        };
        let (served, accepted) = tokio::join!(upstream, subscriber.subscribe_open(writer));
        served?;
        let _accepted = accepted?;
        tokio::time::sleep(Duration::from_millis(3150)).await;
        assert!(
            !relay.task.is_finished(),
            "catalog deadline must not stop serving"
        );
        pub_session.close(0, "test complete");
        sub_session.close(code, "late test close");
        let result = relay.verdict().await?;
        assert!(
            result.duration < Duration::from_secs(3),
            "duration ends at completion, not shutdown"
        );
        if code == 0 {
            assert_eq!(result.message, None);
        } else {
            assert!(result
                .message
                .is_some_and(|message| message.contains("session 2 ended with error or reset")));
        }
        drop(pub_writer);
    }
    Ok(())
}

#[tokio::test]
async fn announce_subscribe_fails_publisher_control_fin_before_upstream_answer() -> Result<()> {
    let mut relay = Relay::start("announce-subscribe", "https").await?;
    let (pub_session, mut control, _peer_control) = connect_raw(&relay).await?;
    let _namespace = publish_namespace(&pub_session, TEST_NAMESPACE).await?;
    let mut drivers = JoinSet::new();
    let (sub_session, _, mut subscriber, _) = relay.connect(&mut drivers).await?;
    let (writer, _reader) = track(TEST_NAMESPACE, TEST_TRACK);
    let subscribe = subscriber.subscribe_open(writer);
    tokio::pin!(subscribe);
    assert!(timeout(Duration::from_millis(60), &mut subscribe)
        .await
        .is_err());
    let (_upstream_send, mut upstream_recv) = pub_session.accept_bi().await?;
    assert_eq!(
        read_frame(&mut upstream_recv).await?.0,
        message::wire_id::Subscribe
    );
    control.finish()?;
    tokio::time::sleep(Duration::from_millis(60)).await;
    pub_session.close(0, "publisher stopped early");
    assert!(timeout(Duration::from_secs(1), &mut subscribe)
        .await?
        .is_err());
    sub_session.close(0, "test complete");
    let result = relay.verdict().await?;
    assert!(
        result.message.as_ref().is_some_and(|message| message
            .contains("publisher session 1 closed the control stream")
            && message.contains("before the forwarded SUBSCRIBE was answered")
            && message.contains("Section 3.3")),
        "{:?}",
        result.message
    );
    Ok(())
}

#[tokio::test]
async fn announce_subscribe_forwards_upstream_track_properties() -> Result<()> {
    let mut relay = Relay::start("announce-subscribe", "https").await?;
    let (pub_session, _control, _peer_control) = connect_raw(&relay).await?;
    let _namespace = publish_namespace(&pub_session, TEST_NAMESPACE).await?;
    let mut drivers = JoinSet::new();
    let (sub_session, _, mut subscriber, _) = relay.connect(&mut drivers).await?;
    let (writer, _reader) = track(TEST_NAMESPACE, TEST_TRACK);
    let mut properties = message::TrackExtensions::default();
    properties.set_int_extension(0x300, 7);
    let upstream = async {
        let (mut send, mut recv) = pub_session.accept_bi().await?;
        let (kind, payload) = read_frame(&mut recv).await?;
        assert_eq!(kind, message::wire_id::Subscribe);
        let request = message::Subscribe::decode(&mut payload.as_slice())?;
        let mut payload = Vec::new();
        request.id.encode(&mut payload)?;
        KeyValuePairs::default().encode_message_params(&mut payload)?;
        properties.encode(&mut payload)?;
        let mut response = Vec::new();
        // Draft-18 Section 10.8: bidi SUBSCRIBE_OK omits Request ID;
        // this first varint is the upstream Track Alias.
        message::wire_id::SubscribeOk.encode(&mut response)?;
        (payload.len() as u16).encode(&mut response)?;
        response.extend_from_slice(&payload);
        write_bytes(&mut send, &response).await?;
        Ok::<_, anyhow::Error>((send, recv))
    };
    let (upstream, accepted) = timeout(Duration::from_secs(2), async {
        tokio::join!(upstream, subscriber.subscribe_open(writer))
    })
    .await?;
    let _upstream = upstream?;
    let accepted = accepted?;
    // Draft 18 Sections 9.6 and 2.5: forward the publisher's properties unchanged.
    assert_eq!(
        accepted
            .subscribe_ok()
            .context("SUBSCRIBE_OK missing")?
            .track_extensions,
        properties
    );
    pub_session.close(0, "test complete");
    sub_session.close(0, "test complete");
    let result = relay.verdict().await?;
    assert_eq!(result.message, None);
    Ok(())
}

#[tokio::test]
async fn announce_subscribe_preserves_publisher_rejection_code() -> Result<()> {
    let mut relay = Relay::start("announce-subscribe", "https").await?;
    let mut drivers = JoinSet::new();
    let (pub_session, mut publisher, _, _) = relay.connect(&mut drivers).await?;
    let _namespace = publish_namespace(&pub_session, TEST_NAMESPACE).await?;
    let (sub_session, _, mut subscriber, _) = relay.connect(&mut drivers).await?;
    let (writer, _reader) =
        Track::new(TrackNamespace::from_utf8_path(TEST_NAMESPACE), TEST_TRACK).produce();
    let reject = async {
        publisher
            .subscribed()
            .await
            .context("forwarded request missing")?
            .close(ServeError::Closed(0x42))?;
        Ok::<_, anyhow::Error>(())
    };
    let (rejected, answer) = timeout(Duration::from_secs(4), async {
        tokio::join!(reject, subscriber.subscribe_open(writer))
    })
    .await
    .context("publisher rejection was not forwarded")?;
    rejected?;
    assert!(matches!(answer, Err(ServeError::Closed(0x42))));
    pub_session.close(0, "test complete");
    sub_session.close(0, "test complete");
    let result = relay.verdict().await?;
    assert!(result.message.is_some_and(
        |message| message.contains("publisher rejected") && message.contains("code=66")
    ));
    Ok(())
}

#[tokio::test]
async fn announce_subscribe_names_wrong_namespace_and_track() -> Result<()> {
    for (namespace, track, expected) in [
        (
            "wrong/namespace",
            TEST_TRACK,
            "PUBLISH_NAMESPACE named /wrong/namespace",
        ),
        (
            TEST_NAMESPACE,
            "wrong-track",
            "SUBSCRIBE named /moq-test/interop/wrong-track",
        ),
    ] {
        let mut relay = Relay::start("announce-subscribe", "https").await?;
        let mut drivers = JoinSet::new();
        let (pub_session, _, _, _) = relay.connect(&mut drivers).await?;
        let _namespace = publish_namespace(&pub_session, namespace).await?;
        let (sub_session, _, mut subscriber, _) = relay.connect(&mut drivers).await?;
        let (writer, _reader) =
            Track::new(TrackNamespace::from_utf8_path(namespace), track).produce();
        assert!(
            timeout(Duration::from_secs(2), subscriber.subscribe_open(writer))
                .await
                .context("wrong-name SUBSCRIBE was not answered")?
                .is_err()
        );
        pub_session.close(0, "test complete");
        sub_session.close(0, "test complete");
        let result = relay.verdict().await?;
        assert!(
            result
                .message
                .as_ref()
                .is_some_and(|message| message.contains(expected)),
            "expected {expected}: {:?}",
            result.message
        );
    }
    Ok(())
}
