// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

#[tokio::test]
async fn subscribe_error_comments_on_later_subscribe_without_judging_its_name() -> Result<()> {
    let mut relay = Relay::start("subscribe-error", "https").await?;
    let mut drivers = JoinSet::new();
    let (session, _, mut subscriber, _) = relay.connect(&mut drivers).await?;
    for namespace in ["nonexistent/namespace", "other/namespace"] {
        let (writer, _reader) = track(namespace, TEST_TRACK);
        assert!(
            timeout(Duration::from_secs(1), subscriber.subscribe_open(writer))
                .await?
                .is_err()
        );
    }
    session.close(0, "test complete");
    let result = relay.verdict().await?;
    assert_eq!(result.message, None);
    assert!(result
        .comments
        .iter()
        .any(|comment| comment == "unexpected request: SUBSCRIBE on session 1"));
    Ok(())
}

#[tokio::test]
async fn subscribe_error_and_rendezvous_timeout_answer_with_draft18_codes() -> Result<()> {
    for scheme in ["https", "moqt"] {
        for (test, namespace, requested) in [
            ("subscribe-error", "nonexistent/namespace", None),
            ("subscribe-error", "nonexistent/namespace", Some(0)),
            ("subscribe-error", "nonexistent/namespace", Some(80)),
            ("rendezvous-timeout", "nonexistent/rendezvous", Some(500)),
            ("rendezvous-timeout", "nonexistent/rendezvous", Some(1)),
            (
                "rendezvous-timeout",
                "nonexistent/rendezvous",
                Some(u64::MAX),
            ),
            ("rendezvous-timeout", "nonexistent/rendezvous", None),
            ("rendezvous-timeout", "nonexistent/rendezvous", Some(0)),
        ] {
            let mut relay = Relay::start(test, scheme).await?;
            let mut drivers = JoinSet::new();
            let (session, _, mut subscriber, _) = relay.connect(&mut drivers).await?;
            let mut params = KeyValuePairs::default();
            if let Some(requested) = requested {
                params.set_rendezvous_timeout(requested);
            }
            let (writer, _reader) = track(namespace, TEST_TRACK);
            let started = tokio::time::Instant::now();
            let answer = timeout(
                Duration::from_secs(2),
                subscriber.subscribe_open_with_params(writer, params),
            )
            .await?;
            if requested.unwrap_or(0) == 0 {
                assert!(
                    matches!(
                        answer,
                        Err(ServeError::NotFound | ServeError::NotFoundWithId(_, _))
                    ),
                    "{test} over {scheme}, requested {requested:?}: {:?}",
                    answer.as_ref().err()
                );
                assert!(
                    started.elapsed() < Duration::from_millis(500),
                    "zero/absent timeout must not be held"
                );
            } else {
                assert!(matches!(answer, Err(ServeError::Timeout)));
                if requested == Some(80) || requested == Some(500) {
                    assert!(
                        started.elapsed() >= Duration::from_millis(requested.unwrap_or(0)),
                        "requested nonzero hold must be exercised"
                    );
                }
            }
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "hold must stay within the catalog deadline"
            );
            session.close(0, "test complete");
            let result = relay.verdict().await?;
            if test == "rendezvous-timeout" && requested != Some(500) {
                let expected = format!(
                    "RENDEZVOUS_TIMEOUT requested {} ms, expected 500 ms",
                    requested.unwrap_or(0)
                );
                assert!(
                    result
                        .message
                        .as_ref()
                        .is_some_and(|message| message.contains(&expected)),
                    "requested {requested:?}: {:?}",
                    result.message
                );
            } else {
                assert_eq!(result.message, None);
            }
            assert!(result
                .comments
                .iter()
                .any(|comment| comment
                    .contains(&format!("requested: {} ms", requested.unwrap_or(0)))));
        }
    }
    Ok(())
}

#[tokio::test]
async fn subscribe_error_and_rendezvous_timeout_name_wrong_track() -> Result<()> {
    for (test, namespace) in [
        ("subscribe-error", "nonexistent/namespace"),
        ("rendezvous-timeout", "nonexistent/rendezvous"),
    ] {
        let mut relay = Relay::start(test, "https").await?;
        let mut drivers = JoinSet::new();
        let (session, _, mut subscriber, _) = relay.connect(&mut drivers).await?;
        let mut params = KeyValuePairs::default();
        params.set_rendezvous_timeout(500);
        let (writer, _reader) = track(namespace, "wrong-track");
        assert!(subscriber
            .subscribe_open_with_params(writer, params)
            .await
            .is_err());
        session.close(0, "test complete");
        assert!(
            relay.verdict().await?.message.is_some_and(
                |message| message.contains("wrong-track") && message.contains("expected")
            )
        );
    }
    Ok(())
}

#[tokio::test]
async fn subscribe_before_announce_reject_and_unavailable_hold_follow_timeout_parameter(
) -> Result<()> {
    for scheme in ["https", "moqt"] {
        for (variant, requested) in [
            ("reject", None),
            ("reject", Some(0)),
            ("reject", Some(1000)),
            ("hold", None),
            ("hold", Some(0)),
            ("hold", Some(80)),
        ] {
            let test = format!("subscribe-before-announce@{variant}");
            let mut relay = Relay::start(&test, scheme).await?;
            let mut drivers = JoinSet::new();
            let (sub_session, _, mut subscriber, _) = relay.connect(&mut drivers).await?;
            let mut params = KeyValuePairs::default();
            if let Some(requested) = requested {
                params.set_rendezvous_timeout(requested);
            }
            let (writer, _reader) = track(TEST_NAMESPACE, TEST_TRACK);
            let started = tokio::time::Instant::now();
            let answer = timeout(
                Duration::from_millis(500),
                subscriber.subscribe_open_with_params(writer, params),
            )
            .await?;
            if requested.unwrap_or(0) == 0 {
                assert!(
                    matches!(
                        answer,
                        Err(ServeError::NotFound | ServeError::NotFoundWithId(_, _))
                    ),
                    "{test} over {scheme}, requested {requested:?}: {:?}",
                    answer.as_ref().err()
                );
            } else {
                assert!(
                    matches!(answer, Err(ServeError::Timeout)),
                    "{test} over {scheme}, requested {requested:?}: {:?}",
                    answer.as_ref().err()
                );
            }
            if variant == "reject" {
                assert!(started.elapsed() < Duration::from_millis(300));
            }
            let (pub_session, _, _, _) = relay.connect(&mut drivers).await?;
            let _namespace = publish_namespace(&pub_session, TEST_NAMESPACE).await?;
            sub_session.close(0, "test complete");
            pub_session.close(0, "test complete");
            let result = relay.verdict().await?;
            assert_eq!(result.message, None);
            if variant == "hold" {
                let reason = result
                    .skip
                    .as_deref()
                    .context("unavailable hold must SKIP")?;
                assert!(reason.contains(if requested.unwrap_or(0) == 0 {
                    "hold needs a nonzero RENDEZVOUS_TIMEOUT"
                } else {
                    "80 ms expired"
                }));
                let entry = registry::find("subscribe-before-announce", Some(variant))
                    .context("entry missing")?;
                let tap = crate::tap::render(entry, &result)?;
                assert!(tap.contains("ok 1 - subscribe-before-announce # SKIP"));
                assert!(
                    tap.contains("variant: \"hold\"") && tap.contains("moqt_version: \"moqt-18\"")
                );
            } else {
                assert_eq!(result.skip, None);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn subscribe_before_announce_reject_requires_separate_publisher_session() -> Result<()> {
    let mut relay = Relay::start("subscribe-before-announce@reject", "https").await?;
    let mut drivers = JoinSet::new();
    let (session, _, mut subscriber, _) = relay.connect(&mut drivers).await?;
    let (writer, _reader) = track(TEST_NAMESPACE, TEST_TRACK);
    assert!(matches!(
        subscriber.subscribe_open(writer).await,
        Err(ServeError::NotFound | ServeError::NotFoundWithId(_, _))
    ));
    let (_writer, mut reader) = send_request(
        &session,
        message::PublishNamespace {
            id: 2,
            track_namespace: TrackNamespace::from_utf8_path(TEST_NAMESPACE),
            params: KeyValuePairs::default(),
        }
        .into(),
    )
    .await?;
    assert_eq!(
        read_frame(&mut reader).await?.0,
        message::wire_id::RequestOk
    );
    session.close(0, "test complete");
    assert!(relay.verdict().await?.message.is_some_and(
        |message| message.contains("PUBLISH_NAMESPACE arrived on the subscriber session")
    ));
    Ok(())
}

#[tokio::test]
async fn subscribe_before_announce_hold_routes_when_publisher_appears() -> Result<()> {
    for scheme in ["https", "moqt"] {
        let mut relay = Relay::start("subscribe-before-announce@hold", scheme).await?;
        let mut drivers = JoinSet::new();
        let (sub_session, _, mut subscriber, _) = relay.connect(&mut drivers).await?;
        let mut params = KeyValuePairs::default();
        params.set_rendezvous_timeout(2000);
        params.set_new_group_request(12);
        let (writer, _reader) = track(TEST_NAMESPACE, TEST_TRACK);
        let subscribe = subscriber.subscribe_open_with_params(writer, params);
        tokio::pin!(subscribe);
        assert!(
            timeout(Duration::from_millis(80), &mut subscribe)
                .await
                .is_err(),
            "nonzero rendezvous must remain pending"
        );
        let (pub_session, mut publisher, _, _) = relay.connect(&mut drivers).await?;
        let _namespace = publish_namespace(&pub_session, TEST_NAMESPACE).await?;
        let (pub_writer, pub_reader) =
            Track::new(TrackNamespace::from_utf8_path(TEST_NAMESPACE), TEST_TRACK).produce();
        let upstream = publisher
            .subscribed()
            .await
            .context("held request was not routed")?;
        assert_eq!(upstream.params.new_group_request()?, Some(12));
        drivers.spawn(upstream.serve(pub_reader));
        let accepted = timeout(Duration::from_secs(1), &mut subscribe).await??;
        assert!(accepted.subscribe_ok().is_some());
        sub_session.close(0, "test complete");
        pub_session.close(0, "test complete");
        let result = relay.verdict().await?;
        assert_eq!(result.message, None);
        assert_eq!(result.skip, None);
        drop(pub_writer);
    }
    Ok(())
}

#[tokio::test]
async fn subscribe_before_announce_names_both_arrivals_when_publisher_is_first() -> Result<()> {
    for variant in ["hold", "reject"] {
        let mut relay =
            Relay::start(&format!("subscribe-before-announce@{variant}"), "https").await?;
        let mut drivers = JoinSet::new();
        let (pub_session, mut publisher, _, _) = relay.connect(&mut drivers).await?;
        let _namespace = publish_namespace(&pub_session, TEST_NAMESPACE).await?;
        let (sub_session, _, mut subscriber, _) = relay.connect(&mut drivers).await?;
        let (pub_writer, pub_reader) =
            Track::new(TrackNamespace::from_utf8_path(TEST_NAMESPACE), TEST_TRACK).produce();
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
        let (writer, _reader) = track(TEST_NAMESPACE, TEST_TRACK);
        let (served, accepted) = tokio::join!(upstream, subscriber.subscribe_open(writer));
        served?;
        let _accepted = accepted?;
        sub_session.close(0, "test complete");
        pub_session.close(0, "test complete");
        let result = relay.verdict().await?;
        let message = result
            .message
            .context("publisher-first incorrectly passed")?;
        assert!(
            message.contains("PUBLISH_NAMESPACE arrived before SUBSCRIBE")
                && message.contains("PUBLISH_NAMESPACE received at")
                && message.contains("SUBSCRIBE received at"),
            "{message}"
        );
        drop(pub_writer);
    }
    Ok(())
}

#[tokio::test]
async fn subscribe_before_announce_reject_requires_graceful_close_after_subscriber_control_fin(
) -> Result<()> {
    for scheme in ["https", "moqt"] {
        for close_code in [Some(0), None, Some(42)] {
            let mut relay = Relay::start("subscribe-before-announce@reject", scheme).await?;
            let (sub_session, mut control, _peer_control) = connect_raw(&relay).await?;
            let (_writer, mut reader) = send_request(
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
            let (kind, payload) = read_frame(&mut reader).await?;
            assert_eq!(kind, message::wire_id::RequestError);
            assert_eq!(
                u64::decode(&mut payload.as_slice())?,
                message::RequestErrorCode::DoesNotExist as u64
            );
            control.finish()?;
            tokio::time::sleep(Duration::from_millis(60)).await;
            if let Some(code) = close_code {
                sub_session.close(code, "subscriber complete");
            }
            let mut drivers = JoinSet::new();
            let (pub_session, _, _, _) = relay.connect(&mut drivers).await?;
            let _namespace = publish_namespace(&pub_session, TEST_NAMESPACE).await?;
            pub_session.close(0, "publisher complete");
            let result = relay.verdict().await?;
            if close_code == Some(0) {
                assert_eq!(result.message, None);
            } else {
                assert!(
                    result.message.as_ref().is_some_and(|message| message
                        .contains("subscriber session 1")
                        && message.contains("FIN received at")
                        && message.contains("no graceful connection close followed before TAP")
                        && message.contains("Section 3.3")),
                    "{scheme}: {:?}",
                    result.message
                );
                if close_code.is_none() {
                    assert!(result
                        .comments
                        .iter()
                        .any(|comment| comment.contains("safety cap reached")));
                }
            }
            assert!(result.comments.iter().any(|comment| comment
                .contains("control-stream FIN on session 1 (subscriber)")
                && comment.contains("Section 3.3")));
        }
    }
    Ok(())
}
