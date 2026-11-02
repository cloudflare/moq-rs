// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Dynamic-group regression tests over real QUIC request and data streams.

use super::*;
use crate::{
    coding::{Decode, Location, TrackNamespace},
    serve::{self, ServeError},
};
use std::time::Duration;

struct Endpoint {
    publisher: Publisher,
    subscriber: Subscriber,
    driver: tokio::task::JoinHandle<Result<(), SessionError>>,
    wire: web_transport::Session,
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

async fn endpoints() -> (Endpoint, Endpoint) {
    let (client, server) = test_support::loopback_session_pair().await;
    let client_wire = client.clone();
    let server_wire = server.clone();
    let (client, server) = tokio::join!(
        Session::connect(client, None, Transport::WebTransport),
        Session::accept(server, None, Transport::WebTransport),
    );
    let spawn =
        |(session, publisher, subscriber): (Session, Publisher, Subscriber), wire| Endpoint {
            publisher,
            subscriber,
            driver: tokio::spawn(session.run()),
            wire,
        };
    let (session, publisher, subscriber) = server.unwrap();
    (
        spawn(client.unwrap(), client_wire),
        spawn(
            (session, publisher.unwrap(), subscriber.unwrap()),
            server_wire,
        ),
    )
}

// Drive only the publisher side so tests can supply initial PUBLISH_OK params
// and control the peer's request-stream lifecycle directly.
async fn publisher_with_raw_peer() -> (Endpoint, Session) {
    let (client, server) = test_support::loopback_session_pair().await;
    let wire = client.clone();
    let (client, server) = tokio::join!(
        Session::connect(client, None, Transport::WebTransport),
        Session::accept(server, None, Transport::WebTransport),
    );
    let (session, publisher, subscriber) = client.unwrap();
    (
        Endpoint {
            publisher,
            subscriber,
            driver: tokio::spawn(session.run()),
            wire,
        },
        server.unwrap().0,
    )
}

async fn accept_raw_publish(peer: &Session) -> (Writer, Reader, message::Publish) {
    let (send, recv) = peer.webtransport.accept_bi().await.unwrap();
    let mut reader = Reader::new(recv);
    let Message::Publish(publish) = reader.decode().await.unwrap() else {
        panic!("expected PUBLISH")
    };
    (Writer::new(send), reader, publish)
}

async fn send_publish_ok(writer: &mut Writer, id: u64, new_group: Option<u64>) {
    let mut params = KeyValuePairs::default();
    if let Some(value) = new_group {
        params.set_new_group_request(value);
    }
    Session::encode_bidi_response(writer, &message::RequestOk { id, params }.into())
        .await
        .unwrap();
}

async fn assert_another_publish_works(origin: &mut Endpoint, peer: &Session) {
    let (_source, track) =
        serve::Track::new(TrackNamespace::from_utf8_path("dynamic"), "other").produce();
    let mut published = origin
        .publisher
        .publish(track, KeyValuePairs::default())
        .await
        .unwrap();
    let (mut writer, mut reader, request) = accept_raw_publish(peer).await;
    send_publish_ok(&mut writer, request.id, None).await;
    published.ok().await.unwrap();
    drop(published);
    assert!(matches!(
        Session::decode_bidi_response(&mut reader, request.id)
            .await
            .unwrap(),
        Message::PublishDone(_)
    ));
    assert!(reader.done().await.unwrap());
}

fn dynamic_track() -> (
    serve::TrackWriter,
    serve::TrackReader,
    tokio::sync::mpsc::UnboundedReceiver<u64>,
) {
    let (writer, reader) =
        serve::Track::new(TrackNamespace::from_utf8_path("dynamic"), "video").produce();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    writer
        .enable_dynamic_groups(move |value| {
            let tx = tx.clone();
            async move { tx.send(value).map_err(|_| ServeError::Cancel) }
        })
        .unwrap();
    (writer, reader, rx)
}

async fn request_seen(rx: &mut tokio::sync::mpsc::UnboundedReceiver<u64>, expected: u64) {
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .unwrap(),
        Some(expected)
    );
}

fn datagram(group_id: u64) -> serve::Datagram {
    serve::Datagram {
        group_id,
        object_id: 0,
        priority: 128,
        status: crate::data::ObjectStatus::NormalObject,
        end_of_group: true,
        payload: bytes::Bytes::from_static(b"object"),
        extension_headers: Default::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_publish_feedback_survives_relay_subscribe_fanout() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut origin, mut relay_upstream) = endpoints().await;
        let (mut downstream, mut relay_downstream) = endpoints().await;
        let (writer, reader, mut requests) = dynamic_track();
        let published = origin
            .publisher
            .publish(reader, KeyValuePairs::default())
            .await
            .unwrap();
        let mut inbound = relay_upstream.subscriber.publish_received().await.unwrap();
        let upstream_track = inbound.ok(true).unwrap();
        assert_eq!(
            upstream_track
                .track_extensions()
                .unwrap()
                .dynamic_groups()
                .unwrap(),
            Some(true)
        );
        let origin_serving = tokio::spawn(published.serve());

        let (target, received) =
            serve::Track::new(TrackNamespace::from_utf8_path("dynamic"), "video").produce();
        let subscription = downstream.subscriber.subscribe_start(target).await.unwrap();
        let serving = relay_downstream.publisher.subscribed().await.unwrap();
        let relay_serving = tokio::spawn(serving.serve(upstream_track.clone()));
        subscription.ok().await.unwrap();
        assert_eq!(
            subscription
                .subscribe_ok()
                .unwrap()
                .track_extensions
                .dynamic_groups()
                .unwrap(),
            Some(true)
        );

        received.request_new_group(0).await.unwrap();
        request_seen(&mut requests, 0).await;
        // Another downstream request sharing the relay cache must not cause a
        // duplicate upstream request, even via a different reader clone.
        upstream_track.clone().request_new_group(0).await.unwrap();
        assert!(requests.try_recv().is_err());
        received.request_new_group(3).await.unwrap();
        request_seen(&mut requests, 3).await;

        let mut writer = writer.datagrams().unwrap();
        writer.write(datagram(4)).unwrap();
        let serve::TrackReaderMode::Datagrams(mut objects) = received.mode().await.unwrap() else {
            panic!("expected datagrams")
        };
        assert_eq!(objects.read().await.unwrap().unwrap().group_id, 4);
        // The group advance clears outstanding requests. Satisfied nonzero
        // requests are suppressed, but a fresh zero request must go upstream.
        received.request_new_group(3).await.unwrap();
        assert!(requests.try_recv().is_err());
        received.request_new_group(0).await.unwrap();
        request_seen(&mut requests, 0).await;
        assert_eq!(received.largest_location(), Some(Location::new(4, 0)));
        drop(subscription);
        relay_serving.abort();
        origin_serving.abort();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_subscribe_keeps_request_stream_open_for_multiple_updates() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut subscriber, mut publisher) = endpoints().await;
        let (writer, track, mut requests) = dynamic_track();
        let _objects = writer.datagrams().unwrap();
        let (writer, reader) =
            serve::Track::new(TrackNamespace::from_utf8_path("dynamic"), "video").produce();
        let mut params = KeyValuePairs::default();
        params.set_new_group_request(0);
        let subscription = subscriber
            .subscriber
            .subscribe_start_with_params(writer, params)
            .await
            .unwrap();
        let serving = publisher.publisher.subscribed().await.unwrap();
        assert_eq!(serving.params.new_group_request().unwrap(), Some(0));
        let serving = tokio::spawn(serving.serve(track));
        subscription.ok().await.unwrap();
        request_seen(&mut requests, 0).await;
        reader.request_new_group(0).await.unwrap();
        assert!(requests.try_recv().is_err());
        for value in [1, 2, 3] {
            reader.request_new_group(value).await.unwrap();
            request_seen(&mut requests, value).await;
        }
        subscription.unsubscribe().await;
        assert!(matches!(
            serving.await.unwrap(),
            Ok(()) | Err(SessionError::Serve(ServeError::Cancel))
        ));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_feedback_waits_for_application_acceptance() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut origin, mut relay) = endpoints().await;
        let (source, track, mut requests) = dynamic_track();
        let _objects = source.datagrams().unwrap();
        let published = origin
            .publisher
            .publish(track, KeyValuePairs::default())
            .await
            .unwrap();
        let mut inbound = relay.subscriber.publish_received().await.unwrap();
        let track = inbound.take_reader().unwrap();
        let request = track.request_new_group(0);
        tokio::pin!(request);
        assert!(futures::poll!(&mut request).is_pending());
        assert!(requests.try_recv().is_err());
        inbound.accept(true).unwrap();
        let serving = tokio::spawn(published.serve());
        request.await.unwrap();
        request_seen(&mut requests, 0).await;
        serving.abort();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_publish_update_delivers_update_failed_done() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut origin, mut relay) = endpoints().await;
        let (source, track) =
            serve::Track::new(TrackNamespace::from_utf8_path("dynamic"), "video").produce();
        source
            .enable_dynamic_groups(|_| async { Err(ServeError::Cancel) })
            .unwrap();
        let _objects = source.datagrams().unwrap();
        let published = origin
            .publisher
            .publish(track, KeyValuePairs::default())
            .await
            .unwrap();
        let mut inbound = relay.subscriber.publish_received().await.unwrap();
        let track = inbound.ok(true).unwrap();
        let serving = tokio::spawn(published.serve());
        track.request_new_group(0).await.unwrap();
        assert_eq!(
            inbound.closed().await.unwrap_err(),
            ServeError::Closed(message::PublishDoneCode::UpdateFailed as u64)
        );
        assert!(serving.await.unwrap().is_err());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_publish_ok_delivers_new_group_request() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut origin, peer) = publisher_with_raw_peer().await;
        let (_source, track, mut requests) = dynamic_track();
        let mut published = origin
            .publisher
            .publish(track, KeyValuePairs::default())
            .await
            .unwrap();
        let (mut writer, mut reader, request) = accept_raw_publish(&peer).await;
        assert_eq!(
            request.track_extensions.dynamic_groups().unwrap(),
            Some(true)
        );
        send_publish_ok(&mut writer, request.id, Some(0)).await;
        published.ok().await.unwrap();
        request_seen(&mut requests, 0).await;
        drop(published);
        assert!(matches!(
            Session::decode_bidi_response(&mut reader, request.id)
                .await
                .unwrap(),
            Message::PublishDone(_)
        ));
        assert!(reader.done().await.unwrap());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_publish_ok_handler_failure_is_request_local() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut origin, peer) = publisher_with_raw_peer().await;
        let (source, track) =
            serve::Track::new(TrackNamespace::from_utf8_path("dynamic"), "video").produce();
        source
            .enable_dynamic_groups(|_| async { Err(ServeError::Cancel) })
            .unwrap();
        let published = origin
            .publisher
            .publish(track, KeyValuePairs::default())
            .await
            .unwrap();
        let serving = tokio::spawn(published.serve());
        let (mut writer, mut reader, request) = accept_raw_publish(&peer).await;
        send_publish_ok(&mut writer, request.id, Some(1)).await;
        let Message::PublishDone(done) = Session::decode_bidi_response(&mut reader, request.id)
            .await
            .unwrap()
        else {
            panic!("expected PUBLISH_DONE, not a response to PUBLISH_OK")
        };
        assert_eq!(
            done.status_code,
            message::PublishDoneCode::UpdateFailed as u64
        );
        assert!(reader.done().await.unwrap());
        assert!(serving.await.unwrap().is_err());
        assert_another_publish_works(&mut origin, &peer).await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_publish_ok_requires_advertised_dynamic_groups() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut origin, peer) = publisher_with_raw_peer().await;
        let (_source, track) =
            serve::Track::new(TrackNamespace::from_utf8_path("dynamic"), "video").produce();
        let _published = origin
            .publisher
            .publish(track, KeyValuePairs::default())
            .await
            .unwrap();
        let (mut writer, _reader, request) = accept_raw_publish(&peer).await;
        assert_eq!(request.track_extensions.dynamic_groups().unwrap(), None);
        send_publish_ok(&mut writer, request.id, Some(0)).await;
        assert!(matches!(
            (&mut origin.driver).await.unwrap(),
            Err(SessionError::ProtocolViolation(_))
        ));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn speculative_subscribe_feedback_is_ignored_without_dynamic_groups() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut subscriber, mut publisher) = endpoints().await;
        let (source, track) =
            serve::Track::new(TrackNamespace::from_utf8_path("dynamic"), "video").produce();
        let _objects = source.datagrams().unwrap();
        let (writer, _reader) =
            serve::Track::new(TrackNamespace::from_utf8_path("dynamic"), "video").produce();
        let mut params = KeyValuePairs::default();
        params.set_new_group_request(0);
        let subscription = subscriber
            .subscriber
            .subscribe_start_with_params(writer, params)
            .await
            .unwrap();
        let subscribed = publisher.publisher.subscribed().await.unwrap();
        let serving = tokio::spawn(subscribed.serve(track));
        subscription.ok().await.unwrap();
        assert_eq!(
            subscription
                .subscribe_ok()
                .unwrap()
                .track_extensions
                .dynamic_groups()
                .unwrap(),
            None
        );
        subscription.unsubscribe().await;
        assert!(matches!(
            serving.await.unwrap(),
            Ok(()) | Err(SessionError::Serve(ServeError::Cancel))
        ));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_publish_feedback_does_not_block_done_or_peer_stop() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for update in [false, true] {
            for peer_stop in [false, true] {
                let (mut origin, peer) = publisher_with_raw_peer().await;
                let (source, track) =
                    serve::Track::new(TrackNamespace::from_utf8_path("dynamic"), "video").produce();
                let (started, mut requests) = tokio::sync::mpsc::unbounded_channel();
                source
                    .enable_dynamic_groups(move |value| {
                        let started = started.clone();
                        async move {
                            started.send(value).unwrap();
                            std::future::pending::<Result<(), ServeError>>().await
                        }
                    })
                    .unwrap();
                let mut published = origin
                    .publisher
                    .publish(track, KeyValuePairs::default())
                    .await
                    .unwrap();
                let (mut writer, mut reader, request) = accept_raw_publish(&peer).await;
                send_publish_ok(&mut writer, request.id, if update { None } else { Some(0) }).await;
                published.ok().await.unwrap();
                if update {
                    let mut params = KeyValuePairs::default();
                    params.set_new_group_request(0);
                    Session::encode_bidi_response(
                        &mut writer,
                        &message::RequestUpdate {
                            id: 1,
                            existing_request_id: request.id,
                            params,
                        }
                        .into(),
                    )
                    .await
                    .unwrap();
                }
                request_seen(&mut requests, 0).await;
                if peer_stop {
                    reader.stop(CANCELLED_STREAM_CODE);
                    assert_eq!(published.closed().await, Err(ServeError::Cancel));
                } else {
                    published.close(ServeError::Done).unwrap();
                    let Message::PublishDone(done) =
                        Session::decode_bidi_response(&mut reader, request.id)
                            .await
                            .unwrap()
                    else {
                        panic!("pending handler blocked PUBLISH_DONE")
                    };
                    assert_eq!(
                        done.status_code,
                        message::PublishDoneCode::TrackEnded as u64
                    );
                    assert!(reader.done().await.unwrap());
                }
                assert_another_publish_works(&mut origin, &peer).await;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dynamic_group_property_is_validated_before_publish_reservations() {
    let (client, server) = test_support::loopback_session_pair().await;
    let (outgoing, _recv) = Queue::default().split();
    let (tasks, _task_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut subscriber = Subscriber::new(
        outgoing,
        server,
        Transport::WebTransport,
        None,
        RequestId::new(1, 0),
        tasks,
        Default::default(),
    );
    let mut properties = message::TrackExtensions::default();
    properties.set_int_extension(message::extension_type::DYNAMIC_GROUPS, 2);
    let message = message::Publish {
        id: 0,
        track_namespace: TrackNamespace::from_utf8_path("dynamic"),
        track_name: "video".into(),
        track_alias: 12,
        params: KeyValuePairs::default(),
        track_extensions: properties,
    };
    assert!(matches!(
        subscriber.recv_message(message::Publisher::Publish(message)),
        Err(SessionError::Decode(_))
    ));
    // The same name and alias are usable after the invalid property is rejected.
    let mut properties = message::TrackExtensions::default();
    properties.set_dynamic_groups(false);
    subscriber
        .recv_message(message::Publisher::Publish(message::Publish {
            id: 2,
            track_namespace: TrackNamespace::from_utf8_path("dynamic"),
            track_name: "video".into(),
            track_alias: 12,
            params: KeyValuePairs::default(),
            track_extensions: properties,
        }))
        .unwrap();
    let mut inbound = subscriber.publish_received().await.unwrap();
    let reader = inbound.take_reader().unwrap();
    assert_eq!(
        reader.track_extensions().unwrap().dynamic_groups().unwrap(),
        Some(false)
    );
    assert!(matches!(
        reader.request_new_group(0).await,
        Err(ServeError::NotImplemented(_) | ServeError::NotImplementedWithId(_, _))
    ));
    drop(client);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_subscription_update_sends_error_then_update_failed_done() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (client, mut server) = endpoints().await;
        let (send, recv) = client.wire.open_bi().await.unwrap();
        let mut writer = Writer::new(send);
        let mut reader = Reader::new(recv);
        writer
            .write(
                &encode_request_frame(&Message::Subscribe(message::Subscribe {
                    id: 0,
                    track_namespace: TrackNamespace::from_utf8_path("dynamic"),
                    track_name: "video".into(),
                    params: KeyValuePairs::default(),
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        let subscribed = server.publisher.subscribed().await.unwrap();
        let (source, track, _requests) = dynamic_track();
        let _objects = source.datagrams().unwrap();
        let serving = tokio::spawn(subscribed.serve(track));
        assert!(matches!(
            Session::decode_bidi_response(&mut reader, 0).await.unwrap(),
            Message::SubscribeOk(_)
        ));
        let mut params = KeyValuePairs::default();
        params.set_forward(false);
        Session::encode_bidi_response(
            &mut writer,
            &Message::RequestUpdate(message::RequestUpdate {
                id: 2,
                existing_request_id: 0,
                params,
            }),
        )
        .await
        .unwrap();
        assert!(matches!(
            Session::decode_bidi_response(&mut reader, 0).await.unwrap(),
            Message::RequestError(_)
        ));
        let Message::PublishDone(done) =
            Session::decode_bidi_response(&mut reader, 0).await.unwrap()
        else {
            panic!("expected PUBLISH_DONE")
        };
        assert_eq!(
            done.status_code,
            message::PublishDoneCode::UpdateFailed as u64
        );
        let _ = serving.await.unwrap();
    })
    .await
    .unwrap();
}

#[test]
fn request_update_wire_keeps_new_id_and_omits_original_id() {
    let mut params = KeyValuePairs::default();
    params.set_new_group_request(5);
    let frame =
        Session::encode_bidi_response_frame(&Message::RequestUpdate(message::RequestUpdate {
            id: 6,
            existing_request_id: 2,
            params,
        }))
        .unwrap();
    assert_eq!(&frame[..], &[0x02, 0, 4, 6, 1, 0x32, 5]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_drains_update_error_until_publish_done() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut client, mut server) = endpoints().await;
        let (source, track) =
            serve::Track::new(TrackNamespace::from_utf8_path("dynamic"), "video").produce();
        source
            .enable_dynamic_groups(|_| async { Err(ServeError::Cancel) })
            .unwrap();
        let _objects = source.datagrams().unwrap();
        let (writer, reader) =
            serve::Track::new(TrackNamespace::from_utf8_path("dynamic"), "video").produce();
        let subscription = client.subscriber.subscribe_start(writer).await.unwrap();
        let subscribed = server.publisher.subscribed().await.unwrap();
        let serving = tokio::spawn(subscribed.serve(track));
        subscription.ok().await.unwrap();
        reader.request_new_group(0).await.unwrap();
        assert_eq!(
            subscription.closed().await.unwrap_err(),
            ServeError::Closed(message::PublishDoneCode::UpdateFailed as u64)
        );
        assert_eq!(
            subscription.publish_done().unwrap().status_code,
            message::PublishDoneCode::UpdateFailed as u64
        );
        assert!(serving.await.unwrap().is_err());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn track_status_ok_preserves_dynamic_group_properties() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (client, mut server) = endpoints().await;
        let (send, recv) = client.wire.open_bi().await.unwrap();
        let mut writer = Writer::new(send);
        let mut reader = Reader::new(recv);
        writer
            .write(
                &encode_request_frame(&Message::TrackStatus(message::TrackStatus {
                    id: 0,
                    track_namespace: TrackNamespace::from_utf8_path("dynamic"),
                    track_name: "video".into(),
                    params: KeyValuePairs::default(),
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        let status = server.publisher.track_status_requested().await.unwrap();
        let (_source, track, _requests) = dynamic_track();
        status.respond_ok(&track).unwrap();
        assert_eq!(
            reader.decode::<u64>().await.unwrap(),
            message::wire_id::RequestOk
        );
        let length = reader.decode::<u16>().await.unwrap() as usize;
        let mut payload = bytes::BytesMut::new();
        while payload.len() < length {
            payload.extend_from_slice(
                &reader
                    .read_chunk(length - payload.len())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        let mut payload = &payload[..];
        assert!(KeyValuePairs::decode_message_params(&mut payload)
            .unwrap()
            .0
            .is_empty());
        let properties = message::TrackExtensions::decode(&mut payload).unwrap();
        assert_eq!(properties.dynamic_groups().unwrap(), Some(true));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_update_request_id_is_session_fatal() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (client, mut server) = endpoints().await;
        let (send, recv) = client.wire.open_bi().await.unwrap();
        let mut writer = Writer::new(send);
        let mut reader = Reader::new(recv);
        writer
            .write(
                &encode_request_frame(&Message::Subscribe(message::Subscribe {
                    id: 0,
                    track_namespace: TrackNamespace::from_utf8_path("dynamic"),
                    track_name: "video".into(),
                    params: KeyValuePairs::default(),
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        let subscribed = server.publisher.subscribed().await.unwrap();
        let (_source, track, mut requests) = dynamic_track();
        let serving = tokio::spawn(subscribed.serve(track));
        assert!(matches!(
            Session::decode_bidi_response(&mut reader, 0).await.unwrap(),
            Message::SubscribeOk(_)
        ));
        let mut params = KeyValuePairs::default();
        params.set_new_group_request(0);
        let update = Message::RequestUpdate(message::RequestUpdate {
            id: 2,
            existing_request_id: 0,
            params,
        });
        Session::encode_bidi_response(&mut writer, &update)
            .await
            .unwrap();
        assert!(matches!(
            Session::decode_bidi_response(&mut reader, 0).await.unwrap(),
            Message::RequestOk(_)
        ));
        request_seen(&mut requests, 0).await;
        Session::encode_bidi_response(&mut writer, &update)
            .await
            .unwrap();
        assert!(matches!(
            (&mut server.driver).await.unwrap(),
            Err(SessionError::InvalidRequestId)
        ));
        serving.abort();
    })
    .await
    .unwrap();
}
