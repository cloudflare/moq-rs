// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use moq_transport::{
    message::RequestErrorCode,
    serve::ServeError,
    session::{Session, SessionError, Transport},
};
use tokio::{sync::mpsc, time::Instant};

use crate::{registry::Test, routing::Routing, run::Event};

pub async fn session_task(
    id: usize,
    transport_session: web_transport::Session,
    transport: Transport,
    test: Test,
    first_session: Instant,
    routing: Routing,
    tx: mpsc::Sender<Event>,
) -> Event {
    let close = transport_session.clone();
    let (session, publisher, subscriber) = match Session::accept(transport_session, None, transport)
        .await
    {
        Ok(parts) => parts,
        Err(err) => {
            if !err.is_graceful_close() {
                let message = if matches!(
                    err,
                    SessionError::Decode(moq_transport::coding::DecodeError::More(_))
                ) {
                    let received = first_session.elapsed();
                    let _ = tx.send(Event::ControlFinished(id, received)).await;
                    format!("client closed the control stream before completing SETUP; FIN received at {} us after first session (draft 18 Section 3.3)", received.as_micros())
                } else {
                    format!("SETUP exchange failed: {err}")
                };
                let _ = tx.send(Event::DriverFailed(id, message)).await;
                close.close(
                    u32::try_from(err.code()).unwrap_or(1),
                    "SETUP exchange failed",
                );
            }
            return Event::Ended(id, Err(SessionError::from(close.closed().await)));
        }
    };
    let (Some(mut publisher), Some(mut subscriber)) = (publisher, subscriber) else {
        return Event::Ended(id, Err(SessionError::Internal));
    };
    publisher.accept_fetch();
    if tx.send(Event::SetupComplete(id)).await.is_err() {
        return Event::RoutingFinished;
    }
    let mut namespace_requests = publisher.clone();
    let mut status_requests = publisher.clone();
    let mut track_requests = publisher.clone();
    let mut fetch_requests = publisher.clone();
    let mut publish_requests = subscriber.clone();
    let running = session.run();
    tokio::pin!(running);
    loop {
        let event = tokio::select! {
            ended = close.closed() => return Event::Ended(id, Err(SessionError::from(ended))),
            result = &mut running => {
                let result = match result {
                    Ok(()) => Err(SessionError::from(close.closed().await)),
                    // In the pinned driver, Reader::decode propagates Decode(More)
                    // when the control stream FINs. Judge the completed role steps
                    // separately from the later transport close (Section 3.3).
                    Err(SessionError::Decode(moq_transport::coding::DecodeError::More(_))) => {
                        let _ = tx.send(Event::ControlFinished(id, first_session.elapsed())).await;
                        Err(SessionError::from(close.closed().await))
                    },
                    Err(err) => {
                        if !err.is_graceful_close() {
                            let _ = tx.send(Event::DriverFailed(id, err.to_string())).await;
                        }
                        // Driver termination is not transport termination. Retain
                        // the error, but keep the session open until the peer
                        // closes it, SIGTERM arrives, or the safety cap expires.
                        Err(SessionError::from(close.closed().await))
                    }
                };
                return Event::Ended(id, result);
            }
            Some(mut published) = subscriber.published_namespace() => {
                let received = first_session.elapsed();
                tracing::info!(session = id + 1, received_us = received.as_micros(), namespace = %published.namespace, "received PUBLISH_NAMESPACE");
                if test.needs_namespace() {
                    // Register before crossing the observation queue so an
                    // already received namespace is available to other sessions.
                    match routing.register(id, &mut published, subscriber.clone()).await {
                        Ok(notified) => Event::Namespace(id, published, received, notified),
                        Err(err) => Event::NamespaceFailed(id, format!("could not answer PUBLISH_NAMESPACE: {err}"), received),
                    }
                } else {
                    let _ = published.reject(RequestErrorCode::Uninterested as u64, "namespace not served by this test");
                    Event::Unexpected(id, "PUBLISH_NAMESPACE")
                }
            }
            Some(subscribed) = publisher.subscribed() => {
                let received = first_session.elapsed();
                tracing::info!(session = id + 1, received_us = received.as_micros(), namespace = %subscribed.track_namespace, track = %subscribed.track_name, "received SUBSCRIBE");
                if test.needs_subscribe() {
                    // Snapshot at receipt. A namespace arriving while the event
                    // is queued must not turn a zero-timeout early request into
                    // a held request (draft-18 Section 10.2.6).
                    let route = routing.publisher(&subscribed.track_namespace).await;
                    Event::Subscribe(id, Box::new(subscribed), received, route.map(Box::new))
                } else {
                    let _ = subscribed.close(ServeError::NotImplemented("track not served by this test".to_owned()));
                    Event::Unexpected(id, "SUBSCRIBE")
                }
            }
            Some(request) = namespace_requests.subscribed_namespace() => {
                if test == Test::SubscribeNamespaceLifecycle {
                    Event::SubscribeNamespace(id, request)
                } else {
                    let _ = request.reject(RequestErrorCode::NotSupported as u64, "namespace discovery not served by this test");
                    Event::Unexpected(id, "SUBSCRIBE_NAMESPACE")
                }
            }
            Some(mut request) = status_requests.track_status_requested() => {
                let _ = request.respond_error(RequestErrorCode::NotSupported as u64, "track status not served by this test");
                Event::Unexpected(id, "TRACK_STATUS")
            }
            Some(request) = track_requests.subscribed_tracks() => {
                let _ = request.reject(RequestErrorCode::NotSupported as u64, "track discovery not served by this test");
                Event::Unexpected(id, "SUBSCRIBE_TRACKS")
            }
            Some(request) = fetch_requests.fetch_requested() => {
                let _ = request.reject(RequestErrorCode::NotSupported, "fetch not served by this test");
                Event::Unexpected(id, "FETCH")
            }
            Some(request) = publish_requests.publish_received() => {
                request.close(ServeError::Done);
                Event::Unexpected(id, "PUBLISH")
            }
        };
        if tx.send(event).await.is_err() {
            return Event::RoutingFinished;
        }
    }
}
