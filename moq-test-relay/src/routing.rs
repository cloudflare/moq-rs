// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{collections::HashMap, sync::Arc, time::Duration};

use moq_transport::{
    coding::TrackNamespace,
    serve::{ServeError, Track},
    session::{PublishedNamespace, Subscribed, SubscribedNamespace, Subscriber},
};
use tokio::{
    sync::{mpsc, watch, Mutex},
    time::Instant,
};

use crate::{
    registry::Test,
    run::{Event, SubscribeAnswer},
};

#[derive(Clone)]
pub struct PublisherRoute {
    session_id: usize,
    subscriber: Subscriber,
}

#[derive(Default)]
struct Routes {
    publishers: HashMap<TrackNamespace, PublisherRoute>,
    namespace_subscribers: HashMap<(usize, u64), SubscribedNamespace>,
}

#[derive(Clone)]
pub struct Routing {
    routes: Arc<Mutex<Routes>>,
    changed: watch::Sender<()>,
}

impl Default for Routing {
    fn default() -> Self {
        Self {
            routes: Default::default(),
            changed: watch::channel(()).0,
        }
    }
}

impl Routing {
    pub async fn register(
        &self,
        session_id: usize,
        published: &mut PublishedNamespace,
        subscriber: Subscriber,
    ) -> Result<usize, ServeError> {
        let mut routes = self.routes.lock().await;
        published.ok()?;
        let new_namespace = !routes.publishers.contains_key(&published.namespace);
        routes.publishers.insert(
            published.namespace.clone(),
            PublisherRoute {
                session_id,
                subscriber,
            },
        );
        let notified = if new_namespace {
            notify_namespaces(&mut routes, &published.namespace, false)
        } else {
            0
        };
        self.changed.send_replace(());
        Ok(notified)
    }

    pub async fn subscribe_namespace(
        &self,
        session_id: usize,
        mut request: SubscribedNamespace,
    ) -> Result<usize, ServeError> {
        let mut routes = self.routes.lock().await;
        request.ok()?;
        let mut notified = 0;
        // Draft-18 Sections 6.1 and 10.18 require the current matching set,
        // including namespaces published before this subscription arrived.
        for namespace in routes.publishers.keys() {
            if namespace.strip_prefix(&request.namespace_prefix).is_some() {
                request.namespace(namespace)?;
                notified += 1;
            }
        }
        routes
            .namespace_subscribers
            .insert((session_id, request.request_id), request);
        Ok(notified)
    }

    pub async fn withdraw(&self, session_id: usize, namespace: &TrackNamespace) -> usize {
        let mut routes = self.routes.lock().await;
        if routes
            .publishers
            .get(namespace)
            .is_some_and(|route| route.session_id == session_id)
        {
            routes.publishers.remove(namespace);
            // Sections 10.17 and 10.18: only send DONE to subscribers that
            // received the corresponding namespace (including its snapshot).
            notify_namespaces(&mut routes, namespace, true)
        } else {
            0
        }
    }

    pub async fn remove_session(&self, session_id: usize) -> usize {
        let namespaces: Vec<_> = {
            let mut routes = self.routes.lock().await;
            routes
                .namespace_subscribers
                .retain(|(id, _), _| *id != session_id);
            routes
                .publishers
                .iter()
                .filter(|(_, route)| route.session_id == session_id)
                .map(|(ns, _)| ns.clone())
                .collect()
        };
        let mut notified = 0;
        for namespace in namespaces {
            notified += self.withdraw(session_id, &namespace).await;
        }
        notified
    }

    pub async fn publisher(&self, namespace: &TrackNamespace) -> Option<PublisherRoute> {
        self.routes.lock().await.publishers.get(namespace).cloned()
    }

    pub async fn subscribe(
        &self,
        session_id: usize,
        subscribed: Subscribed,
        mut route: Option<PublisherRoute>,
        test: Test,
        hold_until: Instant,
        tx: mpsc::Sender<Event>,
    ) -> Event {
        let requested_ms = match subscribed.params.rendezvous_timeout() {
            Ok(value) => value.unwrap_or(0),
            Err(err) => {
                let _ = subscribed.close(ServeError::NotImplemented(
                    "invalid RENDEZVOUS_TIMEOUT".to_owned(),
                ));
                return Event::SubscribeAnswer(Err(format!("invalid RENDEZVOUS_TIMEOUT: {err}")));
            }
        };
        if test == Test::SubscribeBeforeAnnounceReject && route.is_none() {
            // Draft-18 Section 10.2.6 permits a shorter rendezvous timeout.
            // This variant uses a zero-length hold and returns TIMEOUT for a
            // nonzero parameter; absent/zero requires immediate DOES_NOT_EXIST.
            let error = if requested_ms == 0 {
                ServeError::NotFound
            } else {
                ServeError::Timeout
            };
            let _ = subscribed.close(error);
            return Event::SubscribeAnswer(Ok(SubscribeAnswer::Rejected));
        }
        if route.is_none() && requested_ms != 0 {
            let mut changed = self.changed.subscribe();
            loop {
                if Instant::now() >= hold_until {
                    break;
                }
                route = self.publisher(&subscribed.track_namespace).await;
                if route.is_some() {
                    break;
                }
                tokio::select! {
                    _ = tokio::time::sleep_until(hold_until) => break,
                    _ = subscribed.closed() => return cancelled_subscribe(),
                    _ = changed.changed() => {},
                }
            }
        }
        let Some(route) = route else {
            let error = if requested_ms == 0 {
                ServeError::NotFound
            } else {
                ServeError::Timeout
            };
            let _ = subscribed.close(error);
            return Event::SubscribeAnswer(Ok(
                if test == Test::SubscribeBeforeAnnounceHold && requested_ms != 0 {
                    SubscribeAnswer::HoldExpired(requested_ms)
                } else {
                    SubscribeAnswer::Rejected
                },
            ));
        };
        if route.session_id == session_id {
            let _ = subscribed.close(ServeError::NotFound);
            return Event::SubscribeAnswer(Err(
                "SUBSCRIBE arrived on the publisher session".to_owned()
            ));
        }
        let mut subscriber = route.subscriber;
        let (writer, reader) = Track::new(
            subscribed.track_namespace.clone(),
            subscribed.track_name.clone(),
        )
        .produce();
        // Section 10.2.13 requires forwarding NEW_GROUP_REQUEST upstream.
        // Retain the other downstream parameters through the same request path.
        let params = subscribed.params.clone();
        let answer = tokio::select! {
            biased;
            _ = subscribed.closed() => return cancelled_subscribe(),
            answer = subscriber.subscribe_open_with_params(writer, params) => answer,
        };
        let upstream = match answer {
            Ok(upstream) => upstream,
            Err(err) => {
                let cancelled = err == ServeError::Cancel;
                let message =
                    format!("publisher rejected or failed the forwarded SUBSCRIBE: {err}");
                let _ = subscribed.close(err);
                // Driver teardown cancels the waiter without a publisher response.
                return Event::SubscribeAnswer(if cancelled {
                    Ok(SubscribeAnswer::Cancelled)
                } else {
                    Err(message)
                });
            }
        };
        let largest = match upstream
            .subscribe_ok()
            .map(|ok| ok.params.largest_object())
            .transpose()
        {
            Ok(largest) => largest.flatten(),
            Err(err) => {
                let _ = subscribed.close(ServeError::NotImplemented(
                    "invalid upstream LARGEST_OBJECT".to_owned(),
                ));
                return Event::SubscribeAnswer(Err(format!(
                    "publisher's SUBSCRIBE_OK has invalid LARGEST_OBJECT: {err}"
                )));
            }
        };
        tokio::select! {
            biased;
            _ = subscribed.closed() => return cancelled_subscribe(),
            _ = tx.send(Event::SubscribeAnswer(Ok(SubscribeAnswer::Accepted))) => {},
        }
        // Keep the upstream request alive until downstream serving ends.
        if let Err(err) = subscribed
            .serve_with_largest_location(reader, largest)
            .await
        {
            tracing::debug!(error = %err, "downstream subscription ended");
        }
        drop(upstream);
        Event::RoutingFinished
    }
}

fn cancelled_subscribe() -> Event {
    Event::SubscribeAnswer(Ok(SubscribeAnswer::SubscriberCancelled))
}

fn notify_namespaces(routes: &mut Routes, namespace: &TrackNamespace, done: bool) -> usize {
    let mut notified = 0;
    routes.namespace_subscribers.retain(|_, request| {
        if namespace.strip_prefix(&request.namespace_prefix).is_none() {
            return true;
        }
        let result = if done {
            request.namespace_done(namespace)
        } else {
            request.namespace(namespace)
        };
        match result {
            Ok(()) => {
                notified += 1;
                true
            }
            Err(err) => {
                tracing::debug!(error = %err, "namespace subscription ended");
                false
            }
        }
    });
    notified
}

// Reserve response scheduling time before the catalog deadline. Section 10.2.6
// permits reducing a requested hold to protect the relay's resources.
pub fn hold_deadline(received: Instant, requested_ms: u64, test_deadline: Instant) -> Instant {
    received
        + Duration::from_millis(requested_ms).min(
            test_deadline
                .saturating_duration_since(received)
                .saturating_sub(Duration::from_millis(50)),
        )
}
