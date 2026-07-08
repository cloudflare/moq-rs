// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::session::{Publisher, SessionError, Subscriber};

use crate::{Consumer, Producer};

pub struct Session {
    pub session: moq_transport::session::Session,
    pub producer: Option<Producer>,
    pub consumer: Option<Consumer>,

    /// When `consumer` is `None` (publish not permitted), the transport
    /// `Subscriber` half still exists and will queue incoming
    /// PUBLISH_NAMESPACEs from the peer. We hold it here so we can
    /// actively drain and reject those messages instead of silently
    /// ignoring them.
    pub reject_publishes: Option<Subscriber>,

    /// When `producer` is `None` (subscribe not permitted), the transport
    /// `Publisher` half still exists and will queue incoming SUBSCRIBEs
    /// from the peer. We hold it here so we can actively drain and reject
    /// those messages instead of silently ignoring them.
    pub reject_subscribes: Option<Publisher>,
}

impl Session {
    /// Run the session, producer, and consumer as necessary.
    pub async fn run(self) -> Result<(), SessionError> {
        let mut tasks = FuturesUnordered::new();
        tasks.push(self.session.run().boxed());

        if let Some(producer) = self.producer {
            tasks.push(producer.run().boxed());
        }

        if let Some(consumer) = self.consumer {
            tasks.push(consumer.run().boxed());
        }

        // Reject unauthorized messages for disabled session halves.
        // Without these, a peer that sends a disallowed control message
        // would get no response (no OK, no error) because nobody is
        // draining the transport queue for that message type.
        if let Some(subscriber) = self.reject_publishes {
            tasks.push(Self::drain_and_reject_publishes(subscriber).boxed());
        }

        if let Some(publisher) = self.reject_subscribes {
            tasks.push(Self::drain_and_reject_subscribes(publisher).boxed());
        }

        tasks.select_next_some().await
    }

    /// Drain incoming PUBLISH_NAMESPACE and PUBLISH requests and reject each one.
    ///
    /// Dropping a `PublishedNamespace` without calling `ok()` triggers its
    /// `Drop` impl, which sends REQUEST_ERROR back to the peer.
    async fn drain_and_reject_publishes(subscriber: Subscriber) -> Result<(), SessionError> {
        let mut namespace_subscriber = subscriber.clone();
        let mut publish_subscriber = subscriber;

        loop {
            tokio::select! {
                Some(published_ns) = namespace_subscriber.published_namespace() => {
                    tracing::debug!(
                        namespace = %published_ns.namespace,
                        "rejecting PUBLISH_NAMESPACE: publish not permitted for this session"
                    );
                    drop(published_ns);
                },
                Some(publish) = publish_subscriber.publish_received() => {
                    tracing::debug!(
                        namespace = %publish.namespace(),
                        track = %publish.name(),
                        "rejecting PUBLISH: publish not permitted for this session"
                    );
                    drop(publish);
                },
                else => return Ok(()),
            }
        }
    }

    /// Drain incoming SUBSCRIBEs and reject each one.
    ///
    /// The transport `Publisher` queues incoming SUBSCRIBE messages as
    /// `Subscribed` events. Dropping a `Subscribed` without calling `ok()`
    /// triggers its `Drop` impl, which sends SUBSCRIBE_ERROR back to the
    /// peer.
    async fn drain_and_reject_subscribes(mut publisher: Publisher) -> Result<(), SessionError> {
        while let Some(subscribed) = publisher.subscribed().await {
            tracing::debug!(
                namespace = %subscribed.track_namespace,
                track = %subscribed.track_name,
                "rejecting SUBSCRIBE: subscribe not permitted for this session"
            );
            drop(subscribed);
        }
        Ok(())
    }
}
