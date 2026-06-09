// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::Arc;

use anyhow::Context;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    message::RequestErrorCode,
    serve::{FullTrackName, ServeError, Tracks},
    session::{PublishReceived, PublishedNamespace, SessionError, Subscriber},
};

use crate::{metrics::GaugeGuard, Coordinator, Locals, Producer};

/// Consumer of tracks from a remote Publisher
#[derive(Clone)]
pub struct Consumer {
    subscriber: Subscriber,
    locals: Locals,
    coordinator: Arc<dyn Coordinator>,
    forward: Option<Producer>, // Forward all announcements to this subscriber
    /// The resolved scope identity for this session, if any.
    /// Produced by `Coordinator::resolve_scope()` from the connection path.
    /// Passed to coordinator register/lookup calls to isolate namespaces.
    scope: Option<String>,
}

impl Consumer {
    pub fn new(
        subscriber: Subscriber,
        locals: Locals,
        coordinator: Arc<dyn Coordinator>,
        forward: Option<Producer>,
        scope: Option<String>,
    ) -> Self {
        Self {
            subscriber,
            locals,
            coordinator,
            forward,
            scope,
        }
    }

    /// Run the consumer to handle inbound PUBLISH_NAMESPACE and PUBLISH requests.
    pub async fn run(self) -> Result<(), SessionError> {
        let mut tasks: FuturesUnordered<futures::future::BoxFuture<'static, ()>> =
            FuturesUnordered::new();

        loop {
            let mut namespace_subscriber = self.subscriber.clone();
            let mut publish_subscriber = self.subscriber.clone();
            tokio::select! {
                Some(published_ns) = namespace_subscriber.published_namespace() => {
                    metrics::counter!("moq_relay_publishers_total").increment(1);

                    let this = self.clone();

                    tasks.push(async move {
                        let info = published_ns.clone();
                        let namespace = info.namespace.to_utf8_path();
                        tracing::info!(
                            namespace = %namespace,
                            "serving PUBLISH_NAMESPACE: {:?}", info
                        );

                        if let Err(err) = this.serve(published_ns).await {
                            tracing::warn!(
                                namespace = %namespace,
                                error = %err,
                                "failed serving PUBLISH_NAMESPACE: {:?}", info
                            );
                        }
                    }.boxed());
                },
                Some(publish) = publish_subscriber.publish_received() => {
                    metrics::counter!("moq_relay_published_tracks_total").increment(1);

                    let this = self.clone();
                    tasks.push(async move {
                        let namespace = publish.namespace().to_utf8_path();
                        let track_name = publish.name().clone();
                        tracing::info!(namespace = %namespace, track = %track_name, "serving PUBLISH");

                        if let Err(err) = this.serve_track(publish).await {
                            tracing::warn!(namespace = %namespace, track = %track_name, error = %err, "failed serving PUBLISH");
                        }
                    }.boxed());
                },
                _ = tasks.next(), if !tasks.is_empty() => {},
                else => return Ok(()),
            };
        }
    }

    /// Serve an inbound PUBLISH_NAMESPACE.
    async fn serve(mut self, mut published_ns: PublishedNamespace) -> Result<(), anyhow::Error> {
        // Track active publishers - decrements when this function returns.
        let _publisher_guard = GaugeGuard::new("moq_relay_active_publishers");

        let mut tasks = FuturesUnordered::new();
        let ns = published_ns.namespace.to_utf8_path();

        // Register namespace routing metadata locally. This does not register
        // any media tracks; it only creates a request queue used when a
        // downstream SUBSCRIBE asks for a missing track under this namespace.
        tracing::debug!(namespace = %ns, "registering namespace route source in locals");
        let (_register, mut requests) = match self
            .locals
            .register_namespace(self.scope.as_deref(), published_ns.namespace.clone())
            .await
        {
            Ok(reg) => reg,
            Err(err) => {
                metrics::counter!("moq_relay_announce_errors_total", "phase" => "local_register")
                    .increment(1);
                return Err(err);
            }
        };
        tracing::debug!(namespace = %ns, "namespace route source registered in locals");

        // Register namespace with the coordinator so other relay nodes can route to us.
        tracing::debug!(namespace = %ns, "registering namespace with coordinator");
        let _namespace_registration = match self
            .coordinator
            .register_namespace(self.scope.as_deref(), &published_ns.namespace)
            .await
        {
            Ok(reg) => reg,
            Err(err) => {
                metrics::counter!("moq_relay_announce_errors_total", "phase" => "coordinator_register")
                    .increment(1);
                return Err(err.into());
            }
        };
        tracing::debug!(namespace = %ns, "namespace registered with coordinator");

        // Accept the PUBLISH_NAMESPACE with REQUEST_OK.
        if let Err(err) = published_ns.ok() {
            metrics::counter!("moq_relay_announce_errors_total", "phase" => "send_ok").increment(1);
            return Err(err.into());
        }
        tracing::debug!(namespace = %ns, "sent REQUEST_OK for PUBLISH_NAMESPACE");
        metrics::counter!("moq_relay_announce_ok_total").increment(1);

        // Forward the namespace upstream, if configured.
        if let Some(mut forward) = self.forward {
            // The forwarding API still uses the moq-transport Tracks helper.
            // This helper is not the relay-local registry; it is only an
            // adapter for the outgoing publish_namespace API.
            let (_, _forward_request, forward_reader) =
                Tracks::new(published_ns.namespace.clone()).produce();
            tasks.push(
                async move {
                    let namespace = forward_reader.namespace.to_utf8_path();
                    tracing::info!(
                        namespace = %namespace,
                        "forwarding PUBLISH_NAMESPACE: {:?}", forward_reader.info
                    );
                    forward
                        .publish_namespace(forward_reader)
                        .await
                        .context("failed forwarding PUBLISH_NAMESPACE")
                }
                .boxed(),
            );
        }

        loop {
            tokio::select! {
                res = published_ns.closed() => {
                    let ns = published_ns.namespace.to_utf8_path();
                    res?;
                    tracing::info!(namespace = %ns, "PUBLISH_NAMESPACE closed");
                    return Ok(());
                },
                Some(track) = requests.recv() => {
                    let mut subscriber = self.subscriber.clone();

                    tasks.push(async move {
                        let info = track.clone();
                        let namespace = info.namespace.to_utf8_path();
                        let track_name = info.name.clone();
                        tracing::info!(
                            namespace = %namespace,
                            track = %track_name,
                            "forwarding subscribe: {:?}", info
                        );

                        if let Err(err) = subscriber.subscribe(track).await {
                            tracing::warn!(
                                namespace = %namespace,
                                track = %track_name,
                                error = %err,
                                "failed forwarding subscribe: {:?}", info
                            )
                        }

                        Ok(())
                    }.boxed());
                },
                res = tasks.next(), if !tasks.is_empty() => res.unwrap()?,
                else => return Ok(()),
            }
        }
    }

    /// Serve an inbound PUBLISH for one exact track.
    async fn serve_track(mut self, mut publish: PublishReceived) -> Result<(), anyhow::Error> {
        let namespace = publish.namespace().clone();
        let track_name = publish.name().clone();
        let full_name = FullTrackName {
            namespace: namespace.clone(),
            name: track_name.clone(),
        };

        // First-cut multi-publisher policy: reject a second publisher for the
        // same exact scoped Full Track Name. draft-16 §8.3 allows multiple
        // publishers, but deduplication is follow-up work.
        if self
            .locals
            .retrieve_track(self.scope.as_deref(), &full_name)
            .is_some()
        {
            publish.close(ServeError::Closed(RequestErrorCode::Uninterested as u64));
            return Err(ServeError::Duplicate.into());
        }

        // Take the reader first, then follow the same order as PUBLISH_NAMESPACE:
        // local registration → coordinator registration → PUBLISH_OK.
        let reader = match publish.take_reader() {
            Ok(reader) => reader,
            Err(err) => {
                metrics::counter!("moq_relay_publish_errors_total", "phase" => "take_reader")
                    .increment(1);
                return Err(err.into());
            }
        };

        let _registration = match self
            .locals
            .register_track(self.scope.as_deref(), reader)
            .await
        {
            Ok(registration) => registration,
            Err(err) => {
                metrics::counter!("moq_relay_publish_errors_total", "phase" => "local_register")
                    .increment(1);
                return Err(err);
            }
        };

        let track_string = track_name.to_string();
        let _track_registration = match self
            .coordinator
            .register_track(self.scope.as_deref(), &namespace, &track_string)
            .await
        {
            Ok(registration) => registration,
            Err(err) => {
                metrics::counter!("moq_relay_publish_errors_total", "phase" => "coordinator_register")
                    .increment(1);
                publish.close(ServeError::Closed(RequestErrorCode::InternalError as u64));
                return Err(err.into());
            }
        };

        if let Err(err) = publish.accept(true) {
            metrics::counter!("moq_relay_publish_errors_total", "phase" => "send_ok").increment(1);
            return Err(err.into());
        }

        tracing::debug!(
            namespace = %namespace.to_utf8_path(),
            track = %track_name,
            "PUBLISH registered as exact local track"
        );

        publish.closed().await?;
        tracing::info!(namespace = %namespace.to_utf8_path(), track = %track_name, "PUBLISH closed");

        Ok(())
    }
}
