// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::Arc;

use anyhow::Context;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    serve::Tracks,
    session::{PublishedNamespace, SessionError, Subscriber},
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

    /// Run the consumer to handle inbound PUBLISH_NAMESPACE requests.
    pub async fn run(mut self) -> Result<(), SessionError> {
        let mut tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(published_ns) = self.subscriber.published_namespace() => {
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
                    });
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

        let (_, mut request, reader) = Tracks::new(published_ns.namespace.clone()).produce();

        let ns = reader.namespace.to_utf8_path();

        // Register the namespace locally so downstream subscribers can be served.
        tracing::debug!(namespace = %ns, "registering namespace in locals");
        let _register = match self
            .locals
            .register(self.scope.as_deref(), reader.clone())
            .await
        {
            Ok(reg) => reg,
            Err(err) => {
                metrics::counter!("moq_relay_announce_errors_total", "phase" => "local_register")
                    .increment(1);
                return Err(err);
            }
        };
        tracing::debug!(namespace = %ns, "namespace registered in locals");

        // Register namespace with the coordinator so other relay nodes can route to us.
        tracing::debug!(namespace = %ns, "registering namespace with coordinator");
        let _namespace_registration = match self
            .coordinator
            .register_namespace(self.scope.as_deref(), &reader.namespace)
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
            tasks.push(
                async move {
                    let namespace = reader.namespace.to_utf8_path();
                    tracing::info!(
                        namespace = %namespace,
                        "forwarding PUBLISH_NAMESPACE: {:?}", reader.info
                    );
                    forward
                        .publish_namespace(reader)
                        .await
                        .context("failed forwarding PUBLISH_NAMESPACE")
                }
                .boxed(),
            );
        }

        loop {
            tokio::select! {
                Err(err) = published_ns.closed() => {
                    let ns = published_ns.namespace.to_utf8_path();
                    tracing::info!(namespace = %ns, error = %err, "PUBLISH_NAMESPACE closed");
                    return Err(err.into());
                },
                Some(track) = request.next() => {
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
}
