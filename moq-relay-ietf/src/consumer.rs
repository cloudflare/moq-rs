// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::Arc;

use anyhow::Context;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    serve::{ServeError, TrackLease, TrackRequest, Tracks, TrackWriter},
    session::{Announced, SessionError, Subscriber},
};

use crate::{metrics::GaugeGuard, Coordinator, Locals, Producer, WARM_CACHE_LINGER};

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

    /// Run the consumer to serve announce requests.
    pub async fn run(mut self) -> Result<(), SessionError> {
        let mut tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                // Handle a new announce request
                Some(announce) = self.subscriber.announced() => {
                    metrics::counter!("moq_relay_publishers_total").increment(1);

                    let this = self.clone();

                    tasks.push(async move {
                        let info = announce.clone();
                        let namespace = info.namespace.to_utf8_path();
                        tracing::info!(namespace = %namespace, "serving announce: {:?}", info);

                        // Serve the announce request
                        if let Err(err) = this.serve(announce).await {
                            tracing::warn!(namespace = %namespace, error = %err, "failed serving announce: {:?}, error: {}", info, err);
                            // Note: phase-specific error counters are incremented in serve()
                        }
                    });
                },
                _ = tasks.next(), if !tasks.is_empty() => {},
                else => return Ok(()),
            };
        }
    }

    /// Serve an announce request.
    async fn serve(mut self, mut announce: Announced) -> Result<(), anyhow::Error> {
        // Track active publishers - decrements when this function returns
        let _publisher_guard = GaugeGuard::new("moq_relay_active_publishers");

        let mut tasks = FuturesUnordered::new();

        // Produce the tracks for this announce and return the reader
        let (_, mut request, reader) = Tracks::new(announce.namespace.clone()).produce();

        // should we allow the same namespace being served from multiple relays??
        // Manish: NO.

        let ns = reader.namespace.to_utf8_path();

        // Register the local tracks, unregister on drop
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

        // NOTE(mpandit): once the track is pulled from origin, internally it will be relayed
        // from this metal only, because now coordinator will have entry for the namespace.

        // Register namespace with the coordinator
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

        // Accept the announce with an OK response
        if let Err(err) = announce.ok() {
            metrics::counter!("moq_relay_announce_errors_total", "phase" => "send_ok").increment(1);
            return Err(err.into());
        }
        tracing::debug!(namespace = %ns, "sent ANNOUNCE_OK");

        // Successfully sent ANNOUNCE_OK
        metrics::counter!("moq_relay_announce_ok_total").increment(1);

        // Forward the announce, if needed
        if let Some(mut forward) = self.forward {
            tasks.push(
                async move {
                    let namespace = reader.namespace.to_utf8_path();
                    tracing::info!(namespace = %namespace, "forwarding announce: {:?}", reader.info);
                    forward
                        .announce(reader)
                        .await
                        .context("failed forwarding announce")
                }
                .boxed(),
            );
        }

        // Serve subscribe requests
        loop {
            tokio::select! {
                // If the announce is closed, return the error
                Err(err) = announce.closed() => {
                    let ns = announce.namespace.to_utf8_path();
                    tracing::info!(namespace = %ns, error = %err, "announce closed");
                    return Err(err.into());
                },

                // Wait for the next subscriber and serve the track.
                Some(req) = request.next() => {
                    let subscriber = self.subscriber.clone();

                    // Spawn a new task to handle the subscribe
                    tasks.push(async move {
                        let TrackRequest { writer, lease } = req;
                        let info = writer.clone();
                        let namespace = info.namespace.to_utf8_path();
                        let track_name = info.name.clone();
                        tracing::info!(namespace = %namespace, track = %track_name, "forwarding subscribe: {:?}", info);

                        // Forward the subscribe request, dropping the upstream
                        // subscription (which emits UNSUBSCRIBE) once the last
                        // downstream subscriber leaves and the linger expires.
                        if let Err(err) = Self::serve_track_subscribe(subscriber, writer, lease).await {
                            tracing::warn!(namespace = %namespace, track = %track_name, error = %err, "failed forwarding subscribe: {:?}, error: {}", info, err)
                        }

                        Ok(())
                    }.boxed());
                },
                res = tasks.next(), if !tasks.is_empty() => res.unwrap()?,
                else => return Ok(()),
            }
        }
    }

    /// Forward a single deduplicated subscribe upstream and keep it alive only
    /// while downstream subscribers are interested.
    ///
    /// Sending the SUBSCRIBE and awaiting SUBSCRIBE_OK is done by
    /// [`Subscriber::subscribe_open`]. We then hold the resulting `Subscribe`
    /// handle until one of:
    ///  - the upstream subscription ends on its own (error / PUBLISH_DONE / teardown), or
    ///  - the last downstream subscriber leaves and stays gone for [`WARM_CACHE_LINGER`].
    ///
    /// In the latter case we evict the dedup cache entry and return, dropping the
    /// `Subscribe` — whose `Drop` emits the upstream UNSUBSCRIBE (the only place
    /// the transport crate does so). Previously nothing propagated downstream
    /// interest loss upstream, so the relay kept pulling from the publisher until
    /// the whole session died.
    async fn serve_track_subscribe(
        mut subscriber: Subscriber,
        writer: TrackWriter,
        lease: TrackLease,
    ) -> Result<(), ServeError> {
        let subscribe = subscriber.subscribe_open(writer).await?;

        loop {
            // Wait until either the upstream subscription ends, or the last
            // downstream subscriber goes away (interest drops to zero).
            tokio::select! {
                biased;
                res = subscribe.closed() => return res,
                _ = lease.interest().idle() => {}
            }

            // Warm-cache linger: keep the upstream subscription alive briefly so an
            // instant late-joiner can reattach without a fresh upstream SUBSCRIBE.
            tokio::select! {
                biased;
                res = subscribe.closed() => return res,
                // A new subscriber arrived during the linger — keep serving.
                _ = lease.interest().busy() => continue,
                _ = tokio::time::sleep(WARM_CACHE_LINGER) => {
                    // Linger elapsed. Evict the dedup entry iff still idle. On
                    // success, returning drops `subscribe`, sending UNSUBSCRIBE.
                    if lease.evict_if_idle() {
                        return Ok(());
                    }
                    // Raced with a late joiner right at expiry; re-arm the linger.
                }
            }
        }
    }
}
