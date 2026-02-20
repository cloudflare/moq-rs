use std::sync::Arc;

use anyhow::Context;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    coding::KeyValuePairs,
    message::{FilterType, GroupOrder, PublishOk},
    serve::{ServeError, Tracks},
    session::{PublishNamespaceReceived, PublishReceived, SessionError, Subscriber},
};

use crate::{metrics::GaugeGuard, Coordinator, Locals, Producer};

/// Consumer of tracks from a remote Publisher
#[derive(Clone)]
pub struct Consumer {
    subscriber: Subscriber,
    locals: Locals,
    coordinator: Arc<dyn Coordinator>,
    forward: Option<Producer>, // Forward all announcements to this subscriber
}

impl Consumer {
    pub fn new(
        subscriber: Subscriber,
        locals: Locals,
        coordinator: Arc<dyn Coordinator>,
        forward: Option<Producer>,
    ) -> Self {
        Self {
            subscriber,
            locals,
            coordinator,
            forward,
        }
    }

    /// Run the consumer to serve announce requests and track-level publish messages.
    pub async fn run(self) -> Result<(), SessionError> {
        let mut tasks: FuturesUnordered<futures::future::BoxFuture<'_, ()>> =
            FuturesUnordered::new();

        loop {
            let mut subscriber_ns = self.subscriber.clone();
            let mut subscriber_publish = self.subscriber.clone();

            tokio::select! {
                Some(publish_ns) = subscriber_ns.publish_ns_recvd() => {
                    metrics::counter!("moq_relay_publishers_total").increment(1);
                    let this = self.clone();

                    tasks.push(async move {
                        let info = publish_ns.clone();
                        let namespace = info.namespace.to_utf8_path();
                        tracing::info!(namespace = %namespace, "serving publish namespace: {:?}", info);

                        if let Err(err) = this.serve_publish_namespace(publish_ns).await {
                            tracing::warn!(namespace = %namespace, error = %err, "failed serving publish namespace: {:?}, error: {}", info, err);
                        }
                    }.boxed());
                },
                Some(publish) = subscriber_publish.publish_received() => {
                    let this = self.clone();

                    tasks.push(async move {
                        let info = publish.info.clone();
                        tracing::info!("serving publish (track-level): {:?}", info);

                        if let Err(err) = this.serve_publish(publish).await {
                            tracing::warn!("failed serving publish: {:?}, error: {}", info, err)
                        }
                    }.boxed());
                },
                _ = tasks.next(), if !tasks.is_empty() => {},
                else => return Ok(()),
            };
        }
    }

    async fn serve_publish_namespace(
        mut self,
        mut publish_ns: PublishNamespaceReceived,
    ) -> Result<(), anyhow::Error> {
        // Track active publishers - decrements when this function returns
        let _publisher_guard = GaugeGuard::new("moq_relay_active_publishers");

        let mut tasks = FuturesUnordered::new();

        let (writer, mut request, reader) = Tracks::new(publish_ns.namespace.clone()).produce();

        // NOTE(mpandit): once the track is pulled from origin, internally it will be relayed
        // from this metal only, because now coordinator will have entry for the namespace.

        // should we allow the same namespace being served from multiple relays??

        let ns = reader.namespace.to_utf8_path();

        // Register namespace with the coordinator
        tracing::debug!(namespace = %ns, "registering namespace with coordinator");
        let _namespace_registration = match self
            .coordinator
            .register_namespace(&reader.namespace)
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

        // Register the local tracks, unregister on drop
        tracing::debug!(namespace = %ns, "registering namespace in locals");
        let _register = match self.locals.register(reader.clone(), writer).await {
            Ok(reg) => reg,
            Err(err) => {
                metrics::counter!("moq_relay_announce_errors_total", "phase" => "local_register")
                    .increment(1);
                return Err(err);
            }
        };
        tracing::debug!(namespace = %ns, "namespace registered in locals");

        // Accept the announce with an OK response
        if let Err(err) = publish_ns.ok() {
            metrics::counter!("moq_relay_announce_errors_total", "phase" => "send_ok").increment(1);
            return Err(err.into());
        }
        tracing::debug!(namespace = %ns, "sent PUBLISH_NAMESPACE_OK");

        // Successfully sent ANNOUNCE_OK
        metrics::counter!("moq_relay_announce_ok_total").increment(1);

        if let Some(mut forward) = self.forward.clone() {
            let reader_clone = reader.clone();
            tasks.push(
                async move {
                    let namespace = reader_clone.info.namespace.to_utf8_path();
                    tracing::info!(namespace = %namespace, "forwarding publish namespace");
                    let upstream_ns = forward
                        .publish_namespace(reader_clone)
                        .await
                        .context("failed forwarding publish_namespace")?;
                    upstream_ns
                        .ok()
                        .await
                        .context("publish_namespace not accepted by upstream")?;
                    upstream_ns
                        .closed()
                        .await
                        .context("upstream publish_namespace closed with error")
                }
                .boxed(),
            );
        }

        // Serve subscribe requests
        loop {
            tokio::select! {
                // If the publish namespace is closed, return the error
                Err(err) = publish_ns.closed() => {
                    tracing::info!(namespace = %ns, error = %err, "publish namespace closed");
                    return Err(err.into());
                },

                // Wait for the next subscriber and serve the track.
                Some(track) = request.next() => {
                    let mut subscriber = self.subscriber.clone();

                    // Spawn a new task to handle the subscribe
                    tasks.push(async move {
                        let info = track.clone();
                        let namespace = info.namespace.to_utf8_path();
                        let track_name = info.name.clone();
                        tracing::info!(namespace = %namespace, track = %track_name, "forwarding subscribe: {:?}", info);

                        // Forward the subscribe request
                        if let Err(err) = subscriber.subscribe(track).await {
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

    async fn serve_publish(self, mut publish: PublishReceived) -> Result<(), anyhow::Error> {
        let namespace = publish.info.track_namespace.clone();
        let track_name = publish.info.track_name.clone();

        tracing::info!("received PUBLISH for track: {}/{}", namespace, track_name);

        let track_info = match self
            .locals
            .get_or_create_track_info(&namespace, &track_name)
        {
            Some(info) => info,
            None => {
                tracing::warn!(
                    "PUBLISH rejected: no PUBLISH_NAMESPACE registered for namespace {}",
                    namespace
                );
                let err = ServeError::not_found_full(
                    "no PUBLISH_NAMESPACE registered for namespace",
                    "Namespace not announced",
                );
                publish.close(err.clone())?;
                return Err(err.into());
            }
        };

        let writer = match track_info.publish_arrived() {
            Ok(w) => w,
            Err(ServeError::Uninterested) => {
                tracing::info!(
                    "PUBLISH rejected: already subscribed to {}/{}",
                    namespace,
                    track_name
                );
                publish.close(ServeError::Uninterested)?;
                return Err(ServeError::Uninterested.into());
            }
            Err(ServeError::Duplicate) => {
                tracing::info!(
                    "PUBLISH rejected: already publishing {}/{}",
                    namespace,
                    track_name
                );
                publish.close(ServeError::Duplicate)?;
                return Err(ServeError::Duplicate.into());
            }
            Err(e) => {
                publish.close(e.clone())?;
                return Err(e.into());
            }
        };

        let reader = track_info.get_reader();

        self.locals
            .insert_track(&namespace, reader)
            .context("failed to insert track into namespace")?;

        let msg = PublishOk {
            id: publish.info.id,
            forward: true,
            subscriber_priority: 127,
            group_order: GroupOrder::Publisher,
            filter_type: FilterType::LargestObject,
            start_location: None,
            end_group_id: None,
            params: KeyValuePairs::default(),
        };

        publish.accept(writer, msg)?;

        tracing::info!(
            "PUBLISH accepted, track {}/{} now in Publishing state",
            namespace,
            track_name
        );

        // Hold publish alive until the stream closes (PUBLISH_DONE triggers recv_done)
        let _ = publish.closed().await;

        Ok(())
    }
}
