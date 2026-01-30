use std::sync::Arc;

use anyhow::Context;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    message::{FilterType, GroupOrder},
    serve::{ServeError, Track, Tracks},
    session::{PublishNamespaceReceived, PublishReceived, SessionError, Subscriber},
};

use crate::{Coordinator, Locals, Producer};

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
                    let this = self.clone();

                    tasks.push(async move {
                        let info = publish_ns.clone();
                        log::info!("serving publish_namespace: {:?}", info);

                        if let Err(err) = this.serve_publish_namespace(publish_ns).await {
                            log::warn!("failed serving publish_namespace: {:?}, error: {}", info, err)
                        }
                    }.boxed());
                },
                Some(publish) = subscriber_publish.publish_received() => {
                        log::warn!("not handling publish yet: {:?}", publish.info);
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
        let mut tasks = FuturesUnordered::new();

        let (_, mut request, reader) = Tracks::new(publish_ns.namespace.clone()).produce();

        // NOTE(mpandit): once the track is pulled from origin, internally it will be relayed
        // from this metal only, because now coordinator will have entry for the namespace.

        // should we allow the same namespace being served from multiple relays??

        // Register namespace with the coordinator
        let _namespace_registration = self
            .coordinator
            .register_namespace(&reader.namespace)
            .await?;

        // Register the local tracks, unregister on drop
        let _register = self.locals.register(reader.clone()).await?;

        publish_ns.ok()?;

        if let Some(mut forward) = self.forward.clone() {
            let reader_clone = reader.clone();
            tasks.push(
                async move {
                    log::info!("forwarding publish_namespace: {:?}", reader_clone.info);
                    let publish_ns = forward
                        .publish_namespace(reader_clone)
                        .await
                        .context("failed forwarding publish_namespace")?;
                    publish_ns
                        .ok()
                        .await
                        .context("publish_namespace not accepted")?;
                    publish_ns
                        .closed()
                        .await
                        .context("publish_namespace closed with error")
                }
                .boxed(),
            );
        }

        // Serve subscribe requests
        loop {
            tokio::select! {
                Err(err) = publish_ns.closed() => return Err(err.into()),

                // Wait for the next subscriber and serve the track.
                Some(track) = request.next() => {
                    let mut subscriber = self.subscriber.clone();

                    // Spawn a new task to handle the subscribe
                    tasks.push(async move {
                        let info = track.clone();
                        log::info!("forwarding subscribe: {:?}", info);

                        // Forward the subscribe request
                        if let Err(err) = subscriber.subscribe(track).await {
                            log::warn!("failed forwarding subscribe: {:?}, error: {}", info, err)
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
