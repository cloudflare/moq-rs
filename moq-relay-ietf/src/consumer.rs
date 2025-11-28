use std::sync::Arc;

use anyhow::Context;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    serve::Tracks,
    session::{Announced, SessionError, Subscriber},
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

    /// Run the consumer to serve announce requests.
    pub async fn run(mut self) -> Result<(), SessionError> {
        let mut tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                // Handle a new announce request
                Some(announce) = self.subscriber.announced() => {
                    let this = self.clone();

                    tasks.push(async move {
                        let info = announce.clone();
                        log::info!("serving announce: {:?}", info);

                        // Serve the announce request
                        if let Err(err) = this.serve(announce).await {
                            log::warn!("failed serving announce: {:?}, error: {}", info, err)
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
        let mut tasks = FuturesUnordered::new();

        // Produce the tracks for this announce and return the reader
        let (_, mut request, reader) = Tracks::new(announce.namespace.clone()).produce();

        // Register namespace with the coordinator
        let _namespace_registration = self
            .coordinator
            .register_namespace(&reader.namespace)
            .await
            .context("failed to register namespace with coordinator")?;

        // Register the local tracks, unregister on drop
        let _register = self.locals.register(reader.clone()).await?;

        // Accept the announce with an OK response
        announce.ok()?;

        // Forward the announce, if needed
        if let Some(mut forward) = self.forward {
            tasks.push(
                async move {
                    log::info!("forwarding announce: {:?}", reader.info);
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
                Err(err) = announce.closed() => return Err(err.into()),

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
