use std::sync::Arc;

use anyhow::Context;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    serve::Tracks,
    session::{Announced, SessionError, Subscriber},
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
                        log::info!("serving announce: {:?}", info);

                        // Serve the announce request
                        if let Err(err) = this.serve(announce).await {
                            log::warn!("failed serving announce: {:?}, error: {}", info, err);
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

        // NOTE(mpandit): once the track is pulled from origin, internally it will be relayed
        // from this metal only, because now coordinator will have entry for the namespace.

        // should we allow the same namespace being served from multiple relays??

        // Register namespace with the coordinator
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

        // Register the local tracks, unregister on drop
        let _register = match self.locals.register(reader.clone()).await {
            Ok(reg) => reg,
            Err(err) => {
                metrics::counter!("moq_relay_announce_errors_total", "phase" => "local_register")
                    .increment(1);
                return Err(err);
            }
        };

        // Accept the announce with an OK response
        if let Err(err) = announce.ok() {
            metrics::counter!("moq_relay_announce_errors_total", "phase" => "send_ok").increment(1);
            return Err(err.into());
        }

        // Successfully sent ANNOUNCE_OK
        metrics::counter!("moq_relay_announce_ok_total").increment(1);

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
