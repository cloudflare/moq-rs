use anyhow::Context;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    serve::Tracks,
    session::{Announced, SessionError, Subscriber},
};

use crate::{Locals, Producer};
use crate::control_plane::{ControlPlane, Origin};
use url::Url;

/// Consumer of tracks from a remote Publisher
#[derive(Clone)]
pub struct Consumer<CP: ControlPlane> {
    remote: Subscriber,
    locals: Locals,
    control_plane: Option<CP>,
    node_url: Option<Url>,
    forward: Option<Producer<CP>>, // Forward all announcements to this subscriber
}

impl<CP: ControlPlane> Consumer<CP> {
    pub fn new(
        remote: Subscriber,
        locals: Locals,
        control_plane: Option<CP>,
        node_url: Option<Url>,
        forward: Option<Producer<CP>>,
    ) -> Self {
        Self {
            remote,
            locals,
            control_plane,
            node_url,
            forward,
        }
    }

    /// Run the consumer to serve announce requests.
    pub async fn run(mut self) -> Result<(), SessionError> {
        let mut tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                // Handle a new announce request
                Some(announce) = self.remote.announced() => {
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

        // Start refreshing the control plane origin, if any
        if let Some(control_plane) = self.control_plane.as_ref() {
            if let Some(node_url) = &self.node_url {
                let origin = Origin {
                    url: node_url.clone(),
                };
                let namespace = reader.namespace.to_utf8_path();
                
                // Set the origin initially
                control_plane.set_origin(&namespace, origin.clone()).await?;
                
                // Create and spawn refresher task
                let mut refresh = control_plane.create_refresher(namespace, origin);
                tasks.push(
                    async move { refresh.run().await.context("failed refreshing origin") }.boxed(),
                );
            }
        }

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
                    let mut remote = self.remote.clone();

                    // Spawn a new task to handle the subscribe
                    tasks.push(async move {
                        let info = track.clone();
                        log::info!("forwarding subscribe: {:?}", info);

                        // Forward the subscribe request
                        if let Err(err) = remote.subscribe(track).await {
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
