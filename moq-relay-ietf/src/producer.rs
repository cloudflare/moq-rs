use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    serve::{ServeError, TracksReader},
    session::{Publisher, SessionError, Subscribed, TrackStatusRequested},
};

use crate::{Locals, RemotesConsumer};
use crate::control_plane::ControlPlane;

/// Producer of tracks to a remote Subscriber
#[derive(Clone)]
pub struct Producer<CP: ControlPlane> {
    remote_publisher: Publisher,
    locals: Locals,
    remotes: Option<RemotesConsumer<CP>>,
}

impl<CP: ControlPlane> Producer<CP> {
    pub fn new(remote: Publisher, locals: Locals, remotes: Option<RemotesConsumer<CP>>) -> Self {
        Self {
            remote_publisher: remote,
            locals,
            remotes,
        }
    }

    /// Announce new tracks to the remote server.
    pub async fn announce(&mut self, tracks: TracksReader) -> Result<(), SessionError> {
        self.remote_publisher.announce(tracks).await
    }

    /// Run the producer to serve subscribe requests.
    pub async fn run(self) -> Result<(), SessionError> {
        //let mut tasks = FuturesUnordered::new();
        let mut tasks: FuturesUnordered<futures::future::BoxFuture<'static, ()>> =
            FuturesUnordered::new();

        loop {
            let mut remote_publisher_subscribed = self.remote_publisher.clone();
            let mut remote_publisher_track_status = self.remote_publisher.clone();

            tokio::select! {
                // Handle a new subscribe request
                Some(subscribed) = remote_publisher_subscribed.subscribed() => {
                    let this = self.clone();

                    // Spawn a new task to handle the subscribe
                    tasks.push(async move {
                        let info = subscribed.clone();
                        log::info!("serving subscribe: {:?}", info);

                        // Serve the subscribe request
                        if let Err(err) = this.serve_subscribe(subscribed).await {
                            log::warn!("failed serving subscribe: {:?}, error: {}", info, err)
                        }
                    }.boxed())
                },
                // Handle a new track_status request
                Some(track_status_requested) = remote_publisher_track_status.track_status_requested() => {
                    let this = self.clone();

                    // Spawn a new task to handle the track_status request
                    tasks.push(async move {
                        let info = track_status_requested.request_msg.clone();
                        log::info!("serving track_status: {:?}", info);

                        // Serve the track_status request
                        if let Err(err) = this.serve_track_status(track_status_requested).await {
                            log::warn!("failed serving track_status: {:?}, error: {}", info, err)
                        }
                    }.boxed())
                },
                _= tasks.next(), if !tasks.is_empty() => {},
                else => return Ok(()),
            };
        }
    }

    /// Serve a subscribe request.
    async fn serve_subscribe(self, subscribed: Subscribed) -> Result<(), anyhow::Error> {
        // Check local tracks first, and serve from local if possible
        if let Some(mut local) = self.locals.route(&subscribed.track_namespace) {
            if let Some(track) = local.subscribe(&subscribed.track_name) {
                log::info!("serving subscribe from local: {:?}", track.info);
                return Ok(subscribed.serve(track).await?);
            }
        }

        // Check remote tracks second, and serve from remote if possible
        if let Some(remotes) = &self.remotes {
            // Try to route to a remote for this namespace
            if let Some(remote) = remotes.route(&subscribed.track_namespace).await? {
                if let Some(track) = remote.subscribe(
                    subscribed.track_namespace.clone(),
                    subscribed.track_name.clone(),
                )? {
                    log::info!(
                        "serving subscribe from remote: {:?} {:?}",
                        remote.info,
                        track.info
                    );

                    // NOTE: Depends on drop(track) being called afterwards
                    return Ok(subscribed.serve(track.reader).await?);
                }
            }
        }

        Err(ServeError::NotFound.into())
    }

    /// Serve a track_status request.
    async fn serve_track_status(
        self,
        mut track_status_requested: TrackStatusRequested,
    ) -> Result<(), anyhow::Error> {
        // Check local tracks first, and serve from local if possible
        if let Some(mut local_tracks) = self
            .locals
            .route(&track_status_requested.request_msg.track_namespace)
        {
            if let Some(track) =
                local_tracks.get_track_reader(&track_status_requested.request_msg.track_name)
            {
                log::info!("serving track_status from local: {:?}", track.info);
                return Ok(track_status_requested.respond_ok(&track)?);
            }
        }

        // TODO - forward track status to remotes?
        // Check remote tracks second, and serve from remote if possible
        /*
        if let Some(remotes) = &self.remotes {
            // Try to route to a remote for this namespace
            if let Some(remote) = remotes.route(&subscribe.track_namespace).await? {
                if let Some(track) =
                    remote.subscribe(subscribe.track_namespace.clone(), subscribe.track_name.clone())?
                {
                    log::info!("serving from remote: {:?} {:?}", remote.info, track.info);

                    // NOTE: Depends on drop(track) being called afterwards
                    return Ok(subscribe.serve(track.reader).await?);
                }
            }
        }*/

        track_status_requested.respond_error(4, "Track not found")?;

        Err(ServeError::NotFound.into())
    }
}
