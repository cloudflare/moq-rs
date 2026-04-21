use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    coding::{KeyValuePairs, TrackNamespace},
    message,
    serve::{ServeError, TracksReader},
    session::{
        PublishNamespace, Publisher, SessionError, SubscribeNamespaceReceived, Subscribed,
        TrackStatusRequested,
    },
};

use crate::{Locals, RemotesConsumer, SubscriberRegistry};

/// Producer of tracks to a remote Subscriber
#[derive(Clone)]
pub struct Producer {
    publisher: Publisher,
    locals: Locals,
    remotes: Option<RemotesConsumer>,
    subscriber_registry: Option<SubscriberRegistry>,
}

impl Producer {
    pub fn new(publisher: Publisher, locals: Locals, remotes: Option<RemotesConsumer>) -> Self {
        Self {
            publisher,
            locals,
            remotes,
            subscriber_registry: None,
        }
    }

    /// Creates a producer with a subscriber registry.
    pub fn with_registry(
        publisher: Publisher,
        locals: Locals,
        remotes: Option<RemotesConsumer>,
        subscriber_registry: SubscriberRegistry,
    ) -> Self {
        Self {
            publisher,
            locals,
            remotes,
            subscriber_registry: Some(subscriber_registry),
        }
    }

    pub async fn publish_namespace(
        &mut self,
        tracks: TracksReader,
    ) -> Result<PublishNamespace, SessionError> {
        self.publisher
            .publish_namespace(tracks.namespace.clone())
            .await
    }

    pub async fn run(self) -> Result<(), SessionError> {
        //let mut tasks = FuturesUnordered::new();
        let mut tasks: FuturesUnordered<futures::future::BoxFuture<'static, ()>> =
            FuturesUnordered::new();

        loop {
            let mut publisher_subscribed = self.publisher.clone();
            let mut publisher_track_status = self.publisher.clone();
            let mut publisher_subscribe_ns = self.publisher.clone();

            tokio::select! {
                // Handle a new subscribe request
                Some(subscribed) = publisher_subscribed.subscribed() => {
                    let this = self.clone();

                    // Spawn a new task to handle the subscribe
                    tasks.push(async move {
                        let info = subscribed.clone();
                        log::info!("serving subscribe: {:?}", info);

                        // Serve the subscribe request
                        if let Err(err) = this.serve_subscribe(subscribed).await {
                            log::warn!("failed serving subscribe: {:?}, error: {}", info, err);
                        }
                    }.boxed())
                },
                // Handle a new track_status request
                Some(track_status_requested) = publisher_track_status.track_status_requested() => {
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
                Some(subscribe_ns) = publisher_subscribe_ns.subscribe_namespace_received() => {
                    let this = self.clone();

                    tasks.push(async move {
                        let info = subscribe_ns.info.clone();
                        log::info!("serving subscribe_namespace: {:?}", info);

                        if let Err(err) = this.serve_subscribe_namespace(subscribe_ns).await {
                            log::warn!("failed serving subscribe_namespace: {:?}, error: {}", info, err)
                        }
                    }.boxed())
                },
                _= tasks.next(), if !tasks.is_empty() => {},
                else => return Ok(()),
            };
        }
    }

    async fn serve_subscribe(self, subscribed: Subscribed) -> Result<(), anyhow::Error> {
        let namespace = subscribed.track_namespace.clone();
        let track_name = subscribed.track_name.clone();

        if let Some(track_info) = self
            .locals
            .get_or_create_track_info(&namespace, &track_name)
        {
            if track_info.should_subscribe_upstream() {
                log::info!(
                    "subscribe needs upstream request: {}/{}",
                    namespace,
                    track_name
                );

                if let Some(reader) = self.locals.subscribe_upstream(track_info.clone()) {
                    log::info!(
                        "forwarding subscribe upstream via TrackInfo: {}/{}",
                        namespace,
                        track_name
                    );
                    return Ok(subscribed.serve(reader).await?);
                }
            }

            // If the track is in Publishing state and forward=0, request forwarding
            // This will trigger the consumer to send REQUEST_UPDATE to the publisher
            if track_info.is_publishing() && !track_info.is_forwarding() {
                log::info!(
                    "subscriber arrived for paused track {}/{}, requesting forward",
                    namespace,
                    track_name
                );
                track_info.request_forward();
            }

            let reader = track_info.get_reader();
            log::info!(
                "serving subscribe from local: {}/{} (state: {:?}, forwarding: {})",
                namespace,
                track_name,
                track_info.state(),
                track_info.is_forwarding()
            );
            return Ok(subscribed.serve(reader).await?);
        }

        if let Some(remotes) = self.remotes {
            match remotes.route(&namespace).await {
                Ok(remote) => {
                    if let Some(remote) = remote {
                        if let Some(track) = remote.subscribe(&namespace, &track_name)? {
                            log::info!("serving subscribe from remote: {:?}", track.info);
                            return Ok(subscribed.serve(track.reader).await?);
                        }
                    }
                }
                Err(e) => {
                    log::error!("failed to route to remote: {}", e);
                }
            }
        }

        let err = ServeError::not_found_ctx(format!(
            "track '{}/{}' not found in local or remote tracks",
            namespace, track_name
        ));
        subscribed.close(err.clone())?;
        Err(err.into())
    }

    async fn serve_subscribe_namespace(
        mut self,
        mut subscribe_ns: SubscribeNamespaceReceived,
    ) -> Result<(), anyhow::Error> {
        let namespace_prefix = subscribe_ns.namespace_prefix.clone();

        // Register with subscriber registry to receive PUBLISH and PUBLISH_NAMESPACE notifications
        let (_subscription_guard, mut publish_rx, mut publish_ns_rx) =
            if let Some(ref registry) = self.subscriber_registry {
                let (id, rx, rx_ns) = registry.register(namespace_prefix.clone());
                (
                    Some(crate::SubscriptionGuard::new(registry.clone(), id)),
                    Some(rx),
                    Some(rx_ns),
                )
            } else {
                (None, None, None)
            };

        // Find existing namespaces that match the prefix
        let matching_namespaces: Vec<TrackNamespace> = self
            .locals
            .matching_namespaces(&namespace_prefix)
            .into_iter()
            .collect();

        // Accept the subscription (even if no current matches - publisher may arrive later)
        subscribe_ns.ok()?;

        log::info!(
            "accepted SUBSCRIBE_NAMESPACE for prefix {:?}, {} existing matches",
            namespace_prefix,
            matching_namespaces.len()
        );

        // Send PUBLISH_NAMESPACE for existing namespaces
        // Keep handles alive to prevent PublishNamespaceDone from being sent
        let mut publish_ns_handles = Vec::new();
        for namespace in matching_namespaces {
            log::info!(
                "sending PUBLISH_NAMESPACE for {:?} (matched prefix {:?})",
                namespace,
                namespace_prefix
            );
            match self.publisher.publish_namespace(namespace.clone()).await {
                Ok(publish_ns) => {
                    log::debug!("sent PUBLISH_NAMESPACE for {:?}", namespace);
                    publish_ns_handles.push(publish_ns);
                }
                Err(e) => {
                    log::warn!(
                        "failed to send PUBLISH_NAMESPACE for {:?}: {}",
                        namespace,
                        e
                    );
                }
            }
        }
        let _publish_ns_handles = publish_ns_handles;

        // If we have a publish receiver, listen for new PUBLISH and PUBLISH_NAMESPACE notifications
        if publish_rx.is_some() || publish_ns_rx.is_some() {
            loop {
                tokio::select! {
                    // Wait for the subscription to close
                    result = subscribe_ns.closed() => {
                        result?;
                        break;
                    }
                    // Wait for PUBLISH notifications
                    notification = async {
                        if let Some(ref mut rx) = publish_rx {
                            rx.recv().await
                        } else {
                            std::future::pending().await
                        }
                    } => {
                        match notification {
                            Ok(publish_notif) => {
                                log::info!(
                                    "received PUBLISH notification for {}/{} on subscription prefix {:?}",
                                    publish_notif.namespace,
                                    publish_notif.track_name,
                                    namespace_prefix
                                );

                                // Get the TrackReader for this track so we can stream data
                                if let Some(track_info) = self.locals.get_track_info(
                                    &publish_notif.namespace,
                                    &publish_notif.track_name,
                                ) {
                                    let track_reader = track_info.get_reader();

                                    // Use publisher.publish() which sends PUBLISH with forward=1
                                    // This allows forwarding objects immediately
                                    let mut publisher = self.publisher.clone();
                                    let ns = publish_notif.namespace.clone();
                                    let name = publish_notif.track_name.clone();
                                    tokio::spawn(async move {
                                        match publisher.publish(track_reader.clone()).await {
                                            Ok(published) => {
                                                log::info!(
                                                    "forwarded PUBLISH for {}/{} with forward=1, streaming immediately",
                                                    ns, name
                                                );
                                                // serve_immediately() starts streaming without waiting for PUBLISH_OK
                                                // Since forward=1, subscriber expects data immediately
                                                // If subscriber sends error, serve will end and we cleanup
                                                match published.serve_immediately(track_reader).await {
                                                    Ok(()) => {
                                                        log::info!("track {}/{} serving completed", ns, name);
                                                    }
                                                    Err(e) => {
                                                        log::warn!(
                                                            "track {}/{} serving ended: {}",
                                                            ns, name, e
                                                        );
                                                        // Cleanup handled by Published drop
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                log::warn!(
                                                    "failed to publish track {}/{}: {}",
                                                    ns, name, e
                                                );
                                            }
                                        }
                                    });
                                } else {
                                    log::warn!(
                                        "no track info found for {}/{}, cannot forward PUBLISH",
                                        publish_notif.namespace,
                                        publish_notif.track_name
                                    );
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                log::warn!("subscription lagged by {} messages", n);
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                log::debug!("publish notification channel closed");
                                break;
                            }
                        }
                    }
                    // Wait for PUBLISH_NAMESPACE notifications -> forward as NAMESPACE message
                    notification = async {
                        if let Some(ref mut rx) = publish_ns_rx {
                            rx.recv().await
                        } else {
                            std::future::pending().await
                        }
                    } => {
                        match notification {
                            Ok(ns_notif) => {
                                log::info!(
                                    "received PUBLISH_NAMESPACE notification for {:?} on subscription prefix {:?}",
                                    ns_notif.namespace,
                                    namespace_prefix
                                );
                                // Forward NAMESPACE message to the subscriber (not PUBLISH_NAMESPACE)
                                // NAMESPACE (0x08) is the draft-16 message for announcing namespaces
                                // to SUBSCRIBE_NAMESPACE subscribers
                                let namespace_msg = message::Namespace {
                                    id: subscribe_ns.info.request_id,
                                    track_namespace: ns_notif.namespace.clone(),
                                    params: KeyValuePairs::new(),
                                };
                                self.publisher.forward_namespace(namespace_msg);
                                log::debug!(
                                    "forwarded NAMESPACE for {:?} (request_id={})",
                                    ns_notif.namespace,
                                    subscribe_ns.info.request_id
                                );
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                log::warn!("namespace subscription lagged by {} messages", n);
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                log::debug!("publish_namespace notification channel closed");
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            // No registry, just wait for close
            subscribe_ns.closed().await?;
        }

        Ok(())
    }

    async fn serve_track_status(
        self,
        mut track_status_requested: TrackStatusRequested,
    ) -> Result<(), anyhow::Error> {
        // Check local tracks first, and serve from local if possible
        if let Some(mut local_tracks) = self
            .locals
            .retrieve(&track_status_requested.request_msg.track_namespace)
        {
            if let Some(track) = local_tracks.get_track_reader(
                &track_status_requested.request_msg.track_namespace,
                &track_status_requested.request_msg.track_name,
            ) {
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

        Err(ServeError::not_found_ctx(format!(
            "track '{}/{}' not found for track_status",
            track_status_requested.request_msg.track_namespace,
            track_status_requested.request_msg.track_name
        ))
        .into())
    }
}
