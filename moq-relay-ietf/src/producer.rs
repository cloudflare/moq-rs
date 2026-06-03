use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
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
    session_id: u64,
}

impl Producer {
    pub fn new(publisher: Publisher, locals: Locals, remotes: Option<RemotesConsumer>) -> Self {
        Self {
            publisher,
            locals,
            remotes,
            subscriber_registry: None,
            session_id: 0,
        }
    }

    /// Creates a producer with a subscriber registry.
    pub fn with_registry(
        publisher: Publisher,
        locals: Locals,
        remotes: Option<RemotesConsumer>,
        subscriber_registry: SubscriberRegistry,
        session_id: u64,
    ) -> Self {
        Self {
            publisher,
            locals,
            remotes,
            subscriber_registry: Some(subscriber_registry),
            session_id,
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

        // Parse TRACK_FILTER from params if present
        // TRACK_FILTER key is 0x12 (even = int value)
        // Value format: (property_type << 8) | max_selected packed into u64
        const TRACK_FILTER_KEY: u64 = 0x12;
        let track_filter = subscribe_ns.info.params.get(TRACK_FILTER_KEY).and_then(|kvp| {
            if let moq_transport::coding::Value::IntValue(packed) = &kvp.value {
                // Unpack: property_type in high byte, max_selected in low byte
                let property_type = (*packed >> 8) & 0xFF;
                let max_selected = (*packed & 0xFF) as u8;
                log::info!(
                    "parsed TRACK_FILTER: property_type={}, max_selected={}",
                    property_type,
                    max_selected
                );
                Some(crate::TrackFilter {
                    property_type,
                    max_selected,
                })
            } else {
                None
            }
        });

        // Register with subscriber registry to receive PUBLISH and PUBLISH_NAMESPACE notifications
        // Uses session_id so we can exclude PUBLISH messages from the same session (self-exclusion)
        let (_subscription_guard, mut publish_rx, mut publish_ns_rx) =
            if let Some(ref registry) = self.subscriber_registry {
                let (id, rx, rx_ns) = registry.register_with_filter(
                    namespace_prefix.clone(),
                    self.session_id,
                    track_filter,
                );
                (
                    Some(crate::SubscriptionGuard::new(registry.clone(), id)),
                    Some(rx),
                    Some(rx_ns),
                )
            } else {
                (None, None, None)
            };

        // Accept the subscription (even if no current matches - publisher may arrive later)
        subscribe_ns.ok()?;

        log::info!(
            "accepted SUBSCRIBE_NAMESPACE for prefix {:?}",
            namespace_prefix
        );

        // Send PUBLISH for existing tracks in matching namespaces
        // This triggers the client's onMatch callback for track discovery
        // Note: We skip PUBLISH_NAMESPACE and send PUBLISH directly - client expects PUBLISH for tracks
        let matching_tracks = self.locals.matching_tracks(&namespace_prefix);
        log::info!(
            "found {} existing tracks matching prefix {:?}",
            matching_tracks.len(),
            namespace_prefix
        );

        for (ns, track_name, track_info) in matching_tracks {
            let track_extensions = track_info.track_extensions().unwrap_or_default();
            log::info!(
                "sending PUBLISH for existing track {}/{} (matched prefix {:?}, extensions={:?})",
                ns,
                track_name,
                namespace_prefix,
                track_extensions
            );

            let track_reader = track_info.get_reader();
            let mut publisher = self.publisher.clone();
            let registry = self.subscriber_registry.clone();
            let session_id = self.session_id;

            tokio::spawn(async move {
                match publisher.publish_with_extensions(track_reader.clone(), track_extensions).await {
                    Ok(published) => {
                        log::info!(
                            "sent PUBLISH for existing track {}/{}, waiting for PUBLISH_OK",
                            ns,
                            track_name
                        );
                        // Create filter-only observer (update_track_value is handled by ingest observer in Consumer)
                        let observer = if let Some(ref reg) = registry {
                            let reg = reg.clone();
                            let ns_for_observer = ns.clone();
                            let name_for_observer = track_name.clone();
                            let track_filter = reg.get_track_filter_for_session(session_id);
                            let epoch = reg.snapshot_epoch();
                            let cached_epoch = AtomicU64::new(u64::MAX);
                            let cached_result = AtomicBool::new(true);
                            if track_filter.is_some() {
                                Some(moq_transport::session::ObjectObserverFn::from(
                                    Box::new(move |_group_id: u64, _object_id: u64, _ext_headers: &moq_transport::data::ExtensionHeaders| {
                                        if let Some(ref filter) = track_filter {
                                            let current_epoch = epoch.load(Ordering::Acquire);
                                            if current_epoch != cached_epoch.load(Ordering::Relaxed) {
                                                let in_top_n = reg.is_track_in_top_n(
                                                    &ns_for_observer,
                                                    &name_for_observer,
                                                    session_id,
                                                    filter.property_type,
                                                    filter.max_selected,
                                                );
                                                cached_epoch.store(current_epoch, Ordering::Relaxed);
                                                cached_result.store(in_top_n, Ordering::Relaxed);
                                            }
                                            cached_result.load(Ordering::Relaxed)
                                        } else {
                                            true
                                        }
                                    }) as Box<dyn Fn(u64, u64, &moq_transport::data::ExtensionHeaders) -> bool + Send + Sync>
                                ))
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        let result = if let Some(obs) = observer {
                            published.serve_with_observer(track_reader, obs).await
                        } else {
                            published.serve(track_reader).await
                        };

                        match result {
                            Ok(()) => {
                                log::info!("existing track {}/{} serving completed", ns, track_name);
                            }
                            Err(e) => {
                                log::warn!("existing track {}/{} serving ended: {}", ns, track_name, e);
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("failed to send PUBLISH for existing track {}/{}: {}", ns, track_name, e);
                    }
                }
            });
        }

        // If we have a publish receiver, listen for new PUBLISH and PUBLISH_NAMESPACE notifications
        if publish_rx.is_some() || publish_ns_rx.is_some() {
            loop {
                tokio::select! {
                    // Wait for the subscription to close
                    result = subscribe_ns.closed() => {
                        result?;
                        break;
                    }
                    // Wait for PUBLISH notifications -> forward PUBLISH to subscriber
                    // Subscriber sends PUBLISH_OK, then relay starts streaming data
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
                                    let track_extensions = track_info.track_extensions().unwrap_or_default();

                                    // Send PUBLISH and wait for PUBLISH_OK before streaming
                                    let mut publisher = self.publisher.clone();
                                    let ns = publish_notif.namespace.clone();
                                    let name = publish_notif.track_name.clone();
                                    let registry = self.subscriber_registry.clone();
                                    let session_id = self.session_id;
                                    log::info!(
                                        "forwarding PUBLISH for {}/{} with extensions {:?}",
                                        ns, name, track_extensions
                                    );
                                    tokio::spawn(async move {
                                        match publisher.publish_with_extensions(track_reader.clone(), track_extensions).await {
                                            Ok(published) => {
                                                log::info!(
                                                    "sent PUBLISH for {}/{}, waiting for PUBLISH_OK",
                                                    ns, name
                                                );
                                                // Create filter-only observer (update_track_value handled by ingest observer in Consumer)
                                                let observer = if let Some(ref reg) = registry {
                                                    let reg = reg.clone();
                                                    let ns_for_observer = ns.clone();
                                                    let name_for_observer = name.clone();
                                                    let track_filter = reg.get_track_filter_for_session(session_id);
                                                    let epoch = reg.snapshot_epoch();
                                                    let cached_epoch = AtomicU64::new(u64::MAX);
                                                    let cached_result = AtomicBool::new(true);
                                                    if track_filter.is_some() {
                                                        Some(moq_transport::session::ObjectObserverFn::from(
                                                            Box::new(move |_group_id: u64, _object_id: u64, _ext_headers: &moq_transport::data::ExtensionHeaders| {
                                                                if let Some(ref filter) = track_filter {
                                                                    let current_epoch = epoch.load(Ordering::Acquire);
                                                                    if current_epoch != cached_epoch.load(Ordering::Relaxed) {
                                                                        let in_top_n = reg.is_track_in_top_n(
                                                                            &ns_for_observer,
                                                                            &name_for_observer,
                                                                            session_id,
                                                                            filter.property_type,
                                                                            filter.max_selected,
                                                                        );
                                                                        cached_epoch.store(current_epoch, Ordering::Relaxed);
                                                                        cached_result.store(in_top_n, Ordering::Relaxed);
                                                                    }
                                                                    cached_result.load(Ordering::Relaxed)
                                                                } else {
                                                                    true
                                                                }
                                                            }) as Box<dyn Fn(u64, u64, &moq_transport::data::ExtensionHeaders) -> bool + Send + Sync>
                                                        ))
                                                    } else {
                                                        None
                                                    }
                                                } else {
                                                    None
                                                };

                                                // serve with observer to update TopN from object extension headers
                                                let result = if let Some(obs) = observer {
                                                    published.serve_with_observer(track_reader, obs).await
                                                } else {
                                                    published.serve(track_reader).await
                                                };

                                                match result {
                                                    Ok(()) => {
                                                        log::info!("track {}/{} serving completed", ns, name);
                                                    }
                                                    Err(e) => {
                                                        log::warn!(
                                                            "track {}/{} serving ended: {}",
                                                            ns, name, e
                                                        );
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                log::warn!(
                                                    "failed to send PUBLISH for {}/{}: {}",
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
                    // PUBLISH_NAMESPACE notifications - we don't forward these as NAMESPACE messages
                    // Client expects PUBLISH for individual tracks, not namespace announcements
                    notification = async {
                        if let Some(ref mut rx) = publish_ns_rx {
                            rx.recv().await
                        } else {
                            std::future::pending().await
                        }
                    } => {
                        match notification {
                            Ok(ns_notif) => {
                                log::debug!(
                                    "ignoring PUBLISH_NAMESPACE notification for {:?} (client expects PUBLISH for tracks)",
                                    ns_notif.namespace
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
