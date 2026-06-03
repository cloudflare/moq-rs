use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    coding::KeyValuePairs,
    message::PublishOk,
    serve::{ServeError, Tracks},
    session::{PublishNamespaceReceived, PublishReceived, SessionError, Subscriber},
};

use crate::{Coordinator, Locals, Producer, SubscriberRegistry};

/// Consumer of tracks from a remote Publisher
#[derive(Clone)]
pub struct Consumer {
    subscriber: Subscriber,
    locals: Locals,
    coordinator: Arc<dyn Coordinator>,
    forward: Option<Producer>, // Forward all announcements to this subscriber
    subscriber_registry: Option<SubscriberRegistry>,
    session_id: u64,
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
            subscriber_registry: None,
            session_id: 0,
        }
    }

    /// Creates a consumer with a subscriber registry for PUBLISH notifications.
    pub fn with_registry(
        subscriber: Subscriber,
        locals: Locals,
        coordinator: Arc<dyn Coordinator>,
        forward: Option<Producer>,
        subscriber_registry: SubscriberRegistry,
        session_id: u64,
    ) -> Self {
        Self {
            subscriber,
            locals,
            coordinator,
            forward,
            subscriber_registry: Some(subscriber_registry),
            session_id,
        }
    }

    /// Run the consumer to serve announce requests and track-level publish messages.
    pub async fn run(self) -> Result<(), SessionError> {
        let mut tasks: FuturesUnordered<futures::future::BoxFuture<'_, ()>> =
            FuturesUnordered::new();

        log::debug!("[CONSUMER] run: starting main loop");

        loop {
            let mut subscriber_ns = self.subscriber.clone();
            let mut subscriber_publish = self.subscriber.clone();

            log::trace!("[CONSUMER] run: waiting on select (tasks={})", tasks.len());

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
                    log::debug!("[CONSUMER] run: received track-level PUBLISH");
                    let this = self.clone();

                    tasks.push(async move {
                        let info = publish.info.clone();
                        log::info!("serving publish (track-level): {:?}", info);

                        if let Err(err) = this.serve_publish(publish).await {
                            log::warn!("failed serving publish: {:?}, error: {}", info, err)
                        }
                    }.boxed());
                },
                _ = tasks.next(), if !tasks.is_empty() => {
                    log::trace!("[CONSUMER] run: a task completed");
                },
                else => {
                    log::debug!("[CONSUMER] run: else branch triggered, returning");
                    return Ok(());
                },
            };
        }
    }

    async fn serve_publish_namespace(
        mut self,
        mut publish_ns: PublishNamespaceReceived,
    ) -> Result<(), anyhow::Error> {
        let mut tasks: FuturesUnordered<futures::future::BoxFuture<'_, Result<(), anyhow::Error>>> =
            FuturesUnordered::new();

        let (writer, mut request, reader) = Tracks::new(publish_ns.namespace.clone()).produce();

        // NOTE(mpandit): once the track is pulled from origin, internally it will be relayed
        // from this metal only, because now coordinator will have entry for the namespace.

        // should we allow the same namespace being served from multiple relays??

        // Register namespace with the coordinator
        let _namespace_registration = self
            .coordinator
            .register_namespace(&reader.namespace)
            .await?;

        // Register the local tracks, unregister on drop
        let _register = self.locals.register(reader.clone(), writer).await?;

        publish_ns.ok()?;

        // Notify subscriber registry of the new PUBLISH_NAMESPACE
        // This will trigger forwarding to matching SUBSCRIBE_NAMESPACE subscriptions
        // Uses session_id for self-exclusion
        if let Some(ref registry) = self.subscriber_registry {
            let notified = registry.notify_publish_namespace(&publish_ns.namespace, self.session_id);
            if notified > 0 {
                log::info!(
                    "notified {} SUBSCRIBE_NAMESPACE subscriptions of PUBLISH_NAMESPACE {:?}",
                    notified,
                    publish_ns.namespace
                );
            }
        }

        // Forward publish_namespace upstream - keep handle alive in this scope
        let _forwarded_publish_ns = if let Some(mut forward) = self.forward.clone() {
            let reader_clone = reader.clone();
            log::info!("forwarding publish_namespace: {:?}", reader_clone.info);
            match forward.publish_namespace(reader_clone).await {
                Ok(publish_ns) => {
                    if let Err(e) = publish_ns.ok().await {
                        log::warn!("publish_namespace not accepted: {}", e);
                        None
                    } else {
                        log::info!(
                            "publish_namespace forwarded and accepted: {:?}",
                            publish_ns.info.namespace
                        );
                        Some(publish_ns)
                    }
                }
                Err(e) => {
                    log::warn!("failed forwarding publish_namespace: {}", e);
                    None
                }
            }
        } else {
            None
        };

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

    async fn serve_publish(mut self, publish: PublishReceived) -> Result<(), anyhow::Error> {
        let namespace = publish.info.track_namespace.clone();
        let track_name = publish.info.track_name.clone();
        let track_alias = publish.info.track_alias;
        let initial_forward = publish.info.forward;
        let publish_request_id = publish.info.id;
        let track_extensions = publish.info.track_extensions.clone();

        log::info!(
            "received PUBLISH for track: {}/{} (forward={}, extensions={:?})",
            namespace,
            track_name,
            initial_forward,
            track_extensions
        );

        // Use auto-register variant to support SUBSCRIBE_NAMESPACE flow
        // where PUBLISH can arrive without prior PUBLISH_NAMESPACE
        let track_info = self
            .locals
            .get_or_create_track_info_auto_register(&namespace, &track_name);

        let writer = match track_info.publish_arrived() {
            Ok(w) => w,
            Err(ServeError::Uninterested) => {
                log::info!(
                    "PUBLISH rejected: already subscribed to {}/{}",
                    namespace,
                    track_name
                );
                publish.reject(ServeError::Uninterested.code(), "Already subscribed")?;
                return Err(ServeError::Uninterested.into());
            }
            Err(ServeError::Duplicate) => {
                log::info!(
                    "PUBLISH rejected: already publishing {}/{}",
                    namespace,
                    track_name
                );
                publish.reject(ServeError::Duplicate.code(), "Already publishing")?;
                return Err(ServeError::Duplicate.into());
            }
            Err(e) => {
                publish.reject(e.code(), &e.to_string())?;
                return Err(e.into());
            }
        };

        let reader = track_info.get_reader();

        self.locals
            .insert_track(&namespace, reader)
            .context("failed to insert track into namespace")?;

        // Store publish info for forward state management
        track_info.set_publish_info(publish_request_id, initial_forward);

        // Store track extensions for forwarding to subscribers
        track_info.set_track_extensions(track_extensions);

        // Include forward=1 in PUBLISH_OK to tell publisher to start sending immediately
        let mut params = KeyValuePairs::default();
        params.set_intvalue(0x10, 1); // Forward = 1

        let msg = PublishOk {
            id: publish.info.id,
            params,
        };

        publish.accept(writer, msg)?;

        log::info!(
            "PUBLISH accepted, track {}/{} now in Publishing state (forward={})",
            namespace,
            track_name,
            initial_forward
        );

        // Register track with TopN tracker if track_extensions contain property values
        // This enables top-N filtering for SUBSCRIBE_NAMESPACE with TRACK_FILTER
        if let Some(ref registry) = self.subscriber_registry {
            // Check for known property types in track_extensions
            // AUDIO_LEVEL_EXT = 0x12 (18) - audio level for active speaker detection
            const AUDIO_LEVEL_EXT: u64 = 0x12;

            if let Some(track_exts) = track_info.track_extensions() {
                if let Some(kvp) = track_exts.get(AUDIO_LEVEL_EXT) {
                    if let moq_transport::coding::Value::IntValue(audio_level) = kvp.value {
                        registry.register_track(
                            &namespace,
                            &track_name,
                            AUDIO_LEVEL_EXT,
                            audio_level,
                            self.session_id,
                        );
                        log::info!(
                            "registered track {}/{} with TopN tracker (audio_level={})",
                            namespace,
                            track_name,
                            audio_level
                        );
                    }
                }
            }

            // Spawn ingest observer: single task that reads objects and calls
            // update_track_value once per value change (removes 799/800 redundant
            // mutex locks from subscriber observer path)
            let reg = registry.clone();
            let ingest_ns = namespace.clone();
            let ingest_name = track_name.clone();
            let ingest_session_id = self.session_id;
            let ingest_reader = track_info.get_reader();
            tokio::spawn(async move {
                Self::run_ingest_observer(
                    ingest_reader,
                    reg,
                    ingest_ns,
                    ingest_name,
                    track_alias,
                    ingest_session_id,
                )
                .await;
            });
        }

        // Notify subscriber registry of the new PUBLISH
        // This will trigger forwarding to matching SUBSCRIBE_NAMESPACE subscriptions
        // Uses session_id for self-exclusion (don't notify the same session that sent the PUBLISH)
        if let Some(ref registry) = self.subscriber_registry {
            let notified = registry.notify_publish(&namespace, &track_name, track_alias, self.session_id);
            if notified > 0 {
                log::info!(
                    "notified {} SUBSCRIBE_NAMESPACE subscriptions of PUBLISH {}/{}",
                    notified,
                    namespace,
                    track_name
                );
            }
        }

        // If forward=0 (paused), wait for subscribers to request forwarding
        // When forward state changes to 1, send REQUEST_UPDATE to publisher
        if !initial_forward {
            let forward_rx = track_info.forward_receiver();
            if let Some(mut rx) = forward_rx {
                log::info!(
                    "track {}/{} is paused (forward=0), waiting for subscriber to request forwarding",
                    namespace,
                    track_name
                );

                // Wait for forward state to change to true
                loop {
                    rx.changed().await.ok();
                    if *rx.borrow() {
                        // Forward state changed to true, send REQUEST_UPDATE
                        log::info!(
                            "subscriber arrived for paused track {}/{}, sending REQUEST_UPDATE with forward=1",
                            namespace,
                            track_name
                        );

                        let mut params = KeyValuePairs::default();
                        params.set_intvalue(0x10, 1); // Forward = 1

                        let request_update = moq_transport::message::SubscribeUpdate {
                            id: self.subscriber.get_next_request_id(),
                            existing_request_id: publish_request_id,
                            params,
                        };

                        self.subscriber.send_message(request_update);
                        log::info!(
                            "sent REQUEST_UPDATE for track {}/{} (existing_request_id={})",
                            namespace,
                            track_name,
                            publish_request_id
                        );
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn run_ingest_observer(
        reader: moq_transport::serve::TrackReader,
        registry: SubscriberRegistry,
        namespace: moq_transport::coding::TrackNamespace,
        track_name: String,
        track_alias: u64,
        session_id: u64,
    ) {
        const AUDIO_LEVEL_EXT: u64 = 0x12;
        let last_value = AtomicU64::new(u64::MAX);

        let mode = match reader.mode().await {
            Ok(mode) => mode,
            Err(_) => return,
        };

        match mode {
            moq_transport::serve::TrackReaderMode::Subgroups(mut subgroups) => {
                while let Ok(Some(mut subgroup)) = subgroups.next().await {
                    while let Ok(Some(object)) = subgroup.next().await {
                        if let Some(kvp) = object.extension_headers.get(AUDIO_LEVEL_EXT) {
                            if let moq_transport::coding::Value::IntValue(value) = kvp.value {
                                if last_value.swap(value, Ordering::Relaxed) != value {
                                    registry.update_track_value(
                                        &namespace,
                                        &track_name,
                                        AUDIO_LEVEL_EXT,
                                        value,
                                        track_alias,
                                        session_id,
                                    );
                                }
                            }
                        }
                    }
                }
            }
            moq_transport::serve::TrackReaderMode::Datagrams(mut datagrams) => {
                while let Ok(Some(datagram)) = datagrams.read().await {
                    if let Some(kvp) = datagram.extension_headers.get(AUDIO_LEVEL_EXT) {
                        if let moq_transport::coding::Value::IntValue(value) = kvp.value {
                            if last_value.swap(value, Ordering::Relaxed) != value {
                                registry.update_track_value(
                                    &namespace,
                                    &track_name,
                                    AUDIO_LEVEL_EXT,
                                    value,
                                    track_alias,
                                    session_id,
                                );
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
}
