use std::sync::Arc;

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
                    let this = self.clone();

                    tasks.push(async move {
                        let info = publish.info.clone();
                        log::info!("serving publish (track-level): {:?}", info);

                        if let Err(err) = this.serve_publish(publish).await {
                            log::warn!("failed serving publish: {:?}, error: {}", info, err)
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

        log::info!(
            "received PUBLISH for track: {}/{} (forward={})",
            namespace,
            track_name,
            initial_forward
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
}
