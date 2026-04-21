use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use moq_transport::coding::TrackNamespace;
use tokio::sync::broadcast;

/// Information about an active SUBSCRIBE_NAMESPACE subscription
#[derive(Clone)]
pub struct NamespaceSubscription {
    /// The namespace prefix this subscription is for
    pub prefix: TrackNamespace,
    /// Session ID of the subscriber (for self-exclusion)
    pub session_id: u64,
    /// Channel to send PUBLISH notifications to this subscriber
    pub publish_tx: broadcast::Sender<PublishNotification>,
    /// Channel to send PUBLISH_NAMESPACE notifications to this subscriber
    pub publish_ns_tx: broadcast::Sender<PublishNamespaceNotification>,
}

/// Notification sent when a PUBLISH arrives that matches a subscription
#[derive(Clone, Debug)]
pub struct PublishNotification {
    pub namespace: TrackNamespace,
    pub track_name: String,
    pub track_alias: u64,
}

/// Notification sent when a PUBLISH_NAMESPACE arrives that matches a subscription
#[derive(Clone, Debug)]
pub struct PublishNamespaceNotification {
    pub namespace: TrackNamespace,
}

/// Registry for tracking active SUBSCRIBE_NAMESPACE subscriptions
///
/// When a subscriber sends SUBSCRIBE_NAMESPACE, they register here.
/// When a publisher sends PUBLISH, we find matching subscriptions and notify.
#[derive(Clone)]
pub struct SubscriberRegistry {
    inner: Arc<Mutex<SubscriberRegistryInner>>,
}

struct SubscriberRegistryInner {
    /// Map from subscription ID to subscription info
    subscriptions: HashMap<u64, NamespaceSubscription>,
    /// Next subscription ID
    next_id: u64,
}

impl SubscriberRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SubscriberRegistryInner {
                subscriptions: HashMap::new(),
                next_id: 0,
            })),
        }
    }

    /// Register a SUBSCRIBE_NAMESPACE subscription
    /// Returns (subscription_id, receiver for PUBLISH notifications, receiver for PUBLISH_NAMESPACE notifications)
    pub fn register(
        &self,
        prefix: TrackNamespace,
        session_id: u64,
    ) -> (
        u64,
        broadcast::Receiver<PublishNotification>,
        broadcast::Receiver<PublishNamespaceNotification>,
    ) {
        let mut inner = self.inner.lock().unwrap();

        let id = inner.next_id;
        inner.next_id += 1;

        // Create broadcast channels for PUBLISH and PUBLISH_NAMESPACE notifications
        let (publish_tx, publish_rx) = broadcast::channel(64);
        let (publish_ns_tx, publish_ns_rx) = broadcast::channel(64);

        let subscription = NamespaceSubscription {
            prefix,
            session_id,
            publish_tx,
            publish_ns_tx,
        };

        inner.subscriptions.insert(id, subscription);

        log::debug!(
            "registered namespace subscription id={} session_id={}",
            id,
            session_id
        );

        (id, publish_rx, publish_ns_rx)
    }

    /// Unregister a subscription
    pub fn unregister(&self, id: u64) {
        let mut inner = self.inner.lock().unwrap();
        if inner.subscriptions.remove(&id).is_some() {
            log::debug!("unregistered namespace subscription id={}", id);
        }
    }

    /// Find all subscriptions that match a given namespace and notify them of a PUBLISH
    /// Excludes the session that originated the PUBLISH (self-exclusion)
    /// Returns the number of matching subscriptions notified
    pub fn notify_publish(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
        track_alias: u64,
        origin_session_id: u64,
    ) -> usize {
        let inner = self.inner.lock().unwrap();

        let notification = PublishNotification {
            namespace: namespace.clone(),
            track_name: track_name.to_string(),
            track_alias,
        };

        let mut notified = 0;

        for (id, sub) in inner.subscriptions.iter() {
            // Skip if this subscription belongs to the same session that sent the PUBLISH
            if sub.session_id == origin_session_id {
                log::debug!(
                    "skipping subscription id={} (same session {})",
                    id,
                    origin_session_id
                );
                continue;
            }

            // Check if the namespace matches the subscription prefix
            // The subscription prefix should be a prefix of the namespace
            if Self::prefix_matches(&sub.prefix, namespace) {
                if let Err(e) = sub.publish_tx.send(notification.clone()) {
                    log::warn!("failed to notify subscription id={}: {}", id, e);
                } else {
                    log::debug!(
                        "notified subscription id={} of PUBLISH {}/{}",
                        id,
                        namespace,
                        track_name
                    );
                    notified += 1;
                }
            }
        }

        notified
    }

    /// Find all subscriptions that match a given namespace and notify them of a PUBLISH_NAMESPACE
    /// Excludes the session that originated the PUBLISH_NAMESPACE (self-exclusion)
    /// Returns the number of matching subscriptions notified
    pub fn notify_publish_namespace(&self, namespace: &TrackNamespace, origin_session_id: u64) -> usize {
        let inner = self.inner.lock().unwrap();

        let notification = PublishNamespaceNotification {
            namespace: namespace.clone(),
        };

        let mut notified = 0;

        for (id, sub) in inner.subscriptions.iter() {
            // Skip if this subscription belongs to the same session that sent the PUBLISH_NAMESPACE
            if sub.session_id == origin_session_id {
                log::debug!(
                    "skipping subscription id={} for PUBLISH_NAMESPACE (same session {})",
                    id,
                    origin_session_id
                );
                continue;
            }

            // Check if the namespace matches the subscription prefix
            if Self::prefix_matches(&sub.prefix, namespace) {
                if let Err(e) = sub.publish_ns_tx.send(notification.clone()) {
                    log::warn!(
                        "failed to notify subscription id={} of PUBLISH_NAMESPACE: {}",
                        id,
                        e
                    );
                } else {
                    log::debug!(
                        "notified subscription id={} of PUBLISH_NAMESPACE {:?}",
                        id,
                        namespace
                    );
                    notified += 1;
                }
            }
        }

        notified
    }

    /// Check if prefix is a prefix of namespace
    fn prefix_matches(prefix: &TrackNamespace, namespace: &TrackNamespace) -> bool {
        if prefix.fields.len() > namespace.fields.len() {
            return false;
        }

        prefix
            .fields
            .iter()
            .zip(namespace.fields.iter())
            .all(|(a, b)| a == b)
    }

    /// Get all subscriptions matching a prefix (for debugging)
    pub fn matching_subscriptions(&self, namespace: &TrackNamespace) -> Vec<u64> {
        let inner = self.inner.lock().unwrap();

        inner
            .subscriptions
            .iter()
            .filter(|(_, sub)| Self::prefix_matches(&sub.prefix, namespace))
            .map(|(id, _)| *id)
            .collect()
    }
}

impl Default for SubscriberRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard that unregisters on drop
pub struct SubscriptionGuard {
    registry: SubscriberRegistry,
    id: u64,
}

impl SubscriptionGuard {
    pub fn new(registry: SubscriberRegistry, id: u64) -> Self {
        Self { registry, id }
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        self.registry.unregister(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns(path: &str) -> TrackNamespace {
        TrackNamespace::from_utf8_path(path)
    }

    #[test]
    fn test_prefix_matching() {
        assert!(SubscriberRegistry::prefix_matches(&ns("live"), &ns("live/stream1")));
        assert!(SubscriberRegistry::prefix_matches(&ns("live"), &ns("live")));
        // An empty prefix (zero fields) should match everything
        let empty = TrackNamespace::new();
        assert!(SubscriberRegistry::prefix_matches(&empty, &ns("live/stream1")));
        assert!(!SubscriberRegistry::prefix_matches(&ns("live/stream1"), &ns("live")));
        assert!(!SubscriberRegistry::prefix_matches(&ns("other"), &ns("live/stream1")));
    }

    #[test]
    fn test_register_unregister() {
        let registry = SubscriberRegistry::new();

        let (id1, _rx1, _rx1_ns) = registry.register(ns("live"), 100);
        let (id2, _rx2, _rx2_ns) = registry.register(ns("live/room1"), 101);

        assert_eq!(registry.matching_subscriptions(&ns("live/room1/track")).len(), 2);

        registry.unregister(id1);

        assert_eq!(registry.matching_subscriptions(&ns("live/room1/track")).len(), 1);

        registry.unregister(id2);

        assert_eq!(registry.matching_subscriptions(&ns("live/room1/track")).len(), 0);
    }

    #[tokio::test]
    async fn test_notify_publish() {
        let registry = SubscriberRegistry::new();

        // Register with session_id=100
        let (id, mut rx, _rx_ns) = registry.register(ns("live"), 100);

        // Notify from session 200 (different) - should be delivered
        let notified = registry.notify_publish(&ns("live/stream1"), "video", 42, 200);
        assert_eq!(notified, 1);

        let notification = rx.recv().await.unwrap();
        assert_eq!(notification.track_name, "video");
        assert_eq!(notification.track_alias, 42);

        registry.unregister(id);
    }

    #[tokio::test]
    async fn test_self_exclusion() {
        let registry = SubscriberRegistry::new();

        // Register with session_id=100
        let (_id, mut rx, _rx_ns) = registry.register(ns("live"), 100);

        // Notify from the same session (100) - should NOT be delivered
        let notified = registry.notify_publish(&ns("live/stream1"), "video", 42, 100);
        assert_eq!(notified, 0);

        // Verify nothing was received (use try_recv to avoid blocking)
        assert!(rx.try_recv().is_err());
    }
}
