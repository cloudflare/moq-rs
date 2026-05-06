use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use moq_transport::coding::TrackNamespace;
use tokio::sync::broadcast;

use crate::top_n_tracker::{TieBreakPolicy, TopNTracker, TopNTrackerConfig};

/// TRACK_FILTER configuration for a subscription
#[derive(Clone, Debug, Default)]
pub struct TrackFilter {
    /// Property type to sort by (e.g., 0x100 for viewers)
    pub property_type: u64,
    /// Maximum number of tracks to receive (N value)
    pub max_selected: u8,
}

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
    /// Optional TRACK_FILTER configuration
    pub track_filter: Option<TrackFilter>,
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
    /// TopN trackers per (namespace_prefix, property_type)
    /// Key is (prefix, property_type)
    top_n_trackers: HashMap<(TrackNamespace, u64), TopNTracker>,
    /// Enable event logging for TopN trackers (for visualization)
    topn_event_logging: bool,
    /// Tie-break policy for TopN trackers
    tie_break_policy: TieBreakPolicy,
    /// Tracks which (subscription_id, namespace, track_name) have been sent PUBLISH
    /// Used to avoid sending duplicate PUBLISH when track re-enters top-N
    published_tracks: HashSet<(u64, TrackNamespace, String)>,
}

impl SubscriberRegistry {
    pub fn new() -> Self {
        Self::with_config(false, TieBreakPolicy::OldestWins)
    }

    /// Create a new registry with TopN event logging enabled (for visualization)
    pub fn with_topn_logging() -> Self {
        Self::with_config(true, TieBreakPolicy::OldestWins)
    }

    /// Create a new registry with custom configuration
    pub fn with_config(topn_event_logging: bool, tie_break_policy: TieBreakPolicy) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SubscriberRegistryInner {
                subscriptions: HashMap::new(),
                next_id: 0,
                top_n_trackers: HashMap::new(),
                topn_event_logging,
                tie_break_policy,
                published_tracks: HashSet::new(),
            })),
        }
    }

    /// Enable or disable TopN event logging at runtime
    pub fn set_topn_event_logging(&self, enabled: bool) {
        let mut inner = self.inner.lock().unwrap();
        inner.topn_event_logging = enabled;
        // Update all existing trackers
        for tracker in inner.top_n_trackers.values() {
            tracker.set_event_logging(enabled);
        }
    }

    /// Register a SUBSCRIBE_NAMESPACE subscription without TRACK_FILTER
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
        self.register_with_filter(prefix, session_id, None)
    }

    /// Register a SUBSCRIBE_NAMESPACE subscription with optional TRACK_FILTER
    /// Returns (subscription_id, receiver for PUBLISH notifications, receiver for PUBLISH_NAMESPACE notifications)
    pub fn register_with_filter(
        &self,
        prefix: TrackNamespace,
        session_id: u64,
        track_filter: Option<TrackFilter>,
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

        // If TRACK_FILTER is specified, register with the TopN tracker
        if let Some(ref filter) = track_filter {
            let tracker_key = (prefix.clone(), filter.property_type);
            let enable_logging = inner.topn_event_logging;
            let tie_break_policy = inner.tie_break_policy;

            // Get or create tracker for this (prefix, property_type)
            let tracker = inner
                .top_n_trackers
                .entry(tracker_key)
                .or_insert_with(|| {
                    let config = TopNTrackerConfig {
                        enable_event_logging: enable_logging,
                        tie_break_policy,
                        ..Default::default()
                    };
                    TopNTracker::with_config(filter.property_type, config)
                });

            // Update max_n if this subscription has higher N
            let current_max_n = tracker.max_n();
            if filter.max_selected > current_max_n {
                tracker.update_max_n(filter.max_selected);
            }

            log::debug!(
                "registered filtered subscription id={} session_id={} filter={:?}",
                id,
                session_id,
                filter
            );
        }

        let subscription = NamespaceSubscription {
            prefix,
            session_id,
            publish_tx,
            publish_ns_tx,
            track_filter,
        };

        inner.subscriptions.insert(id, subscription);

        log::debug!(
            "registered namespace subscription id={} session_id={}",
            id,
            session_id
        );

        (id, publish_rx, publish_ns_rx)
    }

    /// Register a track with a property value (called on PUBLISH with track_extensions)
    pub fn register_track(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
        property_type: u64,
        property_value: u64,
        publisher_session_id: u64,
    ) {
        let mut inner = self.inner.lock().unwrap();

        // Find all trackers that match this namespace prefix
        for ((prefix, pt), tracker) in inner.top_n_trackers.iter_mut() {
            if *pt != property_type {
                continue;
            }

            // Check if the namespace matches this tracker's prefix
            if Self::prefix_matches(prefix, namespace) {
                tracker.register_track(
                    namespace.clone(),
                    track_name.to_string(),
                    property_value,
                    publisher_session_id,
                );
                log::debug!(
                    "registered track {}/{} with TopN tracker (prefix={}, property_type={}, value={})",
                    namespace,
                    track_name,
                    prefix,
                    property_type,
                    property_value
                );
            }
        }
    }

    /// Update a track's property value and notify subscribers if track enters top-N
    /// Only sends PUBLISH once per (subscription, track) - avoids duplicates
    /// Returns count of new subscriptions notified
    pub fn update_track_value(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
        property_type: u64,
        new_value: u64,
        track_alias: u64,
        origin_session_id: u64,
    ) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let mut notified = 0;
        let mut keys_to_add = Vec::new();

        for ((prefix, pt), tracker) in inner.top_n_trackers.iter() {
            if *pt != property_type {
                continue;
            }

            if !Self::prefix_matches(prefix, namespace) {
                continue;
            }

            // Update the value in the tracker
            tracker.update_value(namespace, track_name, new_value);
            log::debug!(
                "updated track {}/{} in TopN tracker to value {}",
                namespace,
                track_name,
                new_value
            );

            // Now check if this track is in top-N for any subscriptions and notify if needed
            let notification = PublishNotification {
                namespace: namespace.clone(),
                track_name: track_name.to_string(),
                track_alias,
            };

            for (id, sub) in inner.subscriptions.iter() {
                // Skip if this subscription belongs to the same session that sent the update
                if sub.session_id == origin_session_id {
                    continue;
                }

                // Check if prefix matches
                if !Self::prefix_matches(&sub.prefix, namespace) {
                    continue;
                }

                // Check if subscription has TRACK_FILTER for this property type
                if let Some(ref filter) = sub.track_filter {
                    if filter.property_type != property_type {
                        continue;
                    }

                    // Check if track is now in top-N for this subscriber
                    if tracker.is_in_top_n(namespace, track_name, sub.session_id, filter.max_selected) {
                        // Check if we already sent PUBLISH for this (subscription, track) pair
                        let publish_key = (*id, namespace.clone(), track_name.to_string());
                        if inner.published_tracks.contains(&publish_key) {
                            log::debug!(
                                "skipping subscription id={} (PUBLISH already sent for {}/{})",
                                id,
                                namespace,
                                track_name
                            );
                            continue;
                        }

                        // Send notification
                        if let Err(e) = sub.publish_tx.send(notification.clone()) {
                            log::warn!("failed to notify subscription id={} of value change: {}", id, e);
                        } else {
                            log::debug!(
                                "notified subscription id={} that track {}/{} entered top-{} (value={})",
                                id, namespace, track_name, filter.max_selected, new_value
                            );
                            notified += 1;
                            keys_to_add.push(publish_key);
                        }
                    }
                }
            }
        }

        // Record sent PUBLISH notifications
        for key in keys_to_add {
            inner.published_tracks.insert(key);
        }

        notified
    }

    /// Remove a track from TopN tracking
    pub fn remove_track(&self, namespace: &TrackNamespace, track_name: &str) {
        let inner = self.inner.lock().unwrap();

        for ((prefix, _pt), tracker) in inner.top_n_trackers.iter() {
            if Self::prefix_matches(prefix, namespace) {
                tracker.remove_track(namespace, track_name);
                log::debug!(
                    "removed track {}/{} from TopN tracker (prefix={})",
                    namespace,
                    track_name,
                    prefix
                );
            }
        }
    }

    /// Check if a track is in the top-N for a given session (considering self-exclusion)
    /// This is used for per-object filtering during streaming
    pub fn is_track_in_top_n(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
        session_id: u64,
        property_type: u64,
        max_n: u8,
    ) -> bool {
        let inner = self.inner.lock().unwrap();

        // Find the tracker for this namespace prefix and property type
        for ((prefix, pt), tracker) in inner.top_n_trackers.iter() {
            if *pt != property_type {
                continue;
            }

            if Self::prefix_matches(prefix, namespace) {
                return tracker.is_in_top_n(namespace, track_name, session_id, max_n);
            }
        }

        // No tracker found - track not registered, so not in top-N
        false
    }

    /// Get the TRACK_FILTER configuration for a subscriber session
    /// Returns None if the session has no filtered subscription
    pub fn get_track_filter_for_session(&self, session_id: u64) -> Option<TrackFilter> {
        let inner = self.inner.lock().unwrap();

        for sub in inner.subscriptions.values() {
            if sub.session_id == session_id {
                if let Some(ref filter) = sub.track_filter {
                    return Some(filter.clone());
                }
            }
        }

        None
    }

    /// Unregister a subscription
    pub fn unregister(&self, id: u64) {
        let mut inner = self.inner.lock().unwrap();
        if inner.subscriptions.remove(&id).is_some() {
            // Clean up published_tracks entries for this subscription
            inner.published_tracks.retain(|(sub_id, _, _)| *sub_id != id);
            log::debug!("unregistered namespace subscription id={}", id);
        }
    }

    /// Find all subscriptions that match a given namespace and notify them of a PUBLISH
    /// Excludes the session that originated the PUBLISH (self-exclusion)
    /// For subscriptions with TRACK_FILTER, only notifies if track is in their top-N
    /// Only sends PUBLISH once per (subscription, track) - avoids duplicates
    /// Returns the number of matching subscriptions notified
    pub fn notify_publish(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
        track_alias: u64,
        origin_session_id: u64,
    ) -> usize {
        let mut inner = self.inner.lock().unwrap();

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
            if !Self::prefix_matches(&sub.prefix, namespace) {
                continue;
            }

            // If subscription has TRACK_FILTER, check if track is in top-N
            if let Some(ref filter) = sub.track_filter {
                let tracker_key = (sub.prefix.clone(), filter.property_type);

                if let Some(tracker) = inner.top_n_trackers.get(&tracker_key) {
                    // Check with self-exclusion: does this track appear in subscriber's top-N?
                    if !tracker.is_in_top_n(
                        namespace,
                        track_name,
                        sub.session_id,
                        filter.max_selected,
                    ) {
                        log::debug!(
                            "skipping subscription id={} (track not in top-{} for session {})",
                            id,
                            filter.max_selected,
                            sub.session_id
                        );
                        continue;
                    }
                } else {
                    // No tracker exists yet - track hasn't been registered with property value
                    // Skip notification until track is registered
                    log::debug!(
                        "skipping subscription id={} (no TopN tracker for prefix={}, property_type={})",
                        id,
                        sub.prefix,
                        filter.property_type
                    );
                    continue;
                }
            }

            // Check if we already sent PUBLISH for this (subscription, track) pair
            let publish_key = (*id, namespace.clone(), track_name.to_string());
            if inner.published_tracks.contains(&publish_key) {
                log::debug!(
                    "skipping subscription id={} (PUBLISH already sent for {}/{})",
                    id,
                    namespace,
                    track_name
                );
                continue;
            }

            // Send notification
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

        // Record sent PUBLISH notifications (done after iteration to avoid borrow issues)
        // We need to collect the keys first since we can't mutate while iterating
        let keys_to_add: Vec<_> = inner
            .subscriptions
            .iter()
            .filter(|(id, sub)| {
                sub.session_id != origin_session_id
                    && Self::prefix_matches(&sub.prefix, namespace)
                    && !inner.published_tracks.contains(&(**id, namespace.clone(), track_name.to_string()))
            })
            .map(|(id, _)| (*id, namespace.clone(), track_name.to_string()))
            .collect();

        for key in keys_to_add {
            inner.published_tracks.insert(key);
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

    // --- TRACK_FILTER tests ---

    const PROPERTY_VIEWERS: u64 = 0x100;

    #[tokio::test]
    async fn test_track_filter_top_n() {
        let registry = SubscriberRegistry::new();

        // Subscriber wants top-2 tracks by viewers
        let filter = TrackFilter {
            property_type: PROPERTY_VIEWERS,
            max_selected: 2,
        };
        let (_id, mut rx, _rx_ns) =
            registry.register_with_filter(ns("live"), 100, Some(filter));

        // Register 4 tracks with different viewer counts
        // Publisher session IDs are different from subscriber (100)
        registry.register_track(&ns("live"), "a", PROPERTY_VIEWERS, 1000, 1); // highest
        registry.register_track(&ns("live"), "b", PROPERTY_VIEWERS, 500, 2);
        registry.register_track(&ns("live"), "c", PROPERTY_VIEWERS, 800, 3);
        registry.register_track(&ns("live"), "d", PROPERTY_VIEWERS, 200, 4); // lowest

        // Ranked order: a(1000), c(800), b(500), d(200)
        // Top-2 = a, c

        // Track "a" should be notified (in top-2)
        let notified = registry.notify_publish(&ns("live"), "a", 1, 1);
        assert_eq!(notified, 1);
        let msg = rx.try_recv().unwrap();
        assert_eq!(msg.track_name, "a");

        // Track "c" should be notified (in top-2)
        let notified = registry.notify_publish(&ns("live"), "c", 3, 3);
        assert_eq!(notified, 1);
        let msg = rx.try_recv().unwrap();
        assert_eq!(msg.track_name, "c");

        // Track "b" should NOT be notified (rank 3, not in top-2)
        let notified = registry.notify_publish(&ns("live"), "b", 2, 2);
        assert_eq!(notified, 0);
        assert!(rx.try_recv().is_err());

        // Track "d" should NOT be notified (rank 4, not in top-2)
        let notified = registry.notify_publish(&ns("live"), "d", 4, 4);
        assert_eq!(notified, 0);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_track_filter_self_exclusion() {
        let registry = SubscriberRegistry::new();

        // Subscriber (session 1) wants top-2 tracks but also publishes
        let filter = TrackFilter {
            property_type: PROPERTY_VIEWERS,
            max_selected: 2,
        };
        let (_id, mut rx, _rx_ns) =
            registry.register_with_filter(ns("live"), 1, Some(filter));

        // Session 1 publishes the top track
        registry.register_track(&ns("live"), "a", PROPERTY_VIEWERS, 1000, 1); // self
        registry.register_track(&ns("live"), "b", PROPERTY_VIEWERS, 500, 2);
        registry.register_track(&ns("live"), "c", PROPERTY_VIEWERS, 800, 3);
        registry.register_track(&ns("live"), "d", PROPERTY_VIEWERS, 200, 4);

        // Ranked order: a(1000), c(800), b(500), d(200)
        // For session 1 with self-exclusion, top-2 non-self = c, b

        // Track "a" - subscriber won't receive (self-exclusion at session level)
        let notified = registry.notify_publish(&ns("live"), "a", 1, 1);
        assert_eq!(notified, 0); // same session, basic self-exclusion
        assert!(rx.try_recv().is_err());

        // Track "c" should be notified (top-2 non-self)
        let notified = registry.notify_publish(&ns("live"), "c", 3, 3);
        assert_eq!(notified, 1);
        let msg = rx.try_recv().unwrap();
        assert_eq!(msg.track_name, "c");

        // Track "b" should be notified (top-2 non-self, because "a" is excluded)
        let notified = registry.notify_publish(&ns("live"), "b", 2, 2);
        assert_eq!(notified, 1);
        let msg = rx.try_recv().unwrap();
        assert_eq!(msg.track_name, "b");

        // Track "d" should NOT be notified (rank 4 non-self, only top-2)
        let notified = registry.notify_publish(&ns("live"), "d", 4, 4);
        assert_eq!(notified, 0);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_track_filter_value_update() {
        let registry = SubscriberRegistry::new();

        let filter = TrackFilter {
            property_type: PROPERTY_VIEWERS,
            max_selected: 1,
        };
        let (_id, mut rx, _rx_ns) =
            registry.register_with_filter(ns("live"), 100, Some(filter));

        // Initial: a=100, b=50
        registry.register_track(&ns("live"), "a", PROPERTY_VIEWERS, 100, 1);
        registry.register_track(&ns("live"), "b", PROPERTY_VIEWERS, 50, 2);

        // a is top-1
        let notified = registry.notify_publish(&ns("live"), "a", 1, 1);
        assert_eq!(notified, 1);
        rx.try_recv().unwrap();

        let notified = registry.notify_publish(&ns("live"), "b", 2, 2);
        assert_eq!(notified, 0);

        // Update b to 200, making it top-1
        // The update_track_value now also notifies subscribers
        let notified = registry.update_track_value(&ns("live"), "b", PROPERTY_VIEWERS, 200, 2, 2);
        assert_eq!(notified, 1);
        rx.try_recv().unwrap();

        // notify_publish should also work for the updated track
        let notified = registry.notify_publish(&ns("live"), "b", 2, 2);
        assert_eq!(notified, 1);
        rx.try_recv().unwrap();

        // a is no longer top-1
        let notified = registry.notify_publish(&ns("live"), "a", 1, 1);
        assert_eq!(notified, 0);
    }

    #[test]
    fn test_mixed_filtered_unfiltered_subscriptions() {
        let registry = SubscriberRegistry::new();

        // Unfiltered subscription
        let (_id1, _rx1, _) = registry.register(ns("live"), 100);

        // Filtered subscription
        let filter = TrackFilter {
            property_type: PROPERTY_VIEWERS,
            max_selected: 1,
        };
        let (_id2, _rx2, _) =
            registry.register_with_filter(ns("live"), 101, Some(filter));

        // Register tracks
        registry.register_track(&ns("live"), "a", PROPERTY_VIEWERS, 100, 1);
        registry.register_track(&ns("live"), "b", PROPERTY_VIEWERS, 50, 2);

        // Track "a" (top-1): both subscriptions should receive
        let notified = registry.notify_publish(&ns("live"), "a", 1, 1);
        assert_eq!(notified, 2);

        // Track "b" (not top-1): only unfiltered subscription receives
        let notified = registry.notify_publish(&ns("live"), "b", 2, 2);
        assert_eq!(notified, 1); // only unfiltered
    }
}
