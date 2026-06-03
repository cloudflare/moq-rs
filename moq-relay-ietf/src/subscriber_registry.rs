use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use moq_transport::coding::TrackNamespace;
use tokio::sync::broadcast;

use crate::top_n_tracker::{TieBreakPolicy, TopNTracker, TopNTrackerConfig};

/// Key for indexing subscriptions by their namespace prefix and property type
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct FilterGroupKey {
    prefix: TrackNamespace,
    property_type: u64,
}

/// Group of subscriber IDs that share the same (prefix, property_type)
/// Enables O(group_size) iteration instead of O(all_subscriptions)
struct FilterGroup {
    subscription_ids: Vec<u64>,
}

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
    /// Shared version counter, bumped only on actual snapshot rebuild in update_track_value path.
    /// Observers read this lock-free to detect when to recompute their cached top-N result.
    snapshot_epoch: Arc<AtomicU64>,
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
    /// Index: session_id -> subscription_id (for O(1) lookup by session)
    session_to_subscription: HashMap<u64, u64>,
    /// Index: subscription_id -> set of published track keys (for O(1) cleanup on unregister)
    subscription_published: HashMap<u64, Vec<(TrackNamespace, String)>>,
    /// Groups of filtered subscriptions by (prefix, property_type)
    filter_groups: HashMap<FilterGroupKey, FilterGroup>,
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
                session_to_subscription: HashMap::new(),
                subscription_published: HashMap::new(),
                filter_groups: HashMap::new(),
            })),
            snapshot_epoch: Arc::new(AtomicU64::new(0)),
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

            // Add to filter group index
            let group_key = FilterGroupKey {
                prefix: prefix.clone(),
                property_type: filter.property_type,
            };
            inner
                .filter_groups
                .entry(group_key)
                .or_insert_with(|| FilterGroup {
                    subscription_ids: Vec::new(),
                })
                .subscription_ids
                .push(id);

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
        inner.session_to_subscription.insert(session_id, id);

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
        let mut keys_to_add: Vec<(u64, TrackNamespace, String)> = Vec::new();

        // Find the matching tracker and update value
        let matching_prefix: Option<TrackNamespace> = inner
            .top_n_trackers
            .iter()
            .find(|((prefix, pt), _)| *pt == property_type && Self::prefix_matches(prefix, namespace))
            .map(|((prefix, _), _)| prefix.clone());

        let Some(prefix) = matching_prefix else {
            return 0;
        };

        let tracker_key = (prefix.clone(), property_type);
        let version_before = inner
            .top_n_trackers
            .get(&tracker_key)
            .map(|t| t.snapshot_version())
            .unwrap_or(0);

        if let Some(tracker) = inner.top_n_trackers.get(&tracker_key) {
            tracker.update_value(namespace, track_name, new_value);
        }

        let snapshot = inner
            .top_n_trackers
            .get(&tracker_key)
            .map(|t| t.load_snapshot());
        let Some(snapshot) = snapshot else { return 0 };

        // Bump epoch only if snapshot was actually rebuilt
        let version_after = inner
            .top_n_trackers
            .get(&tracker_key)
            .map(|t| t.snapshot_version())
            .unwrap_or(0);
        if version_after != version_before {
            self.snapshot_epoch.fetch_add(1, Ordering::Release);
        }

        // Use filter group index to only iterate relevant subscribers
        let group_key = FilterGroupKey {
            prefix: prefix.clone(),
            property_type,
        };

        let sub_ids: Vec<u64> = inner
            .filter_groups
            .get(&group_key)
            .map(|g| g.subscription_ids.clone())
            .unwrap_or_default();

        let notification = PublishNotification {
            namespace: namespace.clone(),
            track_name: track_name.to_string(),
            track_alias,
        };

        for sub_id in sub_ids {
            let Some(sub) = inner.subscriptions.get(&sub_id) else {
                continue;
            };

            if sub.session_id == origin_session_id {
                continue;
            }

            let Some(ref filter) = sub.track_filter else {
                continue;
            };

            // Use the pre-loaded snapshot for the check
            let tracker = inner.top_n_trackers.get(&tracker_key).unwrap();
            if tracker.is_in_top_n_with_snapshot(
                namespace,
                track_name,
                sub.session_id,
                filter.max_selected,
                &snapshot,
            ) {
                let publish_key = (sub_id, namespace.clone(), track_name.to_string());
                if inner.published_tracks.contains(&publish_key) {
                    continue;
                }

                if sub.publish_tx.send(notification.clone()).is_ok() {
                    notified += 1;
                    keys_to_add.push(publish_key);
                }
            }
        }

        // Record sent PUBLISH notifications with per-subscription index
        for key in keys_to_add {
            let sub_id = key.0;
            let ns = key.1.clone();
            let track = key.2.clone();
            inner.published_tracks.insert(key);
            inner
                .subscription_published
                .entry(sub_id)
                .or_insert_with(Vec::new)
                .push((ns, track));
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
                // N-covers-all fast path: if N >= total tracks, always in top-N
                let snapshot = tracker.load_snapshot();
                let non_self_count = snapshot
                    .iter()
                    .filter(|t| t.publisher_session_id != session_id)
                    .count();
                if max_n as usize >= non_self_count {
                    return true;
                }
                return tracker.is_in_top_n_with_snapshot(
                    namespace, track_name, session_id, max_n, &snapshot,
                );
            }
        }

        false
    }

    /// Get the snapshot epoch handle (lock-free read for observers).
    /// Observers cache this Arc and read it on every object to detect ranking changes.
    pub fn snapshot_epoch(&self) -> Arc<AtomicU64> {
        self.snapshot_epoch.clone()
    }

    /// Get the TRACK_FILTER configuration for a subscriber session
    /// Returns None if the session has no filtered subscription
    pub fn get_track_filter_for_session(&self, session_id: u64) -> Option<TrackFilter> {
        let inner = self.inner.lock().unwrap();

        // O(1) lookup via session index
        if let Some(&sub_id) = inner.session_to_subscription.get(&session_id) {
            if let Some(sub) = inner.subscriptions.get(&sub_id) {
                return sub.track_filter.clone();
            }
        }

        None
    }

    /// Unregister a subscription
    pub fn unregister(&self, id: u64) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(sub) = inner.subscriptions.remove(&id) {
            // Remove from session index
            inner.session_to_subscription.remove(&sub.session_id);

            // Remove from filter group
            if let Some(ref filter) = sub.track_filter {
                let group_key = FilterGroupKey {
                    prefix: sub.prefix.clone(),
                    property_type: filter.property_type,
                };
                if let Some(group) = inner.filter_groups.get_mut(&group_key) {
                    group.subscription_ids.retain(|&sid| sid != id);
                }
            }

            // Clean up published_tracks using the per-subscription index (O(tracks_for_this_sub))
            if let Some(keys) = inner.subscription_published.remove(&id) {
                for (ns, track) in keys {
                    inner.published_tracks.remove(&(id, ns, track));
                }
            }

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
        let mut keys_to_add: Vec<(u64, TrackNamespace, String)> = Vec::new();

        // Pre-load snapshots for trackers that match this namespace
        let mut tracker_snapshots: HashMap<(TrackNamespace, u64), Arc<Vec<crate::top_n_tracker::TrackRank>>> = HashMap::new();
        for ((prefix, pt), tracker) in inner.top_n_trackers.iter() {
            if Self::prefix_matches(prefix, namespace) {
                tracker_snapshots.insert((prefix.clone(), *pt), tracker.load_snapshot());
            }
        }

        for (id, sub) in inner.subscriptions.iter() {
            if sub.session_id == origin_session_id {
                continue;
            }

            if !Self::prefix_matches(&sub.prefix, namespace) {
                continue;
            }

            // If subscription has TRACK_FILTER, check if track is in top-N
            if let Some(ref filter) = sub.track_filter {
                let tracker_key = (sub.prefix.clone(), filter.property_type);

                if let Some(snapshot) = tracker_snapshots.get(&tracker_key) {
                    if let Some(tracker) = inner.top_n_trackers.get(&tracker_key) {
                        if !tracker.is_in_top_n_with_snapshot(
                            namespace,
                            track_name,
                            sub.session_id,
                            filter.max_selected,
                            snapshot,
                        ) {
                            continue;
                        }
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            // Check if we already sent PUBLISH for this (subscription, track) pair
            let publish_key = (*id, namespace.clone(), track_name.to_string());
            if inner.published_tracks.contains(&publish_key) {
                continue;
            }

            if sub.publish_tx.send(notification.clone()).is_ok() {
                notified += 1;
                keys_to_add.push(publish_key);
            }
        }

        // Record sent PUBLISH notifications with per-subscription index
        for key in keys_to_add {
            let sub_id = key.0;
            let ns = key.1.clone();
            let track = key.2.clone();
            inner.published_tracks.insert(key);
            inner
                .subscription_published
                .entry(sub_id)
                .or_insert_with(Vec::new)
                .push((ns, track));
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

        // notify_publish for b should return 0 since PUBLISH was already sent via update_track_value
        let notified = registry.notify_publish(&ns("live"), "b", 2, 2);
        assert_eq!(notified, 0);

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
