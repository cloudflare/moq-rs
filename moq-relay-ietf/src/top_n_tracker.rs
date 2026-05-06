//! Simple N+X TopN Tracker for TRACK_FILTER support.
//!
//! Implements the "Simple N+X" design from the MOQ filter analysis:
//! - Single global sorted snapshot per namespace
//! - Snapshot size = max(N) + max(X) where N = max subscriber filter, X = max tracks per publisher
//! - Self-exclusion computed at query time by skipping publisher's own tracks
//! - Lock-free reads via RwLock (Rust's RwLock allows concurrent readers)
//! - Configurable tie-breaking policy and staleness handling
//!
//! ## Structured Logging for Visualization
//!
//! When the `TOPN_LOG` environment variable is set, this module emits structured
//! JSON logs that can be used to generate timeline visualizations. Log lines are
//! prefixed with `TOPN_EVENT:` followed by JSON.
//!
//! Event types:
//! - `track_registered`: A new track was registered
//! - `value_updated`: A track's property value changed
//! - `top_n_query`: A subscriber queried their top-N (includes selection and self-exclusion)
//!
//! To generate a visualization from logs:
//! ```bash
//! TOPN_LOG=1 cargo run -p moq-relay-ietf ...  2>&1 | grep TOPN_EVENT > topn.log
//! cargo run -p moq-topn-test --bin topn-log-to-svg -- topn.log output.svg
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use moq_transport::coding::TrackNamespace;

/// Structured event logger for visualization
///
/// Emits JSON events to stdout that can be parsed to generate timeline visualizations.
/// Enable via `TopNTrackerConfig::enable_event_logging` or the `TOPN_LOG` env var.
pub struct TopNEventLogger {
    enabled: std::sync::atomic::AtomicBool,
    start: Instant,
}

impl TopNEventLogger {
    pub fn new() -> Self {
        Self {
            enabled: std::sync::atomic::AtomicBool::new(std::env::var("TOPN_LOG").is_ok()),
            start: Instant::now(),
        }
    }

    /// Enable or disable event logging at runtime
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Check if logging is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    fn ts_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    pub fn log_track_registered(&self, track_name: &str, value: u64, publisher_id: u64) {
        if !self.is_enabled() {
            return;
        }
        println!(
            r#"TOPN_EVENT:{{"ts_ms":{},"event":"track_registered","track":"{}","value":{},"publisher_id":{}}}"#,
            self.ts_ms(),
            track_name,
            value,
            publisher_id
        );
    }

    pub fn log_value_updated(&self, track_name: &str, old_value: u64, new_value: u64, publisher_id: u64) {
        if !self.is_enabled() {
            return;
        }
        println!(
            r#"TOPN_EVENT:{{"ts_ms":{},"event":"value_updated","track":"{}","old_value":{},"new_value":{},"publisher_id":{}}}"#,
            self.ts_ms(),
            track_name,
            old_value,
            new_value,
            publisher_id
        );
    }

    pub fn log_top_n_query(&self, subscriber_id: u64, n: u8, selected: &[(String, u64)], excluded_self: Option<u64>) {
        if !self.is_enabled() {
            return;
        }
        let selected_json: Vec<String> = selected
            .iter()
            .map(|(name, val)| format!(r#"{{"track":"{}","value":{}}}"#, name, val))
            .collect();
        println!(
            r#"TOPN_EVENT:{{"ts_ms":{},"event":"top_n_query","subscriber_id":{},"n":{},"selected":[{}],"excluded_self":{}}}"#,
            self.ts_ms(),
            subscriber_id,
            n,
            selected_json.join(","),
            excluded_self.map(|id| id.to_string()).unwrap_or_else(|| "null".to_string())
        );
    }

    pub fn log_track_removed(&self, track_name: &str, publisher_id: u64) {
        if !self.is_enabled() {
            return;
        }
        println!(
            r#"TOPN_EVENT:{{"ts_ms":{},"event":"track_removed","track":"{}","publisher_id":{}}}"#,
            self.ts_ms(),
            track_name,
            publisher_id
        );
    }
}

impl Default for TopNEventLogger {
    fn default() -> Self {
        Self::new()
    }
}

/// Tie-breaking policy when tracks have equal property values
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TieBreakPolicy {
    /// First-come-first-served: earlier arrival wins (stable rankings)
    #[default]
    OldestWins,
    /// Most recently active wins (responsive to activity)
    MostRecentWins,
}

/// Configuration for TopNTracker
#[derive(Clone, Debug)]
pub struct TopNTrackerConfig {
    /// How to break ties when property values are equal
    pub tie_break_policy: TieBreakPolicy,
    /// How long before a track is considered stale (None = never stale)
    pub staleness_timeout: Option<Duration>,
    /// Enable structured event logging for visualization (also enabled via TOPN_LOG env var)
    pub enable_event_logging: bool,
}

impl Default for TopNTrackerConfig {
    fn default() -> Self {
        Self {
            tie_break_policy: TieBreakPolicy::OldestWins,
            staleness_timeout: None,
            enable_event_logging: false,
        }
    }
}

/// Entry in the sorted snapshot
#[derive(Clone, Debug)]
pub struct TrackRank {
    pub namespace: TrackNamespace,
    pub track_name: String,
    pub property_value: u64,
    pub arrival_seq: u64,
    pub last_update: Instant,
    pub publisher_session_id: u64,
}

impl TrackRank {
    /// Compare for sorting with configurable tie-breaking
    fn rank_cmp(&self, other: &Self, policy: TieBreakPolicy) -> std::cmp::Ordering {
        match other.property_value.cmp(&self.property_value) {
            std::cmp::Ordering::Equal => match policy {
                // Earlier arrival wins (lower seq = better)
                TieBreakPolicy::OldestWins => self.arrival_seq.cmp(&other.arrival_seq),
                // More recent update wins (later time = better, so reverse comparison)
                TieBreakPolicy::MostRecentWins => other.last_update.cmp(&self.last_update),
            },
            ord => ord,
        }
    }
}

/// Track key for indexing
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct TrackKey {
    pub namespace: TrackNamespace,
    pub track_name: String,
}

impl TrackKey {
    pub fn new(namespace: TrackNamespace, track_name: String) -> Self {
        Self {
            namespace,
            track_name,
        }
    }
}

/// Internal track metadata
struct TrackInfo {
    property_value: u64,
    arrival_seq: u64,
    last_update: Instant,
    publisher_session_id: u64,
}

/// Simple N+X TopN Tracker
///
/// Maintains a sorted snapshot of top tracks by property value.
/// Self-exclusion is handled at query time, not via pre-computed waterlines.
pub struct TopNTracker {
    /// Sorted snapshot of top tracks (size = max_n + max_x)
    snapshot: RwLock<Arc<Vec<TrackRank>>>,

    /// Track metadata index
    track_index: RwLock<HashMap<TrackKey, TrackInfo>>,

    /// Track count per publisher (for computing max_x)
    publisher_track_count: RwLock<HashMap<u64, usize>>,

    /// Max N across all subscribers
    max_n: AtomicU8,

    /// Max tracks per publisher (for snapshot sizing)
    max_x: AtomicU8,

    /// Arrival sequence counter
    next_seq: AtomicU64,

    /// Property type being tracked
    property_type: u64,

    /// Configuration
    config: TopNTrackerConfig,

    /// Event logger for visualization (enabled via TOPN_LOG env var)
    event_logger: TopNEventLogger,
}

impl TopNTracker {
    /// Create a new tracker for a given property type with default config
    pub fn new(property_type: u64) -> Self {
        Self::with_config(property_type, TopNTrackerConfig::default())
    }

    /// Create a new tracker with custom configuration
    pub fn with_config(property_type: u64, config: TopNTrackerConfig) -> Self {
        let event_logger = TopNEventLogger::new();
        if config.enable_event_logging {
            event_logger.set_enabled(true);
        }
        Self {
            snapshot: RwLock::new(Arc::new(Vec::new())),
            track_index: RwLock::new(HashMap::new()),
            publisher_track_count: RwLock::new(HashMap::new()),
            max_n: AtomicU8::new(0),
            max_x: AtomicU8::new(0),
            next_seq: AtomicU64::new(0),
            property_type,
            config,
            event_logger,
        }
    }

    /// Enable or disable event logging at runtime
    pub fn set_event_logging(&self, enabled: bool) {
        self.event_logger.set_enabled(enabled);
    }

    /// Check if event logging is enabled
    pub fn is_event_logging_enabled(&self) -> bool {
        self.event_logger.is_enabled()
    }

    /// Get the current configuration
    pub fn config(&self) -> &TopNTrackerConfig {
        &self.config
    }

    /// Get the property type this tracker handles
    pub fn property_type(&self) -> u64 {
        self.property_type
    }

    /// Register a new track
    pub fn register_track(
        &self,
        namespace: TrackNamespace,
        track_name: String,
        property_value: u64,
        publisher_session_id: u64,
    ) {
        let key = TrackKey::new(namespace, track_name);
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();

        // Update track index
        {
            let mut index = self.track_index.write().unwrap();
            if index.contains_key(&key) {
                log::warn!("track already registered: {:?}", key);
                return;
            }
            index.insert(
                key.clone(),
                TrackInfo {
                    property_value,
                    arrival_seq: seq,
                    last_update: now,
                    publisher_session_id,
                },
            );
        }

        // Update publisher track count
        {
            let mut counts = self.publisher_track_count.write().unwrap();
            let count = counts.entry(publisher_session_id).or_insert(0);
            *count += 1;
            self.update_max_x(&counts);
        }

        self.rebuild_snapshot();

        // Log with full path (namespace/track_name) for better visualization
        let full_track_path = format!("{}/{}", key.namespace, key.track_name);
        self.event_logger.log_track_registered(&full_track_path, property_value, publisher_session_id);

        log::debug!(
            "registered track {:?} with value {} from session {}",
            key,
            property_value,
            publisher_session_id
        );
    }

    /// Update a track's property value
    pub fn update_value(&self, namespace: &TrackNamespace, track_name: &str, new_value: u64) {
        let key = TrackKey::new(namespace.clone(), track_name.to_string());
        let now = Instant::now();

        let old_value_and_publisher = {
            let mut index = self.track_index.write().unwrap();
            if let Some(info) = index.get_mut(&key) {
                if info.property_value == new_value {
                    // Even if value unchanged, update timestamp (track is still active)
                    info.last_update = now;
                    return;
                }
                let old_value = info.property_value;
                let publisher_id = info.publisher_session_id;
                info.property_value = new_value;
                info.last_update = now;
                Some((old_value, publisher_id))
            } else {
                log::warn!("update_value: track not found: {:?}", key);
                return;
            }
        };

        self.rebuild_snapshot();

        if let Some((old_value, publisher_id)) = old_value_and_publisher {
            self.event_logger.log_value_updated(track_name, old_value, new_value, publisher_id);
        }

        log::debug!("updated track {:?} to value {}", key, new_value);
    }

    /// Touch a track to update its last_update timestamp without changing value
    /// Useful for keeping tracks fresh when they're actively producing content
    pub fn touch_track(&self, namespace: &TrackNamespace, track_name: &str) {
        let key = TrackKey::new(namespace.clone(), track_name.to_string());
        let now = Instant::now();

        let mut index = self.track_index.write().unwrap();
        if let Some(info) = index.get_mut(&key) {
            info.last_update = now;
        }
        // Note: no rebuild needed since touch doesn't change value or rank
        // (unless using MostRecentWins policy, but that would require rebuild)
    }

    /// Remove a track
    pub fn remove_track(&self, namespace: &TrackNamespace, track_name: &str) {
        let key = TrackKey::new(namespace.clone(), track_name.to_string());

        let publisher_session_id = {
            let mut index = self.track_index.write().unwrap();
            if let Some(info) = index.remove(&key) {
                Some(info.publisher_session_id)
            } else {
                None
            }
        };

        // Update publisher track count
        if let Some(session_id) = publisher_session_id {
            let mut counts = self.publisher_track_count.write().unwrap();
            if let Some(count) = counts.get_mut(&session_id) {
                *count -= 1;
                if *count == 0 {
                    counts.remove(&session_id);
                }
            }
            self.update_max_x(&counts);

            self.event_logger.log_track_removed(track_name, session_id);
        }

        self.rebuild_snapshot();

        log::debug!("removed track {:?}", key);
    }

    /// Update max_n when a subscriber joins/leaves
    pub fn update_max_n(&self, new_max_n: u8) {
        let old = self.max_n.swap(new_max_n, Ordering::Relaxed);
        if old != new_max_n {
            self.rebuild_snapshot();
        }
    }

    /// Get current max_n
    pub fn max_n(&self) -> u8 {
        self.max_n.load(Ordering::Relaxed)
    }

    /// Load the current snapshot (for concurrent reads)
    pub fn load_snapshot(&self) -> Arc<Vec<TrackRank>> {
        self.snapshot.read().unwrap().clone()
    }

    /// Compute top-N tracks for a session, excluding self-published tracks
    ///
    /// This is the core self-exclusion logic: scan the snapshot, skip tracks
    /// where publisher_session_id matches the querying session.
    pub fn compute_top_n_for_session(
        &self,
        session_id: u64,
        n: u8,
    ) -> Vec<(TrackNamespace, String)> {
        let snapshot = self.load_snapshot();
        let mut result = Vec::with_capacity(n as usize);
        let mut selected_with_values = Vec::with_capacity(n as usize);
        let mut has_self_tracks = false;

        for track in snapshot.iter() {
            if result.len() >= n as usize {
                break;
            }
            // Self-exclusion: skip if publisher is same session
            if track.publisher_session_id != session_id {
                result.push((track.namespace.clone(), track.track_name.clone()));
                selected_with_values.push((track.track_name.clone(), track.property_value));
            } else {
                has_self_tracks = true;
            }
        }

        // Log the query result
        let excluded_self = if has_self_tracks { Some(session_id) } else { None };
        self.event_logger.log_top_n_query(session_id, n, &selected_with_values, excluded_self);

        result
    }

    /// Check if a track is in the top-N for a session (with self-exclusion)
    pub fn is_in_top_n(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
        session_id: u64,
        n: u8,
    ) -> bool {
        let snapshot = self.load_snapshot();
        self.is_in_top_n_with_snapshot(namespace, track_name, session_id, n, &snapshot)
    }

    /// Check if a track is in the top-N using a pre-loaded snapshot
    ///
    /// This variant allows reusing a snapshot across multiple checks for efficiency.
    pub fn is_in_top_n_with_snapshot(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
        session_id: u64,
        n: u8,
        snapshot: &[TrackRank],
    ) -> bool {
        let mut non_self_count = 0u8;

        for track in snapshot.iter() {
            // Self-exclusion: skip publisher's own tracks
            if track.publisher_session_id == session_id {
                continue;
            }

            // Check if this is the track we're looking for
            if &track.namespace == namespace && track.track_name == track_name {
                return true; // Found before reaching N non-self tracks
            }

            non_self_count += 1;
            if non_self_count >= n {
                return false; // Reached N non-self tracks, target not among them
            }
        }

        false
    }

    /// Check if a track is in the top-N with fast rejection optimization
    ///
    /// Uses cached_last_self_position for O(1) rejection of tracks that definitely
    /// can't be in the subscriber's top-N non-self.
    ///
    /// # Arguments
    /// * `track_position` - The track's position in the snapshot (0-indexed)
    /// * `session_id` - The subscriber's session ID (for self-exclusion)
    /// * `n` - The subscriber's N value
    /// * `cached_last_self_pos` - Cached position of the subscriber's last self-track in snapshot
    /// * `snapshot` - The current snapshot
    ///
    /// # Returns
    /// * `Some(true)` - Track is definitely in top-N
    /// * `Some(false)` - Track is definitely NOT in top-N (fast rejection)
    /// * `None` - Need full scan to determine
    pub fn is_in_top_n_fast(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
        track_position: usize,
        session_id: u64,
        n: u8,
        cached_last_self_pos: u8,
        snapshot: &[TrackRank],
    ) -> bool {
        // Fast rejection: if track position > N + cachedLastSelfPos,
        // it definitely can't be in top-N non-self
        if !Self::might_be_in_top_n(track_position, n, cached_last_self_pos) {
            return false;
        }

        // Need full scan for tracks that might be in top-N
        self.is_in_top_n_with_snapshot(namespace, track_name, session_id, n, snapshot)
    }

    /// O(1) fast rejection check: can this track possibly be in top-N non-self?
    ///
    /// If a subscriber has self-tracks scattered in the snapshot, the worst case
    /// is that all their self-tracks are above the track we're checking. In that
    /// case, the track's effective position would be: actual_position - num_self_above.
    ///
    /// Using cached_last_self_pos (position of last self-track), we know that at most
    /// cached_last_self_pos self-tracks can be above any position. So if:
    ///   track_position > N + cached_last_self_pos
    /// then even if ALL self-tracks were above this track, it still wouldn't make top-N.
    #[inline]
    pub fn might_be_in_top_n(track_position: usize, n: u8, cached_last_self_pos: u8) -> bool {
        track_position <= (n as usize) + (cached_last_self_pos as usize)
    }

    /// Compute the position of the last self-track in the snapshot for a session
    ///
    /// This value should be cached per-subscriber and invalidated when snapshot changes.
    /// Returns 0 if the session has no tracks in the snapshot.
    pub fn compute_last_self_position(&self, session_id: u64) -> u8 {
        let snapshot = self.load_snapshot();
        Self::compute_last_self_position_in_snapshot(session_id, &snapshot)
    }

    /// Compute last self position from a pre-loaded snapshot
    pub fn compute_last_self_position_in_snapshot(session_id: u64, snapshot: &[TrackRank]) -> u8 {
        let mut last_pos: u8 = 0;
        for (i, track) in snapshot.iter().enumerate() {
            if track.publisher_session_id == session_id {
                last_pos = (i as u8).min(255);
            }
        }
        last_pos
    }

    /// Find a track's position in the snapshot (for fast rejection)
    /// Returns None if track is not in the snapshot
    pub fn find_track_position(&self, namespace: &TrackNamespace, track_name: &str) -> Option<usize> {
        let snapshot = self.load_snapshot();
        snapshot.iter().position(|t| &t.namespace == namespace && t.track_name == track_name)
    }

    /// Get number of tracked tracks
    pub fn num_tracks(&self) -> usize {
        self.track_index.read().unwrap().len()
    }

    /// Sweep stale tracks from the index and rebuild snapshot
    ///
    /// Call this periodically (e.g., every 100ms) to clean up tracks from
    /// disconnected publishers. Only does work if staleness_timeout is configured.
    /// Returns the number of tracks removed.
    pub fn sweep_stale(&self) -> usize {
        let timeout = match self.config.staleness_timeout {
            Some(t) => t,
            None => return 0,
        };

        let now = Instant::now();
        let mut removed_count = 0;
        let mut affected_publishers = Vec::new();

        // Remove stale tracks from index
        {
            let mut index = self.track_index.write().unwrap();
            let stale_keys: Vec<_> = index
                .iter()
                .filter(|(_, info)| now.duration_since(info.last_update) >= timeout)
                .map(|(key, info)| (key.clone(), info.publisher_session_id))
                .collect();

            for (key, publisher_id) in stale_keys {
                index.remove(&key);
                affected_publishers.push(publisher_id);
                removed_count += 1;
            }
        }

        // Update publisher track counts
        if !affected_publishers.is_empty() {
            let mut counts = self.publisher_track_count.write().unwrap();
            for publisher_id in affected_publishers {
                if let Some(count) = counts.get_mut(&publisher_id) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        counts.remove(&publisher_id);
                    }
                }
            }
            self.update_max_x(&counts);
        }

        if removed_count > 0 {
            self.rebuild_snapshot();
            log::debug!("swept {} stale tracks", removed_count);
        }

        removed_count
    }

    // --- Private methods ---

    fn update_max_x(&self, counts: &HashMap<u64, usize>) {
        let max = counts.values().copied().max().unwrap_or(0);
        self.max_x
            .store(max.min(255) as u8, Ordering::Relaxed);
    }

    fn rebuild_snapshot(&self) {
        let max_n = self.max_n.load(Ordering::Relaxed) as usize;
        let max_x = self.max_x.load(Ordering::Relaxed) as usize;

        if max_n == 0 {
            *self.snapshot.write().unwrap() = Arc::new(Vec::new());
            return;
        }

        let snapshot_size = max_n + max_x;
        let now = Instant::now();
        let staleness_threshold = self.config.staleness_timeout;

        // Build sorted list from index, filtering out stale tracks
        let index = self.track_index.read().unwrap();
        let mut tracks: Vec<TrackRank> = index
            .iter()
            .filter(|(_, info)| {
                // Filter out stale tracks if staleness is configured
                match staleness_threshold {
                    Some(timeout) => now.duration_since(info.last_update) < timeout,
                    None => true,
                }
            })
            .map(|(key, info)| TrackRank {
                namespace: key.namespace.clone(),
                track_name: key.track_name.clone(),
                property_value: info.property_value,
                arrival_seq: info.arrival_seq,
                last_update: info.last_update,
                publisher_session_id: info.publisher_session_id,
            })
            .collect();

        // Sort by property value descending with configurable tie-breaking
        let policy = self.config.tie_break_policy;
        tracks.sort_by(|a, b| a.rank_cmp(b, policy));

        // Truncate to snapshot_size
        tracks.truncate(snapshot_size);

        *self.snapshot.write().unwrap() = Arc::new(tracks);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns(path: &str) -> TrackNamespace {
        TrackNamespace::from_utf8_path(path)
    }

    #[test]
    fn test_register_and_query() {
        let tracker = TopNTracker::new(0x100);
        tracker.update_max_n(3);

        tracker.register_track(ns("live"), "a".to_string(), 100, 1);
        tracker.register_track(ns("live"), "b".to_string(), 90, 2);
        tracker.register_track(ns("live"), "c".to_string(), 80, 3);
        tracker.register_track(ns("live"), "d".to_string(), 70, 4);

        // Session 99 (pure subscriber) should get top 3
        let top3 = tracker.compute_top_n_for_session(99, 3);
        assert_eq!(top3.len(), 3);
        assert_eq!(top3[0].1, "a");
        assert_eq!(top3[1].1, "b");
        assert_eq!(top3[2].1, "c");
    }

    #[test]
    fn test_self_exclusion() {
        let tracker = TopNTracker::new(0x100);
        tracker.update_max_n(5);

        // Session 1 publishes tracks at positions 0 and 2
        tracker.register_track(ns("live"), "a".to_string(), 100, 1); // session 1
        tracker.register_track(ns("live"), "b".to_string(), 90, 2);
        tracker.register_track(ns("live"), "c".to_string(), 80, 1); // session 1
        tracker.register_track(ns("live"), "d".to_string(), 70, 3);
        tracker.register_track(ns("live"), "e".to_string(), 60, 4);

        // Session 1 wants top 3 non-self: should get b, d, e (skipping a, c)
        let top3 = tracker.compute_top_n_for_session(1, 3);
        assert_eq!(top3.len(), 3);
        assert_eq!(top3[0].1, "b");
        assert_eq!(top3[1].1, "d");
        assert_eq!(top3[2].1, "e");
    }

    #[test]
    fn test_is_in_top_n() {
        let tracker = TopNTracker::new(0x100);
        tracker.update_max_n(3);

        tracker.register_track(ns("live"), "a".to_string(), 100, 1);
        tracker.register_track(ns("live"), "b".to_string(), 90, 2);
        tracker.register_track(ns("live"), "c".to_string(), 80, 3);
        tracker.register_track(ns("live"), "d".to_string(), 70, 4);

        // For session 99, "a", "b", "c" are in top 3
        assert!(tracker.is_in_top_n(&ns("live"), "a", 99, 3));
        assert!(tracker.is_in_top_n(&ns("live"), "b", 99, 3));
        assert!(tracker.is_in_top_n(&ns("live"), "c", 99, 3));
        assert!(!tracker.is_in_top_n(&ns("live"), "d", 99, 3));
    }

    #[test]
    fn test_is_in_top_n_with_self_exclusion() {
        let tracker = TopNTracker::new(0x100);
        tracker.update_max_n(5);

        tracker.register_track(ns("live"), "a".to_string(), 100, 1); // session 1
        tracker.register_track(ns("live"), "b".to_string(), 90, 2);
        tracker.register_track(ns("live"), "c".to_string(), 80, 1); // session 1
        tracker.register_track(ns("live"), "d".to_string(), 70, 3);
        tracker.register_track(ns("live"), "e".to_string(), 60, 4);

        // For session 1 with N=3: should be b, d, e (a and c excluded)
        assert!(!tracker.is_in_top_n(&ns("live"), "a", 1, 3)); // self
        assert!(tracker.is_in_top_n(&ns("live"), "b", 1, 3));
        assert!(!tracker.is_in_top_n(&ns("live"), "c", 1, 3)); // self
        assert!(tracker.is_in_top_n(&ns("live"), "d", 1, 3));
        assert!(tracker.is_in_top_n(&ns("live"), "e", 1, 3));
    }

    #[test]
    fn test_update_value() {
        let tracker = TopNTracker::new(0x100);
        tracker.update_max_n(2);

        tracker.register_track(ns("live"), "a".to_string(), 100, 1);
        tracker.register_track(ns("live"), "b".to_string(), 50, 2);
        tracker.register_track(ns("live"), "c".to_string(), 80, 3);

        // Initial: a (100), c (80) in top 2
        assert!(tracker.is_in_top_n(&ns("live"), "a", 99, 2));
        assert!(tracker.is_in_top_n(&ns("live"), "c", 99, 2));
        assert!(!tracker.is_in_top_n(&ns("live"), "b", 99, 2));

        // Update b to 200 - now b should be #1
        tracker.update_value(&ns("live"), "b", 200);

        assert!(tracker.is_in_top_n(&ns("live"), "b", 99, 2)); // now in top
        assert!(tracker.is_in_top_n(&ns("live"), "a", 99, 2));
        assert!(!tracker.is_in_top_n(&ns("live"), "c", 99, 2)); // pushed out
    }

    #[test]
    fn test_remove_track() {
        let tracker = TopNTracker::new(0x100);
        tracker.update_max_n(2);

        tracker.register_track(ns("live"), "a".to_string(), 100, 1);
        tracker.register_track(ns("live"), "b".to_string(), 90, 2);
        tracker.register_track(ns("live"), "c".to_string(), 80, 3);

        // Remove "a", "b" should still be in top, "c" gets promoted
        tracker.remove_track(&ns("live"), "a");

        assert!(!tracker.is_in_top_n(&ns("live"), "a", 99, 2)); // removed
        assert!(tracker.is_in_top_n(&ns("live"), "b", 99, 2));
        assert!(tracker.is_in_top_n(&ns("live"), "c", 99, 2)); // promoted
    }

    #[test]
    fn test_tie_breaker_earlier_wins() {
        let tracker = TopNTracker::new(0x100);
        tracker.update_max_n(1);

        // Register with same value - first one should win
        tracker.register_track(ns("live"), "first".to_string(), 100, 1);
        tracker.register_track(ns("live"), "second".to_string(), 100, 2);

        let top1 = tracker.compute_top_n_for_session(99, 1);
        assert_eq!(top1.len(), 1);
        assert_eq!(top1[0].1, "first");
    }

    #[test]
    fn test_snapshot_size_n_plus_x() {
        let tracker = TopNTracker::new(0x100);
        tracker.update_max_n(3);

        // Session 1 publishes 4 tracks (max_x = 4)
        tracker.register_track(ns("live"), "a".to_string(), 100, 1);
        tracker.register_track(ns("live"), "b".to_string(), 90, 1);
        tracker.register_track(ns("live"), "c".to_string(), 80, 1);
        tracker.register_track(ns("live"), "d".to_string(), 70, 1);

        // Other sessions publish more tracks
        tracker.register_track(ns("live"), "e".to_string(), 60, 2);
        tracker.register_track(ns("live"), "f".to_string(), 50, 3);
        tracker.register_track(ns("live"), "g".to_string(), 40, 4);

        // Snapshot should be size N + X = 3 + 4 = 7
        let snapshot = tracker.load_snapshot();
        assert_eq!(snapshot.len(), 7);

        // Session 1 wants top 3 non-self: all their tracks at top, so need positions 4,5,6
        let top3 = tracker.compute_top_n_for_session(1, 3);
        assert_eq!(top3.len(), 3);
        assert_eq!(top3[0].1, "e");
        assert_eq!(top3[1].1, "f");
        assert_eq!(top3[2].1, "g");
    }

    #[test]
    fn test_might_be_in_top_n_fast_rejection() {
        // Test the O(1) fast rejection logic

        // N=10, last_self_pos=5 → threshold = 15
        // Tracks at positions 0-15 might be in top-N, positions 16+ definitely not
        assert!(TopNTracker::might_be_in_top_n(0, 10, 5));
        assert!(TopNTracker::might_be_in_top_n(10, 10, 5));
        assert!(TopNTracker::might_be_in_top_n(15, 10, 5));
        assert!(!TopNTracker::might_be_in_top_n(16, 10, 5));
        assert!(!TopNTracker::might_be_in_top_n(100, 10, 5));

        // N=3, last_self_pos=0 (pure subscriber) → threshold = 3
        assert!(TopNTracker::might_be_in_top_n(0, 3, 0));
        assert!(TopNTracker::might_be_in_top_n(3, 3, 0));
        assert!(!TopNTracker::might_be_in_top_n(4, 3, 0));

        // N=5, last_self_pos=50 (many self tracks) → threshold = 55
        assert!(TopNTracker::might_be_in_top_n(55, 5, 50));
        assert!(!TopNTracker::might_be_in_top_n(56, 5, 50));
    }

    #[test]
    fn test_compute_last_self_position() {
        let tracker = TopNTracker::new(0x100);
        tracker.update_max_n(10);

        // Session 1 has tracks at positions 0, 2, 4
        tracker.register_track(ns("live"), "a".to_string(), 100, 1); // pos 0
        tracker.register_track(ns("live"), "b".to_string(), 90, 2);  // pos 1
        tracker.register_track(ns("live"), "c".to_string(), 80, 1);  // pos 2
        tracker.register_track(ns("live"), "d".to_string(), 70, 3);  // pos 3
        tracker.register_track(ns("live"), "e".to_string(), 60, 1);  // pos 4
        tracker.register_track(ns("live"), "f".to_string(), 50, 4);  // pos 5

        // Session 1's last self-track is at position 4
        assert_eq!(tracker.compute_last_self_position(1), 4);

        // Session 2's last (and only) self-track is at position 1
        assert_eq!(tracker.compute_last_self_position(2), 1);

        // Session 99 has no tracks
        assert_eq!(tracker.compute_last_self_position(99), 0);
    }

    #[test]
    fn test_is_in_top_n_fast() {
        let tracker = TopNTracker::new(0x100);
        tracker.update_max_n(10);

        // Create a snapshot with session 1's tracks at positions 0, 2, 4
        tracker.register_track(ns("live"), "a".to_string(), 100, 1); // pos 0, session 1
        tracker.register_track(ns("live"), "b".to_string(), 90, 2);  // pos 1
        tracker.register_track(ns("live"), "c".to_string(), 80, 1);  // pos 2, session 1
        tracker.register_track(ns("live"), "d".to_string(), 70, 3);  // pos 3
        tracker.register_track(ns("live"), "e".to_string(), 60, 1);  // pos 4, session 1
        tracker.register_track(ns("live"), "f".to_string(), 50, 4);  // pos 5
        tracker.register_track(ns("live"), "g".to_string(), 40, 5);  // pos 6
        tracker.register_track(ns("live"), "h".to_string(), 30, 6);  // pos 7
        tracker.register_track(ns("live"), "i".to_string(), 20, 7);  // pos 8
        tracker.register_track(ns("live"), "j".to_string(), 10, 8);  // pos 9

        let snapshot = tracker.load_snapshot();
        let last_self_pos = tracker.compute_last_self_position(1); // = 4

        // Session 1 wants N=3 non-self
        // Their non-self tracks in order: b(pos1), d(pos3), f(pos5), g(pos6)...
        // Top-3 non-self = b, d, f

        // Track "b" at position 1: might be in top-3 (1 <= 3+4=7), actually IS in top-3
        assert!(tracker.is_in_top_n_fast(&ns("live"), "b", 1, 1, 3, last_self_pos, &snapshot));

        // Track "f" at position 5: might be in top-3 (5 <= 7), actually IS in top-3
        assert!(tracker.is_in_top_n_fast(&ns("live"), "f", 5, 1, 3, last_self_pos, &snapshot));

        // Track "g" at position 6: might be in top-3 (6 <= 7), but NOT in top-3 (4th non-self)
        assert!(!tracker.is_in_top_n_fast(&ns("live"), "g", 6, 1, 3, last_self_pos, &snapshot));

        // Track "j" at position 9: fast rejection (9 > 7), definitely NOT in top-3
        assert!(!tracker.is_in_top_n_fast(&ns("live"), "j", 9, 1, 3, last_self_pos, &snapshot));
    }

    #[test]
    fn test_find_track_position() {
        let tracker = TopNTracker::new(0x100);
        tracker.update_max_n(5);

        tracker.register_track(ns("live"), "a".to_string(), 100, 1);
        tracker.register_track(ns("live"), "b".to_string(), 90, 2);
        tracker.register_track(ns("live"), "c".to_string(), 80, 3);

        assert_eq!(tracker.find_track_position(&ns("live"), "a"), Some(0));
        assert_eq!(tracker.find_track_position(&ns("live"), "b"), Some(1));
        assert_eq!(tracker.find_track_position(&ns("live"), "c"), Some(2));
        assert_eq!(tracker.find_track_position(&ns("live"), "nonexistent"), None);
    }

    #[test]
    fn test_tie_break_most_recent_wins() {
        // With MostRecentWins policy, later updates should win ties
        let config = TopNTrackerConfig {
            tie_break_policy: TieBreakPolicy::MostRecentWins,
            staleness_timeout: None,
            ..Default::default()
        };
        let tracker = TopNTracker::with_config(0x100, config);
        tracker.update_max_n(3);

        // Register with same value - with MostRecentWins, last one should win
        tracker.register_track(ns("live"), "first".to_string(), 100, 1);
        std::thread::sleep(std::time::Duration::from_millis(10));
        tracker.register_track(ns("live"), "second".to_string(), 100, 2);
        std::thread::sleep(std::time::Duration::from_millis(10));
        tracker.register_track(ns("live"), "third".to_string(), 100, 3);

        let top = tracker.compute_top_n_for_session(99, 3);
        assert_eq!(top.len(), 3);
        // Most recent should be first
        assert_eq!(top[0].1, "third");
        assert_eq!(top[1].1, "second");
        assert_eq!(top[2].1, "first");
    }

    #[test]
    fn test_tie_break_oldest_wins_default() {
        // Default config should use OldestWins
        let tracker = TopNTracker::new(0x100);
        assert_eq!(tracker.config().tie_break_policy, TieBreakPolicy::OldestWins);
        tracker.update_max_n(3);

        tracker.register_track(ns("live"), "first".to_string(), 100, 1);
        std::thread::sleep(std::time::Duration::from_millis(10));
        tracker.register_track(ns("live"), "second".to_string(), 100, 2);
        std::thread::sleep(std::time::Duration::from_millis(10));
        tracker.register_track(ns("live"), "third".to_string(), 100, 3);

        let top = tracker.compute_top_n_for_session(99, 3);
        assert_eq!(top.len(), 3);
        // Oldest should be first (first-come-first-served)
        assert_eq!(top[0].1, "first");
        assert_eq!(top[1].1, "second");
        assert_eq!(top[2].1, "third");
    }

    #[test]
    fn test_staleness_filtering() {
        let config = TopNTrackerConfig {
            tie_break_policy: TieBreakPolicy::OldestWins,
            staleness_timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        let tracker = TopNTracker::with_config(0x100, config);
        tracker.update_max_n(5);

        // Register tracks
        tracker.register_track(ns("live"), "a".to_string(), 100, 1);
        tracker.register_track(ns("live"), "b".to_string(), 90, 2);

        // Both should be in snapshot
        assert_eq!(tracker.load_snapshot().len(), 2);

        // Wait for staleness
        std::thread::sleep(Duration::from_millis(150));

        // Register a new track (triggers rebuild which filters stale)
        tracker.register_track(ns("live"), "c".to_string(), 80, 3);

        // Only "c" should remain (a and b are stale)
        let snapshot = tracker.load_snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].track_name, "c");
    }

    #[test]
    fn test_sweep_stale() {
        let config = TopNTrackerConfig {
            tie_break_policy: TieBreakPolicy::OldestWins,
            staleness_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        };
        let tracker = TopNTracker::with_config(0x100, config);
        tracker.update_max_n(5);

        tracker.register_track(ns("live"), "a".to_string(), 100, 1);
        tracker.register_track(ns("live"), "b".to_string(), 90, 2);

        assert_eq!(tracker.num_tracks(), 2);

        // Wait for staleness
        std::thread::sleep(Duration::from_millis(100));

        // Sweep should remove both tracks
        let removed = tracker.sweep_stale();
        assert_eq!(removed, 2);
        assert_eq!(tracker.num_tracks(), 0);
        assert_eq!(tracker.load_snapshot().len(), 0);
    }

    #[test]
    fn test_sweep_stale_no_timeout() {
        // Without staleness timeout, sweep should do nothing
        let tracker = TopNTracker::new(0x100);
        tracker.update_max_n(5);

        tracker.register_track(ns("live"), "a".to_string(), 100, 1);

        let removed = tracker.sweep_stale();
        assert_eq!(removed, 0);
        assert_eq!(tracker.num_tracks(), 1);
    }

    #[test]
    fn test_update_value_refreshes_timestamp() {
        let config = TopNTrackerConfig {
            tie_break_policy: TieBreakPolicy::OldestWins,
            staleness_timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        let tracker = TopNTracker::with_config(0x100, config);
        tracker.update_max_n(5);

        tracker.register_track(ns("live"), "a".to_string(), 100, 1);

        // Wait a bit, then update (even with same value refreshes timestamp)
        std::thread::sleep(Duration::from_millis(60));
        tracker.update_value(&ns("live"), "a", 100);  // Same value, but refreshes timestamp

        // Wait more - would be stale without the refresh
        std::thread::sleep(Duration::from_millis(60));

        // Track should still be fresh because update_value refreshed it
        let removed = tracker.sweep_stale();
        assert_eq!(removed, 0);
        assert_eq!(tracker.num_tracks(), 1);
    }

    #[test]
    fn test_three_speakers_scenario_oldest_wins() {
        // The "worked example" from the design doc with OldestWins policy
        let tracker = TopNTracker::new(0x100);  // Default: OldestWins
        tracker.update_max_n(3);

        // t0: A speaks (value=100)
        tracker.register_track(ns("live"), "A".to_string(), 100, 1);
        std::thread::sleep(Duration::from_millis(5));

        // t1: B speaks (value=100)
        tracker.register_track(ns("live"), "B".to_string(), 100, 2);
        std::thread::sleep(Duration::from_millis(5));

        // t2: C speaks (value=100)
        tracker.register_track(ns("live"), "C".to_string(), 100, 3);

        // With OldestWins: A (oldest) > B > C (newest)
        let top = tracker.compute_top_n_for_session(99, 3);
        assert_eq!(top[0].1, "A");  // Position 1
        assert_eq!(top[1].1, "B");  // Position 2
        assert_eq!(top[2].1, "C");  // Position 3
    }

    #[test]
    fn test_three_speakers_scenario_most_recent_wins() {
        // The "worked example" from the design doc with MostRecentWins policy
        let config = TopNTrackerConfig {
            tie_break_policy: TieBreakPolicy::MostRecentWins,
            staleness_timeout: None,
            ..Default::default()
        };
        let tracker = TopNTracker::with_config(0x100, config);
        tracker.update_max_n(3);

        // t0: A speaks (value=100)
        tracker.register_track(ns("live"), "A".to_string(), 100, 1);
        std::thread::sleep(Duration::from_millis(5));

        // t1: B speaks (value=100)
        tracker.register_track(ns("live"), "B".to_string(), 100, 2);
        std::thread::sleep(Duration::from_millis(5));

        // t2: C speaks (value=100)
        tracker.register_track(ns("live"), "C".to_string(), 100, 3);

        // With MostRecentWins: C (newest) > B > A (oldest)
        let top = tracker.compute_top_n_for_session(99, 3);
        assert_eq!(top[0].1, "C");  // Position 1
        assert_eq!(top[1].1, "B");  // Position 2
        assert_eq!(top[2].1, "A");  // Position 3
    }
}
