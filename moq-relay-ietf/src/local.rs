// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::hash_map;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use moq_transport::{
    coding::{TrackNamespace, TrackNamespacePrefix},
    serve::{FullTrackName, ServeError, Track, TrackReader, TrackWriter},
};
use tokio::sync::{broadcast, mpsc};

use crate::metrics::GaugeGuard;

/// Scope key for the outer level of the two-level registry.
///
/// An empty string (`""`) represents the global/unscoped bucket. All unscoped
/// connections share this bucket — any publisher without a scope can be reached
/// by any subscriber without a scope. This is the default behavior for backward
/// compatibility with pre-scope deployments.
type ScopeKey = String;

/// The scope key used for unscoped (global) registrations.
const UNSCOPED: &str = "";

const NAMESPACE_REQUEST_CHANNEL_CAPACITY: usize = 1024;

/// Capacity of the namespace add/remove notification broadcast channel used by
/// SUBSCRIBE_NAMESPACE handlers.
const NAMESPACE_CHANGE_CHANNEL_CAPACITY: usize = 1024;

/// Capacity of the PUBLISH track add/remove notification broadcast channel used
/// by Publish/Both fan-out.
const TRACK_CHANGE_CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone)]
struct NamespaceSource {
    requests: mpsc::Sender<TrackWriter>,
}

struct NamespaceEntry {
    local: Option<NamespaceSource>,
    remote: Weak<RemoteNamespaceSource>,
}

struct RemoteNamespaceSource {
    locals: Locals,
    scope_key: ScopeKey,
    namespace: TrackNamespace,
}

struct TrackEntry {
    reader: TrackReader,
    source: TrackSource,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum TrackSource {
    Published,
    Cache,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceChange {
    pub scope: Option<String>,
    pub namespace: TrackNamespace,
    pub added: bool,
}

#[derive(Clone)]
pub enum TrackChange {
    Added {
        scope: Option<String>,
        track: TrackReader,
    },
    Removed {
        scope: Option<String>,
        full_name: FullTrackName,
    },
}

/// Relay-local registry.
///
/// Actual media tracks are always stored by exact Full Track Name. Namespace
/// entries combine discovery metadata with an optional local PUBLISH_NAMESPACE
/// route source that can be asked for a missing track.
#[derive(Clone)]
pub struct Locals {
    /// Actual media tracks, indexed by (scope, full track name).
    tracks: Arc<Mutex<HashMap<ScopeKey, HashMap<FullTrackName, TrackEntry>>>>,

    /// Namespace sources, indexed by (scope, namespace) and matched by prefix.
    /// Each entry can have one local PUBLISH_NAMESPACE route source and shared
    /// ownership by any number of remote discovery registrations.
    namespaces: Arc<Mutex<HashMap<ScopeKey, HashMap<TrackNamespace, NamespaceEntry>>>>,

    /// Namespace add/remove notifications for SUBSCRIBE_NAMESPACE handlers.
    namespace_changes: broadcast::Sender<NamespaceChange>,

    /// Actual PUBLISH track add/remove notifications for Publish/Both fan-out.
    track_changes: broadcast::Sender<TrackChange>,
}

impl Default for Locals {
    fn default() -> Self {
        Self::new()
    }
}

impl Locals {
    pub fn new() -> Self {
        let (namespace_changes, _) = broadcast::channel(NAMESPACE_CHANGE_CHANNEL_CAPACITY);
        let (track_changes, _) = broadcast::channel(TRACK_CHANGE_CHANNEL_CAPACITY);
        Self {
            tracks: Default::default(),
            namespaces: Default::default(),
            namespace_changes,
            track_changes,
        }
    }

    pub fn subscribe_namespace_changes(&self) -> broadcast::Receiver<NamespaceChange> {
        self.namespace_changes.subscribe()
    }

    pub fn subscribe_track_changes(&self) -> broadcast::Receiver<TrackChange> {
        self.track_changes.subscribe()
    }

    pub fn list_namespaces_matching(
        &self,
        scope: Option<&str>,
        prefix: &TrackNamespacePrefix,
    ) -> Vec<TrackNamespace> {
        let Ok(namespaces) = self.namespaces.lock() else {
            return Vec::new();
        };
        let Some(bucket) = namespaces.get(scope.unwrap_or(UNSCOPED)) else {
            return Vec::new();
        };

        bucket
            .keys()
            .filter(|namespace| prefix.is_prefix_of(namespace))
            .cloned()
            .collect()
    }

    pub fn list_tracks_matching(
        &self,
        scope: Option<&str>,
        prefix: &TrackNamespacePrefix,
    ) -> Vec<TrackReader> {
        let Ok(mut tracks) = self.tracks.lock() else {
            return Vec::new();
        };
        let Some(bucket) = tracks.get_mut(scope.unwrap_or(UNSCOPED)) else {
            return Vec::new();
        };

        // Prune closed tracks and collect matching readers in a single pass so the
        // `tracks` lock (contended with register/retrieve/drop) is held as briefly
        // as possible.
        let mut matches = Vec::new();
        bucket.retain(|full_name, entry| {
            if entry.reader.is_closed() {
                return false;
            }
            if entry.source == TrackSource::Published && prefix.is_prefix_of(&full_name.namespace) {
                matches.push(entry.reader.clone());
            }
            true
        });
        matches
    }

    /// Register namespace routing metadata from PUBLISH_NAMESPACE.
    ///
    /// This does not register any media tracks. It only creates a request queue
    /// used when a downstream SUBSCRIBE asks for a missing track under this
    /// namespace.
    pub async fn register_namespace(
        &mut self,
        scope: Option<&str>,
        namespace: TrackNamespace,
    ) -> anyhow::Result<(LocalNamespaceRegistration, mpsc::Receiver<TrackWriter>)> {
        let scope_key = scope.unwrap_or(UNSCOPED).to_string();
        let (tx, rx) = mpsc::channel(NAMESPACE_REQUEST_CHANNEL_CAPACITY);

        let added = {
            let mut namespaces = self
                .namespaces
                .lock()
                .map_err(|_| ServeError::internal_ctx("locals namespace registry lock poisoned"))?;
            let bucket = namespaces.entry(scope_key.clone()).or_default();
            match bucket.entry(namespace.clone()) {
                hash_map::Entry::Vacant(entry) => {
                    entry.insert(NamespaceEntry {
                        local: Some(NamespaceSource { requests: tx }),
                        remote: Weak::new(),
                    });
                    true
                }
                hash_map::Entry::Occupied(mut entry) => {
                    if entry.get().local.is_some() {
                        return Err(ServeError::Duplicate.into());
                    }
                    entry.get_mut().local = Some(NamespaceSource { requests: tx });
                    false
                }
            }
        };

        let registration = LocalNamespaceRegistration {
            locals: self.clone(),
            scope_key,
            namespace: namespace.clone(),
            _gauge_guard: GaugeGuard::new("moq_relay_announced_namespaces"),
        };

        if added {
            let _ = self.namespace_changes.send(NamespaceChange {
                scope: scope_key_to_option(&registration.scope_key),
                namespace,
                added: true,
            });
        }

        Ok((registration, rx))
    }

    /// Register remote discovery metadata for one exact namespace.
    pub fn register_remote_namespace(
        &self,
        scope: Option<&str>,
        namespace: TrackNamespace,
    ) -> anyhow::Result<RemoteNamespaceRegistration> {
        let scope_key = scope.unwrap_or(UNSCOPED).to_string();
        let (source, added) = {
            let mut namespaces = self
                .namespaces
                .lock()
                .map_err(|_| ServeError::internal_ctx("locals namespace registry lock poisoned"))?;
            let bucket = namespaces.entry(scope_key.clone()).or_default();
            match bucket.entry(namespace.clone()) {
                hash_map::Entry::Vacant(entry) => {
                    let source = Arc::new(RemoteNamespaceSource {
                        locals: self.clone(),
                        scope_key: scope_key.clone(),
                        namespace: namespace.clone(),
                    });
                    entry.insert(NamespaceEntry {
                        local: None,
                        remote: Arc::downgrade(&source),
                    });
                    (source, true)
                }
                hash_map::Entry::Occupied(mut entry) => {
                    if let Some(source) = entry.get().remote.upgrade() {
                        (source, false)
                    } else {
                        let source = Arc::new(RemoteNamespaceSource {
                            locals: self.clone(),
                            scope_key: scope_key.clone(),
                            namespace: namespace.clone(),
                        });
                        entry.get_mut().remote = Arc::downgrade(&source);
                        (source, false)
                    }
                }
            }
        };

        let registration = RemoteNamespaceRegistration { _source: source };

        if added {
            let _ = self.namespace_changes.send(NamespaceChange {
                scope: scope_key_to_option(&scope_key),
                namespace,
                added: true,
            });
        }

        Ok(registration)
    }

    /// Register one exact track received via PUBLISH.
    pub async fn register_track(
        &mut self,
        scope: Option<&str>,
        track: TrackReader,
    ) -> anyhow::Result<LocalTrackRegistration> {
        let full_name = FullTrackName {
            namespace: track.namespace.clone(),
            name: track.name.clone(),
        };
        self.insert_track_with_registration(scope, full_name, track)
            .await
    }

    async fn insert_track_with_registration(
        &mut self,
        scope: Option<&str>,
        full_name: FullTrackName,
        track: TrackReader,
    ) -> anyhow::Result<LocalTrackRegistration> {
        let scope_key = scope.unwrap_or(UNSCOPED).to_string();

        let mut tracks = self
            .tracks
            .lock()
            .map_err(|_| ServeError::internal_ctx("locals track registry lock poisoned"))?;
        let bucket = tracks.entry(scope_key.clone()).or_default();
        match bucket.entry(full_name.clone()) {
            hash_map::Entry::Vacant(entry) => entry.insert(TrackEntry {
                reader: track.clone(),
                source: TrackSource::Published,
            }),
            hash_map::Entry::Occupied(_) => return Err(ServeError::Duplicate.into()),
        };

        let _ = self.track_changes.send(TrackChange::Added {
            scope: scope_key_to_option(&scope_key),
            track,
        });

        Ok(LocalTrackRegistration {
            locals: self.clone(),
            scope_key,
            full_name,
            _gauge_guard: GaugeGuard::new("moq_relay_active_published_tracks"),
        })
    }

    /// Retrieve one actual media track by exact Full Track Name.
    pub fn retrieve_track(
        &self,
        scope: Option<&str>,
        full_name: &FullTrackName,
    ) -> Option<TrackReader> {
        let mut tracks = self.tracks.lock().ok()?;
        let bucket = tracks.get_mut(scope.unwrap_or(UNSCOPED))?;
        if bucket
            .get(full_name)
            .is_some_and(|entry| entry.reader.is_closed())
        {
            bucket.remove(full_name);
            return None;
        }
        bucket.get(full_name).map(|entry| entry.reader.clone())
    }

    /// Return the best namespace route source for a requested namespace.
    fn route_namespace(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> Option<NamespaceSource> {
        let namespaces = self.namespaces.lock().ok()?;
        let bucket = namespaces.get(scope.unwrap_or(UNSCOPED))?;

        let mut best_match: Option<NamespaceSource> = None;
        let mut best_len = 0;

        for (registered_ns, entry) in bucket.iter() {
            let Some(source) = entry.local.as_ref() else {
                continue;
            };

            if namespace.fields.len() >= registered_ns.fields.len() {
                let is_prefix = registered_ns
                    .fields
                    .iter()
                    .zip(namespace.fields.iter())
                    .all(|(a, b)| a == b);

                if is_prefix && registered_ns.fields.len() > best_len {
                    best_match = Some(source.clone());
                    best_len = registered_ns.fields.len();
                }
            }
        }

        best_match
    }

    /// Get an existing exact track or request it from a matching namespace source.
    ///
    /// This replaces the old `TracksReader::subscribe` relay registry behavior:
    /// the actual track reader is stored in `tracks`, while PUBLISH_NAMESPACE is
    /// only a source to ask when a track is missing.
    pub async fn get_or_request_track(
        &mut self,
        scope: Option<&str>,
        namespace: TrackNamespace,
        track_name: impl Into<moq_transport::coding::TrackName>,
    ) -> Option<TrackReader> {
        let track_name = track_name.into();
        let full_name = FullTrackName {
            namespace: namespace.clone(),
            name: track_name.clone(),
        };

        if let Some(track) = self.retrieve_track(scope, &full_name) {
            return Some(track);
        }

        let source = self.route_namespace(scope, &namespace)?;

        // Reserve the pull-through cache slot under a single lock so concurrent
        // misses for the same track collapse onto one produced reader. The caller
        // that finds no live entry claims the slot with a freshly produced track and
        // owns requesting it from the source; concurrent callers share the reserved
        // reader instead of racing to insert and failing with a spurious `None`.
        let (writer, reader) = {
            let mut tracks = self.tracks.lock().ok()?;
            let bucket = tracks
                .entry(scope.unwrap_or(UNSCOPED).to_string())
                .or_default();

            if let Some(entry) = bucket.get(&full_name) {
                if !entry.reader.is_closed() {
                    return Some(entry.reader.clone());
                }
            }

            // Vacant slot (or a stale, closed reader): claim it with a fresh track.
            let (writer, reader) = Track::new(namespace, track_name).produce();
            bucket.insert(
                full_name.clone(),
                TrackEntry {
                    reader: reader.clone(),
                    source: TrackSource::Cache,
                },
            );
            (writer, reader)
        };

        if source.requests.send(writer).await.is_err() {
            if let Ok(mut tracks) = self.tracks.lock() {
                if let Some(bucket) = tracks.get_mut(scope.unwrap_or(UNSCOPED)) {
                    bucket.remove(&full_name);
                }
            }
            return None;
        }

        Some(reader)
    }
}

pub struct LocalNamespaceRegistration {
    locals: Locals,
    scope_key: ScopeKey,
    namespace: TrackNamespace,
    _gauge_guard: GaugeGuard,
}

impl Drop for LocalNamespaceRegistration {
    fn drop(&mut self) {
        let ns = self.namespace.to_utf8_path();
        let scope = if self.scope_key.is_empty() {
            "<unscoped>"
        } else {
            &self.scope_key
        };
        tracing::debug!(namespace = %ns, scope = %scope, "deregistering namespace route source from locals");

        let mut removed = false;
        if let Ok(mut namespaces) = self.locals.namespaces.lock() {
            if let Some(bucket) = namespaces.get_mut(self.scope_key.as_str()) {
                let remove_namespace = if let Some(entry) = bucket.get_mut(&self.namespace) {
                    entry.local.take().is_some() && entry.remote.upgrade().is_none()
                } else {
                    false
                };
                if remove_namespace {
                    removed = bucket.remove(&self.namespace).is_some();
                }
                if bucket.is_empty() {
                    namespaces.remove(self.scope_key.as_str());
                }
            }
        }

        if removed {
            let _ = self.locals.namespace_changes.send(NamespaceChange {
                scope: scope_key_to_option(&self.scope_key),
                namespace: self.namespace.clone(),
                added: false,
            });
        }
    }
}

pub struct RemoteNamespaceRegistration {
    _source: Arc<RemoteNamespaceSource>,
}

impl Drop for RemoteNamespaceSource {
    fn drop(&mut self) {
        let mut removed = false;
        if let Ok(mut namespaces) = self.locals.namespaces.lock() {
            if let Some(bucket) = namespaces.get_mut(self.scope_key.as_str()) {
                let remove_namespace = if let Some(entry) = bucket.get_mut(&self.namespace) {
                    if std::ptr::eq(entry.remote.as_ptr(), self) {
                        entry.remote = Weak::new();
                        entry.local.is_none()
                    } else {
                        false
                    }
                } else {
                    false
                };
                if remove_namespace {
                    removed = bucket.remove(&self.namespace).is_some();
                }
                if bucket.is_empty() {
                    namespaces.remove(self.scope_key.as_str());
                }
            }
        }

        if removed {
            let _ = self.locals.namespace_changes.send(NamespaceChange {
                scope: scope_key_to_option(&self.scope_key),
                namespace: self.namespace.clone(),
                added: false,
            });
        }
    }
}

fn scope_key_to_option(scope_key: &str) -> Option<String> {
    if scope_key.is_empty() {
        None
    } else {
        Some(scope_key.to_string())
    }
}

pub struct LocalTrackRegistration {
    locals: Locals,
    scope_key: ScopeKey,
    full_name: FullTrackName,
    _gauge_guard: GaugeGuard,
}

impl Drop for LocalTrackRegistration {
    fn drop(&mut self) {
        let namespace = self.full_name.namespace.to_utf8_path();
        let track = self.full_name.name.to_string();
        let scope = if self.scope_key.is_empty() {
            "<unscoped>"
        } else {
            &self.scope_key
        };
        tracing::debug!(namespace = %namespace, track = %track, scope = %scope, "deregistering track from locals");

        let mut removed = false;
        if let Ok(mut tracks) = self.locals.tracks.lock() {
            if let Some(bucket) = tracks.get_mut(self.scope_key.as_str()) {
                removed = bucket.remove(&self.full_name).is_some();
                if bucket.is_empty() {
                    tracks.remove(self.scope_key.as_str());
                }
            }
        }

        if removed {
            let _ = self.locals.track_changes.send(TrackChange::Removed {
                scope: scope_key_to_option(&self.scope_key),
                full_name: self.full_name.clone(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moq_transport::coding::TrackName;

    fn ns(path: &str) -> TrackNamespace {
        TrackNamespace::from_utf8_path(path)
    }

    fn full(namespace: &TrackNamespace, name: &str) -> FullTrackName {
        FullTrackName {
            namespace: namespace.clone(),
            name: TrackName::from(name),
        }
    }

    async fn assert_no_namespace_change(changes: &mut broadcast::Receiver<NamespaceChange>) {
        let change =
            tokio::time::timeout(std::time::Duration::from_millis(50), changes.recv()).await;
        assert!(change.is_err(), "unexpected namespace change: {change:?}");
    }

    #[tokio::test]
    async fn remote_namespace_is_visible_but_not_a_track_route() {
        let mut locals = Locals::new();
        let namespace = ns("room/remote");
        let registration = locals
            .register_remote_namespace(Some("scope-a"), namespace.clone())
            .expect("remote namespace should register");

        assert_eq!(
            locals.list_namespaces_matching(
                Some("scope-a"),
                &TrackNamespacePrefix::from_utf8_path("room"),
            ),
            vec![namespace.clone()]
        );
        assert!(locals
            .get_or_request_track(Some("scope-a"), namespace, "video")
            .await
            .is_none());

        drop(registration);
    }

    #[tokio::test]
    async fn local_then_remote_emits_only_union_transitions() {
        let mut locals = Locals::new();
        let mut changes = locals.subscribe_namespace_changes();
        let namespace = ns("room/shared");
        let (local, _requests) = locals
            .register_namespace(None, namespace.clone())
            .await
            .expect("local namespace should register");
        assert!(changes.recv().await.expect("local add").added);

        let remote = locals
            .register_remote_namespace(None, namespace.clone())
            .expect("remote namespace should coexist");
        assert_no_namespace_change(&mut changes).await;

        drop(local);
        assert_no_namespace_change(&mut changes).await;
        assert_eq!(
            locals.list_namespaces_matching(
                None,
                &TrackNamespacePrefix::from_utf8_path("room/shared"),
            ),
            vec![namespace.clone()]
        );

        drop(remote);
        let removed = changes.recv().await.expect("last-source removal");
        assert_eq!(removed.namespace, namespace);
        assert!(!removed.added);
    }

    #[tokio::test]
    async fn remote_then_local_keeps_local_route_until_last_drop() {
        let mut locals = Locals::new();
        let mut changes = locals.subscribe_namespace_changes();
        let namespace = ns("room/shared");
        let remote = locals
            .register_remote_namespace(None, namespace.clone())
            .expect("remote namespace should register");
        assert!(changes.recv().await.expect("remote add").added);

        let (local, mut requests) = locals
            .register_namespace(None, namespace.clone())
            .await
            .expect("local namespace should coexist");
        assert_no_namespace_change(&mut changes).await;

        drop(remote);
        assert_no_namespace_change(&mut changes).await;
        let requested = locals
            .get_or_request_track(None, namespace.clone(), "video")
            .await
            .expect("local route should remain usable");
        drop(requested);
        assert!(requests.recv().await.is_some());

        drop(local);
        let removed = changes.recv().await.expect("local removal");
        assert_eq!(removed.namespace, namespace);
        assert!(!removed.added);
    }

    #[tokio::test]
    async fn multiple_remote_sources_remove_on_last_drop() {
        let locals = Locals::new();
        let mut changes = locals.subscribe_namespace_changes();
        let namespace = ns("room/remote");
        let first = locals
            .register_remote_namespace(None, namespace.clone())
            .expect("first remote should register");
        assert!(changes.recv().await.expect("first add").added);

        let second = locals
            .register_remote_namespace(None, namespace.clone())
            .expect("second remote should register");
        assert_no_namespace_change(&mut changes).await;

        drop(first);
        assert_no_namespace_change(&mut changes).await;
        drop(second);

        let removed = changes.recv().await.expect("last remote removal");
        assert_eq!(removed.namespace, namespace);
        assert!(!removed.added);
    }

    #[tokio::test]
    async fn remote_source_does_not_allow_duplicate_local_registration() {
        let mut locals = Locals::new();
        let namespace = ns("room/shared");
        let _remote = locals
            .register_remote_namespace(None, namespace.clone())
            .expect("remote namespace should register");
        let (_local, _requests) = locals
            .register_namespace(None, namespace.clone())
            .await
            .expect("first local namespace should register");

        let duplicate = locals.register_namespace(None, namespace).await;
        assert!(duplicate.is_err());
    }

    #[tokio::test]
    async fn remote_namespace_scopes_stay_isolated() {
        let locals = Locals::new();
        let _a = locals
            .register_remote_namespace(Some("scope-a"), ns("room/a"))
            .expect("scope-a remote should register");
        let _b = locals
            .register_remote_namespace(Some("scope-b"), ns("room/b"))
            .expect("scope-b remote should register");

        assert_eq!(
            locals.list_namespaces_matching(
                Some("scope-a"),
                &TrackNamespacePrefix::from_utf8_path("room"),
            ),
            vec![ns("room/a")]
        );
        assert_eq!(
            locals.list_namespaces_matching(
                Some("scope-b"),
                &TrackNamespacePrefix::from_utf8_path("room"),
            ),
            vec![ns("room/b")]
        );
    }

    #[tokio::test]
    async fn register_track_makes_exact_track_retrievable_until_drop() {
        let mut locals = Locals::new();
        let namespace = ns("room/123");
        let (writer, reader) = Track::new(namespace.clone(), "audio").produce();
        let key = full(&namespace, "audio");

        let registration = locals
            .register_track(None, reader.clone())
            .await
            .expect("track registration should succeed");

        assert!(locals.retrieve_track(None, &key).is_some());

        drop(registration);
        assert!(locals.retrieve_track(None, &key).is_none());

        drop(writer);
    }

    #[tokio::test]
    async fn get_or_request_track_uses_namespace_source_and_caches_reader() {
        let mut locals = Locals::new();
        let namespace = ns("room/123");
        let (_registration, mut requests) = locals
            .register_namespace(None, namespace.clone())
            .await
            .expect("namespace source should register");

        let reader = locals
            .get_or_request_track(None, namespace.clone(), "video")
            .await
            .expect("missing track should be requested from namespace source");

        let requested = requests
            .recv()
            .await
            .expect("source should get TrackWriter");
        assert_eq!(requested.namespace, namespace);
        assert_eq!(requested.name, TrackName::from("video"));

        let key = full(&namespace, "video");
        let cached = locals
            .retrieve_track(None, &key)
            .expect("requested track should be cached");
        assert_eq!(cached.namespace, reader.namespace);
        assert_eq!(cached.name, reader.name);

        let reader_again = locals
            .get_or_request_track(None, namespace.clone(), "video")
            .await
            .expect("cached track should be returned");
        assert_eq!(reader_again.namespace, namespace);
        assert_eq!(reader_again.name, TrackName::from("video"));

        let no_second_request =
            tokio::time::timeout(std::time::Duration::from_millis(50), requests.recv()).await;
        assert!(
            no_second_request.is_err(),
            "cache hit should not request again"
        );
    }

    #[tokio::test]
    async fn concurrent_get_or_request_track_deduplicates_request() {
        let mut locals = Locals::new();
        let namespace = ns("room/123");
        let (_registration, mut requests) = locals
            .register_namespace(None, namespace.clone())
            .await
            .expect("namespace source should register");

        let mut first = locals.clone();
        let mut second = locals.clone();
        let (first_reader, second_reader) = tokio::join!(
            first.get_or_request_track(None, namespace.clone(), "video"),
            second.get_or_request_track(None, namespace.clone(), "video"),
        );

        let first_reader = first_reader.expect("first request should get a reader");
        let second_reader = second_reader.expect("second request should get cached reader");
        assert_eq!(first_reader.namespace, namespace);
        assert_eq!(second_reader.namespace, namespace);
        assert_eq!(first_reader.name, TrackName::from("video"));
        assert_eq!(second_reader.name, TrackName::from("video"));

        requests
            .recv()
            .await
            .expect("source should receive one TrackWriter");
        let no_second_request =
            tokio::time::timeout(std::time::Duration::from_millis(50), requests.recv()).await;
        assert!(
            no_second_request.is_err(),
            "concurrent misses should be deduplicated"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_get_or_request_track_multi_thread_never_returns_spurious_none() {
        // Exercise the cache-slot race across real threads: many callers miss the
        // cache for the same track at once. Every caller must receive a reader (the
        // winner's reserved reader), and the source must see exactly one request.
        for _ in 0..64 {
            let mut locals = Locals::new();
            let namespace = ns("room/123");
            let (_registration, mut requests) = locals
                .register_namespace(None, namespace.clone())
                .await
                .expect("namespace source should register");

            let mut handles = Vec::new();
            for _ in 0..8 {
                let mut clone = locals.clone();
                let namespace = namespace.clone();
                handles.push(tokio::spawn(async move {
                    clone.get_or_request_track(None, namespace, "video").await
                }));
            }

            for handle in handles {
                let reader = handle.await.expect("task should not panic");
                assert!(
                    reader.is_some(),
                    "every concurrent caller should receive a reader"
                );
            }

            let first = requests
                .recv()
                .await
                .expect("source should receive exactly one TrackWriter");
            assert_eq!(first.namespace, namespace);
            let extra =
                tokio::time::timeout(std::time::Duration::from_millis(50), requests.recv()).await;
            assert!(
                extra.is_err(),
                "concurrent misses across threads must be deduplicated"
            );
        }
    }

    #[tokio::test]
    async fn get_or_request_track_uses_longest_namespace_prefix() {
        let mut locals = Locals::new();
        let (_short_registration, mut short_requests) = locals
            .register_namespace(None, ns("room"))
            .await
            .expect("short prefix should register");
        let (_long_registration, mut long_requests) = locals
            .register_namespace(None, ns("room/123"))
            .await
            .expect("long prefix should register");

        let requested_ns = ns("room/123/camera");
        let reader = locals
            .get_or_request_track(None, requested_ns.clone(), "video")
            .await
            .expect("track should be requested from longest prefix");
        assert_eq!(reader.namespace, requested_ns);

        let long_request = long_requests
            .recv()
            .await
            .expect("longest prefix should receive the request");
        assert_eq!(long_request.namespace, ns("room/123/camera"));
        assert_eq!(long_request.name, TrackName::from("video"));

        let no_short_request =
            tokio::time::timeout(std::time::Duration::from_millis(50), short_requests.recv()).await;
        assert!(
            no_short_request.is_err(),
            "shorter prefix should not receive request"
        );
    }

    #[tokio::test]
    async fn get_or_request_track_returns_none_without_track_or_namespace_source() {
        let mut locals = Locals::new();
        let result = locals
            .get_or_request_track(None, ns("unknown"), "video")
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn list_namespaces_matching_filters_by_prefix_and_scope() {
        let mut locals = Locals::new();
        let _room = locals
            .register_namespace(Some("scope-a"), ns("room/123"))
            .await
            .expect("room should register");
        let _camera = locals
            .register_namespace(Some("scope-a"), ns("room/123/camera"))
            .await
            .expect("camera should register");
        let _other_scope = locals
            .register_namespace(Some("scope-b"), ns("room/123/other"))
            .await
            .expect("other scope should register");

        let mut matches = locals.list_namespaces_matching(
            Some("scope-a"),
            &TrackNamespacePrefix::from_utf8_path("room/123"),
        );
        matches.sort_by_key(|namespace| namespace.to_utf8_path());

        assert_eq!(matches, vec![ns("room/123"), ns("room/123/camera")]);
    }

    #[tokio::test]
    async fn list_namespaces_matching_keeps_unscoped_and_scoped_separate() {
        let mut locals = Locals::new();
        let _global = locals
            .register_namespace(None, ns("room/global"))
            .await
            .expect("global namespace should register");
        let _scoped = locals
            .register_namespace(Some("scope-a"), ns("room/scoped"))
            .await
            .expect("scoped namespace should register");

        let global =
            locals.list_namespaces_matching(None, &TrackNamespacePrefix::from_utf8_path("room"));
        assert_eq!(global, vec![ns("room/global")]);

        let scoped = locals.list_namespaces_matching(
            Some("scope-a"),
            &TrackNamespacePrefix::from_utf8_path("room"),
        );
        assert_eq!(scoped, vec![ns("room/scoped")]);
    }

    #[tokio::test]
    async fn namespace_changes_reports_register_and_drop() {
        let mut locals = Locals::new();
        let mut changes = locals.subscribe_namespace_changes();
        let namespace = ns("room/123");

        let registration = locals
            .register_namespace(Some("scope-a"), namespace.clone())
            .await
            .expect("namespace should register")
            .0;

        let added = changes.recv().await.expect("added event");
        assert_eq!(
            added,
            NamespaceChange {
                scope: Some("scope-a".to_string()),
                namespace: namespace.clone(),
                added: true,
            }
        );

        drop(registration);

        let removed = changes.recv().await.expect("removed event");
        assert_eq!(
            removed,
            NamespaceChange {
                scope: Some("scope-a".to_string()),
                namespace,
                added: false,
            }
        );
    }

    #[tokio::test]
    async fn namespace_changes_uses_none_for_unscoped() {
        let mut locals = Locals::new();
        let mut changes = locals.subscribe_namespace_changes();
        let namespace = ns("room/123");

        let _registration = locals
            .register_namespace(None, namespace.clone())
            .await
            .expect("namespace should register")
            .0;

        let added = changes.recv().await.expect("added event");
        assert_eq!(
            added,
            NamespaceChange {
                scope: None,
                namespace,
                added: true,
            }
        );
    }

    #[tokio::test]
    async fn list_tracks_matching_filters_by_namespace_prefix_and_scope() {
        let mut locals = Locals::new();
        let (_writer_a, reader_a) = Track::new(ns("room/123"), "audio").produce();
        let (_writer_b, reader_b) = Track::new(ns("room/123/camera"), "video").produce();
        let (_writer_other_scope, reader_other_scope) =
            Track::new(ns("room/123"), "other").produce();

        let _a = locals
            .register_track(Some("scope-a"), reader_a)
            .await
            .expect("track a should register");
        let _b = locals
            .register_track(Some("scope-a"), reader_b)
            .await
            .expect("track b should register");
        let _other = locals
            .register_track(Some("scope-b"), reader_other_scope)
            .await
            .expect("other scope should register");

        let mut tracks = locals.list_tracks_matching(
            Some("scope-a"),
            &TrackNamespacePrefix::from_utf8_path("room/123"),
        );
        tracks.sort_by_key(|track| format!("{}/{}", track.namespace, track.name));

        assert_eq!(tracks.len(), 2);
        assert_eq!(tracks[0].namespace, ns("room/123"));
        assert_eq!(tracks[0].name, TrackName::from("audio"));
        assert_eq!(tracks[1].namespace, ns("room/123/camera"));
        assert_eq!(tracks[1].name, TrackName::from("video"));
    }

    #[tokio::test]
    async fn list_tracks_matching_excludes_pull_through_cache_entries() {
        let mut locals = Locals::new();
        let namespace = ns("room/123");
        let (_registration, mut requests) = locals
            .register_namespace(None, namespace.clone())
            .await
            .expect("namespace source should register");

        let _reader = locals
            .get_or_request_track(None, namespace.clone(), "video")
            .await
            .expect("missing track should be requested and cached");
        let _requested = requests.recv().await.expect("source should get request");

        let tracks =
            locals.list_tracks_matching(None, &TrackNamespacePrefix::from_utf8_path("room/123"));

        assert!(tracks.is_empty());
    }

    #[tokio::test]
    async fn list_tracks_matching_keeps_unscoped_and_scoped_separate() {
        let mut locals = Locals::new();
        let (_global_writer, global_reader) = Track::new(ns("room/global"), "audio").produce();
        let (_scoped_writer, scoped_reader) = Track::new(ns("room/scoped"), "video").produce();

        let _global = locals
            .register_track(None, global_reader)
            .await
            .expect("global track should register");
        let _scoped = locals
            .register_track(Some("scope-a"), scoped_reader)
            .await
            .expect("scoped track should register");

        let global =
            locals.list_tracks_matching(None, &TrackNamespacePrefix::from_utf8_path("room"));
        assert_eq!(global.len(), 1);
        assert_eq!(global[0].namespace, ns("room/global"));
        assert_eq!(global[0].name, TrackName::from("audio"));

        let scoped = locals.list_tracks_matching(
            Some("scope-a"),
            &TrackNamespacePrefix::from_utf8_path("room"),
        );
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].namespace, ns("room/scoped"));
        assert_eq!(scoped[0].name, TrackName::from("video"));
    }

    #[tokio::test]
    async fn track_changes_reports_register_and_drop() {
        let mut locals = Locals::new();
        let mut changes = locals.subscribe_track_changes();
        let namespace = ns("room/123");
        let (_writer, reader) = Track::new(namespace.clone(), "audio").produce();

        let registration = locals
            .register_track(Some("scope-a"), reader)
            .await
            .expect("track should register");

        let added = changes.recv().await.expect("added event");
        match added {
            TrackChange::Added { scope, track } => {
                assert_eq!(scope, Some("scope-a".to_string()));
                assert_eq!(track.namespace, namespace);
                assert_eq!(track.name, TrackName::from("audio"));
            }
            TrackChange::Removed { .. } => panic!("expected added event"),
        }

        drop(registration);

        let removed = changes.recv().await.expect("removed event");
        match removed {
            TrackChange::Removed { scope, full_name } => {
                assert_eq!(scope, Some("scope-a".to_string()));
                assert_eq!(full_name.namespace, ns("room/123"));
                assert_eq!(full_name.name, TrackName::from("audio"));
            }
            TrackChange::Added { .. } => panic!("expected removed event"),
        }
    }
}
