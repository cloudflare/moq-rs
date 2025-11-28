use anyhow::Result;
use async_trait::async_trait;
use moq_transport::coding::TrackNamespace;
use url::Url;

/// Handle returned when a namespace is registered with the coordinator.
///
/// Dropping this handle automatically unregisters the namespace.
/// This provides RAII-based cleanup - when the publisher disconnects
/// or the namespace is no longer served, cleanup happens automatically.
pub struct NamespaceRegistration {
    _inner: Box<dyn Send + Sync>,
    _metadata: Option<Vec<(String, String)>>,
}

impl NamespaceRegistration {
    /// Create a new registration handle wrapping any Send + Sync type.
    ///
    /// The wrapped value's `Drop` implementation will be called when
    /// this registration is dropped.
    pub fn new<T: Send + Sync + 'static>(inner: T) -> Self {
        Self {
            _inner: Box::new(inner),
            _metadata: None,
        }
    }

    /// Add metadata as list of key value pair of string: string
    pub fn with_metadata(mut self, metadata: Vec<(String, String)>) -> Self {
        self._metadata = Some(metadata);
        self
    }
}

/// Handle returned when a track is registered under a namespace.
///
/// Dropping this handle automatically unregisters the track.
/// The namespace remains registered even after all tracks are dropped.
pub struct TrackRegistration {
    _inner: Box<dyn Send + Sync>,
}

impl TrackRegistration {
    /// Create a new track registration handle.
    pub fn new<T: Send + Sync + 'static>(inner: T) -> Self {
        Self {
            _inner: Box::new(inner),
        }
    }
}

/// Result of a namespace lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceOrigin(TrackNamespace, Url);

impl NamespaceOrigin {
    /// Create a new NamespaceOrigin.
    pub fn new(namespace: TrackNamespace, url: Url) -> Self {
        Self(namespace, url)
    }

    /// Get the namespace.
    pub fn namespace(&self) -> &TrackNamespace {
        &self.0
    }

    /// Get the URL of the relay serving this namespace.
    pub fn url(&self) -> Url {
        self.1.clone()
    }
}

/// Information about a track within a namespace.
#[derive(Debug, Clone)]
pub struct TrackInfo {
    /// The track namespace
    pub namespace: TrackNamespace,

    /// The track name within the namespace
    pub track_name: String,

    /// Track alias for quick lookup
    pub track_alias: u64,
}

/// Coordinator handles namespace and track registration/discovery across relays.
///
/// Implementations are responsible for:
/// - Tracking which namespaces are served locally
/// - Tracking which tracks are available under each namespace
/// - Caching remote namespace lookups
/// - Communicating with external registries (HTTP API, Redis, etc.)
/// - Periodic refresh/heartbeat of registrations
/// - Cleanup when registrations are dropped
///
/// # Thread Safety
///
/// All methods take `&self` and implementations must be thread-safe.
/// Multiple tasks will call these methods concurrently.
#[async_trait]
pub trait Coordinator: Send + Sync {
    /// Register a namespace as locally available on this relay.
    ///
    /// Called when a publisher sends PUBLISH_NAMESPACE.
    /// The coordinator should:
    /// 1. Record the namespace as locally available
    /// 2. Advertise to external registry if configured
    /// 3. Start any refresh/heartbeat tasks
    /// 4. Return a handle that unregisters on drop
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace being registered
    ///
    /// # Returns
    ///
    /// A `NamespaceRegistration` handle. The namespace remains registered
    /// as long as this handle is held. Dropping it unregisters the namespace.
    async fn register_namespace(&self, namespace: &TrackNamespace)
        -> Result<NamespaceRegistration>;

    /// Unregister a namespace.
    ///
    /// Called when a publisher sends PUBLISH_NAMESPACE_DONE.
    /// This is an explicit unregistration - the registration handle may still exist
    /// but the namespace should be removed from the registry.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to unregister
    async fn unregister_namespace(&self, namespace: &TrackNamespace) -> Result<()>;

    /// Register a track as available under a namespace.
    ///
    /// Called when a publisher sends PUBLISH for a track.
    /// The namespace must already be registered.
    ///
    /// # Arguments
    ///
    /// * `track_info` - Information about the track being registered
    ///
    /// # Returns
    ///
    /// A `TrackRegistration` handle. The track remains registered
    /// as long as this handle is held.
    async fn register_track(&self, track_info: TrackInfo) -> Result<TrackRegistration>;

    /// Unregister a track.
    ///
    /// Called when a publisher sends PUBLISH_DONE.
    /// Only the track is removed, not the namespace.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace containing the track
    /// * `track_name` - The track name to unregister
    async fn unregister_track(&self, namespace: &TrackNamespace, track_name: &str) -> Result<()>;

    /// Lookup where a namespace is served from.
    ///
    /// Called when a subscriber requests a namespace.
    /// The coordinator should check in order:
    /// 1. Local registrations (return `Local`)
    /// 2. Cached remote lookups (return `Remote(url)` if not expired)
    /// 3. External registry (cache and return result)
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to look up
    ///
    /// # Returns
    ///
    /// - `Ok(NamespaceOrigin::Local)` - Served by this relay
    /// - `Ok(NamespaceOrigin::Remote(url))` - Served by remote relay
    /// - `Err` - Namespace not found anywhere
    async fn lookup(&self, namespace: &TrackNamespace) -> Result<NamespaceOrigin>;

    /// Graceful shutdown of the coordinator.
    ///
    /// Called when the relay is shutting down. Implementations should:
    /// - Unregister all local namespaces and tracks
    /// - Cancel refresh tasks
    /// - Close connections to external registries
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}
