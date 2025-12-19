use std::net::SocketAddr;

use async_trait::async_trait;
use moq_native_ietf::quic;
use moq_transport::coding::TrackNamespace;
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    #[error("namespace not found")]
    NamespaceNotFound,

    #[error("namespace already registered")]
    NamespaceAlreadyRegistered,

    #[error("Internal Error: {0}")]
    Other(anyhow::Error),
}

impl From<anyhow::Error> for CoordinatorError {
    fn from(err: anyhow::Error) -> Self {
        Self::Other(err)
    }
}

impl From<tokio::task::JoinError> for CoordinatorError {
    fn from(err: tokio::task::JoinError) -> Self {
        Self::Other(err.into())
    }
}

impl From<std::io::Error> for CoordinatorError {
    fn from(err: std::io::Error) -> Self {
        Self::Other(err.into())
    }
}

pub type CoordinatorResult<T> = std::result::Result<T, CoordinatorError>;

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

/// Result of a namespace lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceOrigin {
    /// The namespace of the track belongs to
    namespace: TrackNamespace,
    /// The URL of the relay serving this namespace
    /// If the relay is not discoverable via this URL, use `socket_addr`
    /// But you still have to pass a valid URL because the TLS verification
    /// happens for hostname
    url: Url,
    /// The socket address of the relay if the relay is not approachable
    /// via DNS lookup, This is to bypass DNS lookups.
    socket_addr: Option<SocketAddr>,
    /// Additional metadata associated with this namespace
    metadata: Option<Vec<(String, String)>>,
}

impl NamespaceOrigin {
    /// Create a new NamespaceOrigin.
    pub fn new(namespace: TrackNamespace, url: Url, addr: Option<SocketAddr>) -> Self {
        Self {
            namespace,
            url,
            socket_addr: addr,
            metadata: None,
        }
    }
    pub fn with_metadata(mut self, values: (String, String)) -> Self {
        if let Some(metadata) = &mut self.metadata {
            metadata.push(values);
        } else {
            self.metadata = Some(vec![values]);
        }
        self
    }

    /// Get the namespace.
    pub fn namespace(&self) -> &TrackNamespace {
        &self.namespace
    }

    /// Get the URL of the relay serving this namespace.
    pub fn url(&self) -> Url {
        self.url.clone()
    }

    pub fn addr(&self) -> Option<SocketAddr> {
        self.socket_addr
    }

    /// Get the metadata associated with this namespace.
    pub fn metadata(&self) -> Option<Vec<(String, String)>> {
        self.metadata.clone()
    }
}

/// Coordinator handles namespace registration/discovery across relays.
///
/// Implementations are responsible for:
/// - Tracking which namespaces are served locally
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
    async fn register_namespace(
        &self,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<NamespaceRegistration>;

    /// Unregister a namespace.
    ///
    /// Called when a publisher sends PUBLISH_NAMESPACE_DONE.
    /// This is an explicit unregistration - the registration handle may still exist
    /// but the namespace should be removed from the registry.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to unregister
    async fn unregister_namespace(&self, namespace: &TrackNamespace) -> CoordinatorResult<()>;

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
    /// - `Ok(NamespaceOrigin, Option<quic::Client>)` - Namespace origin and optional client if available
    /// - `Err` - Namespace not found anywhere
    async fn lookup(
        &self,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)>;

    /// Graceful shutdown of the coordinator.
    ///
    /// Called when the relay is shutting down. Implementations should:
    /// - Unregister all local namespaces and tracks
    /// - Cancel refresh tasks
    /// - Close connections to external registries
    async fn shutdown(&self) -> CoordinatorResult<()> {
        Ok(())
    }
}
