// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

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

/// Information about the resolved scope for a connection.
///
/// Returned by [`Coordinator::resolve_scope()`] to tell the relay:
/// - Which scope this connection belongs to (for routing and namespace isolation)
/// - What the connection is allowed to do (for permission enforcement)
///
/// Multiple connection paths can map to the same `scope_id` — for example,
/// a publisher path and a subscriber path that share a scope but have
/// different permissions.
#[derive(Debug, Clone)]
pub struct ScopeInfo {
    /// The resolved scope identity. Used as the key for namespace
    /// registration and lookup in all subsequent coordinator operations.
    ///
    /// Multiple connection paths can map to the same `scope_id`.
    pub scope_id: String,

    /// What this connection is allowed to do within the scope.
    pub permissions: ScopePermissions,
}

/// Permissions granted to a connection within its scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopePermissions {
    /// Can both publish (PUBLISH_NAMESPACE) and subscribe (SUBSCRIBE/FETCH).
    ReadWrite,
    /// Can subscribe/fetch only. Publishing attempts will be rejected
    /// by the relay (the Consumer side of the session will not be created).
    ReadOnly,
}

impl ScopePermissions {
    /// Whether this permission level allows publishing (PUBLISH_NAMESPACE).
    pub fn can_publish(&self) -> bool {
        matches!(self, Self::ReadWrite)
    }

    /// Whether this permission level allows subscribing (SUBSCRIBE/FETCH).
    ///
    /// Always returns `true` — both `ReadWrite` and `ReadOnly` connections
    /// can subscribe. This is intentional: the asymmetry with [`can_publish()`]
    /// reflects that subscribing is the baseline capability, while publishing
    /// requires elevated permissions. If a future permission level needs to
    /// deny subscribing, a new variant should be added.
    ///
    /// [`can_publish()`]: ScopePermissions::can_publish
    pub fn can_subscribe(&self) -> bool {
        true
    }
}

// ============================================================================
// Types for extended Coordinator functionality
// ============================================================================

/// Per-scope configuration retrieved from the coordinator.
///
/// Called after [`Coordinator::resolve_scope()`] to get operational parameters
/// for the scope. This configuration applies to all sessions within the scope.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ScopeConfig {
    /// Origin server to fall back to when namespace not found locally or on
    /// other relays. The relay will attempt to subscribe from this origin
    /// before returning "not found" to the subscriber.
    pub origin_fallback: Option<Url>,

    /// Whether to pre-register subscriber interest for tracks that don't exist
    /// yet. When true, enables "subscriber-first" workflows where subscribers
    /// can wait for publishers that haven't connected yet.
    ///
    /// This corresponds to the "rendezvous" concept in the MoQT specification
    /// (`RENDEZVOUS_TIMEOUT` parameter, see moq-transport PR #1447). The
    /// "lingering subscribe" terminology from moq-transport issue #1402 is
    /// used here for consistency with existing implementations.
    ///
    /// Future: A `rendezvous_timeout` field may be added to control how long
    /// the relay waits for a publisher before giving up.
    pub lingering_subscribe: bool,
}

/// Result of subscribing to a namespace prefix via SUBSCRIBE_NAMESPACE.
///
/// The subscription remains active until this handle is dropped.
/// On drop, cleanup is performed (e.g., unregistering from the coordinator).
pub struct NamespaceSubscription {
    /// Namespaces that currently match the subscribed prefix.
    /// The relay should send PUBLISH_NAMESPACE for each of these.
    pub existing_namespaces: Vec<NamespaceInfo>,

    /// RAII handle — drop triggers unsubscription cleanup.
    _registration: Box<dyn Send + Sync>,
}

impl Default for NamespaceSubscription {
    fn default() -> Self {
        Self {
            existing_namespaces: vec![],
            _registration: Box::new(()),
        }
    }
}

impl NamespaceSubscription {
    /// Create a new subscription with existing namespaces and a cleanup handle.
    pub fn new<T: Send + Sync + 'static>(existing: Vec<NamespaceInfo>, inner: T) -> Self {
        Self {
            existing_namespaces: existing,
            _registration: Box::new(inner),
        }
    }
}

/// Information about a registered namespace.
///
/// Returned in [`NamespaceSubscription`] to describe namespaces matching
/// a SUBSCRIBE_NAMESPACE prefix.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NamespaceInfo {
    /// The namespace identity.
    pub namespace: TrackNamespace,
}

impl NamespaceInfo {
    /// Create a new NamespaceInfo.
    pub fn new(namespace: TrackNamespace) -> Self {
        Self { namespace }
    }
}

/// Information about a relay to forward messages to.
///
/// Returned by [`Coordinator::lookup_namespace_subscribers()`] and
/// [`Coordinator::lookup_track_subscribers()`] to tell the relay where
/// to forward PUBLISH_NAMESPACE or track availability notifications.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RelayInfo {
    /// Relay URL (used for TLS SNI and connection establishment).
    pub url: Url,

    /// Optional direct socket address (bypasses DNS resolution).
    pub addr: Option<SocketAddr>,
}

impl RelayInfo {
    /// Create a new RelayInfo with URL only.
    pub fn new(url: Url) -> Self {
        Self { url, addr: None }
    }

    /// Create a new RelayInfo with URL and direct socket address.
    pub fn with_addr(url: Url, addr: SocketAddr) -> Self {
        Self {
            url,
            addr: Some(addr),
        }
    }
}

/// Handle returned when a track is registered with the coordinator.
///
/// Dropping this handle automatically unregisters the track.
/// This provides RAII-based cleanup for track-level PUBLISH.
pub struct TrackRegistration {
    _registration: Box<dyn Send + Sync>,
}

impl Default for TrackRegistration {
    fn default() -> Self {
        Self {
            _registration: Box::new(()),
        }
    }
}

impl TrackRegistration {
    /// Create a new track registration handle wrapping any Send + Sync type.
    pub fn new<T: Send + Sync + 'static>(inner: T) -> Self {
        Self {
            _registration: Box::new(inner),
        }
    }
}

/// Information about a registered track.
///
/// Returned by [`Coordinator::list_tracks()`] to describe tracks
/// registered under a namespace.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TrackEntry {
    /// The namespace this track belongs to.
    pub namespace: TrackNamespace,

    /// The track name within the namespace.
    pub name: String,
}

impl TrackEntry {
    /// Create a new TrackEntry.
    pub fn new(namespace: TrackNamespace, name: String) -> Self {
        Self { namespace, name }
    }
}

/// Handle returned when subscribing to a track for rendezvous/lingering subscriber support.
///
/// Dropping this handle automatically unregisters the track subscription.
/// This provides RAII-based cleanup for pre-registered subscriber interest
/// (the "rendezvous" concept from MoQT's `RENDEZVOUS_TIMEOUT` parameter).
pub struct TrackSubscription {
    _registration: Box<dyn Send + Sync>,
}

impl Default for TrackSubscription {
    fn default() -> Self {
        Self {
            _registration: Box::new(()),
        }
    }
}

impl TrackSubscription {
    /// Create a new track subscription handle wrapping any Send + Sync type.
    pub fn new<T: Send + Sync + 'static>(inner: T) -> Self {
        Self {
            _registration: Box::new(inner),
        }
    }
}

/// Coordinator handles namespace registration/discovery across relays.
///
/// Implementations are responsible for:
/// - Resolving connection paths to scopes (identity + permissions)
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
///
/// ## Scope Resolution
///
/// When a new session is accepted, the relay calls [`resolve_scope()`] with
/// the raw connection path (from WebTransport URL or CLIENT_SETUP PATH
/// parameter). The coordinator returns a [`ScopeInfo`] containing:
///
/// - **`scope_id`**: The resolved scope identity, used as the key for all
///   subsequent `register_namespace()` and `lookup()` calls. This is
///   intentionally separate from the raw connection path — multiple paths
///   can map to the same scope.
///
/// - **`permissions`**: What the connection is allowed to do. The relay
///   enforces permissions by selectively enabling the publish and/or
///   subscribe sides of the session.
///
/// If `resolve_scope()` returns `None`, the session is unscoped — all
/// subsequent operations use `scope: None` and both publish and subscribe
/// are allowed.
///
/// [`resolve_scope()`]: Coordinator::resolve_scope
#[async_trait]
pub trait Coordinator: Send + Sync {
    /// Resolve a connection path to scope information.
    ///
    /// Called once per accepted session, before any register/lookup calls.
    /// The relay uses the returned [`ScopeInfo`] to:
    /// - Scope all subsequent coordinator operations to `scope_id`
    /// - Enforce permissions (e.g., skip creating the publish side for
    ///   `ReadOnly` connections)
    ///
    /// # Arguments
    ///
    /// * `connection_path` - The raw connection path from the WebTransport
    ///   URL or CLIENT_SETUP PATH parameter. `None` if no path was present.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(ScopeInfo))` - Connection is scoped with the given
    ///   identity and permissions.
    /// - `Ok(None)` - Connection is unscoped. The relay will pass
    ///   `scope: None` to all subsequent coordinator calls and allow
    ///   both publish and subscribe.
    /// - `Err(...)` - Connection should be rejected (e.g., unrecognized
    ///   path, unauthorized).
    ///
    /// # Default Implementation
    ///
    /// Passes through the connection path as the `scope_id` with
    /// `ReadWrite` permissions. Connections without a path are unscoped.
    async fn resolve_scope(
        &self,
        connection_path: Option<&str>,
    ) -> CoordinatorResult<Option<ScopeInfo>> {
        Ok(connection_path.map(|path| ScopeInfo {
            scope_id: path.to_string(),
            permissions: ScopePermissions::ReadWrite,
        }))
    }

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
    /// * `scope` - The resolved scope identity from [`resolve_scope()`],
    ///   or `None` for unscoped sessions. Used to isolate namespace
    ///   registrations — the same namespace in different scopes may
    ///   route independently.
    /// * `namespace` - The namespace being registered
    ///
    /// # Returns
    ///
    /// A `NamespaceRegistration` handle. The namespace remains registered
    /// as long as this handle is held. Dropping it unregisters the namespace.
    ///
    /// [`resolve_scope()`]: Coordinator::resolve_scope
    async fn register_namespace(
        &self,
        scope: Option<&str>,
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
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace to unregister
    async fn unregister_namespace(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<()>;

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
    /// * `scope` - The resolved scope identity, or `None` for unscoped
    ///   sessions. Coordinators use this to scope lookups (e.g., to route
    ///   to the correct origin for a particular application).
    /// * `namespace` - The namespace to look up
    ///
    /// # Returns
    ///
    /// - `Ok(NamespaceOrigin, Option<quic::Client>)` - Namespace origin and optional client if available
    /// - `Err` - Namespace not found anywhere
    async fn lookup(
        &self,
        scope: Option<&str>,
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

    // ========================================================================
    // Scope configuration
    // ========================================================================

    /// Get configuration for a resolved scope.
    ///
    /// Called after [`resolve_scope()`] to retrieve operational parameters
    /// for the scope, such as origin fallback URLs and lingering subscriber
    /// settings.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity from [`resolve_scope()`],
    ///   or `None` for unscoped sessions.
    ///
    /// # Default Implementation
    ///
    /// Returns default configuration (no origin fallback, lingering subscribe
    /// disabled).
    ///
    /// [`resolve_scope()`]: Coordinator::resolve_scope
    async fn get_scope_config(&self, _scope: Option<&str>) -> CoordinatorResult<ScopeConfig> {
        Ok(ScopeConfig::default())
    }

    // ========================================================================
    // SUBSCRIBE_NAMESPACE support
    // ========================================================================

    /// Register interest in a namespace prefix (SUBSCRIBE_NAMESPACE).
    ///
    /// Called when a subscriber sends SUBSCRIBE_NAMESPACE. The coordinator
    /// should:
    /// 1. Record that this relay is interested in the prefix
    /// 2. Return currently-matching namespaces
    /// 3. Return an RAII handle for cleanup on disconnect
    ///
    /// When publishers later register namespaces matching this prefix, the
    /// relay uses [`lookup_namespace_subscribers()`] to find interested relays
    /// and forward PUBLISH_NAMESPACE to them.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `prefix` - The namespace prefix to subscribe to.
    ///
    /// # Default Implementation
    ///
    /// Returns an empty subscription (no existing namespaces, no-op cleanup).
    ///
    /// [`lookup_namespace_subscribers()`]: Coordinator::lookup_namespace_subscribers
    async fn subscribe_namespace(
        &self,
        _scope: Option<&str>,
        _prefix: &TrackNamespace,
    ) -> CoordinatorResult<NamespaceSubscription> {
        Ok(NamespaceSubscription::default())
    }

    /// Unregister interest in a namespace prefix (UNSUBSCRIBE_NAMESPACE).
    ///
    /// Called when a subscriber sends UNSUBSCRIBE_NAMESPACE or disconnects.
    /// This is an explicit unregistration — the subscription handle may still
    /// exist but interest should be removed from the registry.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `prefix` - The namespace prefix to unsubscribe from.
    ///
    /// # Default Implementation
    ///
    /// No-op (returns success).
    async fn unsubscribe_namespace(
        &self,
        _scope: Option<&str>,
        _prefix: &TrackNamespace,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    /// Find relays interested in a namespace (reverse lookup).
    ///
    /// Called when a publisher registers a new namespace. The relay uses this
    /// to find other relays that have active SUBSCRIBE_NAMESPACE subscriptions
    /// matching this namespace, then forwards PUBLISH_NAMESPACE to them.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The newly-registered namespace.
    ///
    /// # Returns
    ///
    /// List of relay endpoints to forward PUBLISH_NAMESPACE to.
    ///
    /// # Default Implementation
    ///
    /// Returns an empty list (no subscribers).
    async fn lookup_namespace_subscribers(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
    ) -> CoordinatorResult<Vec<RelayInfo>> {
        Ok(vec![])
    }

    // ========================================================================
    // Track-level PUBLISH support
    // ========================================================================

    /// Register a track as available on this relay (track-level PUBLISH).
    ///
    /// Called when a publisher sends PUBLISH for a specific track. This
    /// provides finer-grained routing than namespace-level registration.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace the track belongs to.
    /// * `track` - The track name within the namespace.
    ///
    /// # Returns
    ///
    /// A `TrackRegistration` handle. The track remains registered as long as
    /// this handle is held. Dropping it unregisters the track.
    ///
    /// # Default Implementation
    ///
    /// Returns a no-op registration handle.
    async fn register_track(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
        _track: &str,
    ) -> CoordinatorResult<TrackRegistration> {
        Ok(TrackRegistration::default())
    }

    /// Unregister a track.
    ///
    /// Called when a publisher sends PUBLISH_DONE or disconnects.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace the track belongs to.
    /// * `track` - The track name to unregister.
    ///
    /// # Default Implementation
    ///
    /// No-op (returns success).
    async fn unregister_track(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
        _track: &str,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    /// List tracks registered under a namespace.
    ///
    /// Used for track discovery within a namespace, supporting SUBSCRIBE_NAMESPACE
    /// workflows where subscribers need to know what tracks are available.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace to list tracks from.
    ///
    /// # Default Implementation
    ///
    /// Returns an empty list.
    async fn list_tracks(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
    ) -> CoordinatorResult<Vec<TrackEntry>> {
        Ok(vec![])
    }

    // ========================================================================
    // Lingering subscriber / rendezvous support
    // ========================================================================
    //
    // These methods implement the "rendezvous" concept from the MoQT
    // specification (RENDEZVOUS_TIMEOUT parameter, moq-transport PR #1447),
    // also known as "lingering subscribe" (moq-transport issue #1402) or
    // "early media" (Cisco's original framing at IETF 122).
    //
    // The relay uses these to pre-register subscriber interest before a
    // publisher exists, enabling subscriber-first workflows.
    //
    // Future: timeout handling for rendezvous — how long to wait before
    // giving up on a publisher.

    /// Pre-register interest in a track that may not exist yet (rendezvous).
    ///
    /// Enables "subscriber-first" workflows where a subscriber can wait for a
    /// publisher that hasn't connected yet. Called when
    /// [`ScopeConfig::lingering_subscribe`] is true and a subscriber requests
    /// a track that doesn't exist.
    ///
    /// Also known as "lingering subscribe" (moq-transport issue #1402) or
    /// "rendezvous" (MoQT spec's `RENDEZVOUS_TIMEOUT` parameter).
    ///
    /// When a publisher later registers the track, the relay uses
    /// [`lookup_track_subscribers()`] to find waiting subscribers.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace the track would belong to.
    /// * `track` - The track name to pre-register interest in.
    ///
    /// # Returns
    ///
    /// A `TrackSubscription` handle. Interest remains registered as long as
    /// this handle is held. Dropping it removes the interest.
    ///
    /// # Default Implementation
    ///
    /// Returns a no-op subscription handle.
    ///
    /// [`lookup_track_subscribers()`]: Coordinator::lookup_track_subscribers
    async fn subscribe_track(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
        _track: &str,
    ) -> CoordinatorResult<TrackSubscription> {
        Ok(TrackSubscription::default())
    }

    /// Unregister track subscription interest.
    ///
    /// Called when a subscriber disconnects or no longer needs the track.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace the track belongs to.
    /// * `track` - The track name to unsubscribe from.
    ///
    /// # Default Implementation
    ///
    /// No-op (returns success).
    async fn unsubscribe_track(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
        _track: &str,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    /// Find relays with subscribers waiting for a track (reverse lookup).
    ///
    /// Called when a publisher registers a track, to notify lingering
    /// subscribers that the track is now available.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace the track belongs to.
    /// * `track` - The track name.
    ///
    /// # Returns
    ///
    /// List of relay endpoints with waiting subscribers.
    ///
    /// # Default Implementation
    ///
    /// Returns an empty list.
    async fn lookup_track_subscribers(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
        _track: &str,
    ) -> CoordinatorResult<Vec<RelayInfo>> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    // ========================================================================
    // Test helpers and fixtures
    // ========================================================================

    /// Helper to build a TrackNamespace from slash-separated path segments.
    fn ns(path: &str) -> TrackNamespace {
        TrackNamespace::from_utf8_path(path)
    }

    /// Returns true if `namespace` starts with all the fields in `prefix`.
    fn ns_has_prefix(namespace: &TrackNamespace, prefix: &TrackNamespace) -> bool {
        namespace.fields.len() >= prefix.fields.len()
            && prefix
                .fields
                .iter()
                .zip(namespace.fields.iter())
                .all(|(p, n)| p == n)
    }

    // --------------------------------------------------------------------
    // MockCoordinator — a fully in-memory Coordinator for testing
    //
    // The in-tree FileCoordinator and ApiCoordinator only implement the
    // three required methods (register_namespace, unregister_namespace,
    // lookup) and rely on defaults for all new stub methods. This mock
    // provides a complete reference implementation of the full trait,
    // including SUBSCRIBE_NAMESPACE, track-level PUBLISH, and lingering
    // subscriber support.
    //
    // It serves two purposes:
    //   1. Executable documentation of intended method semantics for
    //      external implementors who can't see the bin-only coordinators
    //   2. A test fixture that exercises non-trivial behavior for the new
    //      methods (which no existing coordinator implements yet)
    //
    // Test data models a broadcast/live-streaming scenario:
    //   - Scopes represent content providers or tenants
    //     (e.g., "content-provider-123")
    //   - Namespaces represent broadcast events or channels
    //     (e.g., "sports/football/match-42", "sports/football/match-42/camera-1")
    //   - Tracks represent individual media renditions
    //     (e.g., "video-1080p", "video-480p", "audio-en", "audio-es")
    //   - Multiple relays in a CDN cluster subscribe to namespace prefixes
    //     to discover new broadcasts and forward them to edge viewers
    // --------------------------------------------------------------------

    /// In-memory state for the mock coordinator.
    struct MockState {
        /// Maps scope → registered namespaces (keyed by TrackNamespace)
        /// Value is the relay URL that registered it.
        namespaces: HashMap<String, HashMap<TrackNamespace, String>>,

        /// Maps scope → registered tracks → relay URL
        /// Key: (namespace, track_name)
        tracks: HashMap<String, HashMap<(TrackNamespace, String), String>>,

        /// Maps scope → SUBSCRIBE_NAMESPACE prefixes → list of relay URLs
        namespace_subscribers: HashMap<String, HashMap<TrackNamespace, Vec<String>>>,

        /// Maps scope → subscribed tracks → list of relay URLs
        /// Key: (namespace, track_name)
        track_subscribers: HashMap<String, HashMap<(TrackNamespace, String), Vec<String>>>,

        /// Maps scope → ScopeConfig
        scope_configs: HashMap<String, ScopeConfig>,

        /// Maps raw connection path → ScopeInfo (for resolve_scope)
        path_to_scope: HashMap<String, ScopeInfo>,
    }

    impl MockState {
        fn scope_key(scope: Option<&str>) -> String {
            scope.unwrap_or("").to_string()
        }

        fn track_key(namespace: &TrackNamespace, track: &str) -> (TrackNamespace, String) {
            (namespace.clone(), track.to_string())
        }
    }

    /// Drop-based handle for namespace unregistration.
    struct MockNamespaceHandle {
        state: std::sync::Arc<Mutex<MockState>>,
        scope_key: String,
        namespace: TrackNamespace,
    }

    impl Drop for MockNamespaceHandle {
        fn drop(&mut self) {
            let mut state = self.state.lock().unwrap();
            if let Some(bucket) = state.namespaces.get_mut(&self.scope_key) {
                bucket.remove(&self.namespace);
            }
        }
    }

    /// Drop-based handle for track unregistration.
    struct MockTrackHandle {
        state: std::sync::Arc<Mutex<MockState>>,
        scope_key: String,
        track_key: (TrackNamespace, String),
    }

    impl Drop for MockTrackHandle {
        fn drop(&mut self) {
            let mut state = self.state.lock().unwrap();
            if let Some(bucket) = state.tracks.get_mut(&self.scope_key) {
                bucket.remove(&self.track_key);
            }
        }
    }

    /// Drop-based handle for namespace subscription cleanup.
    struct MockNamespaceSubHandle {
        state: std::sync::Arc<Mutex<MockState>>,
        scope_key: String,
        prefix: TrackNamespace,
        relay_url: String,
    }

    impl Drop for MockNamespaceSubHandle {
        fn drop(&mut self) {
            let mut state = self.state.lock().unwrap();
            if let Some(bucket) = state.namespace_subscribers.get_mut(&self.scope_key) {
                if let Some(relays) = bucket.get_mut(&self.prefix) {
                    relays.retain(|r| r != &self.relay_url);
                }
            }
        }
    }

    /// Drop-based handle for track subscription cleanup.
    struct MockTrackSubHandle {
        state: std::sync::Arc<Mutex<MockState>>,
        scope_key: String,
        track_key: (TrackNamespace, String),
        relay_url: String,
    }

    impl Drop for MockTrackSubHandle {
        fn drop(&mut self) {
            let mut state = self.state.lock().unwrap();
            if let Some(bucket) = state.track_subscribers.get_mut(&self.scope_key) {
                if let Some(relays) = bucket.get_mut(&self.track_key) {
                    relays.retain(|r| r != &self.relay_url);
                }
            }
        }
    }

    /// A mock coordinator that stores all state in memory.
    ///
    /// Provides a complete reference implementation of the Coordinator trait
    /// including all new stub methods. Useful for testing relay integration
    /// and as executable documentation of the intended method semantics.
    struct MockCoordinator {
        state: std::sync::Arc<Mutex<MockState>>,
        /// URL of "this" relay (used when registering namespaces/tracks)
        relay_url: Url,
    }

    impl MockCoordinator {
        fn new(relay_url: &str) -> Self {
            Self {
                state: std::sync::Arc::new(Mutex::new(MockState {
                    namespaces: HashMap::new(),
                    tracks: HashMap::new(),
                    namespace_subscribers: HashMap::new(),
                    track_subscribers: HashMap::new(),
                    scope_configs: HashMap::new(),
                    path_to_scope: HashMap::new(),
                })),
                relay_url: Url::parse(relay_url).unwrap(),
            }
        }

        /// Configure scope resolution: connection path → ScopeInfo.
        fn add_path_mapping(&self, path: &str, scope_id: &str, permissions: ScopePermissions) {
            let mut state = self.state.lock().unwrap();
            state.path_to_scope.insert(
                path.to_string(),
                ScopeInfo {
                    scope_id: scope_id.to_string(),
                    permissions,
                },
            );
        }

        /// Configure per-scope settings.
        fn set_scope_config(&self, scope: &str, config: ScopeConfig) {
            let mut state = self.state.lock().unwrap();
            state.scope_configs.insert(scope.to_string(), config);
        }
    }

    #[async_trait]
    impl Coordinator for MockCoordinator {
        async fn resolve_scope(
            &self,
            connection_path: Option<&str>,
        ) -> CoordinatorResult<Option<ScopeInfo>> {
            let state = self.state.lock().unwrap();
            match connection_path {
                Some(path) => {
                    state
                        .path_to_scope
                        .get(path)
                        .cloned()
                        .map(Some)
                        .ok_or(CoordinatorError::Other(anyhow::anyhow!(
                            "unknown path: {}",
                            path
                        )))
                }
                None => Ok(None),
            }
        }

        async fn register_namespace(
            &self,
            scope: Option<&str>,
            namespace: &TrackNamespace,
        ) -> CoordinatorResult<NamespaceRegistration> {
            let scope_key = MockState::scope_key(scope);
            let relay_url = self.relay_url.to_string();

            {
                let mut state = self.state.lock().unwrap();
                let bucket = state.namespaces.entry(scope_key.clone()).or_default();
                if bucket.contains_key(namespace) {
                    return Err(CoordinatorError::NamespaceAlreadyRegistered);
                }
                bucket.insert(namespace.clone(), relay_url);
            }

            let handle = MockNamespaceHandle {
                state: self.state.clone(),
                scope_key,
                namespace: namespace.clone(),
            };
            Ok(NamespaceRegistration::new(handle))
        }

        async fn unregister_namespace(
            &self,
            scope: Option<&str>,
            namespace: &TrackNamespace,
        ) -> CoordinatorResult<()> {
            let scope_key = MockState::scope_key(scope);
            let mut state = self.state.lock().unwrap();
            if let Some(bucket) = state.namespaces.get_mut(&scope_key) {
                bucket.remove(namespace);
            }
            Ok(())
        }

        async fn lookup(
            &self,
            scope: Option<&str>,
            namespace: &TrackNamespace,
        ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)> {
            let scope_key = MockState::scope_key(scope);
            let state = self.state.lock().unwrap();

            let bucket = state
                .namespaces
                .get(&scope_key)
                .ok_or(CoordinatorError::NamespaceNotFound)?;

            // Exact match first
            if let Some(relay_url) = bucket.get(namespace) {
                let url = Url::parse(relay_url).unwrap();
                return Ok((NamespaceOrigin::new(namespace.clone(), url, None), None));
            }

            // Prefix match (longest wins)
            let mut best: Option<(&TrackNamespace, &String)> = None;
            for (registered, url) in bucket {
                if ns_has_prefix(namespace, registered) {
                    match &best {
                        Some((prev, _)) if registered.fields.len() > prev.fields.len() => {
                            best = Some((registered, url));
                        }
                        None => {
                            best = Some((registered, url));
                        }
                        _ => {}
                    }
                }
            }

            match best {
                Some((matched_ns, relay_url)) => {
                    let url = Url::parse(relay_url).unwrap();
                    Ok((NamespaceOrigin::new(matched_ns.clone(), url, None), None))
                }
                None => Err(CoordinatorError::NamespaceNotFound),
            }
        }

        async fn get_scope_config(&self, scope: Option<&str>) -> CoordinatorResult<ScopeConfig> {
            let state = self.state.lock().unwrap();
            let scope_key = MockState::scope_key(scope);
            Ok(state
                .scope_configs
                .get(&scope_key)
                .cloned()
                .unwrap_or_default())
        }

        async fn subscribe_namespace(
            &self,
            scope: Option<&str>,
            prefix: &TrackNamespace,
        ) -> CoordinatorResult<NamespaceSubscription> {
            let scope_key = MockState::scope_key(scope);
            let relay_url = self.relay_url.to_string();

            let mut state = self.state.lock().unwrap();

            // Find currently-registered namespaces that match the prefix
            let existing: Vec<NamespaceInfo> = state
                .namespaces
                .get(&scope_key)
                .map(|bucket| {
                    bucket
                        .keys()
                        .filter(|ns| ns_has_prefix(ns, prefix))
                        .map(|ns| NamespaceInfo::new(ns.clone()))
                        .collect()
                })
                .unwrap_or_default();

            // Register this relay as interested in the prefix
            state
                .namespace_subscribers
                .entry(scope_key.clone())
                .or_default()
                .entry(prefix.clone())
                .or_default()
                .push(relay_url.clone());

            let handle = MockNamespaceSubHandle {
                state: self.state.clone(),
                scope_key,
                prefix: prefix.clone(),
                relay_url,
            };

            Ok(NamespaceSubscription::new(existing, handle))
        }

        async fn unsubscribe_namespace(
            &self,
            scope: Option<&str>,
            prefix: &TrackNamespace,
        ) -> CoordinatorResult<()> {
            let scope_key = MockState::scope_key(scope);
            let relay_url = self.relay_url.to_string();
            let mut state = self.state.lock().unwrap();
            if let Some(bucket) = state.namespace_subscribers.get_mut(&scope_key) {
                if let Some(relays) = bucket.get_mut(prefix) {
                    relays.retain(|r| r != &relay_url);
                }
            }
            Ok(())
        }

        async fn lookup_namespace_subscribers(
            &self,
            scope: Option<&str>,
            namespace: &TrackNamespace,
        ) -> CoordinatorResult<Vec<RelayInfo>> {
            let scope_key = MockState::scope_key(scope);
            let state = self.state.lock().unwrap();

            let mut relays = Vec::new();
            if let Some(subs) = state.namespace_subscribers.get(&scope_key) {
                for (prefix, relay_urls) in subs {
                    if ns_has_prefix(namespace, prefix) {
                        for url_str in relay_urls {
                            relays.push(RelayInfo::new(Url::parse(url_str).unwrap()));
                        }
                    }
                }
            }
            Ok(relays)
        }

        async fn register_track(
            &self,
            scope: Option<&str>,
            namespace: &TrackNamespace,
            track: &str,
        ) -> CoordinatorResult<TrackRegistration> {
            let scope_key = MockState::scope_key(scope);
            let track_key = MockState::track_key(namespace, track);

            {
                let mut state = self.state.lock().unwrap();
                state
                    .tracks
                    .entry(scope_key.clone())
                    .or_default()
                    .insert(track_key.clone(), self.relay_url.to_string());
            }

            let handle = MockTrackHandle {
                state: self.state.clone(),
                scope_key,
                track_key,
            };
            Ok(TrackRegistration::new(handle))
        }

        async fn unregister_track(
            &self,
            scope: Option<&str>,
            namespace: &TrackNamespace,
            track: &str,
        ) -> CoordinatorResult<()> {
            let scope_key = MockState::scope_key(scope);
            let track_key = MockState::track_key(namespace, track);
            let mut state = self.state.lock().unwrap();
            if let Some(bucket) = state.tracks.get_mut(&scope_key) {
                bucket.remove(&track_key);
            }
            Ok(())
        }

        async fn list_tracks(
            &self,
            scope: Option<&str>,
            namespace: &TrackNamespace,
        ) -> CoordinatorResult<Vec<TrackEntry>> {
            let scope_key = MockState::scope_key(scope);
            let state = self.state.lock().unwrap();

            let entries = state
                .tracks
                .get(&scope_key)
                .map(|bucket| {
                    bucket
                        .keys()
                        .filter_map(|(ns, track_name)| {
                            if ns == namespace {
                                Some(TrackEntry::new(ns.clone(), track_name.clone()))
                            } else {
                                None
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            Ok(entries)
        }

        async fn subscribe_track(
            &self,
            scope: Option<&str>,
            namespace: &TrackNamespace,
            track: &str,
        ) -> CoordinatorResult<TrackSubscription> {
            let scope_key = MockState::scope_key(scope);
            let track_key = MockState::track_key(namespace, track);
            let relay_url = self.relay_url.to_string();

            {
                let mut state = self.state.lock().unwrap();
                state
                    .track_subscribers
                    .entry(scope_key.clone())
                    .or_default()
                    .entry(track_key.clone())
                    .or_default()
                    .push(relay_url.clone());
            }

            let handle = MockTrackSubHandle {
                state: self.state.clone(),
                scope_key,
                track_key,
                relay_url,
            };
            Ok(TrackSubscription::new(handle))
        }

        async fn unsubscribe_track(
            &self,
            scope: Option<&str>,
            namespace: &TrackNamespace,
            track: &str,
        ) -> CoordinatorResult<()> {
            let scope_key = MockState::scope_key(scope);
            let track_key = MockState::track_key(namespace, track);
            let relay_url = self.relay_url.to_string();
            let mut state = self.state.lock().unwrap();
            if let Some(bucket) = state.track_subscribers.get_mut(&scope_key) {
                if let Some(relays) = bucket.get_mut(&track_key) {
                    relays.retain(|r| r != &relay_url);
                }
            }
            Ok(())
        }

        async fn lookup_track_subscribers(
            &self,
            scope: Option<&str>,
            namespace: &TrackNamespace,
            track: &str,
        ) -> CoordinatorResult<Vec<RelayInfo>> {
            let scope_key = MockState::scope_key(scope);
            let track_key = MockState::track_key(namespace, track);
            let state = self.state.lock().unwrap();

            let relays = state
                .track_subscribers
                .get(&scope_key)
                .and_then(|bucket| bucket.get(&track_key))
                .map(|urls| {
                    urls.iter()
                        .map(|u| RelayInfo::new(Url::parse(u).unwrap()))
                        .collect()
                })
                .unwrap_or_default();

            Ok(relays)
        }
    }

    // ========================================================================
    // Type construction and defaults
    // ========================================================================

    #[test]
    fn scope_config_defaults() {
        let config = ScopeConfig::default();
        assert!(config.origin_fallback.is_none());
        assert!(!config.lingering_subscribe);
    }

    #[test]
    fn scope_config_with_origin_fallback() {
        let config = ScopeConfig {
            origin_fallback: Some(Url::parse("https://origin.example.com").unwrap()),
            lingering_subscribe: true,
        };
        assert_eq!(
            config.origin_fallback.unwrap().as_str(),
            "https://origin.example.com/"
        );
        assert!(config.lingering_subscribe);
    }

    #[test]
    fn namespace_info_construction() {
        let info = NamespaceInfo::new(ns("sports/football/match-42"));
        assert_eq!(info.namespace.to_utf8_path(), "/sports/football/match-42");
    }

    #[test]
    fn relay_info_without_addr() {
        let info = RelayInfo::new(Url::parse("https://relay-us-east.example.com").unwrap());
        assert_eq!(info.url.as_str(), "https://relay-us-east.example.com/");
        assert!(info.addr.is_none());
    }

    #[test]
    fn relay_info_with_direct_addr() {
        let addr: SocketAddr = "10.0.1.5:4443".parse().unwrap();
        let info = RelayInfo::with_addr(
            Url::parse("https://relay-us-east.example.com").unwrap(),
            addr,
        );
        assert_eq!(info.url.as_str(), "https://relay-us-east.example.com/");
        assert_eq!(info.addr.unwrap(), addr);
    }

    #[test]
    fn track_entry_construction() {
        let entry = TrackEntry::new(ns("sports/football/match-42"), "video-1080p".to_string());
        assert_eq!(entry.namespace.to_utf8_path(), "/sports/football/match-42");
        assert_eq!(entry.name, "video-1080p");
    }

    #[test]
    fn namespace_subscription_default_is_empty() {
        let sub = NamespaceSubscription::default();
        assert!(sub.existing_namespaces.is_empty());
    }

    #[test]
    fn track_registration_default_is_no_op() {
        // Default handle should not panic on drop
        let _reg = TrackRegistration::default();
    }

    #[test]
    fn track_subscription_default_is_no_op() {
        // Default handle should not panic on drop
        let _sub = TrackSubscription::default();
    }

    #[test]
    fn scope_permissions_publish_and_subscribe() {
        assert!(ScopePermissions::ReadWrite.can_publish());
        assert!(ScopePermissions::ReadWrite.can_subscribe());
        assert!(!ScopePermissions::ReadOnly.can_publish());
        assert!(ScopePermissions::ReadOnly.can_subscribe());
    }

    // ========================================================================
    // Scope resolution
    // ========================================================================

    #[tokio::test]
    async fn resolve_scope_maps_path_to_scope_identity() {
        // A broadcast platform might use connection paths that encode
        // a content provider identity and role. Multiple paths can map
        // to the same scope with different permissions.
        let coord = MockCoordinator::new("https://relay-1.example.com");
        coord.add_path_mapping(
            "/provider/acme-sports/ingest",
            "content-provider-123",
            ScopePermissions::ReadWrite,
        );
        coord.add_path_mapping(
            "/provider/acme-sports/watch",
            "content-provider-123",
            ScopePermissions::ReadOnly,
        );

        let ingest_scope = coord
            .resolve_scope(Some("/provider/acme-sports/ingest"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ingest_scope.scope_id, "content-provider-123");
        assert!(ingest_scope.permissions.can_publish());

        let watch_scope = coord
            .resolve_scope(Some("/provider/acme-sports/watch"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(watch_scope.scope_id, "content-provider-123");
        assert!(!watch_scope.permissions.can_publish());
        assert!(watch_scope.permissions.can_subscribe());
    }

    #[tokio::test]
    async fn resolve_scope_none_path_returns_unscoped() {
        let coord = MockCoordinator::new("https://relay-1.example.com");
        let result = coord.resolve_scope(None).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn resolve_scope_unknown_path_returns_error() {
        let coord = MockCoordinator::new("https://relay-1.example.com");
        let result = coord.resolve_scope(Some("/unknown/path")).await;
        assert!(result.is_err());
    }

    // ========================================================================
    // Scope configuration
    // ========================================================================

    #[tokio::test]
    async fn get_scope_config_returns_configured_settings() {
        // A content provider with an origin ingest server and lingering
        // subscriber support (viewers can tune in before the broadcast starts)
        let coord = MockCoordinator::new("https://relay-1.example.com");
        coord.set_scope_config(
            "content-provider-123",
            ScopeConfig {
                origin_fallback: Some(Url::parse("https://ingest.example.com/origin").unwrap()),
                lingering_subscribe: true,
            },
        );

        let config = coord
            .get_scope_config(Some("content-provider-123"))
            .await
            .unwrap();
        assert!(config.lingering_subscribe);
        assert!(config.origin_fallback.is_some());
    }

    #[tokio::test]
    async fn get_scope_config_unconfigured_returns_defaults() {
        let coord = MockCoordinator::new("https://relay-1.example.com");
        let config = coord.get_scope_config(Some("unknown-scope")).await.unwrap();
        assert!(!config.lingering_subscribe);
        assert!(config.origin_fallback.is_none());
    }

    // ========================================================================
    // Namespace registration and lookup
    // ========================================================================

    #[tokio::test]
    async fn register_and_lookup_namespace() {
        // Ingest server registers a broadcast namespace; edge relay looks it up
        let coord = MockCoordinator::new("https://relay-1.example.com");
        let scope = Some("content-provider-123");

        let _reg = coord
            .register_namespace(scope, &ns("sports/football/match-42"))
            .await
            .unwrap();

        let (origin, _client) = coord
            .lookup(scope, &ns("sports/football/match-42"))
            .await
            .unwrap();

        assert_eq!(origin.url().as_str(), "https://relay-1.example.com/");
    }

    #[tokio::test]
    async fn lookup_prefix_matching() {
        // A broadcaster registers a top-level event namespace; subscribers
        // looking up specific camera angles under it should still resolve.
        let coord = MockCoordinator::new("https://relay-1.example.com");
        let scope = Some("content-provider-123");

        let _reg = coord
            .register_namespace(scope, &ns("sports/football/match-42"))
            .await
            .unwrap();

        // Lookup a more specific namespace (camera angle) under the event
        let (origin, _) = coord
            .lookup(scope, &ns("sports/football/match-42/camera-1"))
            .await
            .unwrap();
        assert_eq!(origin.url().as_str(), "https://relay-1.example.com/");
    }

    #[tokio::test]
    async fn lookup_not_found() {
        let coord = MockCoordinator::new("https://relay-1.example.com");
        let result = coord
            .lookup(Some("content-provider-123"), &ns("nonexistent"))
            .await;
        assert!(matches!(result, Err(CoordinatorError::NamespaceNotFound)));
    }

    #[tokio::test]
    async fn scopes_are_isolated() {
        // Two content providers using the same namespace structure should
        // not see each other's registrations.
        let coord = MockCoordinator::new("https://relay-1.example.com");

        let _reg = coord
            .register_namespace(Some("provider-abc"), &ns("live/main"))
            .await
            .unwrap();

        // Different provider can't see it
        let result = coord.lookup(Some("provider-xyz"), &ns("live/main")).await;
        assert!(matches!(result, Err(CoordinatorError::NamespaceNotFound)));

        // Same provider can
        let (origin, _) = coord
            .lookup(Some("provider-abc"), &ns("live/main"))
            .await
            .unwrap();
        assert_eq!(origin.url().as_str(), "https://relay-1.example.com/");
    }

    #[tokio::test]
    async fn duplicate_registration_rejected() {
        let coord = MockCoordinator::new("https://relay-1.example.com");
        let scope = Some("content-provider-123");

        let _reg = coord
            .register_namespace(scope, &ns("sports/football/match-42"))
            .await
            .unwrap();

        let result = coord
            .register_namespace(scope, &ns("sports/football/match-42"))
            .await;
        assert!(matches!(
            result,
            Err(CoordinatorError::NamespaceAlreadyRegistered)
        ));
    }

    #[tokio::test]
    async fn namespace_unregistered_on_handle_drop() {
        let coord = MockCoordinator::new("https://relay-1.example.com");
        let scope = Some("content-provider-123");

        {
            let _reg = coord
                .register_namespace(scope, &ns("sports/football/match-42"))
                .await
                .unwrap();

            // Should be findable while registration is held
            assert!(coord
                .lookup(scope, &ns("sports/football/match-42"))
                .await
                .is_ok());
        }
        // _reg dropped — broadcast ended, ingest disconnected

        let result = coord.lookup(scope, &ns("sports/football/match-42")).await;
        assert!(matches!(result, Err(CoordinatorError::NamespaceNotFound)));
    }

    // ========================================================================
    // SUBSCRIBE_NAMESPACE — namespace prefix subscriptions
    // ========================================================================

    #[tokio::test]
    async fn subscribe_namespace_returns_existing_matches() {
        // An edge relay subscribes to all broadcasts from a content provider's
        // "sports/football" prefix. Two matches are already live; a third
        // match under "sports/tennis" should NOT match.
        let coord = MockCoordinator::new("https://relay-1.example.com");
        let scope = Some("content-provider-123");

        // Two football matches already live
        let _reg_match42 = coord
            .register_namespace(scope, &ns("sports/football/match-42"))
            .await
            .unwrap();
        let _reg_match43 = coord
            .register_namespace(scope, &ns("sports/football/match-43"))
            .await
            .unwrap();

        // A tennis match (should NOT match the football prefix)
        let _reg_tennis = coord
            .register_namespace(scope, &ns("sports/tennis/open-7"))
            .await
            .unwrap();

        // Subscribe to the football prefix
        let sub = coord
            .subscribe_namespace(scope, &ns("sports/football"))
            .await
            .unwrap();

        assert_eq!(sub.existing_namespaces.len(), 2);
        let paths: Vec<String> = sub
            .existing_namespaces
            .iter()
            .map(|n| n.namespace.to_utf8_path())
            .collect();
        assert!(paths.contains(&"/sports/football/match-42".to_string()));
        assert!(paths.contains(&"/sports/football/match-43".to_string()));
    }

    #[tokio::test]
    async fn lookup_namespace_subscribers_finds_interested_relays() {
        // An edge relay has subscribers interested in football broadcasts.
        // When a new match starts (namespace registered), the origin relay
        // calls lookup_namespace_subscribers to discover interested edge
        // relays and forward PUBLISH_NAMESPACE to them.
        let coord = MockCoordinator::new("https://edge-us-west.example.com");
        let scope = Some("content-provider-123");

        // Edge relay subscribes to the football prefix
        let _sub = coord
            .subscribe_namespace(scope, &ns("sports/football"))
            .await
            .unwrap();

        // New match starts — who needs to know?
        let interested = coord
            .lookup_namespace_subscribers(scope, &ns("sports/football/match-44"))
            .await
            .unwrap();

        assert_eq!(interested.len(), 1);
        assert_eq!(
            interested[0].url.as_str(),
            "https://edge-us-west.example.com/"
        );
    }

    #[tokio::test]
    async fn namespace_subscription_cleaned_up_on_drop() {
        let coord = MockCoordinator::new("https://edge-us-west.example.com");
        let scope = Some("content-provider-123");

        {
            let _sub = coord
                .subscribe_namespace(scope, &ns("sports/football"))
                .await
                .unwrap();

            let interested = coord
                .lookup_namespace_subscribers(scope, &ns("sports/football/match-44"))
                .await
                .unwrap();
            assert_eq!(interested.len(), 1);
        }
        // _sub dropped — edge relay disconnected

        let interested = coord
            .lookup_namespace_subscribers(scope, &ns("sports/football/match-44"))
            .await
            .unwrap();
        assert!(interested.is_empty());
    }

    // ========================================================================
    // Track-level PUBLISH registration
    // ========================================================================

    #[tokio::test]
    async fn register_and_list_tracks() {
        // A broadcaster publishes multiple renditions (video qualities,
        // audio languages) as individual tracks under a match namespace.
        let coord = MockCoordinator::new("https://relay-1.example.com");
        let scope = Some("content-provider-123");
        let match_ns = ns("sports/football/match-42");

        let _reg_1080 = coord
            .register_track(scope, &match_ns, "video-1080p")
            .await
            .unwrap();
        let _reg_480 = coord
            .register_track(scope, &match_ns, "video-480p")
            .await
            .unwrap();
        let _reg_audio = coord
            .register_track(scope, &match_ns, "audio-en")
            .await
            .unwrap();

        let tracks = coord.list_tracks(scope, &match_ns).await.unwrap();
        assert_eq!(tracks.len(), 3);

        let names: Vec<&str> = tracks.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"video-1080p"));
        assert!(names.contains(&"video-480p"));
        assert!(names.contains(&"audio-en"));
    }

    #[tokio::test]
    async fn track_unregistered_on_handle_drop() {
        let coord = MockCoordinator::new("https://relay-1.example.com");
        let scope = Some("content-provider-123");
        let match_ns = ns("sports/football/match-42");

        {
            let _reg = coord
                .register_track(scope, &match_ns, "video-1080p")
                .await
                .unwrap();

            let tracks = coord.list_tracks(scope, &match_ns).await.unwrap();
            assert_eq!(tracks.len(), 1);
        }
        // _reg dropped — broadcaster stopped the 1080p rendition

        let tracks = coord.list_tracks(scope, &match_ns).await.unwrap();
        assert!(tracks.is_empty());
    }

    // ========================================================================
    // Lingering subscriber / rendezvous
    // ========================================================================

    #[tokio::test]
    async fn subscribe_track_before_publisher_exists() {
        // A viewer tunes into a pre-game show before the main broadcast has
        // started. The edge relay pre-registers interest in the track so
        // that when the broadcaster begins, it can be notified immediately.
        let coord = MockCoordinator::new("https://edge-eu-west.example.com");
        let scope = Some("content-provider-123");
        let match_ns = ns("sports/football/match-42");

        // Viewer's edge relay pre-registers interest (lingering/rendezvous)
        let _sub = coord
            .subscribe_track(scope, &match_ns, "video-1080p")
            .await
            .unwrap();

        // Broadcaster starts — origin relay checks who's waiting
        let waiting = coord
            .lookup_track_subscribers(scope, &match_ns, "video-1080p")
            .await
            .unwrap();
        assert_eq!(waiting.len(), 1);
        assert_eq!(waiting[0].url.as_str(), "https://edge-eu-west.example.com/");

        // No one is waiting for the Spanish audio (not pre-subscribed)
        let waiting_es = coord
            .lookup_track_subscribers(scope, &match_ns, "audio-es")
            .await
            .unwrap();
        assert!(waiting_es.is_empty());
    }

    #[tokio::test]
    async fn track_subscription_cleaned_up_on_drop() {
        let coord = MockCoordinator::new("https://edge-eu-west.example.com");
        let scope = Some("content-provider-123");
        let match_ns = ns("sports/football/match-42");

        {
            let _sub = coord
                .subscribe_track(scope, &match_ns, "video-1080p")
                .await
                .unwrap();

            let waiting = coord
                .lookup_track_subscribers(scope, &match_ns, "video-1080p")
                .await
                .unwrap();
            assert_eq!(waiting.len(), 1);
        }
        // _sub dropped — viewer left

        let waiting = coord
            .lookup_track_subscribers(scope, &match_ns, "video-1080p")
            .await
            .unwrap();
        assert!(waiting.is_empty());
    }

    // ========================================================================
    // End-to-end scenario: live broadcast across a relay cluster
    // ========================================================================

    #[tokio::test]
    async fn broadcast_multi_relay_scenario() {
        // A content provider ("content-provider-123") broadcasts a football
        // match through a relay cluster:
        //
        //   origin relay (us-east): broadcaster ingests video + audio
        //   edge relay (eu-west): viewers in Europe subscribe, including
        //     one who tunes in before halftime coverage starts
        //
        // This exercises namespace registration, SUBSCRIBE_NAMESPACE for
        // event discovery, track-level PUBLISH, and lingering subscriber
        // for pre-broadcast rendezvous.

        let origin = MockCoordinator::new("https://relay-us-east.example.com");
        let edge = MockCoordinator::new("https://edge-eu-west.example.com");

        let scope = Some("content-provider-123");

        // --- Origin relay: broadcaster starts the match ---

        // Register the match namespace
        let _match_reg = origin
            .register_namespace(scope, &ns("sports/football/match-42"))
            .await
            .unwrap();

        // Register individual track renditions
        let _video_1080 = origin
            .register_track(scope, &ns("sports/football/match-42"), "video-1080p")
            .await
            .unwrap();
        let _video_480 = origin
            .register_track(scope, &ns("sports/football/match-42"), "video-480p")
            .await
            .unwrap();
        let _audio_en = origin
            .register_track(scope, &ns("sports/football/match-42"), "audio-en")
            .await
            .unwrap();

        // --- Edge relay: viewers subscribe ---

        // Edge subscribes to all football events (SUBSCRIBE_NAMESPACE)
        let _football_sub = edge
            .subscribe_namespace(scope, &ns("sports/football"))
            .await
            .unwrap();

        // A viewer pre-subscribes to halftime analysis (not started yet)
        let _halftime_sub = edge
            .subscribe_track(
                scope,
                &ns("sports/football/match-42/halftime"),
                "video-720p",
            )
            .await
            .unwrap();

        // When halftime coverage starts, the origin can find waiting viewers
        let waiting = edge
            .lookup_track_subscribers(
                scope,
                &ns("sports/football/match-42/halftime"),
                "video-720p",
            )
            .await
            .unwrap();
        assert_eq!(waiting.len(), 1);
        assert_eq!(waiting[0].url.as_str(), "https://edge-eu-west.example.com/");

        // Verify the origin's track inventory
        let tracks = origin
            .list_tracks(scope, &ns("sports/football/match-42"))
            .await
            .unwrap();
        assert_eq!(tracks.len(), 3);

        // Verify scope isolation — a different provider sees nothing
        let other_result = origin
            .lookup(Some("other-provider"), &ns("sports/football/match-42"))
            .await;
        assert!(matches!(
            other_result,
            Err(CoordinatorError::NamespaceNotFound)
        ));
    }

    // ========================================================================
    // Default trait implementations (no-op behavior)
    // ========================================================================

    /// A minimal coordinator that only implements the required methods.
    /// Used to verify that all defaulted methods work correctly — this is
    /// what existing implementors experience after upgrading.
    struct MinimalCoordinator;

    #[async_trait]
    impl Coordinator for MinimalCoordinator {
        async fn register_namespace(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
        ) -> CoordinatorResult<NamespaceRegistration> {
            Ok(NamespaceRegistration::new(()))
        }

        async fn unregister_namespace(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
        ) -> CoordinatorResult<()> {
            Ok(())
        }

        async fn lookup(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
        ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)> {
            Err(CoordinatorError::NamespaceNotFound)
        }
    }

    #[tokio::test]
    async fn default_resolve_scope_passes_through_path() {
        let coord = MinimalCoordinator;
        let scope = coord
            .resolve_scope(Some("/provider/acme-sports"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(scope.scope_id, "/provider/acme-sports");
        assert!(scope.permissions.can_publish());
        assert!(scope.permissions.can_subscribe());
    }

    #[tokio::test]
    async fn default_resolve_scope_none_is_unscoped() {
        let coord = MinimalCoordinator;
        let result = coord.resolve_scope(None).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn default_get_scope_config_returns_defaults() {
        let coord = MinimalCoordinator;
        let config = coord.get_scope_config(Some("any-scope")).await.unwrap();
        assert!(!config.lingering_subscribe);
        assert!(config.origin_fallback.is_none());
    }

    #[tokio::test]
    async fn default_subscribe_namespace_returns_empty() {
        let coord = MinimalCoordinator;
        let sub = coord
            .subscribe_namespace(Some("scope"), &ns("sports/football"))
            .await
            .unwrap();
        assert!(sub.existing_namespaces.is_empty());
    }

    #[tokio::test]
    async fn default_lookup_namespace_subscribers_returns_empty() {
        let coord = MinimalCoordinator;
        let relays = coord
            .lookup_namespace_subscribers(Some("scope"), &ns("sports/football/match-42"))
            .await
            .unwrap();
        assert!(relays.is_empty());
    }

    #[tokio::test]
    async fn default_register_track_returns_no_op_handle() {
        let coord = MinimalCoordinator;
        let _reg = coord
            .register_track(
                Some("scope"),
                &ns("sports/football/match-42"),
                "video-1080p",
            )
            .await
            .unwrap();
        // Handle drops without panic
    }

    #[tokio::test]
    async fn default_list_tracks_returns_empty() {
        let coord = MinimalCoordinator;
        let tracks = coord
            .list_tracks(Some("scope"), &ns("sports/football/match-42"))
            .await
            .unwrap();
        assert!(tracks.is_empty());
    }

    #[tokio::test]
    async fn default_subscribe_track_returns_no_op_handle() {
        let coord = MinimalCoordinator;
        let _sub = coord
            .subscribe_track(
                Some("scope"),
                &ns("sports/football/match-42"),
                "video-1080p",
            )
            .await
            .unwrap();
        // Handle drops without panic
    }

    #[tokio::test]
    async fn default_lookup_track_subscribers_returns_empty() {
        let coord = MinimalCoordinator;
        let relays = coord
            .lookup_track_subscribers(
                Some("scope"),
                &ns("sports/football/match-42"),
                "video-1080p",
            )
            .await
            .unwrap();
        assert!(relays.is_empty());
    }
}
