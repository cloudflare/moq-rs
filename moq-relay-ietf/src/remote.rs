// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use moq_native_ietf::quic;
use moq_transport::coding::{KeyValuePairs, TrackName, TrackNamespace, TrackNamespacePrefix};
use moq_transport::message::SubscribeOptions;
use moq_transport::serve::{Track, TrackReader, TracksReader};
use moq_transport::session::{Publisher, SessionConfig, SubscribeNamespace};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{metrics::GaugeGuard, Coordinator, CoordinatorError, RelayInfo, SessionContext};

/// Cache key for upstream relay-to-relay connections.
///
/// Keyed by both URL and destination address so that connections are reused
/// only when both match.
type RemoteCacheKey = (Url, Option<SocketAddr>);
type RemoteSlot = Arc<Mutex<Option<Remote>>>;
type TrackCacheKey = (TrackNamespace, TrackName);
type TrackSlot = Arc<Mutex<Option<TrackReader>>>;

/// Manages connections to remote relays.
///
/// When a subscription request comes in for a namespace that isn't local,
/// RemoteManager uses the coordinator to find which remote relay serves it,
/// establishes a connection if needed, and subscribes to the track.
#[derive(Clone)]
pub struct RemoteManager {
    coordinator: Arc<dyn Coordinator>,
    clients: Vec<quic::Client>,
    session_config: SessionConfig,
    remotes: Arc<Mutex<HashMap<RemoteCacheKey, RemoteSlot>>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopCoordinator;

    #[async_trait::async_trait]
    impl Coordinator for NoopCoordinator {
        async fn register_namespace(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
            _context: &crate::CoordinatorContext,
        ) -> crate::CoordinatorResult<crate::NamespaceRegistration> {
            Ok(crate::NamespaceRegistration::new(()))
        }

        async fn unregister_namespace(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
        ) -> crate::CoordinatorResult<()> {
            Ok(())
        }

        async fn lookup(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
        ) -> crate::CoordinatorResult<(crate::NamespaceOrigin, Option<quic::Client>)> {
            Err(crate::CoordinatorError::NamespaceNotFound)
        }
    }

    #[test]
    fn new_uses_default_session_config() {
        let manager = RemoteManager::new(Arc::new(NoopCoordinator), vec![]);

        assert_eq!(manager.session_config, SessionConfig::default());
    }

    #[test]
    fn new_with_session_config_stores_custom_config() {
        let config = SessionConfig { max_request_id: 7 };
        let manager =
            RemoteManager::new_with_session_config(Arc::new(NoopCoordinator), vec![], config);

        assert_eq!(manager.session_config, config);
    }

    #[test]
    fn remote_endpoint_context_is_internal() {
        let url = Url::parse("https://relay.example.com/live").unwrap();
        let addr = "127.0.0.1:4433".parse().unwrap();

        let context = Remote::context_for_endpoint(url.clone(), Some(addr));

        assert_eq!(context.interface, crate::SessionInterface::Internal);
        assert!(context.scope().is_none());
        let peer = context.peer.unwrap();
        assert_eq!(peer.url, url);
        assert_eq!(peer.addr, Some(addr));
    }

    #[tokio::test]
    async fn subscribe_namespace_to_without_clients_errors() {
        // No QUIC clients configured, so get_or_connect() fails before any
        // network activity. This exercises the new forwarding entry point's
        // connect + error plumbing without needing a live peer.
        let manager = RemoteManager::new(Arc::new(NoopCoordinator), vec![]);
        let relay = RelayInfo::new(Url::parse("https://relay.example.com/live").unwrap());

        let result = manager
            .subscribe_namespace_to(
                &relay,
                TrackNamespacePrefix::from_utf8_path("example.com"),
                SubscribeOptions::Namespace,
            )
            .await;

        // `SubscribeNamespace` is not `Debug`, so match rather than expect_err.
        let err = match result {
            Ok(_) => panic!("expected connect failure with no clients"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("no QUIC clients configured"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn publish_namespace_to_without_clients_errors() {
        let manager = RemoteManager::new(Arc::new(NoopCoordinator), vec![]);
        let relay = RelayInfo::new(Url::parse("https://relay.example.com/live").unwrap());
        let (_writer, _request, reader) =
            moq_transport::serve::Tracks::new(TrackNamespace::from_utf8_path("example.com"))
                .produce();

        let result = manager.publish_namespace_to(&relay, reader).await;

        let err = result.expect_err("expected connect failure with no clients");
        assert!(
            err.to_string().contains("no QUIC clients configured"),
            "unexpected error: {err}"
        );
    }
}

impl RemoteManager {
    /// Create a new RemoteManager.
    pub fn new(coordinator: Arc<dyn Coordinator>, clients: Vec<quic::Client>) -> Self {
        Self::new_with_session_config(coordinator, clients, SessionConfig::default())
    }

    /// Create a new RemoteManager with explicit MoQT session configuration.
    pub fn new_with_session_config(
        coordinator: Arc<dyn Coordinator>,
        clients: Vec<quic::Client>,
        session_config: SessionConfig,
    ) -> Self {
        Self {
            coordinator,
            clients,
            session_config,
            remotes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Subscribe to a track from a remote relay.
    ///
    /// `scope` is the resolved scope identity from `Coordinator::resolve_scope()`,
    /// passed through to the coordinator's `lookup()` to scope the search.
    ///
    /// Returns None if the namespace isn't found in any remote relay.
    pub async fn subscribe(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
        track_name: impl Into<TrackName>,
    ) -> anyhow::Result<Option<TrackReader>> {
        let track_name = track_name.into();

        // Coordinator::lookup_track is the ergonomic routing entry point: it
        // tries exact PUBLISH track registrations first, then falls back to
        // PUBLISH_NAMESPACE namespace routing.
        let (origin, client) = match self
            .coordinator
            .lookup_track(scope, namespace, &track_name.to_string())
            .await
        {
            Ok(result) => result,
            Err(CoordinatorError::NamespaceNotFound) => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        let url = origin.url();
        let cache_key = (url.clone(), origin.addr());

        let remote = match self
            .get_or_connect(cache_key.clone(), client.as_ref())
            .await
        {
            Ok(remote) => remote,
            Err(err) => {
                tracing::error!(remote_url = %url, error = %err, "failed to connect to remote relay: {}", err);
                return Err(err);
            }
        };

        match remote.subscribe(namespace.clone(), track_name).await {
            Ok(reader) => Ok(reader),
            Err(err) => {
                tracing::warn!(remote_url = %url, error = %err, "remote subscribe failed, removing from cache");
                self.remove_if_same_remote(&cache_key, &remote).await;

                Err(err)
            }
        }
    }

    /// Forward a `SUBSCRIBE_NAMESPACE` to a specific relay peer.
    ///
    /// Connects to (or reuses a connection to) `relay` and opens a namespace
    /// subscription for `prefix`, returning the [`SubscribeNamespace`] handle.
    /// The target relay is explicit (supplied by the caller from coordinator
    /// routing hops) rather than discovered via a coordinator lookup — that is
    /// how this differs from [`Self::subscribe`].
    pub async fn subscribe_namespace_to(
        &self,
        relay: &RelayInfo,
        prefix: TrackNamespacePrefix,
        options: SubscribeOptions,
    ) -> anyhow::Result<SubscribeNamespace> {
        let cache_key = (relay.url.clone(), relay.addr);

        let remote = match self.get_or_connect(cache_key.clone(), None).await {
            Ok(remote) => remote,
            Err(err) => {
                tracing::error!(remote_url = %relay.url, error = %err, "failed to connect to remote relay: {}", err);
                return Err(err);
            }
        };

        match remote.subscribe_namespace(prefix, options).await {
            Ok(handle) => Ok(handle),
            Err(err) => {
                tracing::warn!(remote_url = %relay.url, error = %err, "remote subscribe_namespace failed, removing from cache");
                self.remove_if_same_remote(&cache_key, &remote).await;
                Err(err)
            }
        }
    }

    /// Forward a `PUBLISH_NAMESPACE` to a specific relay peer.
    ///
    /// Connects to (or reuses a connection to) `relay` and advertises
    /// `tracks.namespace`, serving its tracks from `tracks`. Blocks until the
    /// namespace is unannounced or the session errors (see
    /// [`Remote::publish_namespace`]), so callers typically drive it from a
    /// dedicated task.
    pub async fn publish_namespace_to(
        &self,
        relay: &RelayInfo,
        tracks: TracksReader,
    ) -> anyhow::Result<()> {
        let cache_key = (relay.url.clone(), relay.addr);

        let remote = match self.get_or_connect(cache_key.clone(), None).await {
            Ok(remote) => remote,
            Err(err) => {
                tracing::error!(remote_url = %relay.url, error = %err, "failed to connect to remote relay: {}", err);
                return Err(err);
            }
        };

        match remote.publish_namespace(tracks).await {
            Ok(()) => Ok(()),
            Err(err) => {
                tracing::warn!(remote_url = %relay.url, error = %err, "remote publish_namespace failed, removing from cache");
                self.remove_if_same_remote(&cache_key, &remote).await;
                Err(err)
            }
        }
    }

    /// Get an existing remote connection or create a new one.
    async fn get_or_connect(
        &self,
        cache_key: RemoteCacheKey,
        client: Option<&quic::Client>,
    ) -> anyhow::Result<Remote> {
        let client = match client {
            Some(client) => client,
            None => self.clients.first().ok_or_else(|| {
                anyhow::anyhow!("no QUIC clients configured for remote connections")
            })?,
        };

        loop {
            // The manager lock only protects the map. The per-key slot lock protects
            // that key's connection state, so unrelated remotes can connect in parallel.
            let slot = {
                let mut remotes = self.remotes.lock().await;
                remotes
                    .entry(cache_key.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(None)))
                    .clone()
            };

            let mut cached = slot.lock().await;

            let is_current_slot = {
                let remotes = self.remotes.lock().await;
                matches!(remotes.get(&cache_key), Some(current) if Arc::ptr_eq(current, &slot))
            };

            if !is_current_slot {
                continue;
            }

            if let Some(remote) = cached.as_ref() {
                if remote.is_connected() {
                    return Ok(remote.clone());
                }

                tracing::info!(remote_url = %cache_key.0, "removing dead connection to remote relay");
            };

            if let Some(remote) = cached.take() {
                remote.shutdown().await;
            }

            tracing::info!(remote_url = %cache_key.0, "connecting to remote relay");
            let remote = match Remote::connect(
                cache_key.0.clone(),
                cache_key.1,
                client,
                self.session_config,
                Arc::downgrade(&self.remotes),
                cache_key.clone(),
                Arc::downgrade(&slot),
            )
            .await
            {
                Ok(remote) => remote,
                Err(err) => {
                    drop(cached);
                    remove_empty_remote_slot(&self.remotes, &cache_key, &slot).await;
                    return Err(err);
                }
            };

            *cached = Some(remote.clone());
            return Ok(remote);
        }
    }

    async fn remove_if_same_remote(&self, cache_key: &RemoteCacheKey, remote: &Remote) {
        let slot = {
            let remotes = self.remotes.lock().await;
            remotes.get(cache_key).cloned()
        };

        if let Some(slot) = slot {
            let removed = {
                let mut cached = slot.lock().await;
                match cached.as_ref() {
                    Some(current) if current.is_same_connection(remote) => cached.take(),
                    _ => None,
                }
            };

            if let Some(remote) = removed {
                remote.shutdown().await;
                remove_empty_remote_slot(&self.remotes, cache_key, &slot).await;
            }
        }
    }

    /// Shutdown all remote connections.
    pub(crate) async fn shutdown(&self) {
        let remotes = {
            let mut remotes = self.remotes.lock().await;
            remotes.drain().collect::<Vec<_>>()
        };

        for (cache_key, slot) in remotes {
            tracing::info!(remote_url = %cache_key.0, "shutting down remote connection");
            let mut remote = slot.lock().await;
            if let Some(remote) = remote.take() {
                remote.shutdown().await;
            }
        }
    }
}

async fn remove_empty_remote_slot(
    remotes: &Arc<Mutex<HashMap<RemoteCacheKey, RemoteSlot>>>,
    cache_key: &RemoteCacheKey,
    slot: &RemoteSlot,
) {
    let cached = slot.lock().await;
    if cached.is_some() {
        return;
    }

    let mut remotes = remotes.lock().await;
    if matches!(remotes.get(cache_key), Some(current) if Arc::ptr_eq(current, slot)) {
        remotes.remove(cache_key);
    }
}

async fn remove_empty_track_slot(
    tracks: &Arc<Mutex<HashMap<TrackCacheKey, TrackSlot>>>,
    key: &TrackCacheKey,
    slot: &TrackSlot,
) {
    let cached = slot.lock().await;
    if cached.is_some() {
        return;
    }

    let mut tracks = tracks.lock().await;
    if matches!(tracks.get(key), Some(current) if Arc::ptr_eq(current, slot)) {
        tracks.remove(key);
    }
}

/// A connection to a single remote relay with its own QUIC client.
#[derive(Clone)]
struct Remote {
    url: Url,
    context: SessionContext,
    /// Subscriber role: exact-track `SUBSCRIBE` and `SUBSCRIBE_NAMESPACE`.
    subscriber: moq_transport::session::Subscriber,
    /// Publisher role: outbound `PUBLISH_NAMESPACE` to advertise namespaces
    /// to this peer.
    publisher: Publisher,
    /// Track subscriptions keyed by full track name.
    tracks: Arc<Mutex<HashMap<TrackCacheKey, TrackSlot>>>,
    /// Flag indicating if the connection is still alive.
    connected: Arc<AtomicBool>,
    /// Cancellation token for the session task.
    cancel: CancellationToken,
}

impl Remote {
    /// Connect to a remote relay with a dedicated QUIC client.
    async fn connect(
        url: Url,
        addr: Option<SocketAddr>,
        client: &quic::Client,
        session_config: SessionConfig,
        remotes: Weak<Mutex<HashMap<RemoteCacheKey, RemoteSlot>>>,
        cache_key: RemoteCacheKey,
        cache_slot: Weak<Mutex<Option<Remote>>>,
    ) -> anyhow::Result<Self> {
        let (session, _quic_client_initial_cid, transport) = match client.connect(&url, addr).await
        {
            Ok(session) => session,
            Err(err) => {
                metrics::counter!("moq_relay_upstream_errors_total", "stage" => "connect")
                    .increment(1);
                return Err(err);
            }
        };

        // Establish a full relay-to-relay MoQT session so this connection can
        // act in both roles: Subscriber (exact-track SUBSCRIBE and
        // SUBSCRIBE_NAMESPACE discovery) and Publisher (outbound
        // PUBLISH_NAMESPACE). This mirrors the `--announce` forward path in
        // relay.rs rather than the subscriber-only upstream pull it replaces.
        let (session, publisher, subscriber) =
            match moq_transport::session::Session::connect_with_config(
                session,
                None,
                transport,
                session_config,
            )
            .await
            {
                Ok(parts) => parts,
                Err(err) => {
                    metrics::counter!("moq_relay_upstream_errors_total", "stage" => "session")
                        .increment(1);
                    return Err(err.into());
                }
            };

        let connected = Arc::new(AtomicBool::new(true));
        let cancel = CancellationToken::new();
        let upstream_guard = GaugeGuard::new("moq_relay_upstream_connections");
        let context = Self::context_for_endpoint(url.clone(), addr);

        let session_url = url.clone();
        let session_connected = connected.clone();
        let session_cancel = cancel.clone();

        tokio::spawn(async move {
            let _upstream_guard = upstream_guard;
            tokio::select! {
                result = session.run() => {
                    if let Err(err) = result {
                        tracing::warn!(remote_url = %session_url, error = %err, "remote session closed: {}", err);
                    } else {
                        tracing::info!(remote_url = %session_url, "remote session closed normally");
                    }
                }
                _ = session_cancel.cancelled() => {
                    tracing::info!(remote_url = %session_url, "remote session cancelled");
                }
            }

            session_connected.store(false, Ordering::Release);

            if let Some(cache_slot) = cache_slot.upgrade() {
                let mut cleared = false;
                let mut cached = cache_slot.lock().await;
                if matches!(cached.as_ref(), Some(remote) if Arc::ptr_eq(&remote.connected, &session_connected))
                {
                    cached.take();
                    cleared = true;
                    tracing::info!(remote_url = %session_url, "cleared closed remote connection from cache");
                }
                drop(cached);

                if cleared {
                    if let Some(remotes) = remotes.upgrade() {
                        remove_empty_remote_slot(&remotes, &cache_key, &cache_slot).await;
                    }
                }
            }
        });

        Ok(Self {
            url,
            context,
            subscriber,
            publisher,
            tracks: Arc::new(Mutex::new(HashMap::new())),
            connected,
            cancel,
        })
    }

    /// Check if the connection is still alive.
    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn is_same_connection(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.connected, &other.connected)
    }

    /// Build the [`SessionContext`] for an outbound connection dialed by the
    /// [`RemoteManager`].
    ///
    /// Every connection originated here targets a relay-mesh peer resolved via
    /// the coordinator for relay-to-relay routing, so it is always tagged
    /// [`SessionContext::internal`]. Inbound classification (public vs internal)
    /// is handled separately in `relay.rs` via the connection tagger.
    ///
    /// This label is local to this relay and is **not** sent over the wire: the
    /// relay on the accepting end sees only the raw connection and classifies it
    /// independently with its own [`ConnectionTagger`]. To have the peer treat
    /// this link as internal, dial it on an address/SNI/path its tagger
    /// recognizes.
    ///
    /// [`ConnectionTagger`]: crate::ConnectionTagger
    fn context_for_endpoint(url: Url, addr: Option<SocketAddr>) -> SessionContext {
        let endpoint = match addr {
            Some(addr) => RelayInfo::with_addr(url, addr),
            None => RelayInfo::new(url),
        };

        SessionContext::internal(None, Some(endpoint))
    }

    /// Shutdown the remote connection.
    async fn shutdown(&self) {
        self.cancel.cancel();
        self.connected.store(false, Ordering::Release);
        self.tracks.lock().await.clear();
    }

    /// Subscribe to a track on this remote relay.
    async fn subscribe(
        &self,
        namespace: TrackNamespace,
        track_name: TrackName,
    ) -> anyhow::Result<Option<TrackReader>> {
        let key = (namespace.clone(), track_name.clone());

        loop {
            if !self.is_connected() {
                anyhow::bail!("remote connection to {} is closed", self.url);
            }

            let slot = {
                let mut tracks = self.tracks.lock().await;
                tracks
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(None)))
                    .clone()
            };

            let mut cached = slot.lock().await;

            let is_current_slot = {
                let tracks = self.tracks.lock().await;
                matches!(tracks.get(&key), Some(current) if Arc::ptr_eq(current, &slot))
            };

            if !is_current_slot {
                continue;
            }

            if let Some(reader) = cached.as_ref() {
                if !reader.is_closed() {
                    return Ok(Some(reader.clone()));
                }

                tracing::debug!(remote_url = %self.url, namespace = %key.0, track = %key.1, "removing closed remote track from cache");
            }

            cached.take();

            let mut subscriber = self.subscriber.clone();
            let url = self.url.clone();
            let tracks = Arc::downgrade(&self.tracks);
            let cancel = self.cancel.clone();

            tracing::info!(remote_url = %url, namespace = %key.0, track = %key.1, "subscribing to remote track");

            let (writer, reader) = Track::new(namespace.clone(), track_name.clone()).produce();
            let subscribe_result = tokio::select! {
                result = subscriber.subscribe_open(writer) => result,
                _ = cancel.cancelled() => {
                    drop(cached);
                    remove_empty_track_slot(&self.tracks, &key, &slot).await;
                    anyhow::bail!("subscribe cancelled, remote connection to {} is closed", self.url);
                }
            };

            let subscribe = match subscribe_result {
                Ok(subscribe) => subscribe,
                Err(err) => {
                    drop(cached);
                    remove_empty_track_slot(&self.tracks, &key, &slot).await;
                    return Err(err.into());
                }
            };

            if !self.is_connected() {
                drop(cached);
                remove_empty_track_slot(&self.tracks, &key, &slot).await;
                anyhow::bail!("remote connection to {} is closed", self.url);
            }

            *cached = Some(reader.clone());
            drop(cached);

            let cleanup_key = key.clone();
            let cleanup_reader = reader.clone();
            let cleanup_slot = slot.clone();
            tokio::spawn(async move {
                tokio::select! {
                    result = subscribe.closed() => {
                        match result {
                            Ok(()) => {
                                tracing::debug!(remote_url = %url, namespace = %cleanup_key.0, track = %cleanup_key.1, "remote track subscription ended");
                            }
                            Err(err) => {
                                tracing::warn!(remote_url = %url, namespace = %cleanup_key.0, track = %cleanup_key.1, error = %err, "remote track subscription ended with error: {}", err);
                            }
                        }
                    }
                    _ = cancel.cancelled() => {
                        tracing::debug!(remote_url = %url, namespace = %cleanup_key.0, track = %cleanup_key.1, "remote track subscription cancelled");
                    }
                }

                if let Some(tracks) = tracks.upgrade() {
                    let mut cached = cleanup_slot.lock().await;
                    if matches!(cached.as_ref(), Some(current) if Arc::ptr_eq(&current.info, &cleanup_reader.info))
                    {
                        cached.take();
                    }
                    drop(cached);

                    remove_empty_track_slot(&tracks, &cleanup_key, &cleanup_slot).await;
                }
            });

            return Ok(Some(reader));
        }
    }

    /// Forward a `SUBSCRIBE_NAMESPACE` to this peer (Subscriber role).
    ///
    /// Opens a namespace subscription for `prefix` and returns the
    /// [`SubscribeNamespace`] handle; the caller drives it via
    /// [`SubscribeNamespace::next`] / [`SubscribeNamespace::closed`], the same
    /// way [`Self::subscribe`] returns a [`TrackReader`] for the caller to read.
    ///
    /// Unlike [`Self::subscribe`], namespace subscriptions are not cached or
    /// deduplicated here: per-session prefix aggregation (to avoid draft-16
    /// `PREFIX_OVERLAP` on reused sessions) is deferred to the multi-relay
    /// routing work.
    async fn subscribe_namespace(
        &self,
        prefix: TrackNamespacePrefix,
        options: SubscribeOptions,
    ) -> anyhow::Result<SubscribeNamespace> {
        if !self.is_connected() {
            anyhow::bail!("remote connection to {} is closed", self.url);
        }

        tracing::info!(remote_url = %self.url, prefix = %prefix.to_utf8_path(), "forwarding SUBSCRIBE_NAMESPACE to remote relay");

        let mut subscriber = self.subscriber.clone();
        let subscribe_namespace = tokio::select! {
            result = subscriber.subscribe_namespace(prefix, options, KeyValuePairs::default()) => result?,
            _ = self.cancel.cancelled() => {
                anyhow::bail!("subscribe_namespace cancelled, remote connection to {} is closed", self.url);
            }
        };

        Ok(subscribe_namespace)
    }

    /// Forward a `PUBLISH_NAMESPACE` to this peer (Publisher role).
    ///
    /// Advertises `tracks.namespace` to the peer and serves its tracks from the
    /// provided [`TracksReader`]. Like [`moq_transport::session::Publisher::publish_namespace`]
    /// this blocks until the namespace is unannounced or the session errors, so
    /// callers typically drive it from a dedicated task (mirroring the
    /// `--announce` forward path in `consumer.rs`).
    async fn publish_namespace(&self, tracks: TracksReader) -> anyhow::Result<()> {
        if !self.is_connected() {
            anyhow::bail!("remote connection to {} is closed", self.url);
        }

        tracing::info!(remote_url = %self.url, namespace = %tracks.namespace.to_utf8_path(), "forwarding PUBLISH_NAMESPACE to remote relay");

        let mut publisher = self.publisher.clone();
        tokio::select! {
            result = publisher.publish_namespace(tracks) => result?,
            _ = self.cancel.cancelled() => {
                anyhow::bail!("publish_namespace cancelled, remote connection to {} is closed", self.url);
            }
        }

        Ok(())
    }
}

impl std::fmt::Debug for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Remote")
            .field("url", &self.url.to_string())
            .field("interface", &self.context.interface)
            .field("connected", &self.is_connected())
            .finish()
    }
}
