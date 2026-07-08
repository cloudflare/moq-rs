// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use moq_native_ietf::quic;
use moq_transport::coding::TrackNamespace;
use moq_transport::serve::{Track, TrackInterest, TrackReader};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{metrics::GaugeGuard, Coordinator, CoordinatorError, WARM_CACHE_LINGER};

/// Cache key for upstream relay-to-relay connections.
///
/// Keyed by both URL and destination address so that connections are reused
/// only when both match.
type RemoteCacheKey = (Url, Option<SocketAddr>);
type RemoteSlot = Arc<Mutex<Option<Remote>>>;
type TrackCacheKey = (TrackNamespace, String);
type TrackSlot = Arc<Mutex<Option<CachedTrack>>>;

/// A deduplicated cross-relay track subscription.
///
/// Mirrors the local dedup cache in `moq-transport`'s `serve::tracks`: many
/// downstream subscribers share one upstream subscription to a remote relay. The
/// pinned `reader` clone keeps the track warm; `interest` counts how many
/// downstream readers are currently live so the subscription task can tear the
/// upstream subscription down (emitting UNSUBSCRIBE) once the last one leaves.
#[derive(Clone)]
struct CachedTrack {
    reader: TrackReader,
    interest: TrackInterest,
}

/// Manages connections to remote relays.
///
/// When a subscription request comes in for a namespace that isn't local,
/// RemoteManager uses the coordinator to find which remote relay serves it,
/// establishes a connection if needed, and subscribes to the track.
#[derive(Clone)]
pub struct RemoteManager {
    coordinator: Arc<dyn Coordinator>,
    clients: Vec<quic::Client>,
    remotes: Arc<Mutex<HashMap<RemoteCacheKey, RemoteSlot>>>,
}

impl RemoteManager {
    /// Create a new RemoteManager.
    pub fn new(coordinator: Arc<dyn Coordinator>, clients: Vec<quic::Client>) -> Self {
        Self {
            coordinator,
            clients,
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
        track_name: &str,
    ) -> anyhow::Result<Option<TrackReader>> {
        let (origin, client) = match self.coordinator.lookup(scope, namespace).await {
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

        match remote
            .subscribe(namespace.clone(), track_name.to_string())
            .await
        {
            Ok(reader) => Ok(reader),
            Err(err) => {
                tracing::warn!(remote_url = %url, error = %err, "remote subscribe failed, removing from cache");
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

/// Log the outcome of a cross-relay track subscription ending.
fn log_remote_subscription_ended(
    url: &Url,
    key: &TrackCacheKey,
    result: Result<(), moq_transport::serve::ServeError>,
) {
    match result {
        Ok(()) => {
            tracing::debug!(remote_url = %url, namespace = %key.0, track = %key.1, "remote track subscription ended");
        }
        Err(err) => {
            tracing::warn!(remote_url = %url, namespace = %key.0, track = %key.1, error = %err, "remote track subscription ended with error: {}", err);
        }
    }
}

/// Evict a cross-relay track from its cache slot, but only if the slot still
/// holds the given reader AND no downstream interest remains.
///
/// The interest check runs under the same slot lock that `Remote::subscribe`
/// holds when handing out readers, so a subscriber that arrives concurrently is
/// never stranded on a torn-down upstream subscription: it either wins the lock
/// (interest becomes non-zero and eviction is skipped) or takes the cache-miss
/// path afterwards and creates a fresh upstream subscription. Returns true if the
/// entry was evicted.
async fn evict_remote_track_if_idle(
    slot: &TrackSlot,
    reader: &TrackReader,
    interest: &TrackInterest,
) -> bool {
    let mut cached = slot.lock().await;
    match cached.as_ref() {
        Some(current)
            if Arc::ptr_eq(&current.reader.info, &reader.info) && interest.count() == 0 =>
        {
            cached.take();
            true
        }
        _ => false,
    }
}

/// A connection to a single remote relay with its own QUIC client.
#[derive(Clone)]
struct Remote {
    url: Url,
    subscriber: moq_transport::session::Subscriber,
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

        let (session, subscriber) =
            match moq_transport::session::Subscriber::connect(session, transport).await {
                Ok(session) => session,
                Err(err) => {
                    metrics::counter!("moq_relay_upstream_errors_total", "stage" => "session")
                        .increment(1);
                    return Err(err.into());
                }
            };

        let connected = Arc::new(AtomicBool::new(true));
        let cancel = CancellationToken::new();
        let upstream_guard = GaugeGuard::new("moq_relay_upstream_connections");

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
            subscriber,
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
        track_name: String,
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

            if let Some(entry) = cached.as_ref() {
                if !entry.reader.is_closed() {
                    // Deduplicate onto the warm upstream subscription. Hand out a
                    // clone carrying a fresh interest guard (incremented under the
                    // slot lock, serialized with eviction) so the subscription
                    // task knows a downstream subscriber is present.
                    return Ok(Some(
                        entry.reader.clone().with_interest(entry.interest.guard()),
                    ));
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

            // Per-track interest counter, shared between the pinned cache clone,
            // the handed-out reader's guard, and the subscription task below.
            let interest = TrackInterest::new();

            // Attach the first interest guard (count -> 1) BEFORE spawning the
            // subscription task, so it never observes a spurious idle() at startup.
            let handed = reader.clone().with_interest(interest.guard());

            *cached = Some(CachedTrack {
                reader: reader.clone(),
                interest: interest.clone(),
            });
            drop(cached);

            let cleanup_key = key.clone();
            let cleanup_reader = reader;
            let cleanup_slot = slot.clone();
            let cleanup_interest = interest.clone();
            tokio::spawn(async move {
                // Hold the upstream subscription until it ends on its own, the
                // connection is cancelled, or downstream interest stays idle
                // through the linger window.
                loop {
                    tokio::select! {
                        biased;
                        result = subscribe.closed() => {
                            log_remote_subscription_ended(&url, &cleanup_key, result);
                            break;
                        }
                        _ = cancel.cancelled() => {
                            tracing::debug!(remote_url = %url, namespace = %cleanup_key.0, track = %cleanup_key.1, "remote track subscription cancelled");
                            break;
                        }
                        _ = cleanup_interest.idle() => {
                            // Last downstream subscriber left; linger briefly so an
                            // instant late-joiner can reattach to the warm upstream
                            // subscription instead of forcing a fresh SUBSCRIBE.
                            tokio::select! {
                                biased;
                                result = subscribe.closed() => {
                                    log_remote_subscription_ended(&url, &cleanup_key, result);
                                    break;
                                }
                                _ = cancel.cancelled() => {
                                    tracing::debug!(remote_url = %url, namespace = %cleanup_key.0, track = %cleanup_key.1, "remote track subscription cancelled");
                                    break;
                                }
                                // A new subscriber arrived during the linger; keep serving.
                                _ = cleanup_interest.busy() => {}
                                _ = tokio::time::sleep(WARM_CACHE_LINGER) => {
                                    // Linger elapsed. Evict iff still idle, then tear
                                    // down (dropping `subscribe` emits UNSUBSCRIBE).
                                    if evict_remote_track_if_idle(&cleanup_slot, &cleanup_reader, &cleanup_interest).await {
                                        tracing::info!(remote_url = %url, namespace = %cleanup_key.0, track = %cleanup_key.1, "remote track idle; unsubscribing upstream");
                                        break;
                                    }
                                    // Raced with a late joiner right at expiry; re-arm.
                                }
                            }
                        }
                    }
                }

                // Dropping the upstream subscription emits UNSUBSCRIBE (unless it
                // already ended). Done explicitly for clarity before slot cleanup.
                drop(subscribe);

                if let Some(tracks) = tracks.upgrade() {
                    let mut cached = cleanup_slot.lock().await;
                    if matches!(cached.as_ref(), Some(current) if Arc::ptr_eq(&current.reader.info, &cleanup_reader.info))
                    {
                        cached.take();
                    }
                    drop(cached);

                    remove_empty_track_slot(&tracks, &cleanup_key, &cleanup_slot).await;
                }
            });

            return Ok(Some(handed));
        }
    }
}

impl std::fmt::Debug for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Remote")
            .field("url", &self.url.to_string())
            .field("connected", &self.is_connected())
            .finish()
    }
}
