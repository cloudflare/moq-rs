// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use moq_native_ietf::quic;
use moq_transport::coding::TrackNamespace;
use moq_transport::serve::{Track, TrackReader};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{metrics::GaugeGuard, Coordinator, CoordinatorError};

/// Cache key for upstream relay-to-relay connections.
///
/// Keyed by both URL and destination address so that connections are reused
/// only when both match.
type RemoteCacheKey = (Url, Option<SocketAddr>);

/// Manages connections to remote relays.
///
/// When a subscription request comes in for a namespace that isn't local,
/// RemoteManager uses the coordinator to find which remote relay serves it,
/// establishes a connection if needed, and subscribes to the track.
#[derive(Clone)]
pub struct RemoteManager {
    coordinator: Arc<dyn Coordinator>,
    clients: Vec<quic::Client>,
    remotes: Arc<Mutex<HashMap<RemoteCacheKey, Remote>>>,
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
                self.remove(&cache_key).await;
                return Err(err);
            }
        };

        match remote
            .subscribe(namespace.clone(), track_name.to_string())
            .await
        {
            Ok(reader) => Ok(reader),
            Err(err) => {
                if !remote.is_connected() {
                    tracing::warn!(remote_url = %url, "remote connection is dead, removing from cache");
                    self.remove(&cache_key).await;
                }

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
        let mut remotes = self.remotes.lock().await;

        if let Some(remote) = remotes.get(&cache_key) {
            if remote.is_connected() {
                return Ok(remote.clone());
            }

            tracing::info!(remote_url = %cache_key.0, "removing dead connection to remote relay");
            remotes.remove(&cache_key);
        }

        let client = match client {
            Some(client) => client,
            None => self.clients.first().ok_or_else(|| {
                anyhow::anyhow!("no QUIC clients configured for remote connections")
            })?,
        };

        tracing::info!(remote_url = %cache_key.0, "connecting to remote relay");
        let remote = Remote::connect(cache_key.0.clone(), cache_key.1, client).await?;
        remotes.insert(cache_key, remote.clone());

        Ok(remote)
    }

    /// Remove a remote connection (called when connection fails).
    async fn remove(&self, cache_key: &RemoteCacheKey) {
        let mut remotes = self.remotes.lock().await;
        if let Some(remote) = remotes.remove(cache_key) {
            remote.shutdown();
        }
    }

    /// Shutdown all remote connections.
    pub async fn shutdown(&self) {
        let mut remotes = self.remotes.lock().await;
        for (cache_key, remote) in remotes.drain() {
            tracing::info!(remote_url = %cache_key.0, "shutting down remote connection");
            remote.shutdown();
        }
    }
}

/// A connection to a single remote relay with its own QUIC client.
#[derive(Clone)]
pub struct Remote {
    url: Url,
    subscriber: moq_transport::session::Subscriber,
    /// Track subscriptions - maps (namespace, track_name) to track reader
    tracks: Arc<Mutex<HashMap<(TrackNamespace, String), TrackReader>>>,
    /// Flag indicating if the connection is still alive
    connected: Arc<AtomicBool>,
    /// Cancellation token for the session task
    cancel: CancellationToken,
}

impl Remote {
    /// Connect to a remote relay with a dedicated QUIC client.
    async fn connect(
        url: Url,
        addr: Option<SocketAddr>,
        client: &quic::Client,
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
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    /// Shutdown the remote connection.
    pub fn shutdown(&self) {
        self.cancel.cancel();
        self.connected.store(false, Ordering::Release);
    }

    /// Subscribe to a track on this remote relay.
    pub async fn subscribe(
        &self,
        namespace: TrackNamespace,
        track_name: String,
    ) -> anyhow::Result<Option<TrackReader>> {
        if !self.is_connected() {
            anyhow::bail!("remote connection to {} is closed", self.url);
        }

        let key = (namespace.clone(), track_name.clone());
        let mut tracks = self.tracks.lock().await;

        if let Some(reader) = tracks.get(&key) {
            return Ok(Some(reader.clone()));
        }

        let (writer, reader) = Track::new(namespace, track_name).produce();
        tracks.insert(key.clone(), reader.clone());
        drop(tracks);

        let mut subscriber = self.subscriber.clone();
        let tracks = self.tracks.clone();
        let url = self.url.clone();

        tokio::spawn(async move {
            tracing::info!(remote_url = %url, namespace = %key.0, track = %key.1, "subscribing to remote track");

            if let Err(err) = subscriber.subscribe(writer).await {
                tracing::warn!(remote_url = %url, namespace = %key.0, track = %key.1, error = %err, "failed subscribing to remote track: {}", err);
            }

            tracks.lock().await.remove(&key);
            tracing::debug!(remote_url = %url, namespace = %key.0, track = %key.1, "remote track subscription ended");
        });

        Ok(Some(reader))
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
