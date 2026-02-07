use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use moq_native_ietf::quic;
use moq_transport::coding::TrackNamespace;
use moq_transport::serve::{Track, TrackReader};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::Coordinator;

/// Manages connections to remote relays.
///
/// When a subscription request comes in for a namespace that isn't local,
/// RemoteManager uses the coordinator to find which remote relay serves it,
/// establishes a connection if needed, and subscribes to the track.
#[derive(Clone)]
pub struct RemoteManager {
    coordinator: Arc<dyn Coordinator>,
    clients: Vec<quic::Client>,
    remotes: Arc<Mutex<HashMap<Url, Remote>>>,
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
    /// This will:
    /// 1. Use the coordinator to lookup which relay serves the namespace
    /// 2. Connect to that relay if not already connected
    /// 3. Subscribe to the specific track
    ///
    /// Returns None if the namespace isn't found in any remote relay.
    pub async fn subscribe(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
    ) -> anyhow::Result<Option<TrackReader>> {
        // Ask coordinator where this namespace lives
        let (origin, client) = match self.coordinator.lookup(namespace).await {
            Ok((origin, client)) => (origin, client),
            Err(e) => {
                log::error!("failed to lookup namespace: {}", e);
                return Ok(None); // Namespace not found anywhere
            }
        };

        let url = origin.url();

        // Get or create a connection to the remote relay
        let remote = match self.get_or_connect(&url, client.as_ref()).await {
            Ok(remote) => remote,
            Err(e) => {
                log::error!("failed to connect to remote relay {}: {}", url, e);
                // Remove failed connection from cache
                self.remove(&url).await;
                return Err(e);
            }
        };

        // Subscribe to the track on the remote
        match remote
            .subscribe(namespace.clone(), track_name.to_string())
            .await
        {
            Ok(reader) => Ok(reader),
            Err(e) => {
                // If subscription fails, check if connection is dead and remove it
                if !remote.is_connected() {
                    log::warn!("remote connection {} is dead, removing from cache", url);
                    self.remove(&url).await;
                }
                Err(e)
            }
        }
    }

    /// Get an existing remote connection or create a new one.
    async fn get_or_connect(
        &self,
        url: &Url,
        client: Option<&quic::Client>,
    ) -> anyhow::Result<Remote> {
        let mut remotes = self.remotes.lock().await;

        // Check if we already have a connection
        if let Some(remote) = remotes.get(url) {
            if remote.is_connected() {
                return Ok(remote.clone());
            }
            // Connection is dead, remove it
            log::info!("removing dead connection to remote relay: {}", url);
            remotes.remove(url);
        }

        // Get client, with proper error handling for empty clients vec
        let client = match client {
            Some(c) => c,
            None => self.clients.first().ok_or_else(|| {
                anyhow::anyhow!("no QUIC clients configured for remote connections")
            })?,
        };

        // Create a new connection with its own QUIC client
        log::info!("connecting to remote relay: {}", url);
        let remote = Remote::connect(url.clone(), client).await?;

        remotes.insert(url.clone(), remote.clone());

        Ok(remote)
    }

    /// Remove a remote connection (called when connection fails).
    pub async fn remove(&self, url: &Url) {
        let mut remotes = self.remotes.lock().await;
        if let Some(remote) = remotes.remove(url) {
            // Cancel the session task when removing
            remote.shutdown();
        }
    }

    /// Shutdown all remote connections.
    pub async fn shutdown(&self) {
        let mut remotes = self.remotes.lock().await;
        for (url, remote) in remotes.drain() {
            log::info!("shutting down remote connection: {}", url);
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
    async fn connect(url: Url, client: &quic::Client) -> anyhow::Result<Self> {
        // Connect to the remote relay (DNS resolution happens inside connect)
        let (session, _cid) = client.connect(&url, None).await?;
        let (session, subscriber) = moq_transport::session::Subscriber::connect(session).await?;

        let connected = Arc::new(AtomicBool::new(true));
        let cancel = CancellationToken::new();

        // Spawn a task to run the session
        let session_url = url.clone();
        let session_connected = connected.clone();
        let session_cancel = cancel.clone();

        tokio::spawn(async move {
            tokio::select! {
                result = session.run() => {
                    if let Err(err) = result {
                        log::warn!("remote session closed: {} - {}", session_url, err);
                    } else {
                        log::info!("remote session closed normally: {}", session_url);
                    }
                }
                _ = session_cancel.cancelled() => {
                    log::info!("remote session cancelled: {}", session_url);
                }
            }
            // Mark connection as dead
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
        // Check connection state first
        if !self.is_connected() {
            return Err(anyhow::anyhow!(
                "remote connection to {} is closed",
                self.url
            ));
        }

        let key = (namespace.clone(), track_name.clone());

        // Hold lock for entire check-and-insert to prevent race conditions
        let mut tracks = self.tracks.lock().await;

        // Check if we already have this track
        if let Some(reader) = tracks.get(&key) {
            return Ok(Some(reader.clone()));
        }

        // Create a new track and subscribe
        let (writer, reader) = Track::new(namespace.clone(), track_name.clone()).produce();

        // Insert BEFORE spawning to prevent race with removal in the spawned task
        tracks.insert(key.clone(), reader.clone());

        // Drop lock before spawning async task
        drop(tracks);

        // Subscribe to the track on the remote
        let mut subscriber = self.subscriber.clone();
        let track_key = key;
        let tracks_clone = self.tracks.clone();
        let url = self.url.clone();

        tokio::spawn(async move {
            log::info!(
                "subscribing to remote track: {} - {}/{}",
                url,
                track_key.0,
                track_key.1
            );

            if let Err(err) = subscriber.subscribe(writer).await {
                log::warn!(
                    "failed subscribing to remote track: {} - {}/{} - {}",
                    url,
                    track_key.0,
                    track_key.1,
                    err
                );
                // NOTE(itzmanish): should we assume the connection is bad?
                // connected.store(false, Ordering::Release);
            }

            // Remove track from map when subscription ends
            tracks_clone.lock().await.remove(&track_key);
            log::debug!(
                "remote track subscription ended: {} - {}/{}",
                url,
                track_key.0,
                track_key.1
            );
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
