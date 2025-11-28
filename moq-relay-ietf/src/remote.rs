use std::collections::HashMap;
use std::sync::Arc;

use moq_native_ietf::quic;
use moq_transport::coding::TrackNamespace;
use moq_transport::serve::{Track, TrackReader};
use tokio::sync::Mutex;
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
    tls: moq_native_ietf::tls::Config,
    remotes: Arc<Mutex<HashMap<Url, Remote>>>,
}

impl RemoteManager {
    /// Create a new RemoteManager.
    pub fn new(coordinator: Arc<dyn Coordinator>, tls: moq_native_ietf::tls::Config) -> Self {
        Self {
            coordinator,
            tls,
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
        namespace: TrackNamespace,
        track_name: String,
    ) -> anyhow::Result<Option<TrackReader>> {
        // Ask coordinator where this namespace lives
        let origin = match self.coordinator.lookup(&namespace).await {
            Ok(origin) => origin,
            Err(_) => return Ok(None), // Namespace not found anywhere
        };

        let url = origin.url();

        // Get or create a connection to the remote relay
        let remote = self.get_or_connect(&url).await?;

        // Subscribe to the track on the remote
        remote.subscribe(namespace, track_name).await
    }

    /// Get an existing remote connection or create a new one.
    async fn get_or_connect(&self, url: &Url) -> anyhow::Result<Remote> {
        let mut remotes = self.remotes.lock().await;

        // Check if we already have a connection
        if let Some(remote) = remotes.get(url) {
            if remote.is_connected() {
                return Ok(remote.clone());
            }
            // Connection is dead, remove it
            remotes.remove(url);
        }

        // Create a new connection with its own QUIC client
        log::info!("connecting to remote relay: {}", url);
        let remote = Remote::connect(url.clone(), self.tls.clone()).await?;

        remotes.insert(url.clone(), remote.clone());

        Ok(remote)
    }

    /// Remove a remote connection (called when connection fails).
    pub async fn remove(&self, url: &Url) {
        let mut remotes = self.remotes.lock().await;
        remotes.remove(url);
    }
}

/// A connection to a single remote relay with its own QUIC client.
#[derive(Clone)]
pub struct Remote {
    url: Url,
    subscriber: moq_transport::session::Subscriber,
    /// Track subscriptions - maps (namespace, track_name) to track reader
    tracks: Arc<Mutex<HashMap<(TrackNamespace, String), TrackReader>>>,
}

impl Remote {
    /// Connect to a remote relay with a dedicated QUIC client.
    async fn connect(url: Url, tls: moq_native_ietf::tls::Config) -> anyhow::Result<Self> {
        // Create a dedicated QUIC endpoint for this remote connection
        let bind_addr: std::net::SocketAddr = "[::]:0".parse()?;
        let quic_config = quic::Config::new(bind_addr, None, tls);
        let quic = quic::Endpoint::new(quic_config)?;

        // Connect to the remote relay (DNS resolution happens inside connect)
        let (session, _cid) = quic.client.connect(&url, None).await?;
        let (session, subscriber) = moq_transport::session::Subscriber::connect(session).await?;

        // Spawn a task to run the session
        let session_url = url.clone();
        tokio::spawn(async move {
            if let Err(err) = session.run().await {
                log::warn!("remote session closed: {} - {}", session_url, err);
            }
        });

        Ok(Self {
            url,
            subscriber,
            tracks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Check if the connection is still alive.
    /// Note: This is a simple heuristic - we assume connected until proven otherwise.
    fn is_connected(&self) -> bool {
        // We don't have a direct way to check if the subscriber is closed,
        // so we assume it's connected. Dead connections will be cleaned up
        // when subscribe operations fail.
        true
    }

    /// Subscribe to a track on this remote relay.
    pub async fn subscribe(
        &self,
        namespace: TrackNamespace,
        track_name: String,
    ) -> anyhow::Result<Option<TrackReader>> {
        let key = (namespace.clone(), track_name.clone());

        // Check if we already have this track
        {
            let tracks = self.tracks.lock().await;
            if let Some(reader) = tracks.get(&key) {
                return Ok(Some(reader.clone()));
            }
        }

        // Create a new track and subscribe
        let (writer, reader) = Track::new(namespace.clone(), track_name.clone()).produce();

        // Subscribe to the track on the remote
        let mut subscriber = self.subscriber.clone();
        let track_key = key.clone();
        let tracks = self.tracks.clone();
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
            }

            // Remove track from map when subscription ends
            tracks.lock().await.remove(&track_key);
        });

        // Store the reader for deduplication
        {
            let mut tracks = self.tracks.lock().await;
            tracks.insert(key, reader.clone());
        }

        Ok(Some(reader))
    }
}

impl std::fmt::Debug for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Remote")
            .field("url", &self.url.to_string())
            .finish()
    }
}
