use std::collections::HashMap;

use std::collections::VecDeque;
use std::fmt;
use std::ops;
use std::sync::Arc;
use std::sync::Weak;

use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use moq_native_ietf::quic;
use moq_transport::coding::TrackNamespace;
use moq_transport::serve::{Track, TrackReader, TrackWriter};
use moq_transport::watch::State;
use url::Url;

use crate::control_plane::ControlPlane;

/// Information about remote origins.
pub struct Remotes<CP: ControlPlane> {
    /// The control plane we use to fetch/store origin information.
    pub control_plane: CP,

    // A QUIC endpoint we'll use to fetch from other origins.
    pub quic: quic::Client,
}

impl<CP: ControlPlane> Remotes<CP> {
    pub fn produce(self) -> (RemotesProducer<CP>, RemotesConsumer<CP>) {
        let (send, recv) = State::default().split();
        let info = Arc::new(self);

        let producer = RemotesProducer::new(info.clone(), send);
        let consumer = RemotesConsumer::new(info, recv);

        (producer, consumer)
    }
}

struct RemotesState<CP: ControlPlane> {
    lookup: HashMap<Url, RemoteConsumer<CP>>,
    requested: VecDeque<RemoteProducer<CP>>,
}

impl<CP: ControlPlane> Default for RemotesState<CP> {
    fn default() -> Self {
        Self {
            lookup: HashMap::default(),
            requested: VecDeque::default(),
        }
    }
}

// Clone for convenience, but there should only be one instance of this
#[derive(Clone)]
pub struct RemotesProducer<CP: ControlPlane> {
    info: Arc<Remotes<CP>>,
    state: State<RemotesState<CP>>,
}

impl<CP: ControlPlane> RemotesProducer<CP> {
    fn new(info: Arc<Remotes<CP>>, state: State<RemotesState<CP>>) -> Self {
        Self { info, state }
    }

    /// Block until the next remote requested by a consumer.
    async fn next(&mut self) -> Option<RemoteProducer<CP>> {
        loop {
            {
                let state = self.state.lock();
                if !state.requested.is_empty() {
                    return state.into_mut()?.requested.pop_front();
                }

                state.modified()?
            }
            .await;
        }
    }

    /// Run the remotes producer to serve remote requests.
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(mut remote) = self.next() => {
                    let url = remote.url.clone();

                    // Spawn a task to serve the remote
                    tasks.push(async move {
                        let info = remote.info.clone();

                        log::warn!("serving remote: {:?}", info);

                        // Run the remote producer
                        if let Err(err) = remote.run().await {
                            log::warn!("failed serving remote: {:?}, error: {}", info, err);
                        }

                        url
                    });
                }

                // Handle finished remote producers
                res = tasks.next(), if !tasks.is_empty() => {
                    let url = res.unwrap();

                    if let Some(mut state) = self.state.lock_mut() {
                        state.lookup.remove(&url);
                    }
                },
                else => return Ok(()),
            }
        }
    }
}

impl<CP: ControlPlane> ops::Deref for RemotesProducer<CP> {
    type Target = Remotes<CP>;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

#[derive(Clone)]
pub struct RemotesConsumer<CP: ControlPlane> {
    pub info: Arc<Remotes<CP>>,
    state: State<RemotesState<CP>>,
}

impl<CP: ControlPlane> RemotesConsumer<CP> {
    fn new(info: Arc<Remotes<CP>>, state: State<RemotesState<CP>>) -> Self {
        Self { info, state }
    }

    /// Route to a remote origin based on the namespace.
    pub async fn route(
        &self,
        namespace: &TrackNamespace,
    ) -> anyhow::Result<Option<RemoteConsumer<CP>>> {
        // Always fetch the origin instead of using the (potentially invalid) cache.
        let origin = match self.control_plane.get_origin(&namespace.to_utf8_path()).await? {
            None => return Ok(None),
            Some(origin) => origin,
        };

        // Check if we already have a remote for this origin
        let state = self.state.lock();
        if let Some(remote) = state.lookup.get(&origin.url).cloned() {
            return Ok(Some(remote));
        }

        // Create a new remote for this origin
        let mut state = match state.into_mut() {
            Some(state) => state,
            None => return Ok(None),
        };

        let remote = Remote {
            url: origin.url.clone(),
            remotes: self.info.clone(),
        };

        // Produce the remote
        let (writer, reader) = remote.produce();
        state.requested.push_back(writer);

        // Insert the remote into our Map
        state.lookup.insert(origin.url, reader.clone());

        Ok(Some(reader))
    }
}

impl<CP: ControlPlane> ops::Deref for RemotesConsumer<CP> {
    type Target = Remotes<CP>;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

pub struct Remote<CP: ControlPlane> {
    pub remotes: Arc<Remotes<CP>>,
    pub url: Url,
}

impl<CP: ControlPlane> fmt::Debug for Remote<CP> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Remote")
            .field("url", &self.url.to_string())
            .finish()
    }
}

impl<CP: ControlPlane> ops::Deref for Remote<CP> {
    type Target = Remotes<CP>;

    fn deref(&self) -> &Self::Target {
        &self.remotes
    }
}

impl<CP: ControlPlane> Remote<CP> {
    /// Create a new broadcast.
    pub fn produce(self) -> (RemoteProducer<CP>, RemoteConsumer<CP>) {
        let (send, recv) = State::default().split();
        let info = Arc::new(self);

        let consumer = RemoteConsumer::new(info.clone(), recv);
        let producer = RemoteProducer::new(info, send);

        (producer, consumer)
    }
}

#[derive(Default)]
struct RemoteState {
    tracks: HashMap<(TrackNamespace, String), RemoteTrackWeak>,
    requested: VecDeque<TrackWriter>,
}

pub struct RemoteProducer<CP: ControlPlane> {
    pub info: Arc<Remote<CP>>,
    state: State<RemoteState>,
}

impl<CP: ControlPlane> RemoteProducer<CP> {
    fn new(info: Arc<Remote<CP>>, state: State<RemoteState>) -> Self {
        Self { info, state }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        // TODO reuse QUIC and MoQ sessions
        let (session, _quic_client_initial_cid) = self.quic.connect(&self.url).await?;
        let (session, subscriber) = moq_transport::session::Subscriber::connect(session).await?;

        // Run the session
        let mut session = session.run().boxed();
        let mut tasks = FuturesUnordered::new();

        let mut done = None;

        // Serve requested tracks
        loop {
            tokio::select! {
                track = self.next(), if done.is_none() => {
                    let track = match track {
                        Ok(Some(track)) => track,
                        Ok(None) => { done = Some(Ok(())); continue },
                        Err(err) => { done = Some(Err(err)); continue },
                    };

                    let info = track.info.clone();
                    let mut subscriber = subscriber.clone();

                    tasks.push(async move {
                        if let Err(err) = subscriber.subscribe(track).await {
                            log::warn!("failed serving track: {:?}, error: {}", info, err);
                        }
                    });
                }
                _ = tasks.next(), if !tasks.is_empty() => {},

                // Keep running the session
                res = &mut session, if !tasks.is_empty() || done.is_none() => return Ok(res?),

                else => return done.unwrap(),
            }
        }
    }

    /// Block until the next track requested by a consumer.
    async fn next(&self) -> anyhow::Result<Option<TrackWriter>> {
        loop {
            let notify = {
                let state = self.state.lock();

                // Check if we have any requested tracks
                if !state.requested.is_empty() {
                    return Ok(state
                        .into_mut()
                        .and_then(|mut state| state.requested.pop_front()));
                }

                match state.modified() {
                    Some(notified) => notified,
                    None => return Ok(None),
                }
            };

            notify.await
        }
    }
}

impl<CP: ControlPlane> ops::Deref for RemoteProducer<CP> {
    type Target = Remote<CP>;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

#[derive(Clone)]
pub struct RemoteConsumer<CP: ControlPlane> {
    pub info: Arc<Remote<CP>>,
    state: State<RemoteState>,
}

impl<CP: ControlPlane> RemoteConsumer<CP> {
    fn new(info: Arc<Remote<CP>>, state: State<RemoteState>) -> Self {
        Self { info, state }
    }

    /// Request a track from the broadcast.
    pub fn subscribe(
        &self,
        namespace: TrackNamespace,
        name: String,
    ) -> anyhow::Result<Option<RemoteTrackReader>> {
        let key = (namespace.clone(), name.clone());
        let state = self.state.lock();
        if let Some(track) = state.tracks.get(&key) {
            if let Some(track) = track.upgrade() {
                return Ok(Some(track));
            }
        }

        let mut state = match state.into_mut() {
            Some(state) => state,
            None => return Ok(None),
        };

        let (writer, reader) = Track::new(namespace, name).produce();
        let reader = RemoteTrackReader::new(reader, self.state.clone());

        // Insert the track into our Map so we deduplicate future requests.
        state.tracks.insert(key, reader.downgrade());
        state.requested.push_back(writer);

        Ok(Some(reader))
    }
}

impl<CP: ControlPlane> ops::Deref for RemoteConsumer<CP> {
    type Target = Remote<CP>;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

#[derive(Clone)]
pub struct RemoteTrackReader {
    pub reader: TrackReader,
    drop: Arc<RemoteTrackDrop>,
}

impl RemoteTrackReader {
    fn new(reader: TrackReader, parent: State<RemoteState>) -> Self {
        let drop = Arc::new(RemoteTrackDrop {
            parent,
            key: (reader.namespace.clone(), reader.name.clone()),
        });

        Self { reader, drop }
    }

    fn downgrade(&self) -> RemoteTrackWeak {
        RemoteTrackWeak {
            reader: self.reader.clone(),
            drop: Arc::downgrade(&self.drop),
        }
    }
}

impl ops::Deref for RemoteTrackReader {
    type Target = TrackReader;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl ops::DerefMut for RemoteTrackReader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.reader
    }
}

struct RemoteTrackWeak {
    reader: TrackReader,
    drop: Weak<RemoteTrackDrop>,
}

impl RemoteTrackWeak {
    fn upgrade(&self) -> Option<RemoteTrackReader> {
        Some(RemoteTrackReader {
            reader: self.reader.clone(),
            drop: self.drop.upgrade()?,
        })
    }
}

struct RemoteTrackDrop {
    parent: State<RemoteState>,
    key: (TrackNamespace, String),
}

impl Drop for RemoteTrackDrop {
    fn drop(&mut self) {
        if let Some(mut parent) = self.parent.lock_mut() {
            parent.tracks.remove(&self.key);
        }
    }
}
