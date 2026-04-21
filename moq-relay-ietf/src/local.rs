use std::collections::hash_map;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use moq_transport::{
    coding::TrackNamespace,
    serve::{ServeError, Track, TrackReader, TrackWriter, TracksReader, TracksWriter},
};
use tokio::sync::watch;

#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TrackState {
    Pending = 0,
    Subscribing = 1,
    Subscribed = 2,
    Publishing = 3,
    Closed = 4,
}

impl TrackState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => TrackState::Pending,
            1 => TrackState::Subscribing,
            2 => TrackState::Subscribed,
            3 => TrackState::Publishing,
            _ => TrackState::Closed,
        }
    }
}

pub struct TrackInfo {
    pub namespace: TrackNamespace,
    pub name: String,

    state: AtomicU8,
    track_reader: OnceLock<TrackReader>,
    track_writer: Mutex<Option<TrackWriter>>,
    upstream_subscribe_sent: AtomicBool,
    upstream_request_id: Mutex<Option<u64>>,

    /// The PUBLISH request ID (set when publisher sends PUBLISH)
    publish_request_id: Mutex<Option<u64>>,
    /// Forward state: true = forwarding, false = paused
    /// Publisher watches this to know when to start/stop sending
    forward_state: Mutex<Option<watch::Sender<bool>>>,
    /// Receiver side for forward state changes
    forward_receiver: Mutex<Option<watch::Receiver<bool>>>,
}

impl TrackInfo {
    pub fn new(namespace: TrackNamespace, name: String) -> Self {
        Self {
            namespace,
            name,
            state: AtomicU8::new(TrackState::Pending as u8),
            track_reader: OnceLock::new(),
            track_writer: Mutex::new(None),
            upstream_subscribe_sent: AtomicBool::new(false),
            upstream_request_id: Mutex::new(None),
            publish_request_id: Mutex::new(None),
            forward_state: Mutex::new(None),
            forward_receiver: Mutex::new(None),
        }
    }

    pub fn get_reader(&self) -> TrackReader {
        self.ensure_track_created();
        self.track_reader.get().unwrap().clone()
    }

    pub fn should_subscribe_upstream(&self) -> bool {
        let state = self.state();

        if state == TrackState::Publishing {
            return false;
        }

        !self.upstream_subscribe_sent.swap(true, Ordering::SeqCst)
    }

    pub fn mark_subscribe_sent(&self, request_id: u64) {
        *self.upstream_request_id.lock().unwrap() = Some(request_id);

        let _ = self.state.compare_exchange(
            TrackState::Pending as u8,
            TrackState::Subscribing as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub fn subscribe_ok_received(&self) {
        let _ = self.state.compare_exchange(
            TrackState::Subscribing as u8,
            TrackState::Subscribed as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub fn publish_arrived(&self) -> Result<TrackWriter, ServeError> {
        self.ensure_track_created();

        let current_state = self.state();

        if current_state == TrackState::Subscribed {
            return Err(ServeError::Uninterested);
        }

        if current_state == TrackState::Publishing {
            return Err(ServeError::Duplicate);
        }

        self.state
            .store(TrackState::Publishing as u8, Ordering::SeqCst);

        self.track_writer
            .lock()
            .unwrap()
            .take()
            .ok_or(ServeError::Duplicate)
    }

    pub fn state(&self) -> TrackState {
        TrackState::from_u8(self.state.load(Ordering::SeqCst))
    }

    pub fn is_publishing(&self) -> bool {
        self.state() == TrackState::Publishing
    }

    /// Set up forward state tracking when PUBLISH is received.
    /// Returns the initial forward value that was set.
    pub fn set_publish_info(&self, request_id: u64, initial_forward: bool) {
        *self.publish_request_id.lock().unwrap() = Some(request_id);

        let (tx, rx) = watch::channel(initial_forward);
        *self.forward_state.lock().unwrap() = Some(tx);
        *self.forward_receiver.lock().unwrap() = Some(rx);

        log::debug!(
            "set_publish_info: track {}/{} request_id={} initial_forward={}",
            self.namespace,
            self.name,
            request_id,
            initial_forward
        );
    }

    /// Get the PUBLISH request ID
    pub fn publish_request_id(&self) -> Option<u64> {
        *self.publish_request_id.lock().unwrap()
    }

    /// Get current forward state
    pub fn is_forwarding(&self) -> bool {
        self.forward_receiver
            .lock()
            .unwrap()
            .as_ref()
            .map(|rx| *rx.borrow())
            .unwrap_or(true) // Default to true if not set (legacy behavior)
    }

    /// Request forwarding to start (called when a subscriber arrives).
    /// Returns true if the state changed from false to true.
    pub fn request_forward(&self) -> bool {
        if let Some(tx) = self.forward_state.lock().unwrap().as_ref() {
            let current = *tx.borrow();
            if !current {
                let _ = tx.send(true);
                log::info!(
                    "request_forward: track {}/{} forward state changed 0 -> 1",
                    self.namespace,
                    self.name
                );
                return true;
            }
        }
        false
    }

    /// Get a receiver for forward state changes (for the publisher to watch)
    pub fn forward_receiver(&self) -> Option<watch::Receiver<bool>> {
        self.forward_receiver.lock().unwrap().clone()
    }

    pub fn take_writer_for_upstream(&self) -> Result<TrackWriter, ServeError> {
        self.ensure_track_created();

        let current_state = self.state();

        if current_state == TrackState::Publishing {
            return Err(ServeError::Duplicate);
        }

        if current_state == TrackState::Subscribing || current_state == TrackState::Subscribed {
            return Err(ServeError::Duplicate);
        }

        self.state
            .store(TrackState::Subscribing as u8, Ordering::SeqCst);

        self.track_writer
            .lock()
            .unwrap()
            .take()
            .ok_or(ServeError::Duplicate)
    }

    fn ensure_track_created(&self) {
        self.track_reader.get_or_init(|| {
            let (writer, reader) = Track::new(self.namespace.clone(), self.name.clone()).produce();
            *self.track_writer.lock().unwrap() = Some(writer);
            reader
        });
    }
}

struct LocalsEntry {
    /// reader and writer hold the readers and writers for a namespace
    reader: TracksReader,
    writer: TracksWriter,
    /// tracks holds the individual tracks for a namespace
    tracks: Mutex<HashMap<String, Arc<TrackInfo>>>,
}

/// Locals is a map of TrackNamespace to LocalsEntry
#[derive(Clone)]
pub struct Locals {
    lookup: Arc<Mutex<HashMap<TrackNamespace, LocalsEntry>>>,
}

impl Default for Locals {
    fn default() -> Self {
        Self::new()
    }
}

impl Locals {
    pub fn new() -> Self {
        Self {
            lookup: Default::default(),
        }
    }

    pub async fn register(
        &mut self,
        reader: TracksReader,
        writer: TracksWriter,
    ) -> anyhow::Result<Registration> {
        let namespace = reader.namespace.clone();

        match self.lookup.lock().unwrap().entry(namespace.clone()) {
            hash_map::Entry::Vacant(entry) => entry.insert(LocalsEntry {
                reader,
                writer,
                tracks: Mutex::new(HashMap::new()),
            }),
            hash_map::Entry::Occupied(_) => return Err(ServeError::Duplicate.into()),
        };

        let registration = Registration {
            locals: self.clone(),
            namespace,
        };

        Ok(registration)
    }

    pub fn retrieve(&self, namespace: &TrackNamespace) -> Option<TracksReader> {
        let lookup = self.lookup.lock().unwrap();

        let mut best_match: Option<TracksReader> = None;
        let mut best_len = 0;

        for (registered_ns, entry) in lookup.iter() {
            if namespace.fields.len() >= registered_ns.fields.len() {
                let is_prefix = registered_ns
                    .fields
                    .iter()
                    .zip(namespace.fields.iter())
                    .all(|(a, b)| a == b);

                if is_prefix && registered_ns.fields.len() > best_len {
                    best_match = Some(entry.reader.clone());
                    best_len = registered_ns.fields.len();
                }
            }
        }

        best_match
    }

    pub fn get_or_create_track_info(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
    ) -> Option<Arc<TrackInfo>> {
        let lookup = self.lookup.lock().unwrap();

        let entry = Self::find_best_match_entry(&lookup, namespace)?;

        // Use full namespace + track_name as key to avoid collisions
        let track_key = format!("{}:{}", namespace, track_name);

        let mut tracks = entry.tracks.lock().unwrap();

        let track_info = tracks
            .entry(track_key)
            .or_insert_with(|| Arc::new(TrackInfo::new(namespace.clone(), track_name.to_string())))
            .clone();

        Some(track_info)
    }

    /// Get or create track info, auto-registering the namespace if needed.
    /// This supports the SUBSCRIBE_NAMESPACE flow where PUBLISH can arrive
    /// without a prior PUBLISH_NAMESPACE.
    pub fn get_or_create_track_info_auto_register(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
    ) -> Arc<TrackInfo> {
        let mut lookup = self.lookup.lock().unwrap();

        // Use full namespace + track_name as key to avoid collisions
        // when different namespaces have the same track_name
        let track_key = format!("{}:{}", namespace, track_name);

        // First try to find an existing matching namespace entry
        if let Some(entry) = Self::find_best_match_entry(&lookup, namespace) {
            let mut tracks = entry.tracks.lock().unwrap();
            return tracks
                .entry(track_key.clone())
                .or_insert_with(|| {
                    Arc::new(TrackInfo::new(namespace.clone(), track_name.to_string()))
                })
                .clone();
        }

        // No matching namespace found - auto-register for SUBSCRIBE_NAMESPACE flow
        log::info!(
            "auto-registering namespace {} for PUBLISH (no prior PUBLISH_NAMESPACE)",
            namespace
        );

        let (writer, _request, reader) =
            moq_transport::serve::Tracks::new(namespace.clone()).produce();

        let entry = lookup.entry(namespace.clone()).or_insert(LocalsEntry {
            reader,
            writer,
            tracks: Mutex::new(HashMap::new()),
        });

        let mut tracks = entry.tracks.lock().unwrap();
        tracks
            .entry(track_key)
            .or_insert_with(|| Arc::new(TrackInfo::new(namespace.clone(), track_name.to_string())))
            .clone()
    }

    pub fn get_track_info(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
    ) -> Option<Arc<TrackInfo>> {
        let lookup = self.lookup.lock().unwrap();

        let entry = Self::find_best_match_entry(&lookup, namespace)?;

        // Use full namespace + track_name as key to match get_or_create_track_info
        let track_key = format!("{}:{}", namespace, track_name);
        let tracks = entry.tracks.lock().unwrap();
        tracks.get(&track_key).cloned()
    }

    fn find_best_match_entry<'a>(
        lookup: &'a HashMap<TrackNamespace, LocalsEntry>,
        namespace: &TrackNamespace,
    ) -> Option<&'a LocalsEntry> {
        let mut best_match: Option<&LocalsEntry> = None;
        let mut best_len = 0;

        for (registered_ns, entry) in lookup.iter() {
            if namespace.fields.len() >= registered_ns.fields.len() {
                let is_prefix = registered_ns
                    .fields
                    .iter()
                    .zip(namespace.fields.iter())
                    .all(|(a, b)| a == b);

                if is_prefix && registered_ns.fields.len() > best_len {
                    best_match = Some(entry);
                    best_len = registered_ns.fields.len();
                }
            }
        }

        best_match
    }

    pub fn insert_track(
        &self,
        namespace: &TrackNamespace,
        track_reader: TrackReader,
    ) -> Option<()> {
        let mut lookup = self.lookup.lock().unwrap();

        if let Some(entry) = lookup.get_mut(namespace) {
            entry.writer.insert(track_reader)
        } else {
            None
        }
    }

    pub fn subscribe_upstream(&self, track_info: Arc<TrackInfo>) -> Option<TrackReader> {
        let mut lookup = self.lookup.lock().unwrap();

        let entry = lookup.get_mut(&track_info.namespace)?;

        let writer = track_info.take_writer_for_upstream().ok()?;
        let reader = track_info.get_reader();

        entry.reader.forward_upstream(writer)?;

        let namespace = track_info.namespace.clone();

        let entry_mut = lookup
            .iter_mut()
            .find(|(ns, _)| {
                namespace.fields.len() >= ns.fields.len()
                    && ns
                        .fields
                        .iter()
                        .zip(namespace.fields.iter())
                        .all(|(a, b)| a == b)
            })
            .map(|(_, e)| e)?;

        entry_mut.writer.insert(reader.clone());

        Some(reader)
    }

    pub fn matching_namespaces(&self, prefix: &TrackNamespace) -> Vec<TrackNamespace> {
        let lookup = self.lookup.lock().unwrap();

        lookup
            .keys()
            .filter(|ns| {
                if ns.fields.len() >= prefix.fields.len() {
                    prefix
                        .fields
                        .iter()
                        .zip(ns.fields.iter())
                        .all(|(a, b)| a == b)
                } else {
                    false
                }
            })
            .cloned()
            .collect()
    }
}

pub struct Registration {
    locals: Locals,
    namespace: TrackNamespace,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.locals.lookup.lock().unwrap().remove(&self.namespace);
    }
}
