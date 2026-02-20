use std::collections::hash_map;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::metrics::GaugeGuard;
use moq_transport::{
    coding::TrackNamespace,
    serve::{ServeError, Track, TrackReader, TrackWriter, TracksReader, TracksWriter},
};

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
            _gauge_guard: GaugeGuard::new("moq_relay_announced_namespaces"),
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

        let mut tracks = entry.tracks.lock().unwrap();

        let track_info = tracks
            .entry(track_name.to_string())
            .or_insert_with(|| Arc::new(TrackInfo::new(namespace.clone(), track_name.to_string())))
            .clone();

        Some(track_info)
    }

    pub fn get_track_info(
        &self,
        namespace: &TrackNamespace,
        track_name: &str,
    ) -> Option<Arc<TrackInfo>> {
        let lookup = self.lookup.lock().unwrap();

        let entry = Self::find_best_match_entry(&lookup, namespace)?;

        let tracks = entry.tracks.lock().unwrap();
        tracks.get(track_name).cloned()
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
    /// Gauge guard for tracking announced namespaces - decrements on drop
    _gauge_guard: GaugeGuard,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let ns = self.namespace.to_utf8_path();
        tracing::debug!(namespace = %ns, "deregistering namespace from locals");
        self.locals.lookup.lock().unwrap().remove(&self.namespace);
    }
}
