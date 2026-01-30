use std::collections::hash_map;
use std::collections::HashMap;

use std::sync::{Arc, Mutex};

use moq_transport::{
    coding::TrackNamespace,
    serve::{ServeError, TracksReader},
};

/// Registry of local tracks
#[derive(Clone)]
pub struct Locals {
    lookup: Arc<Mutex<HashMap<TrackNamespace, TracksReader>>>,
}

impl Default for Locals {
    fn default() -> Self {
        Self::new()
    }
}

/// Local tracks registry.
impl Locals {
    pub fn new() -> Self {
        Self {
            lookup: Default::default(),
        }
    }

    /// Register new local tracks.
    pub async fn register(&mut self, tracks: TracksReader) -> anyhow::Result<Registration> {
        let namespace = tracks.namespace.clone();

        // Insert the tracks(TracksReader) into the lookup table
        match self.lookup.lock().unwrap().entry(namespace.clone()) {
            hash_map::Entry::Vacant(entry) => entry.insert(tracks),
            hash_map::Entry::Occupied(_) => return Err(ServeError::Duplicate.into()),
        };

        let registration = Registration {
            locals: self.clone(),
            namespace,
        };

        Ok(registration)
    }

    /// Retrieve local tracks by namespace using hierarchical prefix matching.
    /// Returns the TracksReader for the longest matching namespace prefix.
    pub fn retrieve(&self, namespace: &TrackNamespace) -> Option<TracksReader> {
        let lookup = self.lookup.lock().unwrap();

        // Find the longest matching prefix
        let mut best_match: Option<TracksReader> = None;
        let mut best_len = 0;

        for (registered_ns, tracks) in lookup.iter() {
            // Check if registered_ns is a prefix of namespace
            if namespace.fields.len() >= registered_ns.fields.len() {
                let is_prefix = registered_ns
                    .fields
                    .iter()
                    .zip(namespace.fields.iter())
                    .all(|(a, b)| a == b);

                if is_prefix && registered_ns.fields.len() > best_len {
                    best_match = Some(tracks.clone());
                    best_len = registered_ns.fields.len();
                }
            }
        }

        best_match
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

/// Deregister local tracks on drop.
impl Drop for Registration {
    fn drop(&mut self) {
        self.locals.lookup.lock().unwrap().remove(&self.namespace);
    }
}
