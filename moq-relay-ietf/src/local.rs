use std::collections::hash_map;
use std::collections::HashMap;

use std::sync::{Arc, Mutex};

use moq_transport::{
    coding::TrackNamespace,
    serve::{ServeError, TracksReader},
};

use crate::metrics::GaugeGuard;

/// Registry of local tracks
#[derive(Clone)]
pub struct Locals {
    lookup: Arc<Mutex<HashMap<(Option<String>, TrackNamespace), TracksReader>>>,
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
    pub async fn register(
        &mut self,
        connection_path: Option<&str>,
        tracks: TracksReader,
    ) -> anyhow::Result<Registration> {
        let namespace = tracks.namespace.clone();
        let scope_key = connection_path.map(|path| path.to_string());
        let lookup_key = (scope_key.clone(), namespace.clone());

        // Insert the tracks(TracksReader) into the lookup table
        match self.lookup.lock().unwrap().entry(lookup_key) {
            hash_map::Entry::Vacant(entry) => entry.insert(tracks),
            hash_map::Entry::Occupied(_) => return Err(ServeError::Duplicate.into()),
        };

        let registration = Registration {
            locals: self.clone(),
            scope_key,
            namespace,
            _gauge_guard: GaugeGuard::new("moq_relay_announced_namespaces"),
        };

        Ok(registration)
    }

    /// Retrieve local tracks by namespace using hierarchical prefix matching.
    /// Returns the TracksReader for the longest matching namespace prefix.
    pub fn retrieve(
        &self,
        connection_path: Option<&str>,
        namespace: &TrackNamespace,
    ) -> Option<TracksReader> {
        let lookup = self.lookup.lock().unwrap();
        let scope_key = connection_path.map(|path| path.to_string());

        // Find the longest matching prefix
        let mut best_match: Option<TracksReader> = None;
        let mut best_len = 0;

        for ((registered_scope, registered_ns), tracks) in lookup.iter() {
            if registered_scope.as_deref() != scope_key.as_deref() {
                continue;
            }
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
}

pub struct Registration {
    locals: Locals,
    scope_key: Option<String>,
    namespace: TrackNamespace,
    /// Gauge guard for tracking announced namespaces - decrements on drop
    _gauge_guard: GaugeGuard,
}

/// Deregister local tracks on drop.
impl Drop for Registration {
    fn drop(&mut self) {
        let ns = self.namespace.to_utf8_path();
        let scope = self.scope_key.as_deref().unwrap_or("<default>");
        tracing::debug!(namespace = %ns, scope = %scope, "deregistering namespace from locals");
        self.locals
            .lookup
            .lock()
            .unwrap()
            .remove(&(self.scope_key.clone(), self.namespace.clone()));
    }
}
