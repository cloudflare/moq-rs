//! File-based coordinator for multi-relay deployments.
//!
//! This coordinator uses a shared JSON file with file locking to coordinate
//! namespace registration across multiple relay instances. No separate
//! server process is required.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use async_trait::async_trait;
use fs2::FileExt;
use moq_native_ietf::quic::Client;
use moq_transport::coding::TrackNamespace;
use serde::{Deserialize, Serialize};
use url::Url;

use moq_relay_ietf::{
    Coordinator, CoordinatorError, CoordinatorResult, NamespaceOrigin, NamespaceRegistration,
};

/// Data stored in the shared file
#[derive(Debug, Default, Serialize, Deserialize)]
struct CoordinatorData {
    /// Maps namespace path (e.g., "/foo/bar") to relay URL
    namespaces: HashMap<String, String>,
}

impl CoordinatorData {
    fn namespace_key(namespace: &TrackNamespace) -> String {
        namespace.to_utf8_path()
    }
}

/// Handle that unregisters a namespace when dropped
struct NamespaceUnregisterHandle {
    namespace: TrackNamespace,
    file_path: PathBuf,
}

impl Drop for NamespaceUnregisterHandle {
    fn drop(&mut self) {
        if let Err(err) = unregister_namespace_sync(&self.file_path, &self.namespace) {
            log::warn!("failed to unregister namespace on drop: {}", err);
        }
    }
}

/// Synchronous helper for unregistering namespace (used in Drop)
fn unregister_namespace_sync(file_path: &Path, namespace: &TrackNamespace) -> Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(file_path)?;

    file.lock_exclusive()?;

    let mut data = read_data(&file)?;
    let key = CoordinatorData::namespace_key(namespace);

    log::debug!("unregistering namespace: {}", key);
    data.namespaces.remove(&key);

    write_data(&file, &data)?;
    file.unlock()?;

    Ok(())
}

/// Read coordinator data from file
fn read_data(file: &File) -> Result<CoordinatorData> {
    let mut file = file;
    file.seek(SeekFrom::Start(0))?;

    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    if contents.is_empty() {
        return Ok(CoordinatorData::default());
    }

    serde_json::from_str(&contents).context("failed to parse coordinator data")
}

/// Write coordinator data to file
fn write_data(file: &File, data: &CoordinatorData) -> Result<()> {
    let mut file = file;
    file.seek(SeekFrom::Start(0))?;
    file.set_len(0)?;

    let json = serde_json::to_string_pretty(data)?;
    file.write_all(json.as_bytes())?;
    file.flush()?;

    Ok(())
}

/// A coordinator that uses a shared file for state storage.
///
/// Multiple relay instances can use the same file to share namespace/track
/// registration data. File locking ensures safe concurrent access.
pub struct FileCoordinator {
    /// Path to the shared coordination file
    file_path: PathBuf,
    /// URL of this relay (used when registering namespaces)
    relay_url: Url,
}

impl FileCoordinator {
    /// Create a new file-based coordinator.
    ///
    /// # Arguments
    /// * `file_path` - Path to the shared coordination file
    /// * `relay_url` - URL of this relay instance (advertised to other relays)
    pub fn new(file_path: impl AsRef<Path>, relay_url: Url) -> Self {
        Self {
            file_path: file_path.as_ref().to_path_buf(),
            relay_url,
        }
    }
}

#[async_trait]
impl Coordinator for FileCoordinator {
    async fn register_namespace(
        &self,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<NamespaceRegistration> {
        let namespace = namespace.clone();
        let relay_url = self.relay_url.to_string();
        let file_path = self.file_path.clone();

        // Run blocking file I/O in a separate thread
        let ns_clone = namespace.clone();
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&file_path)?;

            file.lock_exclusive()?;

            let mut data = read_data(&file)?;
            let key = CoordinatorData::namespace_key(&ns_clone);

            log::info!("registering namespace: {} -> {}", key, relay_url);
            data.namespaces.insert(key, relay_url);

            write_data(&file, &data)?;
            file.unlock()?;

            Ok::<_, anyhow::Error>(())
        })
        .await??;

        let handle = NamespaceUnregisterHandle {
            namespace,
            file_path: self.file_path.clone(),
        };

        Ok(NamespaceRegistration::new(handle))
    }

    // FIXME(itzmanish): Not being called currently but we need to call this on publish_namespace_done
    // currently unregister happens on drop of namespace
    async fn unregister_namespace(&self, namespace: &TrackNamespace) -> CoordinatorResult<()> {
        let namespace = namespace.clone();
        let file_path = self.file_path.clone();

        tokio::task::spawn_blocking(move || unregister_namespace_sync(&file_path, &namespace))
            .await??;

        Ok(())
    }

    async fn lookup(
        &self,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<(NamespaceOrigin, Option<Client>)> {
        let namespace = namespace.clone();
        let file_path = self.file_path.clone();

        let result = tokio::task::spawn_blocking(
            move || -> Result<Option<(NamespaceOrigin, Option<Client>)>> {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&file_path)?;

                file.lock_shared()?;

                let data = read_data(&file)?;
                let key = CoordinatorData::namespace_key(&namespace);

                log::debug!("looking up namespace: {}", key);

                // Try exact match first
                if let Some(relay_url) = data.namespaces.get(&key) {
                    file.unlock()?;
                    let url = Url::parse(relay_url)?;
                    return Ok(Some((NamespaceOrigin::new(namespace, url, None), None)));
                }

                // Try prefix matching (find longest matching prefix)
                let mut best_match: Option<(String, String)> = None;
                for (registered_key, url) in &data.namespaces {
                    // FIXME(itzmanish): it would be much better to compare on TupleField
                    // instead of working on strings
                    let is_prefix = registered_key
                        .split('/')
                        .zip(key.split('/'))
                        .all(|(a, b)| a == b);
                    match best_match {
                        Some((ns, _)) if is_prefix && ns.len() < registered_key.len() => {
                            best_match = Some((registered_key.clone(), url.clone()));
                        }
                        None if is_prefix => {
                            best_match = Some((registered_key.clone(), url.clone()));
                        }
                        _ => {}
                    }
                }

                file.unlock()?;

                if let Some((matched_key, relay_url)) = best_match {
                    let matched_ns = TrackNamespace::from_utf8_path(&matched_key);
                    let url = Url::parse(&relay_url)?;
                    return Ok(Some((NamespaceOrigin::new(matched_ns, url, None), None)));
                }

                Ok(None)
            },
        )
        .await??;

        result.ok_or(CoordinatorError::NamespaceNotFound)
    }

    async fn shutdown(&self) -> CoordinatorResult<()> {
        // Nothing to clean up - file will be unlocked automatically
        Ok(())
    }
}
