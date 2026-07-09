// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! File-based coordinator for multi-relay deployments.
//!
//! This coordinator uses a shared JSON file with file locking to coordinate
//! namespace registration across multiple relay instances. No separate
//! server process is required.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use async_trait::async_trait;
use fs2::FileExt;
use moq_native_ietf::quic::Client;
use moq_transport::coding::{TrackNamespace, TrackNamespacePrefix, TupleField};
use serde::{Deserialize, Serialize};
use url::Url;

use moq_relay_ietf::{
    Coordinator, CoordinatorError, CoordinatorResult, NamespaceInfo, NamespaceOrigin,
    NamespaceRegistration, NamespaceSubscription, RelayInfo, TrackRegistration,
};

/// Data stored in the shared file
#[derive(Debug, Default, Serialize, Deserialize)]
struct CoordinatorData {
    /// Maps connection scope to namespace map
    #[serde(default)]
    namespaces: HashMap<String, HashMap<String, String>>,
    /// Maps connection scope to exact full-track map.
    /// Key format: `<namespace utf8 byte len>:<namespace utf8 path><track utf8/lossy>`.
    #[serde(default)]
    tracks: HashMap<String, HashMap<String, String>>,
    /// Maps connection scope to namespace-prefix interest map.
    /// Inner map: prefix key -> relay URL -> reference count.
    #[serde(default)]
    namespace_subscribers: HashMap<String, HashMap<String, HashMap<String, u64>>>,
}

impl CoordinatorData {
    fn scope_key(scope: Option<&str>) -> String {
        scope.unwrap_or("").to_string()
    }

    fn namespace_key(namespace: &TrackNamespace) -> String {
        tuple_key(&namespace.fields)
    }

    fn prefix_key(prefix: &TrackNamespacePrefix) -> String {
        tuple_key(&prefix.fields)
    }

    fn track_key(namespace: &TrackNamespace, track: &str) -> String {
        let namespace = namespace.to_utf8_path();
        format!("{}:{}{}", namespace.len(), namespace, track)
    }
}

/// Handle that unregisters a namespace when dropped
struct NamespaceUnregisterHandle {
    scope_key: String,
    namespace_key: String,
    file_path: PathBuf,
}

/// Handle that unregisters a track when dropped.
struct TrackUnregisterHandle {
    scope_key: String,
    track_key: String,
    file_path: PathBuf,
}

/// Handle that unregisters namespace interest when dropped.
struct NamespaceSubscriptionHandle {
    scope_key: String,
    prefix_key: String,
    relay_url: String,
    file_path: PathBuf,
}

impl Drop for NamespaceSubscriptionHandle {
    fn drop(&mut self) {
        if let Err(err) = unregister_namespace_subscription_sync(
            &self.file_path,
            &self.scope_key,
            &self.prefix_key,
            &self.relay_url,
        ) {
            tracing::warn!(prefix = %self.prefix_key, relay_url = %self.relay_url, error = %err, "failed to unregister namespace subscription on drop: {}", err);
        }
    }
}

impl Drop for TrackUnregisterHandle {
    fn drop(&mut self) {
        if let Err(err) = unregister_track_sync(&self.file_path, &self.scope_key, &self.track_key) {
            tracing::warn!(track = %self.track_key, error = %err, "failed to unregister track on drop: {}", err);
        }
    }
}

impl Drop for NamespaceUnregisterHandle {
    fn drop(&mut self) {
        if let Err(err) =
            unregister_namespace_sync(&self.file_path, &self.scope_key, &self.namespace_key)
        {
            tracing::warn!(namespace = %self.namespace_key, error = %err, "failed to unregister namespace on drop: {}", err);
        }
    }
}

/// Synchronous helper for unregistering namespace (used in Drop)
fn unregister_namespace_sync(file_path: &Path, scope_key: &str, namespace_key: &str) -> Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(file_path)?;

    file.lock_exclusive()?;

    let mut data = read_data(&file)?;
    tracing::debug!(namespace = %namespace_key, scope = %scope_key, "unregistering namespace: {}", namespace_key);
    if let Some(bucket) = data.namespaces.get_mut(scope_key) {
        bucket.remove(namespace_key);
        if bucket.is_empty() {
            data.namespaces.remove(scope_key);
        }
    }

    write_data(&file, &data)?;
    file.unlock()?;

    Ok(())
}

fn unregister_track_sync(file_path: &Path, scope_key: &str, track_key: &str) -> Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(file_path)?;

    file.lock_exclusive()?;

    let mut data = read_data(&file)?;
    tracing::debug!(track = %track_key, scope = %scope_key, "unregistering track: {}", track_key);
    if let Some(bucket) = data.tracks.get_mut(scope_key) {
        bucket.remove(track_key);
        if bucket.is_empty() {
            data.tracks.remove(scope_key);
        }
    }

    write_data(&file, &data)?;
    file.unlock()?;

    Ok(())
}

fn unregister_namespace_subscription_sync(
    file_path: &Path,
    scope_key: &str,
    prefix_key: &str,
    relay_url: &str,
) -> Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(file_path)?;

    file.lock_exclusive()?;

    let mut data = read_data(&file)?;
    if let Some(scope_bucket) = data.namespace_subscribers.get_mut(scope_key) {
        if let Some(prefix_bucket) = scope_bucket.get_mut(prefix_key) {
            match prefix_bucket.get_mut(relay_url) {
                Some(count) if *count > 1 => *count -= 1,
                Some(_) => {
                    prefix_bucket.remove(relay_url);
                }
                None => {}
            }

            if prefix_bucket.is_empty() {
                scope_bucket.remove(prefix_key);
            }
        }

        if scope_bucket.is_empty() {
            data.namespace_subscribers.remove(scope_key);
        }
    }

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

    let data: CoordinatorData =
        serde_json::from_str(&contents).context("failed to parse coordinator data")?;
    Ok(data)
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

fn namespace_key_has_prefix(namespace_key: &str, prefix_key: &str) -> bool {
    let namespace_fields = key_fields(namespace_key);
    let prefix_fields = key_fields(prefix_key);
    prefix_fields.len() <= namespace_fields.len()
        && prefix_fields
            .iter()
            .zip(namespace_fields.iter())
            .all(|(prefix, namespace)| prefix == namespace)
}

fn tuple_key(fields: &[TupleField]) -> String {
    fields
        .iter()
        .map(|field| hex::encode(&field.value))
        .collect::<Vec<_>>()
        .join(".")
}

fn key_fields(key: &str) -> Vec<&str> {
    if key.is_empty() {
        Vec::new()
    } else {
        key.split('.').collect()
    }
}

fn key_field_count(key: &str) -> usize {
    key_fields(key).len()
}

fn decode_namespace_key(key: &str) -> Result<TrackNamespace> {
    let fields = key_fields(key)
        .into_iter()
        .map(|field| {
            hex::decode(field)
                .map(|value| TupleField { value })
                .context("failed to decode namespace key field")
        })
        .collect::<Result<Vec<_>>>()?;

    TrackNamespace::try_from(fields).context("failed to decode namespace key")
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
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<NamespaceRegistration> {
        let scope_key = CoordinatorData::scope_key(scope);
        let namespace_key = CoordinatorData::namespace_key(namespace);
        let relay_url = self.relay_url.to_string();
        let file_path = self.file_path.clone();

        // Run blocking file I/O in a separate thread
        let scope_clone = scope_key.clone();
        let key_clone = namespace_key.clone();
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&file_path)?;

            file.lock_exclusive()?;

            let mut data = read_data(&file)?;
            tracing::info!(namespace = %key_clone, scope = %scope_clone, relay_url = %relay_url, "registering namespace: {} -> {}", key_clone, relay_url);
            data
                .namespaces
                .entry(scope_clone)
                .or_default()
                .insert(key_clone, relay_url);

            write_data(&file, &data)?;
            file.unlock()?;

            Ok::<_, anyhow::Error>(())
        })
        .await??;

        let handle = NamespaceUnregisterHandle {
            scope_key,
            namespace_key,
            file_path: self.file_path.clone(),
        };

        Ok(NamespaceRegistration::new(handle))
    }

    // FIXME(itzmanish): Not being called currently but we need to call this on publish_namespace_done
    // currently unregister happens on drop of namespace
    async fn unregister_namespace(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<()> {
        let scope_key = CoordinatorData::scope_key(scope);
        let namespace_key = CoordinatorData::namespace_key(namespace);
        let file_path = self.file_path.clone();

        tokio::task::spawn_blocking(move || {
            unregister_namespace_sync(&file_path, &scope_key, &namespace_key)
        })
        .await??;

        Ok(())
    }

    async fn lookup(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<(NamespaceOrigin, Option<Client>)> {
        let namespace = namespace.clone();
        let scope_key = CoordinatorData::scope_key(scope);
        let namespace_key = CoordinatorData::namespace_key(&namespace);
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
                tracing::debug!(namespace = %namespace_key, scope = %scope_key, "looking up namespace: {}", namespace_key);

                let Some(bucket) = data.namespaces.get(&scope_key) else {
                    file.unlock()?;
                    return Ok(None);
                };

                // Try exact match first
                if let Some(relay_url) = bucket.get(&namespace_key) {
                    file.unlock()?;
                    let url = Url::parse(relay_url)?;
                    return Ok(Some((NamespaceOrigin::new(namespace, url, None), None)));
                }

                // Try prefix matching (find longest matching prefix)
                let mut best_match: Option<(String, String)> = None;
                for (registered_key, url) in bucket {
                    let is_prefix = namespace_key_has_prefix(&namespace_key, registered_key);
                    match best_match {
                        Some((ns, _))
                            if is_prefix && key_field_count(&ns) < key_field_count(registered_key) =>
                        {
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
                    let matched_ns = decode_namespace_key(&matched_key)?;
                    let url = Url::parse(&relay_url)?;
                    return Ok(Some((NamespaceOrigin::new(matched_ns, url, None), None)));
                }

                Ok(None)
            },
        )
        .await??;

        result.ok_or(CoordinatorError::NamespaceNotFound)
    }

    async fn subscribe_namespace(
        &self,
        scope: Option<&str>,
        prefix: &TrackNamespacePrefix,
    ) -> CoordinatorResult<NamespaceSubscription> {
        let scope_key = CoordinatorData::scope_key(scope);
        let prefix_key = CoordinatorData::prefix_key(prefix);
        let relay_url = self.relay_url.to_string();
        let file_path = self.file_path.clone();

        let existing = tokio::task::spawn_blocking({
            let scope_key = scope_key.clone();
            let prefix_key = prefix_key.clone();
            let relay_url = relay_url.clone();
            move || -> Result<Vec<NamespaceInfo>> {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&file_path)?;

                file.lock_exclusive()?;

                let mut data = read_data(&file)?;
                let existing = data
                    .namespaces
                    .get(&scope_key)
                    .into_iter()
                    .flat_map(|bucket| bucket.keys())
                    .filter(|namespace_key| namespace_key_has_prefix(namespace_key, &prefix_key))
                    .map(|namespace_key| {
                        decode_namespace_key(namespace_key).map(NamespaceInfo::new)
                    })
                    .collect::<Result<Vec<_>>>()?;

                let count = data
                    .namespace_subscribers
                    .entry(scope_key)
                    .or_default()
                    .entry(prefix_key)
                    .or_default()
                    .entry(relay_url)
                    .or_default();
                *count = count.saturating_add(1);

                write_data(&file, &data)?;

                file.unlock()?;

                Ok(existing)
            }
        })
        .await??;

        Ok(NamespaceSubscription::new(
            existing,
            NamespaceSubscriptionHandle {
                scope_key,
                prefix_key,
                relay_url,
                file_path: self.file_path.clone(),
            },
        ))
    }

    async fn lookup_namespace_subscribers(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<Vec<RelayInfo>> {
        let scope_key = CoordinatorData::scope_key(scope);
        let namespace_key = CoordinatorData::namespace_key(namespace);
        let file_path = self.file_path.clone();

        let subscribers = tokio::task::spawn_blocking(move || -> Result<Vec<RelayInfo>> {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&file_path)?;

            file.lock_shared()?;

            let data = read_data(&file)?;
            let mut urls = HashSet::new();
            let mut subscribers = Vec::new();
            if let Some(bucket) = data.namespace_subscribers.get(&scope_key) {
                for (prefix_key, relays) in bucket {
                    if !namespace_key_has_prefix(&namespace_key, prefix_key) {
                        continue;
                    }

                    for relay_url in relays.keys() {
                        if urls.insert(relay_url.clone()) {
                            subscribers.push(RelayInfo::new(Url::parse(relay_url)?));
                        }
                    }
                }
            }

            file.unlock()?;

            Ok(subscribers)
        })
        .await??;

        Ok(subscribers)
    }

    async fn register_track(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
        track: &str,
    ) -> CoordinatorResult<TrackRegistration> {
        let scope_key = CoordinatorData::scope_key(scope);
        let track_key = CoordinatorData::track_key(namespace, track);
        let relay_url = self.relay_url.to_string();
        let file_path = self.file_path.clone();

        let scope_clone = scope_key.clone();
        let key_clone = track_key.clone();
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&file_path)?;

            file.lock_exclusive()?;

            let mut data = read_data(&file)?;
            tracing::info!(track = %key_clone, scope = %scope_clone, relay_url = %relay_url, "registering track: {} -> {}", key_clone, relay_url);
            data.tracks
                .entry(scope_clone)
                .or_default()
                .insert(key_clone, relay_url);

            write_data(&file, &data)?;
            file.unlock()?;

            Ok::<_, anyhow::Error>(())
        })
        .await??;

        let handle = TrackUnregisterHandle {
            scope_key,
            track_key,
            file_path: self.file_path.clone(),
        };

        Ok(TrackRegistration::new(handle))
    }

    async fn unregister_track(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
        track: &str,
    ) -> CoordinatorResult<()> {
        let scope_key = CoordinatorData::scope_key(scope);
        let track_key = CoordinatorData::track_key(namespace, track);
        let file_path = self.file_path.clone();

        tokio::task::spawn_blocking(move || {
            unregister_track_sync(&file_path, &scope_key, &track_key)
        })
        .await??;

        Ok(())
    }

    async fn lookup_track(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
        track: &str,
    ) -> CoordinatorResult<(NamespaceOrigin, Option<Client>)> {
        let namespace = namespace.clone();
        let scope_key = CoordinatorData::scope_key(scope);
        let namespace_key = CoordinatorData::namespace_key(&namespace);
        let track_key = CoordinatorData::track_key(&namespace, track);
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
                tracing::debug!(track = %track_key, scope = %scope_key, "looking up track: {}", track_key);

                if let Some(relay_url) = data
                    .tracks
                    .get(&scope_key)
                    .and_then(|bucket| bucket.get(&track_key))
                {
                    let url = Url::parse(relay_url)?;
                    file.unlock()?;
                    return Ok(Some((NamespaceOrigin::new(namespace.clone(), url, None), None)));
                }

                let Some(bucket) = data.namespaces.get(&scope_key) else {
                    file.unlock()?;
                    return Ok(None);
                };

                if let Some(relay_url) = bucket.get(&namespace_key) {
                    let url = Url::parse(relay_url)?;
                    file.unlock()?;
                    return Ok(Some((NamespaceOrigin::new(namespace.clone(), url, None), None)));
                }

                let mut best_match: Option<(String, String)> = None;
                for (registered_key, url) in bucket {
                    let is_prefix = namespace_key_has_prefix(&namespace_key, registered_key);
                    match best_match {
                        Some((ns, _))
                            if is_prefix && key_field_count(&ns) < key_field_count(registered_key) =>
                        {
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
                    let matched_ns = decode_namespace_key(&matched_key)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_file_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("moq-file-coordinator-{name}-{nanos}.json"))
    }

    fn prefix(path: &str) -> TrackNamespacePrefix {
        TrackNamespacePrefix::from_utf8_path(path)
    }

    fn namespace_from_fields(fields: &[&str]) -> TrackNamespace {
        TrackNamespace::try_from(
            fields
                .iter()
                .map(|field| TupleField::from_utf8(field))
                .collect::<Vec<_>>(),
        )
        .expect("test namespace should be valid")
    }

    #[tokio::test]
    async fn lookup_track_finds_registered_track() {
        let file = temp_file_path("track-lookup");
        let relay_url = Url::parse("https://relay.example.com").unwrap();
        let coordinator = FileCoordinator::new(&file, relay_url.clone());
        let namespace = TrackNamespace::from_utf8_path("room/123");

        let _track = coordinator
            .register_track(Some("scope-a"), &namespace, "audio")
            .await
            .expect("track should register");

        let (origin, client) = coordinator
            .lookup_track(Some("scope-a"), &namespace, "audio")
            .await
            .expect("track lookup should find registration");

        assert_eq!(origin.namespace(), &namespace);
        assert_eq!(origin.url(), relay_url);
        assert!(client.is_none());

        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn lookup_track_distinguishes_separator_text_in_namespace_and_track() {
        let file = temp_file_path("track-key-collision");
        let relay_a = Url::parse("https://relay-a.example.com").unwrap();
        let relay_b = Url::parse("https://relay-b.example.com").unwrap();
        let coordinator_a = FileCoordinator::new(&file, relay_a.clone());
        let coordinator_b = FileCoordinator::new(&file, relay_b.clone());
        let namespace_a = TrackNamespace::from_utf8_path("room--audio");
        let namespace_b = TrackNamespace::from_utf8_path("room");

        let _track_a = coordinator_a
            .register_track(Some("scope-a"), &namespace_a, "main")
            .await
            .expect("first track should register");
        let _track_b = coordinator_b
            .register_track(Some("scope-a"), &namespace_b, "audio--main")
            .await
            .expect("second track should register");

        let (origin_a, _) = coordinator_a
            .lookup_track(Some("scope-a"), &namespace_a, "main")
            .await
            .expect("first track lookup should find original relay");
        let (origin_b, _) = coordinator_a
            .lookup_track(Some("scope-a"), &namespace_b, "audio--main")
            .await
            .expect("second track lookup should find original relay");

        assert_eq!(origin_a.url(), relay_a);
        assert_eq!(origin_b.url(), relay_b);

        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn lookup_track_falls_back_to_namespace() {
        let file = temp_file_path("track-namespace-fallback");
        let relay_url = Url::parse("https://relay.example.com").unwrap();
        let coordinator = FileCoordinator::new(&file, relay_url.clone());
        let namespace = TrackNamespace::from_utf8_path("room/123");

        let _namespace = coordinator
            .register_namespace(Some("scope-a"), &namespace)
            .await
            .expect("namespace should register");

        let (origin, client) = coordinator
            .lookup_track(Some("scope-a"), &namespace, "audio")
            .await
            .expect("track lookup should fall back to namespace");

        assert_eq!(origin.namespace(), &namespace);
        assert_eq!(origin.url(), relay_url);
        assert!(client.is_none());

        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn subscribe_namespace_returns_existing_matches() {
        let file = temp_file_path("subscribe-namespace-existing");
        let relay_url = Url::parse("https://relay.example.com").unwrap();
        let coordinator = FileCoordinator::new(&file, relay_url);

        let _room = coordinator
            .register_namespace(Some("scope-a"), &TrackNamespace::from_utf8_path("room/123"))
            .await
            .expect("room should register");
        let _camera = coordinator
            .register_namespace(
                Some("scope-a"),
                &TrackNamespace::from_utf8_path("room/123/camera"),
            )
            .await
            .expect("camera should register");
        let _other = coordinator
            .register_namespace(
                Some("scope-a"),
                &TrackNamespace::from_utf8_path("other/123"),
            )
            .await
            .expect("other should register");

        let subscription = coordinator
            .subscribe_namespace(Some("scope-a"), &prefix("room/123"))
            .await
            .expect("namespace subscription should succeed");
        let mut matches: Vec<_> = subscription
            .existing_namespaces
            .into_iter()
            .map(|info| info.namespace.to_utf8_path())
            .collect();
        matches.sort();

        assert_eq!(matches, vec!["/room/123", "/room/123/camera"]);

        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn subscribe_namespace_keeps_scopes_isolated() {
        let file = temp_file_path("subscribe-namespace-scopes");
        let relay_url = Url::parse("https://relay.example.com").unwrap();
        let coordinator = FileCoordinator::new(&file, relay_url);

        let _global = coordinator
            .register_namespace(None, &TrackNamespace::from_utf8_path("room/global"))
            .await
            .expect("global should register");
        let _scoped = coordinator
            .register_namespace(
                Some("scope-a"),
                &TrackNamespace::from_utf8_path("room/scoped"),
            )
            .await
            .expect("scoped should register");

        let global = coordinator
            .subscribe_namespace(None, &prefix("room"))
            .await
            .expect("global namespace subscription should succeed");
        assert_eq!(global.existing_namespaces.len(), 1);
        assert_eq!(
            global.existing_namespaces[0].namespace,
            TrackNamespace::from_utf8_path("room/global")
        );

        let scoped = coordinator
            .subscribe_namespace(Some("scope-a"), &prefix("room"))
            .await
            .expect("scoped namespace subscription should succeed");
        assert_eq!(scoped.existing_namespaces.len(), 1);
        assert_eq!(
            scoped.existing_namespaces[0].namespace,
            TrackNamespace::from_utf8_path("room/scoped")
        );

        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn lookup_namespace_subscribers_finds_matching_prefixes() {
        let file = temp_file_path("namespace-subscribers");
        let relay_url = Url::parse("https://relay.example.com").unwrap();
        let coordinator = FileCoordinator::new(&file, relay_url.clone());

        let _subscription = coordinator
            .subscribe_namespace(Some("scope-a"), &prefix("room/123"))
            .await
            .expect("namespace subscription should register");

        let subscribers = coordinator
            .lookup_namespace_subscribers(
                Some("scope-a"),
                &TrackNamespace::from_utf8_path("room/123/camera"),
            )
            .await
            .expect("subscriber lookup should succeed");

        assert_eq!(subscribers.len(), 1);
        assert_eq!(subscribers[0].url, relay_url);

        let no_match = coordinator
            .lookup_namespace_subscribers(
                Some("scope-a"),
                &TrackNamespace::from_utf8_path("room/456"),
            )
            .await
            .expect("subscriber lookup should succeed");
        assert!(no_match.is_empty());

        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn namespace_subscription_drop_unregisters_with_refcount() {
        let file = temp_file_path("namespace-subscriber-refcount");
        let relay_url = Url::parse("https://relay.example.com").unwrap();
        let coordinator = FileCoordinator::new(&file, relay_url.clone());
        let namespace_prefix = prefix("room/123");
        let namespace = TrackNamespace::from_utf8_path("room/123/camera");

        let first = coordinator
            .subscribe_namespace(Some("scope-a"), &namespace_prefix)
            .await
            .expect("first subscription should register");
        let second = coordinator
            .subscribe_namespace(Some("scope-a"), &namespace_prefix)
            .await
            .expect("second subscription should register");

        drop(first);

        let subscribers = coordinator
            .lookup_namespace_subscribers(Some("scope-a"), &namespace)
            .await
            .expect("subscriber lookup should succeed");
        assert_eq!(subscribers.len(), 1);
        assert_eq!(subscribers[0].url, relay_url);

        drop(second);

        let subscribers = coordinator
            .lookup_namespace_subscribers(Some("scope-a"), &namespace)
            .await
            .expect("subscriber lookup should succeed");
        assert!(subscribers.is_empty());

        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn subscribe_namespace_accepts_empty_prefix() {
        let file = temp_file_path("subscribe-namespace-empty-prefix");
        let relay_url = Url::parse("https://relay.example.com").unwrap();
        let coordinator = FileCoordinator::new(&file, relay_url.clone());

        let _room = coordinator
            .register_namespace(Some("scope-a"), &TrackNamespace::from_utf8_path("room/123"))
            .await
            .expect("room should register");
        let _other = coordinator
            .register_namespace(
                Some("scope-a"),
                &TrackNamespace::from_utf8_path("other/123"),
            )
            .await
            .expect("other should register");

        let subscription = coordinator
            .subscribe_namespace(Some("scope-a"), &TrackNamespacePrefix::new())
            .await
            .expect("empty prefix namespace subscription should succeed");
        let mut matches: Vec<_> = subscription
            .existing_namespaces
            .into_iter()
            .map(|info| info.namespace.to_utf8_path())
            .collect();
        matches.sort();

        assert_eq!(matches, vec!["/other/123", "/room/123"]);

        let subscribers = coordinator
            .lookup_namespace_subscribers(
                Some("scope-a"),
                &TrackNamespace::from_utf8_path("room/123/camera"),
            )
            .await
            .expect("subscriber lookup should succeed");

        assert_eq!(subscribers.len(), 1);
        assert_eq!(subscribers[0].url, relay_url);

        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn subscribe_namespace_matches_tuple_fields_not_slash_strings() {
        let file = temp_file_path("subscribe-namespace-tuple-prefix");
        let relay_url = Url::parse("https://relay.example.com").unwrap();
        let coordinator = FileCoordinator::new(&file, relay_url);

        let single_field_namespace = namespace_from_fields(&["room/123"]);
        let two_field_namespace = namespace_from_fields(&["room", "123"]);

        let _single = coordinator
            .register_namespace(Some("scope-a"), &single_field_namespace)
            .await
            .expect("single-field namespace should register");
        let _two = coordinator
            .register_namespace(Some("scope-a"), &two_field_namespace)
            .await
            .expect("two-field namespace should register");

        let subscription = coordinator
            .subscribe_namespace(Some("scope-a"), &prefix("room"))
            .await
            .expect("namespace subscription should succeed");

        let matches: Vec<_> = subscription
            .existing_namespaces
            .into_iter()
            .map(|info| info.namespace)
            .collect();

        assert_eq!(matches, vec![two_field_namespace]);

        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn lookup_namespace_subscribers_keeps_scopes_isolated() {
        let file = temp_file_path("namespace-subscriber-scopes");
        let relay_url = Url::parse("https://relay.example.com").unwrap();
        let coordinator = FileCoordinator::new(&file, relay_url);

        let _global = coordinator
            .subscribe_namespace(None, &prefix("room/global"))
            .await
            .expect("global subscription should register");
        let _scoped = coordinator
            .subscribe_namespace(Some("scope-a"), &prefix("room/scoped"))
            .await
            .expect("scoped subscription should register");

        let global = coordinator
            .lookup_namespace_subscribers(None, &TrackNamespace::from_utf8_path("room/global/cam"))
            .await
            .expect("global lookup should succeed");
        assert_eq!(global.len(), 1);

        let scoped = coordinator
            .lookup_namespace_subscribers(
                Some("scope-a"),
                &TrackNamespace::from_utf8_path("room/scoped/cam"),
            )
            .await
            .expect("scoped lookup should succeed");
        assert_eq!(scoped.len(), 1);

        let no_cross_scope = coordinator
            .lookup_namespace_subscribers(
                Some("scope-a"),
                &TrackNamespace::from_utf8_path("room/global/cam"),
            )
            .await
            .expect("scoped lookup should succeed");
        assert!(no_cross_scope.is_empty());

        let _ = std::fs::remove_file(file);
    }
}
