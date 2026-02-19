//! API-based coordinator for multi-relay deployments.
//!
//! This coordinator uses the moq-api HTTP server as a centralized registry
//! to coordinate namespace registration across multiple relay instances.
//! It provides:
//!
//! - HTTP-based namespace lookups via moq-api
//! - Automatic TTL refresh to maintain registrations
//! - High availability when using the moq-api server

use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use moq_api::{Client, Origin};
use moq_native_ietf::quic;
use moq_transport::coding::TrackNamespace;
use url::Url;

use moq_relay_ietf::{
    Coordinator, CoordinatorError, CoordinatorResult, NamespaceOrigin, NamespaceRegistration,
};

/// Default TTL for namespace registrations (in seconds)
/// moq-api server uses 600 seconds (10 minutes) TTL
const DEFAULT_REGISTRATION_TTL_SECS: u64 = 600;

/// Configuration for the API coordinator
#[derive(Debug, Clone)]
pub struct ApiCoordinatorConfig {
    /// URL of the moq-api server (e.g., "http://localhost:8080")
    pub api_url: Url,
    /// URL of this relay (advertised to other relays)
    pub relay_url: Url,
    /// TTL for namespace registrations in seconds
    pub registration_ttl_secs: u64,
    /// Interval for refreshing registrations (should be less than TTL)
    pub refresh_interval_secs: u64,
}

impl ApiCoordinatorConfig {
    /// Create a new configuration with default TTL values
    pub fn new(api_url: Url, relay_url: Url) -> Self {
        Self {
            api_url,
            relay_url,
            registration_ttl_secs: DEFAULT_REGISTRATION_TTL_SECS,
            // Refresh at half the TTL to ensure we don't expire
            refresh_interval_secs: DEFAULT_REGISTRATION_TTL_SECS / 2,
        }
    }

    /// Set custom TTL for registrations
    pub fn with_ttl(mut self, ttl_secs: u64) -> Self {
        self.registration_ttl_secs = ttl_secs;
        self.refresh_interval_secs = ttl_secs / 2;
        self
    }
}

/// Handle that unregisters a namespace when dropped and manages TTL refresh
struct NamespaceUnregisterHandle {
    namespace_key: String,
    client: Client,
    /// Channel to signal the refresh task to stop (wrapped in Option so we can take it in drop)
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for NamespaceUnregisterHandle {
    fn drop(&mut self) {
        // Signal the refresh task to stop
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        let namespace_key = self.namespace_key.clone();
        let client = self.client.clone();

        // Spawn a task to unregister since we can't do async in drop
        tokio::spawn(async move {
            if let Err(err) = unregister_namespace_async(&client, &namespace_key).await {
                tracing::warn!(namespace = %namespace_key, error = %err, "failed to unregister namespace on drop: {}", err);
            }
        });
    }
}

/// Async helper for unregistering a namespace
async fn unregister_namespace_async(client: &Client, namespace_key: &str) -> Result<()> {
    tracing::debug!(namespace = %namespace_key, "unregistering namespace from API: {}", namespace_key);

    client
        .delete_origin(namespace_key)
        .await
        .context("failed to delete namespace from API")?;

    Ok(())
}

// Tuple-encoding helper kept for potential future scoped API updates.
// fn namespace_key(namespace: &TrackNamespace, connection_path: Option<&str>) -> String {
//     let namespace_str = namespace.to_utf8_path();
//     let path = connection_path.unwrap_or("");
//     let mut key = String::with_capacity(path.len() + namespace_str.len() + 32);
//     key.push('p');
//     key.push_str(&path.len().to_string());
//     key.push(':');
//     key.push_str(path);
//     key.push('n');
//     key.push_str(&namespace_str.len().to_string());
//     key.push(':');
//     key.push_str(&namespace_str);
//     key
// }

/// A coordinator that uses moq-api for state storage.
///
/// Multiple relay instances can connect to the same moq-api server to
/// coordinate namespace registration and discovery. Features:
///
/// - HTTP-based registration and lookup
/// - TTL-based automatic expiration of stale registrations
/// - Background refresh tasks to maintain registrations
///
/// Note: moq-api does not scope namespaces by connection path, so this
/// coordinator treats all namespaces as global.
pub struct ApiCoordinator {
    /// moq-api client
    client: Client,
    /// Configuration
    config: ApiCoordinatorConfig,
}

impl ApiCoordinator {
    /// Create a new API-based coordinator.
    ///
    /// # Arguments
    /// * `config` - Configuration for the API coordinator
    ///
    /// # Returns
    /// A new `ApiCoordinator` instance
    pub fn new(config: ApiCoordinatorConfig) -> Self {
        let client = Client::new(config.api_url.clone());

        Self { client, config }
    }

    /// Start a background task to refresh namespace registration
    fn start_refresh_task(
        client: Client,
        namespace_key: String,
        relay_url: Url,
        refresh_interval: Duration,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let origin = Origin { url: relay_url.clone() };

                        match client.patch_origin(&namespace_key, origin).await {
                            Ok(()) => {
                                tracing::trace!(namespace = %namespace_key, "refreshed namespace registration: {}", namespace_key);
                            }
                            Err(err) => {
                                tracing::warn!(namespace = %namespace_key, error = %err, "failed to refresh namespace registration: {}", err);
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        tracing::debug!("namespace refresh task shutting down");
                        break;
                    }
                }
            }
        });
    }
}

#[async_trait]
impl Coordinator for ApiCoordinator {
    async fn register_namespace(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<NamespaceRegistration> {
        let _scope = scope;
        let namespace_str = namespace.to_utf8_path();
        let origin = Origin {
            url: self.config.relay_url.clone(),
        };

        tracing::info!(
            namespace = %namespace_str,
            relay_url = %self.config.relay_url,
            "registering namespace in API: {} -> {}",
            namespace_str,
            self.config.relay_url
        );

        // Register the namespace with the API
        self.client
            .set_origin(&namespace_str, origin)
            .await
            .context("failed to register namespace in API")
            .map_err(CoordinatorError::Other)?;

        // Create shutdown channel for the refresh task
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        // Start background refresh task
        Self::start_refresh_task(
            self.client.clone(),
            namespace_str.clone(),
            self.config.relay_url.clone(),
            Duration::from_secs(self.config.refresh_interval_secs),
            shutdown_rx,
        );

        let handle = NamespaceUnregisterHandle {
            namespace_key: namespace_str,
            client: self.client.clone(),
            shutdown_tx: Some(shutdown_tx),
        };

        Ok(NamespaceRegistration::new(handle))
    }

    async fn unregister_namespace(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<()> {
        let _scope = scope;
        let namespace_str = namespace.to_utf8_path();
        tracing::info!(namespace = %namespace_str, "unregistering namespace from API: {}", namespace_str);

        self.client
            .delete_origin(&namespace_str)
            .await
            .context("failed to unregister namespace from API")
            .map_err(CoordinatorError::Other)?;

        Ok(())
    }

    async fn lookup(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)> {
        let _scope = scope;
        let namespace_str = namespace.to_utf8_path();
        tracing::debug!(namespace = %namespace_str, "looking up namespace in API: {}", namespace_str);

        // Query the API for the namespace
        let result = self
            .client
            .get_origin(&namespace_str)
            .await
            .context("failed to lookup namespace in API")
            .map_err(CoordinatorError::Other)?;

        match result {
            Some(origin) => {
                tracing::debug!(namespace = %namespace_str, origin_url = %origin.url, "found namespace {} at {}", namespace_str, origin.url);
                Ok((
                    NamespaceOrigin::new(namespace.clone(), origin.url, None),
                    None,
                ))
            }
            None => {
                tracing::debug!(namespace = %namespace_str, "namespace not found: {}", namespace_str);
                Err(CoordinatorError::NamespaceNotFound)
            }
        }
    }

    async fn shutdown(&self) -> CoordinatorResult<()> {
        tracing::info!("shutting down API coordinator");
        // The moq-api client uses reqwest which handles connection cleanup internally
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_new() {
        let api_url = Url::parse("http://localhost:8080").unwrap();
        let relay_url = Url::parse("https://relay.example.com").unwrap();

        let config = ApiCoordinatorConfig::new(api_url.clone(), relay_url.clone());

        assert_eq!(config.api_url, api_url);
        assert_eq!(config.relay_url, relay_url);
        assert_eq!(config.registration_ttl_secs, DEFAULT_REGISTRATION_TTL_SECS);
        assert_eq!(
            config.refresh_interval_secs,
            DEFAULT_REGISTRATION_TTL_SECS / 2
        );
    }

    #[test]
    fn test_config_with_ttl() {
        let api_url = Url::parse("http://localhost:8080").unwrap();
        let relay_url = Url::parse("https://relay.example.com").unwrap();

        let config = ApiCoordinatorConfig::new(api_url, relay_url).with_ttl(120);

        assert_eq!(config.registration_ttl_secs, 120);
        assert_eq!(config.refresh_interval_secs, 60);
    }
}
