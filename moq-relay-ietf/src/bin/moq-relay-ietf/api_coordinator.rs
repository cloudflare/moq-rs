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
    namespace: TrackNamespace,
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

        let namespace = self.namespace.clone();
        let client = self.client.clone();

        // Spawn a task to unregister since we can't do async in drop
        tokio::spawn(async move {
            if let Err(err) = unregister_namespace_async(&client, &namespace).await {
                log::warn!("failed to unregister namespace on drop: {}", err);
            }
        });
    }
}

/// Async helper for unregistering a namespace
async fn unregister_namespace_async(client: &Client, namespace: &TrackNamespace) -> Result<()> {
    let namespace_str = namespace.to_utf8_path();
    log::debug!("unregistering namespace from API: {}", namespace_str);

    client
        .delete_origin(&namespace_str)
        .await
        .context("failed to delete namespace from API")?;

    Ok(())
}

/// A coordinator that uses moq-api for state storage.
///
/// Multiple relay instances can connect to the same moq-api server to
/// coordinate namespace registration and discovery. Features:
///
/// - HTTP-based registration and lookup
/// - TTL-based automatic expiration of stale registrations
/// - Background refresh tasks to maintain registrations
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
        namespace: TrackNamespace,
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
                        let namespace_str = namespace.to_utf8_path();
                        let origin = Origin { url: relay_url.clone() };

                        match client.patch_origin(&namespace_str, origin).await {
                            Ok(()) => {
                                log::trace!("refreshed namespace registration: {}", namespace_str);
                            }
                            Err(err) => {
                                log::warn!("failed to refresh namespace registration: {}", err);
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        log::debug!("namespace refresh task shutting down");
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
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<NamespaceRegistration> {
        let namespace_str = namespace.to_utf8_path();
        let origin = Origin {
            url: self.config.relay_url.clone(),
        };

        log::info!(
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
            namespace.clone(),
            self.config.relay_url.clone(),
            Duration::from_secs(self.config.refresh_interval_secs),
            shutdown_rx,
        );

        let handle = NamespaceUnregisterHandle {
            namespace: namespace.clone(),
            client: self.client.clone(),
            shutdown_tx: Some(shutdown_tx),
        };

        Ok(NamespaceRegistration::new(handle))
    }

    async fn unregister_namespace(&self, namespace: &TrackNamespace) -> CoordinatorResult<()> {
        let namespace_str = namespace.to_utf8_path();
        log::info!("unregistering namespace from API: {}", namespace_str);

        self.client
            .delete_origin(&namespace_str)
            .await
            .context("failed to unregister namespace from API")
            .map_err(CoordinatorError::Other)?;

        Ok(())
    }

    async fn lookup(
        &self,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)> {
        let namespace_str = namespace.to_utf8_path();
        log::debug!("looking up namespace in API: {}", namespace_str);

        // Query the API for the namespace
        let result = self
            .client
            .get_origin(&namespace_str)
            .await
            .context("failed to lookup namespace in API")
            .map_err(CoordinatorError::Other)?;

        match result {
            Some(origin) => {
                log::debug!("found namespace {} at {}", namespace_str, origin.url);
                Ok((
                    NamespaceOrigin::new(namespace.clone(), origin.url, None),
                    None,
                ))
            }
            None => {
                log::debug!("namespace not found: {}", namespace_str);
                Err(CoordinatorError::NamespaceNotFound)
            }
        }
    }

    async fn shutdown(&self) -> CoordinatorResult<()> {
        log::info!("shutting down API coordinator");
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
