use anyhow::Result;
use async_trait::async_trait;
use url::Url;

/// Origin information for routing
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Origin {
    pub url: Url,
}

/// Trait for control plane operations that enable cross-relay routing and state sharing
#[async_trait]
pub trait ControlPlane: Send + Sync + Clone + Default + 'static {
    /// Get the origin URL for a given namespace
    async fn get_origin(&self, namespace: &str) -> Result<Option<Origin>>;

    /// Set/register the origin for a given namespace
    async fn set_origin(&self, namespace: &str, origin: Origin) -> Result<()>;

    /// Delete/unregister the origin for a given namespace
    async fn delete_origin(&self, namespace: &str) -> Result<()>;

    /// Create a refresher that periodically updates the origin registration
    /// Returns a future that runs the refresh loop
    fn create_refresher(
        &self,
        namespace: String,
        origin: Origin,
    ) -> Box<dyn ControlPlaneRefresher>;
}

/// Trait for periodically refreshing origin registrations
#[async_trait]
pub trait ControlPlaneRefresher: Send + 'static {
    /// Run the refresh loop (should run indefinitely until dropped)
    async fn run(&mut self) -> Result<()>;
}
