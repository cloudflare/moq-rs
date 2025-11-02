use anyhow::Result;
use async_trait::async_trait;
use url::Url;

use crate::control_plane::{ControlPlane, ControlPlaneRefresher, Origin};

/// HTTP-based control plane implementation using moq-api
#[derive(Clone)]
pub struct HttpControlPlane {
    client: moq_api::Client,
    node: Url,
}

impl HttpControlPlane {
    pub fn new(api_url: Url, node_url: Url) -> Self {
        let client = moq_api::Client::new(api_url);
        Self {
            client,
            node: node_url,
        }
    }

    pub fn node_url(&self) -> &Url {
        &self.node
    }
}

#[async_trait]
impl ControlPlane for HttpControlPlane {
    async fn get_origin(&self, namespace: &str) -> Result<Option<Origin>> {
        match self.client.get_origin(namespace).await? {
            Some(origin) => Ok(Some(Origin { url: origin.url })),
            None => Ok(None),
        }
    }

    async fn set_origin(&self, namespace: &str, origin: Origin) -> Result<()> {
        let moq_origin = moq_api::Origin { url: origin.url };
        self.client.set_origin(namespace, moq_origin).await?;
        Ok(())
    }

    async fn delete_origin(&self, namespace: &str) -> Result<()> {
        self.client.delete_origin(namespace).await?;
        Ok(())
    }

    fn create_refresher(
        &self,
        namespace: String,
        origin: Origin,
    ) -> Box<dyn ControlPlaneRefresher> {
        Box::new(HttpRefresher::new(self.client.clone(), namespace, origin))
    }
}

/// Periodically refreshes the origin registration via HTTP
pub struct HttpRefresher {
    client: moq_api::Client,
    namespace: String,
    origin: Origin,
    refresh: tokio::time::Interval,
}

impl HttpRefresher {
    fn new(client: moq_api::Client, namespace: String, origin: Origin) -> Self {
        // Refresh every 5 minutes
        let duration = tokio::time::Duration::from_secs(300);
        let mut refresh = tokio::time::interval(duration);
        refresh.reset_after(duration); // skip the first tick

        Self {
            client,
            namespace,
            origin,
            refresh,
        }
    }

    async fn update(&self) -> Result<()> {
        log::debug!(
            "registering origin: namespace={} url={}",
            self.namespace,
            self.origin.url
        );
        let moq_origin = moq_api::Origin {
            url: self.origin.url.clone(),
        };
        self.client.set_origin(&self.namespace, moq_origin).await?;
        Ok(())
    }
}

#[async_trait]
impl ControlPlaneRefresher for HttpRefresher {
    async fn run(&mut self) -> Result<()> {
        loop {
            self.refresh.tick().await;
            self.update().await?;
        }
    }
}

impl Drop for HttpRefresher {
    fn drop(&mut self) {
        let namespace = self.namespace.clone();
        let client = self.client.clone();
        log::debug!("removing origin: namespace={}", namespace);
        tokio::spawn(async move {
            let _ = client.delete_origin(&namespace).await;
        });
    }
}
