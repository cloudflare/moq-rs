// Core modules
mod consumer;
mod local;
mod producer;
mod relay;
mod remote;
mod session;
mod web;

// Control plane abstraction
pub mod control_plane;
pub mod control_plane_http;

// Re-export key types for library users
pub use consumer::*;
pub use local::*;
pub use producer::*;
pub use relay::*;
pub use remote::*;
pub use session::*;
pub use web::*;

pub use control_plane::{ControlPlane, ControlPlaneRefresher, Origin as ControlPlaneOrigin};
pub use control_plane_http::HttpControlPlane;

use std::{net, path::PathBuf};
use url::Url;

/// Configuration for the relay server
#[derive(Clone)]
pub struct RelayServerConfig {
    /// Listen on this address
    pub bind: net::SocketAddr,

    /// The TLS configuration.
    pub tls: moq_native_ietf::tls::Config,

    /// Directory to write qlog files (one per connection)
    pub qlog_dir: Option<PathBuf>,

    /// Directory to write mlog files (one per connection)
    pub mlog_dir: Option<PathBuf>,

    /// Forward all announcements to the (optional) URL.
    pub announce: Option<Url>,

    /// Enable development mode web server
    pub enable_dev_web: bool,

    /// Serve qlog files over HTTPS at /qlog/:cid (requires enable_dev_web)
    pub qlog_serve: bool,

    /// Serve mlog files over HTTPS at /mlog/:cid (requires enable_dev_web)
    pub mlog_serve: bool,
}

/// Main relay server that can work with any ControlPlane implementation
pub struct RelayServer<CP: ControlPlane> {
    relay: Relay<CP>,
    web: Option<Web>,
}

impl<CP: ControlPlane> RelayServer<CP> {
    /// Create a new relay server with the given control plane implementation
    pub fn new(
        config: RelayServerConfig,
        control_plane: Option<CP>,
        node_url: Option<Url>,
    ) -> anyhow::Result<Self> {
        let relay = Relay::new(RelayConfig {
            tls: config.tls.clone(),
            bind: config.bind,
            qlog_dir: config.qlog_dir.clone(),
            mlog_dir: config.mlog_dir.clone(),
            announce: config.announce,
            control_plane,
            node: node_url,
        })?;

        let web = if config.enable_dev_web {
            let qlog_dir = if config.qlog_serve {
                config.qlog_dir
            } else {
                None
            };

            let mlog_dir = if config.mlog_serve {
                config.mlog_dir
            } else {
                None
            };

            Some(Web::new(WebConfig {
                bind: config.bind,
                tls: config.tls,
                qlog_dir,
                mlog_dir,
            }))
        } else {
            None
        };

        Ok(Self { relay, web })
    }

    /// Run the relay server
    pub async fn run(self) -> anyhow::Result<()> {
        if let Some(web) = self.web {
            tokio::spawn(async move {
                web.run().await.expect("failed to run web server");
            });
        }

        self.relay.run().await
    }
}
