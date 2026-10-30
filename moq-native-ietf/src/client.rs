// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Opaque outbound connection establishment, before MoQT SETUP.
//!
//! A connector retains its own TLS, name verification and socket policy. Neither
//! this wrapper nor its Quinn adapter spawns a driver, retries or adds a timeout.
//! Dropping the borrowed connect future cancels the operation; custom connectors
//! must release in-flight resources on cancellation as well as on error.

use std::{fmt, future::Future, net::SocketAddr, pin::Pin, sync::Arc};

use moq_transport::session::{transport, Transport};
use url::Url;

/// A cancellable connection attempt, borrowing the connector and original URL.
pub type ConnectFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<Connection>> + Send + 'a>>;

/// Backend-specific connection establishment without exposing the backend to
/// coordinators or MoQT session owners.
pub trait Connector: Send + Sync {
    /// Connect using the original URL and optional direct destination address.
    ///
    /// The address may bypass DNS, but must not replace the URL's verification
    /// name, path or credentials. Return truthful transport/correlation metadata
    /// and retain the original URL in the session. No MoQT SETUP runs here.
    fn connect<'a>(&'a self, url: &'a Url, addr: Option<SocketAddr>) -> ConnectFuture<'a>;
}

/// An established transport and its metadata, before MoQT SETUP.
// No Debug: the session's URL may contain credentials.
pub struct Connection {
    pub session: transport::Session,
    pub transport: Transport,
    /// The backend's original initial destination CID, if available.
    pub initial_destination_cid: Option<String>,
}

/// Cloneable, backend-injectable outbound client.
///
/// Clones share the connector and its policy. They do not create a pool: callers
/// own reuse and must decide policy equivalence before sharing connections.
#[derive(Clone)]
pub struct Client {
    connector: Arc<dyn Connector>,
}

impl Client {
    pub fn new<C: Connector + 'static>(connector: C) -> Self {
        Self {
            connector: Arc::new(connector),
        }
    }

    pub async fn connect(&self, url: &Url, addr: Option<SocketAddr>) -> anyhow::Result<Connection> {
        self.connector.connect(url, addr).await
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

struct QuinnConnector(crate::quic::Client);

impl Connector for QuinnConnector {
    fn connect<'a>(&'a self, url: &'a Url, addr: Option<SocketAddr>) -> ConnectFuture<'a> {
        Box::pin(async move {
            let (session, cid, transport) = self.0.connect(url, addr).await?;
            Ok(Connection {
                session,
                transport,
                initial_destination_cid: Some(cid),
            })
        })
    }
}

impl From<crate::quic::Client> for Client {
    fn from(client: crate::quic::Client) -> Self {
        // Move the original client: never rebuild trust/bind/name policy.
        Self::new(QuinnConnector(client))
    }
}
