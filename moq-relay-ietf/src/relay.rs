use std::{future::Future, net, path::PathBuf, pin::Pin, sync::Arc};

use anyhow::Context;

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_native_ietf::quic::{self, Endpoint};
use url::Url;

use crate::{
    metrics::GaugeGuard, Consumer, Coordinator, Locals, Producer, Remotes, RemotesConsumer,
    RemotesProducer, Session,
};

// A type alias for boxed future
type ServerFuture = Pin<
    Box<
        dyn Future<
            Output = (
                anyhow::Result<(web_transport::Session, String)>,
                quic::Server,
            ),
        >,
    >,
>;

/// Configuration for the relay.
pub struct RelayConfig {
    /// Listen on this address
    pub bind: Option<net::SocketAddr>,

    /// Optional list of endpoints if provided, we won't use bind
    pub endpoints: Vec<Endpoint>,

    /// The TLS configuration.
    pub tls: moq_native_ietf::tls::Config,

    /// Directory to write qlog files (one per connection)
    pub qlog_dir: Option<PathBuf>,

    /// Directory to write mlog files (one per connection)
    pub mlog_dir: Option<PathBuf>,

    /// Forward all announcements to the (optional) URL.
    pub announce: Option<Url>,

    /// Our hostname which we advertise to other origins.
    /// We use QUIC, so the certificate must be valid for this address.
    pub node: Option<Url>,

    /// The coordinator for namespace/track registration and discovery.
    pub coordinator: Arc<dyn Coordinator>,
}

/// MoQ Relay server.
pub struct Relay {
    quic_endpoints: Vec<Endpoint>,
    announce_url: Option<Url>,
    mlog_dir: Option<PathBuf>,
    locals: Locals,
    remotes: Option<(RemotesProducer, RemotesConsumer)>,
    coordinator: Arc<dyn Coordinator>,
}

impl Relay {
    pub fn new(config: RelayConfig) -> anyhow::Result<Self> {
        if config.bind.is_some() && !config.endpoints.is_empty() {
            anyhow::bail!("cannot specify both bind and endpoints");
        }

        let endpoints = if let Some(bind) = config.bind {
            let endpoint = quic::Endpoint::new(quic::Config::new(
                bind,
                config.qlog_dir.clone(),
                config.tls.clone(),
            ))?;
            vec![endpoint]
        } else {
            config.endpoints
        };

        if endpoints.is_empty() {
            anyhow::bail!("no endpoints available to start the server");
        }

        // Validate mlog directory if provided
        if let Some(mlog_dir) = &config.mlog_dir {
            if !mlog_dir.exists() {
                anyhow::bail!("mlog directory does not exist: {}", mlog_dir.display());
            }
            if !mlog_dir.is_dir() {
                anyhow::bail!("mlog path is not a directory: {}", mlog_dir.display());
            }
            tracing::info!("mlog output enabled: {}", mlog_dir.display());
        }

        let locals = Locals::new();

        // FIXME(itzmanish): have a generic filter to find endpoints for forward, remote etc.
        let remote_clients = endpoints
            .iter()
            .map(|endpoint| endpoint.client.clone())
            .collect::<Vec<_>>();

        // Create remote manager - uses coordinator for namespace lookups
        let remotes = Remotes {
            coordinator: config.coordinator.clone(),
            quic: remote_clients[0].clone(),
        }
        .produce();

        Ok(Self {
            quic_endpoints: endpoints,
            announce_url: config.announce,
            mlog_dir: config.mlog_dir,
            locals,
            remotes: Some(remotes),
            coordinator: config.coordinator,
        })
    }

    /// Run the relay server.
    pub async fn run(self) -> anyhow::Result<()> {
        let mut tasks = FuturesUnordered::new();

        // Split remotes producer/consumer and spawn producer task
        let remotes = self.remotes.map(|(producer, consumer)| {
            tasks.push(producer.run().boxed());
            consumer
        });

        // Start the forwarder, if any
        let forward_producer = if let Some(url) = &self.announce_url {
            tracing::info!("forwarding announces to {}", url);

            // Establish a QUIC connection to the forward URL
            let (session, _quic_client_initial_cid) = self.quic_endpoints[0]
                .client
                .connect(url, None)
                .await
                .context("failed to establish forward connection")?;

            // Create the MoQ session over the connection
            let (session, publisher, subscriber) =
                moq_transport::session::Session::connect(session, None)
                    .await
                    .context("failed to establish forward session")?;

            // Create a normal looking session, except we never forward or register announces.
            let coordinator = self.coordinator.clone();
            let session = Session {
                session,
                producer: Some(Producer::new(
                    publisher,
                    self.locals.clone(),
                    remotes.clone(),
                )),
                consumer: Some(Consumer::new(
                    subscriber,
                    self.locals.clone(),
                    coordinator,
                    None,
                )),
            };

            let forward_producer = session.producer.clone();

            tasks.push(async move { session.run().await.context("forwarding failed") }.boxed());

            forward_producer
        } else {
            None
        };

        let servers: Vec<quic::Server> = self
            .quic_endpoints
            .into_iter()
            .map(|endpoint| {
                endpoint
                    .server
                    .context("missing TLS certificate for server")
            })
            .collect::<anyhow::Result<_>>()?;

        // This will hold the futures for all our listening servers.
        let mut accepts: FuturesUnordered<ServerFuture> = FuturesUnordered::new();
        for mut server in servers {
            tracing::info!("listening on {}", server.local_addr()?);

            // Create a future, box it, and push it to the collection.
            accepts.push(
                async move {
                    let conn = server.accept().await.context("accept failed");
                    (conn, server)
                }
                .boxed(),
            );
        }

        loop {
            tokio::select! {
                // This branch polls all the `accept` futures concurrently.
                Some((conn_result, mut server)) = accepts.next() => {
                    // An accept operation has completed.
                    // First, immediately queue up the next accept() call for this server.
                    accepts.push(
                        async move {
                            let conn = server.accept().await.context("accept failed");
                            (conn, server)
                        }
                        .boxed(),
                    );

                    let (conn, connection_id) = conn_result.context("failed to accept QUIC connection")?;

                    metrics::counter!("moq_relay_connections_total").increment(1);

                    // Construct mlog path from connection ID if mlog directory is configured
                    let mlog_path = self.mlog_dir.as_ref()
                        .map(|dir| dir.join(format!("{}_server.mlog", connection_id)));

                    let locals = self.locals.clone();
                    let remotes = remotes.clone();
                    let forward = forward_producer.clone();
                    let coordinator = self.coordinator.clone();

                    // Spawn a new task to handle the connection
                    tasks.push(async move {
                        // Track active connections - decrements when task completes
                        let _conn_guard = GaugeGuard::new("moq_relay_active_connections");

                        // Create the MoQ session over the connection (setup handshake etc)
                        let (session, publisher, subscriber) = match moq_transport::session::Session::accept(conn, mlog_path).await {
                            Ok(session) => session,
                            Err(err) => {
                                tracing::warn!(error = %err, "failed to accept MoQ session: {}", err);
                                metrics::counter!("moq_relay_connection_errors_total", "stage" => "session_accept").increment(1);
                                // Maintain invariant: connections_total - connections_closed_total == active_connections
                                metrics::counter!("moq_relay_connections_closed_total").increment(1);
                                return Ok(());
                            }
                        };

                        // Create our MoQ relay session
                        let moq_session = session;
                        let session = Session {
                            session: moq_session,
                            producer: publisher.map(|publisher| Producer::new(publisher, locals.clone(), remotes)),
                            consumer: subscriber.map(|subscriber| Consumer::new(subscriber, locals, coordinator, forward)),
                        };

                        match session.run().await {
                            Ok(()) => {
                                // Session ended cleanly (uncommon - usually ends via close)
                                metrics::counter!("moq_relay_connections_closed_total").increment(1);
                            }
                            Err(err) if err.is_graceful_close() => {
                                // Graceful close - peer sent APPLICATION_CLOSE with code 0
                                tracing::debug!("MoQ session closed gracefully");
                                metrics::counter!("moq_relay_connections_closed_total").increment(1);
                            }
                            Err(err) => {
                                // Actual error - protocol violation, timeout, etc.
                                tracing::warn!(error = %err, "MoQ session error: {}", err);
                                metrics::counter!("moq_relay_connection_errors_total", "stage" => "session_run").increment(1);
                                metrics::counter!("moq_relay_connections_closed_total").increment(1);
                            }
                        }

                        Ok(())
                    }.boxed());
                },
                res = tasks.next(), if !tasks.is_empty() => res.unwrap()?,
            }
        }
    }
}
