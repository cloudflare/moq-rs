use std::{net, path::PathBuf};

use anyhow::Context;

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_native_ietf::quic;
use moq_transport::session::SessionMigration;
use tokio::sync::broadcast;
use url::Url;

use crate::{Api, Consumer, Locals, Producer, Remotes, RemotesConsumer, RemotesProducer, Session};

/// Configuration for the relay.
pub struct RelayConfig {
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

    /// Connect to the HTTP moq-api at this URL.
    pub api: Option<Url>,

    /// Our hostname which we advertise to other origins.
    /// We use QUIC, so the certificate must be valid for this address.
    pub node: Option<Url>,
}

/// MoQ Relay server.
pub struct Relay {
    quic: quic::Endpoint,
    announce_url: Option<Url>,
    mlog_dir: Option<PathBuf>,
    locals: Locals,
    api: Option<Api>,
    remotes: Option<(RemotesProducer, RemotesConsumer)>,
}

impl Relay {
    pub fn new(config: RelayConfig) -> anyhow::Result<Self> {
        // Create a QUIC endpoint that can be used for both clients and servers.
        let quic = quic::Endpoint::new(quic::Config {
            bind: config.bind,
            qlog_dir: config.qlog_dir,
            tls: config.tls,
        })?;

        // Validate mlog directory if provided
        if let Some(mlog_dir) = &config.mlog_dir {
            if !mlog_dir.exists() {
                anyhow::bail!("mlog directory does not exist: {}", mlog_dir.display());
            }
            if !mlog_dir.is_dir() {
                anyhow::bail!("mlog path is not a directory: {}", mlog_dir.display());
            }
            log::info!("mlog output enabled: {}", mlog_dir.display());
        }

        // Create an API client if we have the necessary configuration
        let api = if let (Some(url), Some(node)) = (config.api, config.node) {
            log::info!("using moq-api: url={} node={}", url, node);
            Some(Api::new(url, node))
        } else {
            None
        };

        let locals = Locals::new();

        // Create remotes if we have an API client
        let remotes = api.clone().map(|api| {
            Remotes {
                api,
                quic: quic.client.clone(),
            }
            .produce()
        });

        Ok(Self {
            quic,
            announce_url: config.announce,
            mlog_dir: config.mlog_dir,
            api,
            locals,
            remotes,
        })
    }

    /// Run the relay server.
    pub async fn run(self) -> anyhow::Result<()> {
        let mut tasks = FuturesUnordered::new();

        // Setup SIGTERM handler and broadcast channel
        #[cfg(unix)]
        let mut signal_term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let mut signal_int =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;

        let (signal_tx, signal_rx) = broadcast::channel::<SessionMigration>(16);

        // Get server address early for the shutdown signal
        let server_addr = self
            .quic
            .server
            .as_ref()
            .context("missing TLS certificate")?
            .local_addr()?;
        let shutdown_uri = format!("https://{}", server_addr);

        // Spawn task to listen for SIGTERM and broadcast shutdown
        let signal_tx_clone = signal_tx.clone();
        tasks.push(
            async move {
                log::info!("Listening for SIGTERM");
                #[cfg(unix)]
                {
                    tokio::select! {
                        _ = signal_term.recv() => {
                            log::info!("Received SIGTERM");
                        }
                        _ = signal_int.recv() => {
                            log::info!("Received SIGINT");
                        }
                    }
                    log::info!("broadcasting shutdown to all sessions");

                    if let Err(e) = signal_tx.send(SessionMigration { uri: shutdown_uri }) {
                        log::error!("failed to broadcast shutdown: {}", e);
                    }
                }
                #[cfg(not(unix))]
                {
                    std::future::pending::<()>().await;
                }
                Ok(())
            }
            .boxed(),
        );

        // Start the remotes producer task, if any
        let remotes = self.remotes.map(|(producer, consumer)| {
            let signal_rx = signal_rx.resubscribe();
            tasks.push(producer.run(signal_rx).boxed());
            consumer
        });

        // Start the forwarder, if any
        let forward_producer = if let Some(url) = &self.announce_url {
            log::info!("forwarding announces to {}", url);

            // Establish a QUIC connection to the forward URL
            let (session, _quic_client_initial_cid) = self
                .quic
                .client
                .connect(url)
                .await
                .context("failed to establish forward connection")?;

            // Create the MoQ session over the connection
            let (session, publisher, subscriber) =
                moq_transport::session::Session::connect(session, None)
                    .await
                    .context("failed to establish forward session")?;

            // Create a normal looking session, except we never forward or register announces.
            let session = Session {
                session,
                producer: Some(Producer::new(
                    publisher,
                    self.locals.clone(),
                    remotes.clone(),
                )),
                consumer: Some(Consumer::new(subscriber, self.locals.clone(), None, None)),
            };

            let forward_producer = session.producer.clone();

            tasks.push(
                async move {
                    session
                        .run(signal_tx_clone.subscribe())
                        .await
                        .context("forwarding failed")
                }
                .boxed(),
            );

            forward_producer
        } else {
            None
        };

        // Start the QUIC server loop
        let mut server = self.quic.server.context("missing TLS certificate")?;
        log::info!("listening on {}", server.local_addr()?);
        let mut cloned_signal_rx = signal_rx.resubscribe();

        loop {
            tokio::select! {
                // Accept a new QUIC connection
                res = server.accept() => {
                    let (conn, connection_id) = res.context("failed to accept QUIC connection")?;

                    // Construct mlog path from connection ID if mlog directory is configured
                    let mlog_path = self.mlog_dir.as_ref()
                        .map(|dir| dir.join(format!("{}_server.mlog", connection_id)));

                    let locals = self.locals.clone();
                    let remotes = remotes.clone();
                    let forward = forward_producer.clone();
                    let api = self.api.clone();
                    let session_signal_rx = signal_rx.resubscribe();

                    // Spawn a new task to handle the connection
                    tasks.push(async move {
                        // Create the MoQ session over the connection (setup handshake etc)
                        let (session, publisher, subscriber) = match moq_transport::session::Session::accept(conn, mlog_path).await {
                            Ok(session) => session,
                            Err(err) => {
                                log::warn!("failed to accept MoQ session: {}", err);
                                return Ok(());
                            }
                        };

                        // Create our MoQ relay session
                        let session = Session {
                            session,
                            producer: publisher.map(|publisher| Producer::new(publisher, locals.clone(), remotes)),
                            consumer: subscriber.map(|subscriber| Consumer::new(subscriber, locals, api, forward)),
                        };

                        if let Err(err) = session.run(session_signal_rx).await {
                            log::warn!("failed to run MoQ session: {}", err);
                        }

                        Ok(())
                    }.boxed());
                },
                res = tasks.next(), if !tasks.is_empty() => res.unwrap()?,
                _ = cloned_signal_rx.recv() => {
                    log::info!("received shutdown signal, shutting down. Active tasks: {}", tasks.len());
                    // set a timeout for waiting for tasks to be empty
                    // FIXME(itzmanish): make this configurable and revisit
                    let timeout = tokio::time::timeout(tokio::time::Duration::from_secs(20), async move {
                        while !tasks.is_empty() {
                            // sleep 500ms before checking again
                            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                        }
                    });
                    if let Err(e) = timeout.await {
                        log::warn!("timed out waiting for tasks to be empty: {}", e);
                    }
                    break Ok(());
                }
            }
        }
    }
}
