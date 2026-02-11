use std::{
    collections::HashSet,
    fmt,
    fs::File,
    io::BufWriter,
    net::{self, IpAddr},
    path::PathBuf,
    sync::{Arc, Mutex},
    time,
};

use anyhow::Context;
use clap::Parser;
use url::Url;

use crate::tls;

use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;

/// Represents the address family of the local QUIC socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressFamily {
    Ipv4,
    Ipv6,
    /// IPv6 with dual-stack support (Linux)
    Ipv6DualStack,
}

pub enum Host {
    Ip(IpAddr),
    Name(String),
}

impl fmt::Display for AddressFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AddressFamily::Ipv4 => write!(f, "IPv4"),
            AddressFamily::Ipv6 => write!(f, "IPv6"),
            AddressFamily::Ipv6DualStack => write!(f, "IPv6 (dual stack)"),
        }
    }
}

/// Build a TransportConfig with our standard settings
///
/// This is used both for the base endpoint config and when creating
/// per-connection configs with qlog enabled.
fn build_transport_config() -> quinn::TransportConfig {
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(time::Duration::from_secs(10).try_into().unwrap()));
    transport.keep_alive_interval(Some(time::Duration::from_secs(4))); // TODO make this smarter
    transport.congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default()));
    transport.mtu_discovery_config(None); // Disable MTU discovery
    transport
}

#[derive(Parser, Clone)]
pub struct Args {
    /// Listen for UDP packets on the given address.
    #[arg(long, default_value = "[::]:0")]
    pub bind: net::SocketAddr,

    /// Directory to write qlog files (one per connection)
    #[arg(long)]
    pub qlog_dir: Option<PathBuf>,

    #[command(flatten)]
    pub tls: tls::Args,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            bind: "[::]:0".parse().unwrap(),
            qlog_dir: None,
            tls: Default::default(),
        }
    }
}

impl Args {
    pub fn load(&self) -> anyhow::Result<Config> {
        let tls = self.tls.load()?;
        Ok(Config::new(self.bind, self.qlog_dir.clone(), tls))
    }
}

pub struct Config {
    pub bind: Option<net::SocketAddr>,
    pub socket: net::UdpSocket,
    pub qlog_dir: Option<PathBuf>,
    pub tls: tls::Config,
    pub tags: HashSet<String>,
}

impl Config {
    pub fn new(bind: net::SocketAddr, qlog_dir: Option<PathBuf>, tls: tls::Config) -> Self {
        Self {
            bind: Some(bind),
            socket: net::UdpSocket::bind(bind)
                .context("failed to bind socket")
                .unwrap(),
            qlog_dir,
            tls,
            tags: HashSet::new(),
        }
    }

    pub fn with_socket(
        socket: net::UdpSocket,
        qlog_dir: Option<PathBuf>,
        tls: tls::Config,
    ) -> Self {
        Self {
            bind: None,
            socket,
            qlog_dir,
            tls,
            tags: HashSet::new(),
        }
    }

    pub fn with_tag(mut self, tag: String) -> Self {
        self.tags.insert(tag);
        self
    }
}

pub struct Endpoint {
    pub client: Client,
    pub server: Option<Server>,
    /// Tags associated with this endpoint
    /// These are used to filter endpoints for different purposes, for eg-
    /// "server" tag is used to filter endpoints for relay server
    /// "forward" tag is used to filter endpoints for forwarder
    /// This is upto the user to define and use
    pub tags: HashSet<String>,
}

impl Endpoint {
    pub fn new(config: Config) -> anyhow::Result<Self> {
        // Validate qlog directory if provided

        if let Some(qlog_dir) = &config.qlog_dir {
            if !qlog_dir.exists() {
                anyhow::bail!("qlog directory does not exist: {}", qlog_dir.display());
            }
            if !qlog_dir.is_dir() {
                anyhow::bail!("qlog path is not a directory: {}", qlog_dir.display());
            }
            tracing::info!("qlog output enabled: {}", qlog_dir.display());
        }

        // Build transport config with our standard settings
        let transport = Arc::new(build_transport_config());

        let mut server_config = None;

        if let Some(mut config) = config.tls.server {
            config.alpn_protocols = vec![
                web_transport_quinn::ALPN.as_bytes().to_vec(),
                moq_transport::setup::ALPN.to_vec(),
            ];
            config.key_log = Arc::new(rustls::KeyLogFile::new());

            let config: quinn::crypto::rustls::QuicServerConfig = config.try_into()?;
            let mut config = quinn::ServerConfig::with_crypto(Arc::new(config));
            config.transport_config(transport.clone());

            server_config = Some(config);
        }

        // There's a bit more boilerplate to make a generic endpoint.
        let runtime = quinn::default_runtime().context("no async runtime")?;
        let endpoint_config = quinn::EndpointConfig::default();
        let socket = config.socket;

        // Create the generic QUIC endpoint.
        let quic = quinn::Endpoint::new(endpoint_config, server_config.clone(), socket, runtime)
            .context("failed to create QUIC endpoint")?;

        let server = server_config.map(|base_server_config| Server {
            quic: quic.clone(),
            accept: Default::default(),
            qlog_dir: config.qlog_dir.map(Arc::new),
            base_server_config: Arc::new(base_server_config),
        });

        let client = Client {
            quic,
            config: config.tls.client,
            transport,
        };

        Ok(Self {
            client,
            server,
            tags: config.tags,
        })
    }
}

pub struct Server {
    quic: quinn::Endpoint,
    accept: FuturesUnordered<BoxFuture<'static, anyhow::Result<(web_transport::Session, String)>>>,
    qlog_dir: Option<Arc<PathBuf>>,
    base_server_config: Arc<quinn::ServerConfig>,
}

impl Server {
    pub async fn accept(&mut self) -> Option<(web_transport::Session, String)> {
        loop {
            tokio::select! {
                res = self.quic.accept() => {
                    let conn = res?;
                    let qlog_dir = self.qlog_dir.clone();
                    let base_server_config = self.base_server_config.clone();
                    self.accept.push(Self::accept_session(conn, qlog_dir, base_server_config).boxed());
                },
                res = self.accept.next(), if !self.accept.is_empty() => {
                    match res? {
                        Ok(result) => return Some(result),
                        Err(err) => {
                            tracing::warn!("failed to accept QUIC connection: {}", err.root_cause());
                            continue;
                        }
                    }
                }
            }
        }
    }

    async fn accept_session(
        conn: quinn::Incoming,
        qlog_dir: Option<Arc<PathBuf>>,
        base_server_config: Arc<quinn::ServerConfig>,
    ) -> anyhow::Result<(web_transport::Session, String)> {
        // Capture the original destination connection ID BEFORE accepting
        // This is the actual QUIC CID that can be used for qlog/mlog correlation
        let orig_dst_cid = conn.orig_dst_cid();
        let connection_id_hex = orig_dst_cid.to_string();

        // Configure per-connection qlog if enabled
        let mut conn = if let Some(qlog_dir) = qlog_dir {
            // Create qlog file path using connection ID
            let qlog_path = qlog_dir.join(format!("{}_server.qlog", connection_id_hex));

            // Create transport config with our standard settings plus qlog
            let mut transport = build_transport_config();

            let file = File::create(&qlog_path).context("failed to create qlog file")?;
            let writer = BufWriter::new(file);

            let mut qlog = quinn::QlogConfig::default();
            qlog.writer(Box::new(writer))
                .title(Some("moq-relay".into()));
            transport.qlog_stream(qlog.into_stream());

            // Create custom server config with qlog-enabled transport
            let mut server_config = (*base_server_config).clone();
            server_config.transport_config(Arc::new(transport));

            tracing::debug!(
                "qlog enabled: cid={} path={}",
                connection_id_hex,
                qlog_path.display()
            );

            // Accept with custom config
            conn.accept_with(Arc::new(server_config))?
        } else {
            // No qlog - use default config
            conn.accept()?
        };

        let handshake = conn
            .handshake_data()
            .await?
            .downcast::<quinn::crypto::rustls::HandshakeData>()
            .unwrap();

        let alpn = handshake.protocol.context("missing ALPN")?;
        let alpn = String::from_utf8_lossy(&alpn);
        let server_name = handshake.server_name.unwrap_or_default();

        tracing::debug!(
            "received QUIC handshake: cid={} ip={} alpn={} server={}",
            connection_id_hex,
            conn.remote_address(),
            alpn,
            server_name,
        );

        // Wait for the QUIC connection to be established.
        let conn = conn.await.context("failed to establish QUIC connection")?;

        tracing::debug!(
            "established QUIC connection: cid={} stable_id={} ip={} alpn={} server={}",
            connection_id_hex,
            conn.stable_id(),
            conn.remote_address(),
            alpn,
            server_name,
        );

        let alpn_bytes = alpn.as_bytes();
        let session = if alpn_bytes == web_transport_quinn::ALPN.as_bytes() {
            // Wait for the WebTransport CONNECT request (includes H3 SETTINGS exchange).
            let request = web_transport_quinn::Request::accept(conn)
                .await
                .context("failed to receive WebTransport request")?;

            // Accept the CONNECT request.
            request
                .ok()
                .await
                .context("failed to respond to WebTransport request")?
        } else if alpn_bytes == moq_transport::setup::ALPN {
            // Raw QUIC mode — create a "fake" WebTransport session with no H3 framing.
            let request = url::Url::parse("moqt://localhost").unwrap();
            web_transport_quinn::Session::raw(
                conn,
                request,
                web_transport_quinn::proto::ConnectResponse::default(),
            )
        } else {
            anyhow::bail!("unsupported ALPN: {}", alpn)
        };

        Ok((session.into(), connection_id_hex))
    }

    pub fn local_addr(&self) -> anyhow::Result<net::SocketAddr> {
        self.quic
            .local_addr()
            .context("failed to get local address")
    }
}

#[derive(Clone)]
pub struct Client {
    quic: quinn::Endpoint,
    config: rustls::ClientConfig,
    transport: Arc<quinn::TransportConfig>,
}

impl Client {
    /// Returns the local address of the QUIC socket.
    pub fn local_addr(&self) -> anyhow::Result<net::SocketAddr> {
        self.quic
            .local_addr()
            .context("failed to get local address")
    }

    /// Returns the address family of the local QUIC socket.
    pub fn address_family(&self) -> anyhow::Result<AddressFamily> {
        let local_addr = self
            .quic
            .local_addr()
            .context("failed to get local socket address")?;

        if local_addr.is_ipv4() {
            Ok(AddressFamily::Ipv4)
        } else if cfg!(target_os = "linux") {
            Ok(AddressFamily::Ipv6DualStack)
        } else {
            Ok(AddressFamily::Ipv6)
        }
    }

    pub async fn connect(
        &self,
        url: &Url,
        socket_addr: Option<net::SocketAddr>,
    ) -> anyhow::Result<(web_transport::Session, String)> {
        let mut config = self.config.clone();

        // TODO support connecting to both ALPNs at the same time
        config.alpn_protocols = vec![match url.scheme() {
            "https" => web_transport_quinn::ALPN.as_bytes().to_vec(),
            "moqt" => moq_transport::setup::ALPN.to_vec(),
            _ => anyhow::bail!("url scheme must be 'https' or 'moqt'"),
        }];

        config.key_log = Arc::new(rustls::KeyLogFile::new());

        let config: quinn::crypto::rustls::QuicClientConfig = config.try_into()?;
        let mut config = quinn::ClientConfig::new(Arc::new(config));
        config.transport_config(self.transport.clone());

        // Capture the initial destination CID that will be sent to the server
        // This is the CID used for qlog/mlog correlation on the server side
        let cid_capture: Arc<Mutex<Option<quinn::ConnectionId>>> = Arc::new(Mutex::new(None));
        let cid_capture_clone = cid_capture.clone();
        config.initial_dst_cid_provider(Arc::new(move || {
            // Generate a random CID (Quinn's default behavior)
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let random_bytes: [u8; 16] = rng.gen();
            let cid = quinn::ConnectionId::new(&random_bytes);
            *cid_capture_clone.lock().unwrap() = Some(cid);
            cid
        }));

        let host = match url.host().context("missing host")? {
            url::Host::Domain(d) => d.to_string(),
            url::Host::Ipv4(ip) => ip.to_string(),
            url::Host::Ipv6(ip) => ip.to_string(), // No brackets
        };
        let port = url.port().unwrap_or(443);

        // Look up the DNS entry and filter by socket address family.
        let addr = match socket_addr {
            Some(addr) => addr,
            None => {
                // Default DNS resolution logic
                self.resolve_dns(&host, port, self.address_family()?)
                    .await?
            }
        };

        let connection = self.quic.connect_with(config, addr, &host)?.await?;

        // Extract the CID that was used
        let connection_id_hex = cid_capture
            .lock()
            .unwrap()
            .as_ref()
            .context("CID not captured")?
            .to_string();

        let session = match url.scheme() {
            "https" => web_transport_quinn::Session::connect(connection, url.clone()).await?,
            "moqt" => web_transport_quinn::Session::raw(
                connection,
                url.clone(),
                web_transport_quinn::proto::ConnectResponse::default(),
            ),
            _ => unreachable!(),
        };

        Ok((session.into(), connection_id_hex))
    }

    /// Default DNS resolution logic that filters results by address family.
    async fn resolve_dns(
        &self,
        host: &str,
        port: u16,
        address_family: AddressFamily,
    ) -> anyhow::Result<net::SocketAddr> {
        let local_addr = self.local_addr()?;

        // Collect all DNS results
        let addrs: Vec<net::SocketAddr> = match Self::parse_socket_addr(host, port) {
            Ok(addr) => {
                vec![addr]
            }
            Err(_) => tokio::net::lookup_host((host, port))
                .await
                .context("failed DNS lookup")?
                .collect(),
        };

        if addrs.is_empty() {
            anyhow::bail!("DNS lookup for host '{}' returned no addresses", host);
        }

        // Log all DNS results for debugging
        tracing::debug!(
            "DNS lookup for {}, family {:?}: found {} results",
            host,
            address_family,
            addrs.len()
        );
        for (i, addr) in addrs.iter().enumerate() {
            tracing::debug!(
                "  DNS[{}]: {} ({})",
                i,
                addr,
                if addr.is_ipv4() { "IPv4" } else { "IPv6" }
            );
        }

        // Filter DNS results to match our local socket's address family
        let compatible_addr = match address_family {
            AddressFamily::Ipv4 => {
                // IPv4 socket: filter to IPv4 addresses
                addrs
                    .iter()
                    .find(|a| a.is_ipv4())
                    .cloned()
                    .context(format!(
                        "No IPv4 address found for host '{}' (local socket is IPv4: {})",
                        host, local_addr
                    ))?
            }
            AddressFamily::Ipv6DualStack => {
                // IPv6 socket on Linux: dual-stack, use first result
                tracing::debug!(
                    "Using first DNS result (Linux IPv6 dual-stack): {}",
                    addrs[0]
                );
                addrs[0]
            }
            AddressFamily::Ipv6 => {
                // IPv6 socket non-Linux: filter to IPv6 addresses
                addrs
                    .iter()
                    .find(|a| a.is_ipv6())
                    .cloned()
                    .context(format!(
                        "No IPv6 address found for host '{}' (local socket is IPv6: {})",
                        host, local_addr
                    ))?
            }
        };

        tracing::debug!(
            "Connecting from {} to {} (selected from {} DNS results)",
            local_addr,
            compatible_addr,
            addrs.len()
        );

        Ok(compatible_addr)
    }

    fn parse_socket_addr(host: &str, port: u16) -> Result<net::SocketAddr, net::AddrParseError> {
        let host = format!("{}:{}", host, port);
        host.parse::<net::SocketAddr>()
    }
}
