use clap::Parser;
use moq_relay_ietf::{HttpControlPlane, RelayServer, RelayServerConfig};
use std::{net, path::PathBuf};
use url::Url;

#[derive(Parser, Clone)]
pub struct Cli {
    /// Listen on this address
    #[arg(long, default_value = "[::]:443")]
    pub bind: net::SocketAddr,

    /// The TLS configuration.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,

    /// Directory to write qlog files (one per connection)
    #[arg(long)]
    pub qlog_dir: Option<PathBuf>,

    /// Directory to write mlog files (one per connection)
    #[arg(long)]
    pub mlog_dir: Option<PathBuf>,

    /// Forward all announces to the provided server for authentication/routing.
    /// If not provided, the relay accepts every unique announce.
    #[arg(long)]
    pub announce: Option<Url>,

    /// The URL of the moq-api server in order to run a cluster.
    /// Must be used in conjunction with --node to advertise the origin
    #[arg(long)]
    pub api: Option<Url>,

    /// The hostname that we advertise to other origins.
    /// The provided certificate must be valid for this address.
    #[arg(long)]
    pub node: Option<Url>,

    /// Enable development mode.
    /// This hosts a HTTPS web server via TCP to serve the fingerprint of the certificate.
    #[arg(long)]
    pub dev: bool,

    /// Serve qlog files over HTTPS at /qlog/:cid
    /// Requires --dev to enable the web server. Only serves files by exact CID - no index.
    #[arg(long)]
    pub qlog_serve: bool,

    /// Serve mlog files over HTTPS at /mlog/:cid
    /// Requires --dev to enable the web server. Only serves files by exact CID - no index.
    #[arg(long)]
    pub mlog_serve: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    // Disable tracing so we don't get a bunch of Quinn spam.
    let tracer = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::set_global_default(tracer).unwrap();

    let cli = Cli::parse();
    let tls = cli.tls.load()?;

    if tls.server.is_none() {
        anyhow::bail!("missing TLS certificates");
    }

    // Create control plane if both API and node URLs are provided
    let control_plane = if let (Some(api_url), Some(node_url)) = (cli.api, cli.node.clone()) {
        Some(HttpControlPlane::new(api_url, node_url))
    } else {
        None
    };

    // Build relay server config
    let config = RelayServerConfig {
        bind: cli.bind,
        tls,
        qlog_dir: cli.qlog_dir,
        mlog_dir: cli.mlog_dir,
        announce: cli.announce,
        enable_dev_web: cli.dev,
        qlog_serve: cli.qlog_serve,
        mlog_serve: cli.mlog_serve,
    };

    let server = RelayServer::new(config, control_plane, cli.node)?;
    server.run().await
}
