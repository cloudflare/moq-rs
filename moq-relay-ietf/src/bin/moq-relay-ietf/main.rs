// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

mod api_coordinator;
mod file_coordinator;
mod scope_config;

use std::sync::Arc;
use std::{net, path::PathBuf};

use clap::Parser;
use url::Url;

use api_coordinator::{ApiCoordinator, ApiCoordinatorConfig};
use file_coordinator::FileCoordinator;
use moq_relay_ietf::{Coordinator, Relay, RelayConfig, SessionConfig, Web, WebConfig};
use std::time::Duration;

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

    /// Accepted for compatibility; ignored.
    ///
    /// Draft-18 removed MAX_REQUEST_ID, so there is no request budget to
    /// advertise in SETUP. The flag remains so existing invocations keep
    /// working.
    #[arg(long, default_value_t = 100)]
    pub max_request_id: u64,

    /// Seconds to keep a cached track with no subscribers before releasing its
    /// upstream subscription. 0 disables eviction, holding upstream
    /// subscriptions for the lifetime of the upstream session.
    #[arg(long, default_value_t = 30)]
    pub cache_idle_timeout: u64,

    /// Forward all PUBLISH_NAMESPACE messages to the provided server for auth/routing.
    /// If not provided, the relay accepts every unique namespace publish.
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

    /// Path to the shared coordinator file for multi-relay coordination.
    /// Multiple relay instances can share namespace/track registration via this file.
    /// User doesn't have to explicitly create and populate anything. This path will be
    /// used by file coordinator to store namespace/track registration information.
    /// User need to make sure if multiple relay's are being used all of them have same path
    /// to this file.
    #[arg(long, default_value = "/tmp/moq-coordinator.json")]
    pub coordinator_file: PathBuf,

    /// URL of the moq-api server for coordination (e.g., "http://localhost:8080").
    /// When specified, uses moq-api HTTP server instead of file-based coordination.
    /// This is useful when running a cluster of relays with a centralized API server.
    #[arg(long)]
    pub api_url: Option<Url>,

    /// TTL in seconds for namespace registrations in the API.
    /// Only used when --api-url is specified.
    #[arg(long, default_value = "600")]
    pub api_ttl: u64,

    /// Address to expose Prometheus metrics on (e.g., "127.0.0.1:9090").
    /// Requires the `metrics-prometheus` feature to be enabled.
    /// When set, serves metrics at http://<addr>/metrics
    #[arg(long)]
    pub metrics_addr: Option<net::SocketAddr>,

    /// JSON file of per-scope Common Access Token (CAT) policies.
    ///
    /// When set, scopes the file configures require a valid CAT in the
    /// SETUP AUTHORIZATION TOKEN option. The file is re-read for new
    /// sessions, so edits apply without a restart once a scope's cached
    /// policy expires. If the file later becomes missing or invalid, each
    /// scope keeps its cached state until it expires, then refuses new
    /// sessions rather than admitting them without a token. Requires a relay built with `--features auth-cat`.
    /// See the moq-relay-ietf README for the format.
    #[arg(long, env = "MOQ_RELAY_SCOPE_CONFIG")]
    pub scope_config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing with env filter (respects RUST_LOG environment variable)
    // Default to info level, but suppress quinn's verbose output
    //
    // Logs go to stderr, per convention and to keep stdout clean.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=warn")),
        )
        .init();

    let cli = Cli::parse();

    // Checked before anything else so a relay that cannot enforce the
    // requested policy never starts listening.
    let scope_config = scope_config::from_cli(cli.scope_config.as_deref())?;
    if scope_config.is_some()
        && (cli.api.is_some() || cli.api_url.is_some() || cli.announce.is_some())
    {
        // This binary has no connection tagger, so peer relays are public
        // sessions. They present no token, so in a scope that enforces tokens
        // they are refused like any client without one. Not fatal: scopes
        // with `"auth": null` work normally.
        tracing::warn!(
            "--scope-config is combined with multi-relay options (--api, --api-url or \
             --announce). This binary does not tag peer relays as internal, and they present \
             no token, so they are refused in every scope that enforces tokens; cross-relay delivery works only in scopes \
             with no token policy"
        );
    }

    // Initialize Prometheus metrics exporter if --metrics-addr is provided
    #[cfg(feature = "metrics-prometheus")]
    if let Some(metrics_addr) = cli.metrics_addr {
        use metrics_exporter_prometheus::PrometheusBuilder;

        // Configure histogram buckets for subscribe latency (1ms to 10s)
        let subscribe_latency_buckets = vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0,
        ];

        PrometheusBuilder::new()
            .with_http_listener(metrics_addr)
            .set_buckets_for_metric(
                metrics_exporter_prometheus::Matcher::Full(
                    "moq_relay_subscribe_latency_seconds".to_string(),
                ),
                &subscribe_latency_buckets,
            )?
            .install()
            .expect("failed to install Prometheus metrics exporter");

        // Register metric descriptions (shows as # HELP in Prometheus output)
        moq_relay_ietf::metrics::describe_metrics();

        tracing::info!(
            "metrics exporter listening on http://{}/metrics",
            metrics_addr
        );
    }

    #[cfg(not(feature = "metrics-prometheus"))]
    if cli.metrics_addr.is_some() {
        tracing::warn!(
            "--metrics-addr was provided but the metrics-prometheus feature is not enabled. \
             Rebuild with --features metrics-prometheus to enable the Prometheus exporter."
        );
    }

    let tls = cli.tls.load()?;

    if tls.server.is_none() {
        anyhow::bail!("missing TLS certificates");
    }

    // Determine qlog directory for both relay and web server
    let qlog_dir_for_relay = cli.qlog_dir.clone();
    let qlog_dir_for_web = if cli.qlog_serve {
        cli.qlog_dir.clone()
    } else {
        None
    };

    // Determine mlog directory for both relay and web server
    let mlog_dir_for_relay = cli.mlog_dir.clone();
    let mlog_dir_for_web = if cli.mlog_serve {
        cli.mlog_dir.clone()
    } else {
        None
    };

    // Build the relay URL from the node or bind address
    let relay_url = cli
        .node
        .clone()
        .unwrap_or_else(|| Url::parse(&format!("https://{}", cli.bind)).unwrap());

    // Create the coordinator based on CLI arguments
    // Priority: api-url > file coordinator
    let coordinator: Arc<dyn Coordinator> = if let Some(api_url) = &cli.api_url {
        let config = ApiCoordinatorConfig::new(api_url.clone(), relay_url).with_ttl(cli.api_ttl);
        let api_coordinator = ApiCoordinator::new(config).with_scope_config(scope_config);
        tracing::info!("using API coordinator");
        Arc::new(api_coordinator)
    } else {
        tracing::info!("using file coordinator: {}", cli.coordinator_file.display());
        Arc::new(
            FileCoordinator::new(&cli.coordinator_file, relay_url).with_scope_config(scope_config),
        )
    };

    // Create a QUIC server for media.
    let relay = Relay::new_with_cache_idle_timeout(
        RelayConfig {
            tls: tls.clone(),
            bind: Some(cli.bind),
            endpoints: vec![],
            qlog_dir: qlog_dir_for_relay,
            mlog_dir: mlog_dir_for_relay,
            node: cli.node,
            announce: cli.announce,
            coordinator,
            session: SessionConfig {
                max_request_id: cli.max_request_id,
            },
            // No connection tagger: the default binary treats every inbound
            // connection as a public client. Embedders that run relay-to-relay
            // meshes supply a tagger to mark internal peers.
            connection_tagger: None,
        },
        Duration::from_secs(cli.cache_idle_timeout),
    )?;

    if cli.dev {
        // Create a web server too.
        // Currently this only contains the certificate fingerprint (for development only).
        let web = Web::new(WebConfig {
            bind: cli.bind,
            tls,
            qlog_dir: qlog_dir_for_web,
            mlog_dir: mlog_dir_for_web,
        });

        tokio::spawn(async move {
            web.run().await.expect("failed to run web server");
        });
    }

    relay.run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_request_id_flag_overrides_default() {
        let cli = Cli::try_parse_from(["moq-relay-ietf", "--max-request-id", "7"]).unwrap();

        assert_eq!(cli.max_request_id, 7);
    }
}

/// End to end: a relay loading a policy from `--scope-config` admits a client
/// that presents a matching token in SETUP, and closes sessions that present
/// a token for another audience or none at all.
#[cfg(all(test, feature = "auth-cat"))]
mod scope_config_e2e {
    use std::time::Duration;

    use cat_token::{encode_token, CatToken, Es256Algorithm, MoqtAction, MoqtScopeBuilder};
    use moq_native_ietf::{quic, tls};
    use moq_relay_ietf::{Relay, RelayConfig, SessionConfig};
    use moq_transport::session::Session;
    use moq_transport::setup::{AuthorizationToken, CAT_TOKEN_TYPE};
    use p256::pkcs8::EncodePublicKey;

    use super::{scope_config, FileCoordinator};

    const ISSUER: &str = "https://issuer.example";
    const AUDIENCE: &str = "my-relay";

    struct Fixture {
        dir: std::path::PathBuf,
        signer: Es256Algorithm,
        relay_addr: std::net::SocketAddr,
        client: quic::Endpoint,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn tls_files(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let (cert_path, key_path) = (dir.join("tls.crt"), dir.join("tls.key"));
        std::fs::write(&cert_path, cert.cert.pem()).unwrap();
        std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();
        (cert_path, key_path)
    }

    async fn start() -> Fixture {
        let dir = std::env::temp_dir().join(format!("moq-scope-config-e2e-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let signer = Es256Algorithm::new_with_key_pair().unwrap();
        let public = signer
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        std::fs::write(dir.join("issuer.pub.pem"), public).unwrap();
        let config_path = dir.join("scope-config.json");
        std::fs::write(
            &config_path,
            serde_json::json!({ "default": { "auth": {
                "keys": [{ "kid": "k1", "alg": "ES256", "pem_file": "issuer.pub.pem" }],
                "issuers": [ISSUER],
                "audiences": [AUDIENCE],
            } } })
            .to_string(),
        )
        .unwrap();

        let (cert, key) = tls_files(&dir);
        let server_tls = tls::Args {
            cert: vec![cert],
            key: vec![key],
            root: Vec::new(),
            disable_verify: true,
        }
        .load()
        .unwrap();
        let endpoint = quic::Endpoint::new(
            quic::Config::new("127.0.0.1:0".parse().unwrap(), None, server_tls.clone()).unwrap(),
        )
        .unwrap();
        let relay_addr = endpoint.server.as_ref().unwrap().local_addr().unwrap();

        let source = scope_config::from_cli(Some(&config_path)).unwrap();
        let relay_url = url::Url::parse(&format!("https://{relay_addr}")).unwrap();
        let coordinator = std::sync::Arc::new(
            FileCoordinator::new(dir.join("coordinator.json"), relay_url).with_scope_config(source),
        );
        let relay = Relay::new(RelayConfig {
            tls: server_tls.clone(),
            bind: None,
            endpoints: vec![endpoint],
            qlog_dir: None,
            mlog_dir: None,
            node: None,
            announce: None,
            coordinator,
            session: SessionConfig::default(),
            connection_tagger: None,
        })
        .unwrap();
        // `Relay::run` is not `Send`, as in `main`; run it on a local set.
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let _ = rt.block_on(relay.run());
        });

        let client = quic::Endpoint::new(
            quic::Config::new("127.0.0.1:0".parse().unwrap(), None, server_tls).unwrap(),
        )
        .unwrap();

        Fixture {
            dir,
            signer,
            relay_addr,
            client,
        }
    }

    fn mint(signer: &Es256Algorithm, audience: &str) -> AuthorizationToken {
        let claims = CatToken::new()
            .with_issuer(ISSUER)
            .with_single_audience(audience)
            .with_expires_in(600)
            .with_moqt_scope(
                MoqtScopeBuilder::new()
                    .action(MoqtAction::ClientSetup)
                    .build(),
            )
            .with_moqt_scope(
                MoqtScopeBuilder::new()
                    .subscriber()
                    .namespace_prefix(b"clock")
                    .track_prefix(b"")
                    .build(),
            );
        AuthorizationToken::new(CAT_TOKEN_TYPE, encode_token(&claims, signer).unwrap())
    }

    /// Connect with the given tokens and report whether the relay kept the
    /// session open. A rejected session is closed by the relay right after
    /// SETUP, which surfaces as the session's run loop ending.
    async fn admitted(fixture: &Fixture, tokens: Vec<AuthorizationToken>) -> bool {
        let url = url::Url::parse(&format!(
            "https://localhost:{}/demo",
            fixture.relay_addr.port()
        ))
        .unwrap();
        let (session, _, transport) = fixture
            .client
            .client
            .connect(&url, Some(fixture.relay_addr))
            .await
            .unwrap();
        let session = match Session::connect_with_tokens(session, None, transport, tokens).await {
            Ok((session, _, _)) => session,
            // The relay may close before its SETUP reaches us.
            Err(_) => return false,
        };
        // Still running after the timeout: admitted. Any return means the
        // relay closed the session.
        tokio::time::timeout(Duration::from_secs(2), session.run())
            .await
            .is_err()
    }

    #[tokio::test]
    async fn the_relay_enforces_a_file_policy_end_to_end() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let fixture = start().await;

        assert!(
            admitted(&fixture, vec![mint(&fixture.signer, AUDIENCE)]).await,
            "a token minted for this relay must be admitted"
        );
        assert!(
            !admitted(&fixture, vec![mint(&fixture.signer, "another-relay")]).await,
            "a token for another audience must be refused"
        );
        assert!(
            !admitted(&fixture, Vec::new()).await,
            "a session without a token must be refused"
        );
    }
}
