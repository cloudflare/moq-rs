// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use bytes::BytesMut;
use std::net;
use url::Url;

use anyhow::Context;
use clap::Parser;
use tokio::io::AsyncReadExt;

use moq_native_ietf::quic;
use moq_pub::Media;
use moq_transport::{coding::TrackNamespace, serve, session::{Publisher, encode_auth_token}};

#[derive(Parser, Clone)]
pub struct Cli {
    /// Listen for UDP packets on the given address.
    #[arg(long, default_value = "[::]:0")]
    pub bind: net::SocketAddr,

    /// Advertise this frame rate in the catalog (informational)
    // TODO auto-detect this from the input when not provided
    #[arg(long, default_value = "24")]
    pub fps: u8,

    /// Advertise this bit rate in the catalog (informational)
    // TODO auto-detect this from the input when not provided
    #[arg(long, default_value = "1500000")]
    pub bitrate: u32,

    /// Connect to the given URL starting with https://
    #[arg()]
    pub url: Url,

    /// The name of the broadcast
    #[arg(long)]
    pub name: String,

    /// The TLS configuration.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,

    /// C4M auth token string to include in CLIENT_SETUP.
    #[arg(long)]
    pub auth_token: Option<String>,

    /// Token type for the auth token (default: C4M 0x63346d).
    #[arg(long, default_value = "6501485")]
    pub auth_token_type: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing with env filter (respects RUST_LOG environment variable)
    // Default to info level, but suppress quinn's verbose output
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=warn")),
        )
        .init();

    let cli = Cli::parse();

    let (writer, _, reader) =
        serve::Tracks::new(TrackNamespace::from_utf8_path(&cli.name)).produce();
    let media = Media::new(writer)?;

    let tls = cli.tls.load()?;

    let quic = quic::Endpoint::new(moq_native_ietf::quic::Config::new(
        cli.bind,
        None,
        tls.clone(),
    )?)?;

    tracing::info!("connecting to relay: url={}", cli.url);
    let (session, connection_id, transport) = quic.client.connect(&cli.url, None).await?;

    tracing::info!(
        "connected with CID: {} (use this to look up qlog/mlog on server)",
        connection_id
    );

    let auth_raw = match &cli.auth_token {
        Some(token) => encode_auth_token(cli.auth_token_type, token.as_bytes()),
        None => vec![],
    };

    let (session, mut publisher) = Publisher::connect_with_auth(session, transport, auth_raw)
        .await
        .context("failed to create MoQ Transport publisher")?;

    tokio::select! {
        res = session.run() => res.context("session error")?,
        res = run_media(media) => {
            res.context("media error")?
        },
        res = publisher.announce(reader) => res.context("publisher error")?,
    }

    Ok(())
}

async fn run_media(mut media: Media) -> anyhow::Result<()> {
    let mut input = tokio::io::stdin();
    let mut buf = BytesMut::new();
    loop {
        input
            .read_buf(&mut buf)
            .await
            .context("failed to read from stdin")?;
        media.parse(&mut buf).context("failed to parse media")?;
    }
}
