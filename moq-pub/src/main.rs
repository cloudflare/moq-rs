use bytes::BytesMut;
use std::net;
use url::Url;

use anyhow::Context;
use clap::Parser;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::io::AsyncReadExt;

use moq_native_ietf::quic;
use moq_pub::Media;
use moq_transport::{
    coding::TrackNamespace,
    serve::{self, TracksReader},
    session::{PublishNamespace, Publisher, SessionError},
};

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

    /// Whether to publish to the catalog or wait for a subscribe
    /// aka PUSH based publisher
    #[arg(long, default_value = "false")]
    pub publish: bool,
}

async fn serve_subscriptions(
    mut publisher: Publisher,
    tracks: TracksReader,
    publish_ns: &PublishNamespace,
    publish: bool,
) -> Result<(), SessionError> {
    publish_ns.ok().await?;

    if publish {
        return publish_track(publisher, tracks).await;
    }
    let mut tasks: FuturesUnordered<futures::future::BoxFuture<'static, ()>> =
        FuturesUnordered::new();

    loop {
        tokio::select! {
            Some(subscribed) = publisher.subscribed() => {
                let info = subscribed.info.clone();
                let tracks = tracks.clone();
                tracing::info!("serving subscribe: {:?}", info);

                tasks.push(async move {
                    if let Err(err) = Publisher::serve_subscribe(subscribed, tracks).await {
                        tracing::warn!("failed serving subscribe: {:?}, error: {}", info, err);
                    }
                }.boxed());
            }
            _ = tasks.next(), if !tasks.is_empty() => {}
            else => return Ok(()),
        }
    }
}

async fn publish_track(mut publisher: Publisher, tracks: TracksReader) -> Result<(), SessionError> {
    let active_tracks = tracks.get_active_tracks();
    tracing::info!("publishing {} tracks", active_tracks.len());

    let mut tasks = FuturesUnordered::new();
    for track in active_tracks {
        let published = publisher.publish(&track).await?;
        let info = published.info.clone();
        tasks.push(async move {
            if let Err(err) = published.serve(track).await {
                tracing::warn!("failed serving publish: {:?}, error: {}", info, err);
            }
        });
    }

    while tasks.next().await.is_some() {}
    Ok(())
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
    ))?;

    tracing::info!("connecting to relay: url={}", cli.url);
    let (session, connection_id) = quic.client.connect(&cli.url, None).await?;

    tracing::info!(
        "connected with CID: {} (use this to look up qlog/mlog on server)",
        connection_id
    );

    let (session, publisher) = Publisher::connect(session)
        .await
        .context("failed to create MoQ Transport publisher")?;

    let namespace = reader.namespace.clone();

    let publish_ns = publisher
        .clone()
        .publish_namespace(namespace)
        .await
        .context("failed to register namespace")?;

    tracing::info!("namespace registered, starting media and subscription handling");

    tokio::select! {
        res = session.run() => res.context("session error")?,
        res = run_media(media) => res.context("media error")?,
        res = serve_subscriptions(publisher, reader, &publish_ns, cli.publish) => res.context("publisher error")?,
        res = publish_ns.closed() => res.context("namespace closed")?,
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
