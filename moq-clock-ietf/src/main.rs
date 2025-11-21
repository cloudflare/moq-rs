use moq_native_ietf::quic;

use anyhow::Context;

mod cli;
mod clock;

use clap::Parser;
use cli::Cli;

use moq_transport::{
    coding::TrackNamespace,
    serve,
    session::{Publisher, Subscriber},
};

/// The main entry point for the MoQ Clock IETF example.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    // Disable tracing so we don't get a bunch of Quinn spam.
    let tracer = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::set_global_default(tracer).unwrap();

    let config = Cli::parse();
    let tls = config.tls.load()?;

    // Create the QUIC endpoint
    let quic = quic::Endpoint::new(quic::Config {
        bind: config.bind,
        qlog_dir: None,
        tls,
    })?;

    log::info!("connecting to server: url={}", config.url);

    // Connect to the server
    let (session, connection_id) = quic.client.connect(&config.url).await?;

    log::info!(
        "connected with CID: {} (use this to look up qlog/mlog on server)",
        connection_id
    );

    // Depending on whether we are publishing or subscribing, create the appropriate session
    if config.publish {
        // Create the publisher session
        let (session, mut publisher) = Publisher::connect(session)
            .await
            .context("failed to create MoQ Transport session")?;

        if config.datagrams {
            log::info!("publishing clock via datagrams");

            let (mut tracks_writer, _, tracks_reader) = serve::Tracks {
                namespace: TrackNamespace::from_utf8_path(&config.namespace),
            }
            .produce();

            let track_writer = tracks_writer.create(&config.track).unwrap();
            let clock_publisher = clock::Publisher::new_datagram(track_writer.datagrams()?);

            tokio::select! {
                res = session.run(None) => res.context("session error")?,
                res = clock_publisher.run() => res.context("clock error")?,
                res = publisher.announce(tracks_reader) => res.context("failed to serve tracks")?,
            }
        } else {
            log::info!("publishing clock via streams");

            let (mut tracks_writer, _, tracks_reader) = serve::Tracks {
                namespace: TrackNamespace::from_utf8_path(&config.namespace),
            }
            .produce();

            let track_writer = tracks_writer.create(&config.track).unwrap();
            let clock_publisher = clock::Publisher::new(track_writer.subgroups()?);

            tokio::select! {
                res = session.run(None) => res.context("session error")?,
                res = clock_publisher.run() => res.context("clock error")?,
                res = publisher.announce(tracks_reader) => res.context("failed to serve tracks")?,
            }
        }
    } else {
        // Create the subscriber session
        let (session, mut subscriber) = Subscriber::connect(session)
            .await
            .context("failed to create MoQ Transport session")?;

        let track_namespace = TrackNamespace::from_utf8_path(&config.namespace);

        if config.track_status {
            // Request a track_status for the clock track (testing purposes only)
            subscriber.track_status(&track_namespace, &config.track);
        }

        let (track_writer, track_reader) =
            serve::Track::new(track_namespace, config.track).produce();

        let clock_subscriber = clock::Subscriber::new(track_reader);

        tokio::select! {
            res = session.run(None) => res.context("session error")?,
            res = clock_subscriber.run() => res.context("clock error")?,
            res = subscriber.subscribe(track_writer) => res.context("failed to subscribe to track")?,
        }
    }

    Ok(())
}
