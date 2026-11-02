// SPDX-FileCopyrightText: 2026 Cloudflare Inc., Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Reproduction: datagram tracks through a relay collapse to ~1 object/s after
//! a subscriber flips quickly through a few other datagram tracks.
//!
//! One process, three QUIC connections to the relay:
//! - **publisher**: announces `repro` with tracks `t0..t{N}`, each writing one
//!   datagram per tick at `--rate` Hz, forever;
//! - **switcher**: subscribes to `t0` and keeps it, then subscribes to `t1`,
//!   `t2`, `t3` in turn, `--dwell-ms` apart, dropping each one as the next
//!   starts (a viewer flipping through zoom levels);
//! - **bystander**: subscribes to `t0` only, and never changes anything.
//!
//! It prints how many `t0` objects each subscriber got per second.
//!
//! **Pre-fix behavior** (before `transport: never block the datagram receive
//! loop on an alias lookup`): the receive loop waited up to 1 s on any
//! datagram whose alias was unknown. Two cascading effects occurred:
//! 1. **switcher**: the relay's datagrams for a dropped track are still
//!    arriving (or queued) when the switcher forgets its alias. Each one blocks
//!    the switcher's whole session for 1 s, during which the next track's
//!    datagrams queue and that track is dropped too. Its `t0` falls to ~1/s.
//! 2. **bystander**: `--cache-idle-timeout` later, the relay releases its
//!    upstream subscriptions to `t1..t3`, `--dwell-ms` apart. The relay's own
//!    upstream session to the publisher then does exactly what the switcher's
//!    did, so `t0` falls to ~1/s for every subscriber, including the bystander.
//!
//! ```text
//! moq-relay-ietf --bind 127.0.0.1:4443 --tls-cert C --tls-key K --dev --cache-idle-timeout 1
//! cargo run -p moq-clock-ietf --example datagram_collapse -- https://localhost:4443 --tls-disable-verify
//! ```

use std::{
    net,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Context;
use clap::Parser;
use moq_native_ietf::quic;
use moq_transport::{
    coding::TrackNamespace,
    data::ObjectStatus,
    serve::{self, Datagram, TrackReaderMode},
    session::{Publisher, Subscriber},
};
use url::Url;

#[derive(Parser, Clone)]
struct Cli {
    #[arg(long, default_value = "0.0.0.0:0")]
    bind: net::SocketAddr,

    /// Relay URL, e.g. https://localhost:4443
    url: Url,

    #[command(flatten)]
    tls: moq_native_ietf::tls::Args,

    /// Tracks the switcher flips through after `t0`.
    #[arg(long, default_value_t = 3)]
    switch: usize,

    /// Milliseconds the switcher stays on each track.
    #[arg(long, default_value_t = 500)]
    dwell_ms: u64,

    /// Datagrams per second, per track. Range 1–1_000_000 (1_000_000 / rate must be ≥ 1 µs).
    #[arg(long, default_value_t = 15, value_parser = clap::value_parser!(u64).range(1..=1_000_000))]
    rate: u64,

    /// Payload bytes per datagram.
    #[arg(long, default_value_t = 1000)]
    payload: usize,

    /// Seconds to measure.
    #[arg(long, default_value_t = 15)]
    duration: u64,
}

const NAMESPACE: &str = "repro";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();
    let tls = cli.tls.load()?;
    let quic = quic::Endpoint::new(quic::Config::new(cli.bind, None, tls)?)?;

    // Publisher: every track written at a steady rate, subscribed or not.
    let (session, _, transport) = quic.client.connect(&cli.url, None).await?;
    let (pub_session, mut publisher) = Publisher::connect(session, transport)
        .await
        .context("publisher session")?;
    let (mut tracks_writer, _, tracks_reader) = serve::Tracks {
        namespace: TrackNamespace::from_utf8_path(NAMESPACE),
    }
    .produce();
    for i in 0..=cli.switch {
        let mut writer = tracks_writer
            .create(format!("t{i}"))
            .context("create track")?
            .datagrams()?;
        let (rate, payload) = (cli.rate, cli.payload);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_micros(1_000_000 / rate));
            for seq in 0u64.. {
                tick.tick().await;
                let datagram = Datagram {
                    group_id: seq / rate,
                    object_id: seq % rate,
                    priority: 127,
                    status: ObjectStatus::NormalObject,
                    end_of_group: false,
                    payload: vec![i as u8; payload].into(),
                    extension_headers: Default::default(),
                };
                if writer.write(datagram).is_err() {
                    return;
                }
            }
        });
    }
    tokio::spawn(async move {
        tokio::select! {
            res = pub_session.run() => tracing::error!("publisher session ended: {res:?}"),
            res = publisher.publish_namespace(tracks_reader) => tracing::error!("publish_namespace ended: {res:?}"),
        }
    });
    // Let PUBLISH_NAMESPACE reach the relay before anyone subscribes.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let switcher = connect_subscriber(&quic, &cli.url).await?;
    let bystander = connect_subscriber(&quic, &cli.url).await?;

    let switcher_t0 = Arc::new(AtomicU64::new(0));
    let bystander_t0 = Arc::new(AtomicU64::new(0));
    let unused = Arc::new(AtomicU64::new(0));
    let _a = subscribe(&switcher, 0, switcher_t0.clone());
    let _b = subscribe(&bystander, 0, bystander_t0.clone());

    let start = tokio::time::Instant::now();
    let mut current = None;
    for i in 1..=cli.switch {
        let next = subscribe(&switcher, i, unused.clone());
        if let Some(previous) = current.replace(next) {
            tokio::task::JoinHandle::abort(&previous);
        }
        tokio::time::sleep(Duration::from_millis(cli.dwell_ms)).await;
    }
    if let Some(last) = current {
        last.abort();
    }
    println!(
        "switcher flipped through t1..t{} in {:.1} s and dropped them all",
        cli.switch,
        start.elapsed().as_secs_f64()
    );

    println!(
        "sec  switcher_t0  bystander_t0   (expect {} per second)",
        cli.rate
    );
    let mut last = (0, 0);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.tick().await;
    let mut worst = u64::MAX;
    for sec in 1..=cli.duration {
        tick.tick().await;
        let now = (
            switcher_t0.load(Ordering::Relaxed),
            bystander_t0.load(Ordering::Relaxed),
        );
        let got = (now.0 - last.0, now.1 - last.1);
        worst = worst.min(got.1);
        println!("{sec:>3}  {:>11}  {:>12}", got.0, got.1);
        last = now;
    }
    println!(
        "bystander's worst second: {worst} objects ({})",
        if worst.saturating_mul(2) < cli.rate {
            "COLLAPSED"
        } else {
            "ok"
        }
    );
    Ok(())
}

async fn connect_subscriber(quic: &quic::Endpoint, url: &Url) -> anyhow::Result<Subscriber> {
    let (session, _, transport) = quic.client.connect(url, None).await?;
    let (session, subscriber) = Subscriber::connect(session, transport)
        .await
        .context("subscriber session")?;
    tokio::spawn(async move {
        let res = session.run().await;
        tracing::error!("subscriber session ended: {res:?}");
    });
    Ok(subscriber)
}

/// Subscribe to `t{i}`, counting its objects into `count`. Aborting the
/// returned task drops the subscription.
fn subscribe(
    subscriber: &Subscriber,
    i: usize,
    count: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    let mut subscriber = subscriber.clone();
    tokio::spawn(async move {
        let (writer, reader) =
            serve::Track::new(TrackNamespace::from_utf8_path(NAMESPACE), format!("t{i}")).produce();
        let read = async move {
            let TrackReaderMode::Datagrams(mut datagrams) = reader.mode().await? else {
                anyhow::bail!("t{i}: not datagrams");
            };
            while datagrams.read().await?.is_some() {
                count.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        };
        tokio::select! {
            res = subscriber.subscribe(writer) => tracing::warn!("t{i} subscribe ended: {res:?}"),
            res = read => tracing::warn!("t{i} reader ended: {res:?}"),
        }
    })
}
