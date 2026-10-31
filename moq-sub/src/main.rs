// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{net, num::NonZeroU64};

use anyhow::Context;
use clap::{Parser, Subcommand};
use url::Url;

use moq_native_ietf::quic;
use moq_sub::media::{FetchMode, JoiningStart, Media};
use moq_transport::{
    coding::{Location, TrackNamespace},
    serve::Tracks,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing with env filter (respects RUST_LOG environment variable)
    // Default to info level, but suppress quinn's verbose output
    //
    // Logs go to stderr so they can't corrupt the fMP4 byte stream this
    // binary writes to stdout (e.g. `moq-sub ... | ffplay -`).
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=warn")),
        )
        .init();

    let out = tokio::io::stdout();

    let config = Config::parse();
    let fetch = config.fetch_mode()?;
    let tls = config.tls.load()?;
    let quic = quic::Endpoint::new(quic::Config::new(config.bind, None, tls)?)?;

    let (session, connection_id, transport) = quic.client.connect(&config.url, None).await?;

    tracing::info!(
        "connected with CID: {} (use this to look up qlog/mlog on server)",
        connection_id
    );

    let (session, subscriber) = moq_transport::session::Subscriber::connect(session, transport)
        .await
        .context("failed to create MoQ Transport session")?;

    // Associate empty set of Tracks with provided namespace
    let tracks = Tracks::new(TrackNamespace::from_utf8_path(&config.name));

    let mut media = Media::new_with_fetch(subscriber, tracks, out, config.catalog, fetch).await?;

    tokio::select! {
        res = session.run() => res.context("session error")?,
        res = media.run() => res.context("media error")?,
    }

    Ok(())
}

#[derive(Parser, Clone)]
pub struct Config {
    /// Listen for UDP packets on the given address.
    #[arg(long, default_value = "[::]:0")]
    pub bind: net::SocketAddr,

    /// Connect to the given URL starting with https://
    #[arg(value_parser = moq_url)]
    pub url: Url,

    /// The name of the broadcast
    #[arg(long)]
    pub name: String,

    /// The TLS configuration.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,

    /// Request the catalog track (to get other track names)
    ///
    /// First download the track named ".catalog" to find out the
    /// track names to subscribe to.  Other parameters like video
    /// dimension are extracted from the tracks themselves.  Default:
    /// "0.mp4" for the init track, "{track_id}.m4s" for the rest.
    #[arg(long)]
    pub catalog: bool,

    /// Join with N previous groups (in addition to the boundary group).
    #[arg(long, value_parser = parse_varint)]
    pub join_groups: Option<u64>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

impl Config {
    fn fetch_mode(&self) -> anyhow::Result<Option<FetchMode>> {
        anyhow::ensure!(
            self.join_groups.is_none() || self.command.is_none(),
            "--join-groups conflicts with the fetch subcommand"
        );
        if let Some(groups) = self.join_groups {
            return Ok(Some(FetchMode::Joining(JoiningStart::Relative(groups))));
        }
        let Some(command) = self.command.as_ref() else {
            return Ok(None);
        };
        Ok(match command {
            Command::Fetch {
                mode: FetchCommand::Standalone { start, end },
            } => Some(FetchMode::Standalone {
                start: *start,
                end: *end,
            }),
            Command::Fetch {
                mode: FetchCommand::Relative { groups },
            } => Some(FetchMode::Joining(JoiningStart::Relative(groups.get() - 1))),
            Command::Fetch {
                mode: FetchCommand::Absolute { group },
            } => Some(FetchMode::Joining(JoiningStart::Absolute(*group))),
        })
    }
}

#[derive(Clone, Subcommand)]
pub enum Command {
    /// Fetch retained media, optionally continuing live.
    Fetch {
        #[command(subcommand)]
        mode: FetchCommand,
    },
}

#[derive(Clone, Subcommand)]
pub enum FetchCommand {
    /// Fetch a finite range and exit. End Object 0 includes the whole end group.
    Standalone {
        #[arg(value_parser = parse_location)]
        start: Location,
        #[arg(value_parser = parse_location)]
        end: Location,
    },
    /// Fetch GROUPS groups including the boundary group, then continue live.
    Relative {
        #[arg(value_parser = parse_nonzero_varint)]
        groups: NonZeroU64,
    },
    /// Fetch from GROUP through the live boundary, then continue live.
    Absolute {
        #[arg(value_parser = parse_varint)]
        group: u64,
    },
}

fn parse_location(value: &str) -> Result<Location, String> {
    let (group, object) = value
        .split_once(':')
        .ok_or("location must use decimal GROUP:OBJECT syntax")?;
    Ok(Location::new(parse_varint(group)?, parse_varint(object)?))
}

fn parse_nonzero_varint(value: &str) -> Result<NonZeroU64, String> {
    NonZeroU64::new(parse_varint(value)?).ok_or_else(|| "relative GROUPS must be non-zero".into())
}

fn parse_varint(value: &str) -> Result<u64, String> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err("expected unsigned decimal integer".into());
    }
    value.parse::<u64>().map_err(|err| err.to_string())
}

fn moq_url(s: &str) -> Result<Url, String> {
    let url = Url::try_from(s).map_err(|e| e.to_string())?;

    // Make sure the scheme is moq
    if url.scheme() != "https" && url.scheme() != "moqt" {
        return Err("url scheme must be https:// for WebTransport & moqt:// for QUIC".to_string());
    }

    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(extra: &[&str]) -> Config {
        Config::try_parse_from(
            ["moq-sub", "--name", "test", "https://localhost:4443"]
                .into_iter()
                .chain(extra.iter().copied()),
        )
        .unwrap()
    }

    #[test]
    fn fetch_commands_keep_published_group_count_semantics() {
        assert_eq!(config(&[]).fetch_mode().unwrap(), None);
        assert_eq!(
            config(&["fetch", "relative", "1"]).fetch_mode().unwrap(),
            Some(FetchMode::Joining(JoiningStart::Relative(0)))
        );
        assert_eq!(
            config(&["fetch", "relative", "5"]).fetch_mode().unwrap(),
            Some(FetchMode::Joining(JoiningStart::Relative(4)))
        );
        assert_eq!(
            config(&["--join-groups", "2"]).fetch_mode().unwrap(),
            Some(FetchMode::Joining(JoiningStart::Relative(2)))
        );
        assert_eq!(
            config(&["fetch", "absolute", "3"]).fetch_mode().unwrap(),
            Some(FetchMode::Joining(JoiningStart::Absolute(3)))
        );
        assert_eq!(
            config(&["fetch", "standalone", "0:1", "2:0"])
                .fetch_mode()
                .unwrap(),
            Some(FetchMode::Standalone {
                start: Location::new(0, 1),
                end: Location::new(2, 0)
            })
        );
        assert!(config(&["--join-groups", "2", "fetch", "relative", "3"])
            .fetch_mode()
            .is_err());
    }

    #[test]
    fn fetch_arguments_reject_zero_count_and_invalid_varints() {
        assert!(parse_nonzero_varint("0").is_err());
        for value in ["", "-1", "0x1", "18446744073709551616"] {
            assert!(parse_varint(value).is_err());
        }
        assert!(parse_location("2").is_err());
    }
}
