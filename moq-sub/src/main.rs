// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{net, num::NonZeroU64};

use anyhow::Context;
use clap::{Parser, Subcommand};
use url::Url;

use moq_native_ietf::quic;
use moq_sub::media::{FetchMode, Media};
use moq_transport::{
    coding::{Location, TrackNamespace, VarInt},
    serve::Tracks,
    session::JoiningStart,
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
    let fetch = config.fetch_mode();
    let tls = config.tls.load()?;
    let quic = quic::Endpoint::new(quic::Config::new(config.bind, None, tls)?)?;

    let (session, connection_id, transport) = quic.client.connect(&config.url, None).await?;

    tracing::info!(
        "connected with CID: {} (use this to look up qlog/mlog on server)",
        connection_id
    );

    // TODO(itzmanish): When SessionId becomes mandatory in the next breaking API, make
    // `connect` accept it and remove `connect_with_session_id`.
    let (session, subscriber) = moq_transport::session::Subscriber::connect_with_session_id(
        session,
        moq_transport::session::SessionId::new(connection_id.clone()),
        transport,
    )
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

    #[command(subcommand)]
    pub command: Option<Command>,
}

impl Config {
    fn fetch_mode(&self) -> Option<FetchMode> {
        match self.command.as_ref()? {
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
        }
    }
}

#[derive(Clone, Subcommand)]
pub enum Command {
    /// Fetch saved media, optionally joining the live edge.
    Fetch {
        #[command(subcommand)]
        mode: FetchCommand,
    },
}

#[derive(Clone, Subcommand)]
pub enum FetchCommand {
    /// Fetch a finite location range and exit.
    Standalone {
        /// First location as decimal GROUP:OBJECT.
        #[arg(value_parser = parse_location)]
        start: Location,

        /// End as GROUP:OBJECT; OBJECT 0 includes the entire end group.
        #[arg(value_parser = parse_location)]
        end: Location,
    },

    /// Fetch GROUPS groups through the live boundary, then continue live.
    Relative {
        /// Non-zero number of groups to fetch, including the boundary group.
        #[arg(value_parser = parse_nonzero_varint)]
        groups: NonZeroU64,
    },

    /// Fetch from GROUP through the live boundary, then continue live.
    Absolute {
        /// First group as a decimal QUIC variable-length integer.
        #[arg(value_parser = parse_varint)]
        group: u64,
    },
}

fn moq_url(s: &str) -> Result<Url, String> {
    let url = Url::try_from(s).map_err(|e| e.to_string())?;

    // Make sure the scheme is moq
    if url.scheme() != "https" && url.scheme() != "moqt" {
        return Err("url scheme must be https:// for WebTransport & moqt:// for QUIC".to_string());
    }

    Ok(url)
}

fn parse_location(value: &str) -> Result<Location, String> {
    let (group, object) = value
        .split_once(':')
        .ok_or_else(|| "location must use decimal GROUP:OBJECT syntax".to_string())?;
    Ok(Location::new(parse_varint(group)?, parse_varint(object)?))
}

fn parse_nonzero_varint(value: &str) -> Result<NonZeroU64, String> {
    NonZeroU64::new(parse_varint(value)?)
        .ok_or_else(|| "relative GROUPS must be non-zero".to_string())
}

fn parse_varint(value: &str) -> Result<u64, String> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("value must be an unsigned decimal integer".to_string());
    }
    let value = value
        .parse::<u64>()
        .map_err(|error| format!("invalid decimal integer: {error}"))?;
    VarInt::try_from(value)
        .map(u64::from)
        .map_err(|_| format!("value must not exceed {}", VarInt::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Config {
        Config::try_parse_from(args.iter().copied()).expect("CLI should parse")
    }

    #[test]
    fn live_is_the_default_mode() {
        let config = parse(&["moq-sub", "--name", "dev", "https://localhost:4443"]);

        assert_eq!(config.fetch_mode(), None);
    }

    #[test]
    fn parses_standalone_fetch_locations() {
        let config = parse(&[
            "moq-sub",
            "--name",
            "dev",
            "https://localhost:4443",
            "fetch",
            "standalone",
            "12:34",
            "56:78",
        ]);

        assert_eq!(
            config.fetch_mode(),
            Some(FetchMode::Standalone {
                start: Location::new(12, 34),
                end: Location::new(56, 78),
            })
        );
    }

    #[test]
    fn relative_group_count_maps_to_zero_based_offset() {
        let one = parse(&[
            "moq-sub",
            "--name",
            "dev",
            "https://localhost:4443",
            "fetch",
            "relative",
            "1",
        ]);
        let five = parse(&[
            "moq-sub",
            "--name",
            "dev",
            "https://localhost:4443",
            "fetch",
            "relative",
            "5",
        ]);

        assert_eq!(
            one.fetch_mode(),
            Some(FetchMode::Joining(JoiningStart::Relative(0)))
        );
        assert_eq!(
            five.fetch_mode(),
            Some(FetchMode::Joining(JoiningStart::Relative(4)))
        );
    }

    #[test]
    fn parses_absolute_joining_group() {
        let config = parse(&[
            "moq-sub",
            "--name",
            "dev",
            "https://localhost:4443",
            "fetch",
            "absolute",
            "42",
        ]);

        assert_eq!(
            config.fetch_mode(),
            Some(FetchMode::Joining(JoiningStart::Absolute(42)))
        );
    }

    #[test]
    fn rejects_zero_relative_groups_and_non_decimal_locations() {
        for args in [
            vec![
                "moq-sub",
                "--name",
                "dev",
                "https://localhost:4443",
                "fetch",
                "relative",
                "0",
            ],
            vec![
                "moq-sub",
                "--name",
                "dev",
                "https://localhost:4443",
                "fetch",
                "standalone",
                "0x1:2",
                "3:4",
            ],
        ] {
            assert!(Config::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn rejects_values_above_varint_max() {
        let too_large = (moq_transport::coding::VarInt::MAX.into_inner() + 1).to_string();
        let location = format!("{too_large}:0");

        assert!(Config::try_parse_from([
            "moq-sub",
            "--name",
            "dev",
            "https://localhost:4443",
            "fetch",
            "absolute",
            too_large.as_str(),
        ])
        .is_err());
        assert!(Config::try_parse_from([
            "moq-sub",
            "--name",
            "dev",
            "https://localhost:4443",
            "fetch",
            "standalone",
            location.as_str(),
            "1:0",
        ])
        .is_err());
    }
}
