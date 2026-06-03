use clap::Parser;
use std::net;
use url::Url;

#[derive(Parser, Clone)]
pub struct Cli {
    /// Listen for UDP packets on the given address.
    #[arg(long, default_value = "[::]:0")]
    pub bind: net::SocketAddr,

    /// Connect to the given URL starting with https://
    #[arg()]
    pub url: Url,

    /// The TLS configuration.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,

    /// Publish the current time to the relay, otherwise only subscribe.
    #[arg(long)]
    pub publish: bool,

    /// The name of the clock track.
    #[arg(long, default_value = "clock")]
    pub namespace: String,

    /// The name of the clock track.
    #[arg(long, default_value = "now")]
    pub track: String,

    /// Enable sending of TRACK_STATUS before Subscribe for testing purposes only.
    /// Only works if publish is false.
    #[arg(long)]
    pub track_status: bool,

    /// Use datagrams instead of streams for the clock publisher.
    #[arg(long)]
    pub datagrams: bool,
}
