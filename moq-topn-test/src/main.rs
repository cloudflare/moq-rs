//! End-to-end test driver for MOQ TRACK_FILTER (Top-N) functionality.
//!
//! Supports two modes:
//! - `sim` (default): Simulation mode using TopNTracker directly in-memory
//! - `e2e`: End-to-end mode connecting to a real relay over QUIC/WebTransport
//!
//! Simulates realistic speech activity patterns with multiple publishers
//! and verifies that subscribers with TRACK_FILTER receive the correct tracks.

mod e2e;
mod sim;
mod speech;
mod stats;
mod viz;

use clap::{Parser, ValueEnum};
use tracing::info;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TestMode {
    /// Simulation mode - uses TopNTracker directly (no network)
    Sim,
    /// End-to-end mode - connects to a real relay
    E2e,
}

#[derive(Parser, Clone)]
#[command(name = "moq-topn-test")]
#[command(about = "End-to-end test driver for MOQ Top-N filtering")]
pub struct Args {
    /// Test mode: sim (simulation) or e2e (end-to-end with relay)
    #[arg(short, long, default_value = "sim")]
    pub mode: TestMode,

    /// Relay URL for e2e mode (e.g., https://localhost:4443)
    #[arg(short, long, default_value = "https://localhost:4443")]
    pub relay: String,

    /// Number of publishers (X)
    #[arg(short = 'x', long, default_value = "10")]
    pub publishers: usize,

    /// Number of subscribers
    #[arg(short = 'y', long, default_value = "5")]
    pub subscribers: usize,

    /// Top-N filter value for subscribers
    #[arg(short = 'n', long, default_value = "3")]
    pub top_n: u8,

    /// Mixed top-N values (comma-separated, e.g. "1,10,25,45,65,77,85")
    /// When set, subscribers cycle through these N values instead of using --top-n
    #[arg(long)]
    pub mixed_topn: Option<String>,

    /// Test duration in seconds
    #[arg(short, long, default_value = "30")]
    pub duration: u64,

    /// Group interval in milliseconds (2000 for viz, 33 for 30Hz perf tests)
    #[arg(long, default_value = "2000")]
    pub group_interval_ms: u64,

    /// Connection batch size (connections established per batch during setup)
    #[arg(long, default_value = "50")]
    pub connection_batch_size: usize,

    /// Namespace for the test
    #[arg(long, default_value = "topn-test")]
    pub namespace: String,

    /// Tie-breaking policy: "oldest" or "recent"
    #[arg(long, default_value = "oldest")]
    pub tie_break: String,

    /// Staleness timeout in seconds (0 = disabled)
    #[arg(long, default_value = "10")]
    pub staleness_timeout: u64,

    /// TLS options (for e2e mode)
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Output path for timeline visualization SVG
    #[arg(long)]
    pub viz_output: Option<String>,

    /// Disable TOPN_EVENT logging for visualization
    #[arg(long)]
    pub no_topn_log: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize logging
    let filter = if args.verbose {
        "moq_topn_test=debug,moq_transport=debug"
    } else {
        "moq_topn_test=info,moq_transport=warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .init();

    info!("MOQ Top-N Test Driver");
    info!("=====================");
    info!("Mode: {:?}", args.mode);
    if args.mode == TestMode::E2e {
        info!("Relay: {}", args.relay);
    }
    info!("Publishers (X): {}", args.publishers);
    info!("Subscribers (Y): {}", args.subscribers);
    info!("Top-N filter: {}", args.top_n);
    if let Some(ref mixed) = args.mixed_topn {
        info!("Mixed top-N: {}", mixed);
    }
    info!("Duration: {}s", args.duration);
    info!("Group interval: {}ms", args.group_interval_ms);
    info!("Tie-break policy: {}", args.tie_break);
    info!(
        "Staleness timeout: {}",
        if args.staleness_timeout == 0 {
            "disabled".to_string()
        } else {
            format!("{}s", args.staleness_timeout)
        }
    );
    info!("");

    match args.mode {
        TestMode::Sim => sim::run(args).await,
        TestMode::E2e => e2e::run(args).await,
    }
}
