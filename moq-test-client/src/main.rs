//! MoQT Interop Test Client
//!
//! A standardized test client for MoQT interoperability testing.
//! This tool can run various test scenarios against a MoQT relay to verify
//! protocol compliance and interoperability.
//!
//! ## Usage
//!
//! ```bash
//! # Run all tests against a relay
//! moq-test-client --relay https://localhost:4443
//!
//! # Run a specific test
//! moq-test-client --relay https://localhost:4443 --test setup-only
//!
//! # List available tests
//! moq-test-client --list
//! ```

use std::net;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::{Parser, ValueEnum};
use url::Url;

mod scenarios;

/// MoQT Interop Test Client
#[derive(Parser, Clone)]
#[command(name = "moq-test-client")]
#[command(about = "MoQT Interoperability Test Client", long_about = None)]
pub struct Args {
    /// Relay URL to test against (e.g., https://localhost:4443)
    #[arg(
        short,
        long,
        default_value = "https://localhost:4443",
        env = "RELAY_URL"
    )]
    pub relay: Url,

    /// Specific test to run (runs all if not specified)
    #[arg(short, long, env = "TESTCASE")]
    pub test: Option<TestCase>,

    /// List available test cases and exit
    #[arg(short, long)]
    pub list: bool,

    /// Listen for UDP packets on the given address
    #[arg(long, default_value = "[::]:0")]
    pub bind: net::SocketAddr,

    /// The TLS configuration
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,

    /// Enable verbose output
    #[arg(short, long, env = "VERBOSE")]
    pub verbose: bool,
}

/// Available test cases
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TestCase {
    /// T0.1: Connect, complete SETUP exchange, close gracefully
    SetupOnly,
    /// T0.2: Connect, announce namespace, receive OK, close
    AnnounceOnly,
    /// T0.3: Subscribe to non-existent track, expect error
    SubscribeError,
    /// T0.4: Publisher announces, subscriber subscribes, verify handshake
    AnnounceSubscribe,
    /// T0.5: Subscriber subscribes before publisher announces
    SubscribeBeforeAnnounce,
    /// T0.6: Announce namespace, receive OK, send PUBLISH_NAMESPACE_DONE
    PublishNamespaceDone,
}

impl TestCase {
    fn all() -> Vec<TestCase> {
        vec![
            TestCase::SetupOnly,
            TestCase::AnnounceOnly,
            TestCase::SubscribeError,
            TestCase::AnnounceSubscribe,
            TestCase::SubscribeBeforeAnnounce,
            TestCase::PublishNamespaceDone,
        ]
    }

    fn name(&self) -> &'static str {
        match self {
            TestCase::SetupOnly => "setup-only",
            TestCase::AnnounceOnly => "announce-only",
            TestCase::SubscribeError => "subscribe-error",
            TestCase::AnnounceSubscribe => "announce-subscribe",
            TestCase::SubscribeBeforeAnnounce => "subscribe-before-announce",
            TestCase::PublishNamespaceDone => "publish-namespace-done",
        }
    }
}

/// Result of running a test case
#[derive(Debug)]
pub struct TestResult {
    pub test_case: TestCase,
    pub passed: bool,
    pub duration: Duration,
    pub message: Option<String>,
    pub cids: Vec<String>,
}

impl TestResult {
    fn success(test_case: TestCase, duration: Duration, cids: Vec<String>) -> Self {
        Self {
            test_case,
            passed: true,
            duration,
            message: None,
            cids,
        }
    }

    fn failure(test_case: TestCase, duration: Duration, message: String) -> Self {
        Self {
            test_case,
            passed: false,
            duration,
            message: Some(message),
            cids: Vec::new(),
        }
    }
}

/// Run a single test case
async fn run_test(args: &Args, test_case: TestCase) -> TestResult {
    let start = Instant::now();

    let result = match test_case {
        TestCase::SetupOnly => scenarios::test_setup_only(args).await,
        TestCase::AnnounceOnly => scenarios::test_announce_only(args).await,
        TestCase::SubscribeError => scenarios::test_subscribe_error(args).await,
        TestCase::AnnounceSubscribe => scenarios::test_announce_subscribe(args).await,
        TestCase::SubscribeBeforeAnnounce => scenarios::test_subscribe_before_announce(args).await,
        TestCase::PublishNamespaceDone => scenarios::test_publish_namespace_done(args).await,
    };

    let duration = start.elapsed();

    match result {
        Ok(cids) => TestResult::success(test_case, duration, cids.cids),
        Err(e) => TestResult::failure(test_case, duration, format!("{:#}", e)),
    }
}

fn print_tap_result(test_number: usize, result: &TestResult, verbose: bool) {
    let status = if result.passed { "ok" } else { "not ok" };
    let name = result.test_case.name();
    println!("{} {} - {}", status, test_number, name);

    // YAML diagnostic block
    println!("  ---");
    println!("  duration_ms: {}", result.duration.as_millis());

    // Connection IDs for mlog correlation
    match result.cids.len() {
        0 => {}
        1 => println!("  connection_id: {}", result.cids[0]),
        2 => {
            // Multi-connection tests: first is publisher, second is subscriber
            // (except subscribe-before-announce where subscriber connects first)
            if result.test_case == TestCase::SubscribeBeforeAnnounce {
                println!("  subscriber_connection_id: {}", result.cids[0]);
                println!("  publisher_connection_id: {}", result.cids[1]);
            } else {
                println!("  publisher_connection_id: {}", result.cids[0]);
                println!("  subscriber_connection_id: {}", result.cids[1]);
            }
        }
        _ => {
            // More than 2 CIDs - just list them all
            for (i, cid) in result.cids.iter().enumerate() {
                println!("  connection_id_{}: {}", i + 1, cid);
            }
        }
    }

    // Error message for failed tests
    if let Some(ref msg) = result.message {
        // Escape quotes and newlines for YAML string
        let escaped = if verbose {
            msg.replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
        } else {
            // Non-verbose: just first line
            msg.lines()
                .next()
                .unwrap_or(msg)
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
        };
        println!("  message: \"{}\"", escaped);
    }

    println!("  ...");
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // Disable tracing so we don't get a bunch of Quinn spam
    let tracer = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::WARN)
        .finish();
    // Ignore error if subscriber is already set (e.g., in tests)
    let _ = tracing::subscriber::set_global_default(tracer);

    let args = Args::parse();

    // List tests and exit if requested
    // Output one identifier per line for machine parsing (per TEST-CLIENT-INTERFACE.md)
    if args.list {
        for tc in TestCase::all() {
            println!("{}", tc.name());
        }
        return Ok(());
    }

    let tests_to_run = match args.test {
        Some(tc) => vec![tc],
        None => TestCase::all(),
    };

    // TAP version 14 header with run-level comments
    println!("TAP version 14");
    println!("# moq-test-client v{}", env!("CARGO_PKG_VERSION"));
    println!("# Relay: {}", args.relay);
    println!("1..{}", tests_to_run.len());

    let mut failed = 0;

    for (i, test_case) in tests_to_run.iter().enumerate() {
        let result = run_test(&args, *test_case).await;
        print_tap_result(i + 1, &result, args.verbose);

        if !result.passed {
            failed += 1;
        }
    }

    if failed == 0 {
        Ok(())
    } else {
        std::process::exit(1);
    }
}
