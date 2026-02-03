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

fn print_result(result: &TestResult, verbose: bool) {
    let status = if result.passed { "✓" } else { "✗" };
    let name = result.test_case.name();
    let duration_ms = result.duration.as_millis();

    // Format CIDs for display
    let cid_str = if result.cids.is_empty() {
        String::new()
    } else {
        format!(" [CID: {}]", result.cids.join(", "))
    };

    if result.passed {
        println!("{} {} ({} ms){}", status, name, duration_ms, cid_str);
    } else {
        println!("{} {} ({} ms){}", status, name, duration_ms, cid_str);
        if let Some(ref msg) = result.message {
            if verbose {
                println!("  Error: {}", msg);
            } else {
                // Show first line of error
                let first_line = msg.lines().next().unwrap_or(msg);
                println!("  Error: {}", first_line);
            }
        }
    }
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

    println!("MoQT Interop Test Client");
    println!("========================");
    println!("Relay: {}", args.relay);
    println!();

    let tests_to_run = match args.test {
        Some(tc) => vec![tc],
        None => TestCase::all(),
    };

    let mut passed = 0;
    let mut failed = 0;

    for test_case in tests_to_run {
        let result = run_test(&args, test_case).await;
        print_result(&result, args.verbose);

        if result.passed {
            passed += 1;
        } else {
            failed += 1;
        }
    }

    println!();
    println!("Results: {} passed, {} failed", passed, failed);

    // Standard test output for parsing
    if failed == 0 {
        println!("\nMOQT_TEST_RESULT: SUCCESS");
        Ok(())
    } else {
        println!("\nMOQT_TEST_RESULT: FAILURE");
        std::process::exit(1);
    }
}
