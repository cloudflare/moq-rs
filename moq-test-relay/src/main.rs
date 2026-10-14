// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{io::Write, net::SocketAddr, path::PathBuf, process::ExitCode, time::Duration};

use anyhow::{Context, Result};
use clap::Parser;
use moq_native_ietf::{quic, tls};

mod registry;
mod routing;
mod run;
mod sessions;
mod tap;

#[cfg(test)]
mod scenarios_tests;

#[derive(Parser, Debug)]
#[command(about = "MoQT interop test relay")]
struct Args {
    /// Test identifier from --list
    #[arg(short, long, env = "TESTCASE")]
    test: Option<String>,
    /// Relay variant (an empty value selects no variant)
    #[arg(long, env = "TEST_VARIANT")]
    variant: Option<String>,
    /// UDP listening port
    #[arg(short, long, env = "MOQT_PORT", default_value_t = 4443)]
    port: u16,
    /// PEM certificate chain
    #[arg(long, default_value = "/certs/cert.pem")]
    cert: PathBuf,
    /// PEM private key
    #[arg(long, default_value = "/certs/priv.key")]
    key: PathBuf,
    /// List supported test and variant entries
    #[arg(short, long)]
    list: bool,
    /// Enable debug logs on stderr
    #[arg(short, long, env = "VERBOSE", action = clap::ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true", default_value = "false", value_parser = clap::builder::BoolishValueParser::new())]
    verbose: bool,
    /// Maximum wait for the first negotiated client session, in seconds
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
    first_session_wait: u64,
    /// Safety cap on serving after the verdict, in seconds
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    post_verdict_wait: u64,
}

async fn execute(args: Args) -> Result<u8> {
    if args.list {
        for entry in registry::ENTRIES {
            println!("{}", entry.identifier());
        }
        return Ok(0);
    }

    let entry = args
        .test
        .as_deref()
        .and_then(|name| registry::find(name, args.variant.as_deref()));
    let Some(entry) = entry else {
        eprintln!("unsupported test or variant; use --list for supported entries");
        return Ok(127);
    };

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_env_filter(if args.verbose {
            "debug"
        } else {
            "info,quinn=warn"
        })
        .try_init()
        .map_err(|err| anyhow::anyhow!("initialize logging: {err}"))?;

    // Raw QUIC's negotiated ALPN is not exposed by ConnInfo. A single supported
    // version makes it unambiguous; fail rather than guess if the library grows.
    anyhow::ensure!(
        moq_transport::setup::SUPPORTED_ALPNS.len() == 1,
        "raw QUIC version reporting needs the negotiated ALPN from ConnInfo"
    );
    let tls = tls::Args {
        cert: vec![args.cert],
        key: vec![args.key],
        ..Default::default()
    }
    .load()
    .context("load --cert and --key")?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install SIGTERM handler")?;

    let bind = SocketAddr::from(([0, 0, 0, 0], args.port));
    let endpoint = quic::Endpoint::new(quic::Config::new(bind, None, tls)?)?;
    let mut server = endpoint
        .server
        .context("TLS server configuration missing")?;
    tracing::info!(address = %server.local_addr()?, test = entry.name, "ready to accept sessions");
    let result = run::run(
        &mut server,
        entry,
        Duration::from_secs(args.first_session_wait),
        Duration::from_secs(args.post_verdict_wait),
        &mut terminate,
    )
    .await?;
    let rendered = tap::render(entry, &result)?;
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(rendered.as_bytes()).context("write TAP")?;
    stdout.flush().context("flush TAP")?;
    Ok(if result.message.is_none() { 0 } else { 1 })
}

#[tokio::main]
async fn main() -> ExitCode {
    match execute(Args::parse()).await {
        Ok(code) => ExitCode::from(code),
        Err(err) => {
            eprintln!("test relay could not run: {err:#}");
            ExitCode::from(2)
        }
    }
}
