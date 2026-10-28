// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::fmt::Write;

use anyhow::Result;
use moq_transport::session::Transport;

use crate::{registry::Entry, run::RunResult};

pub fn render(entry: &Entry, result: &RunResult) -> Result<String> {
    let mut output = format!(
        "TAP version 14\n# moq-test-relay v{}\n# Test: {}\n",
        env!("CARGO_PKG_VERSION"),
        entry.name,
    );
    if let Some(variant) = entry.variant {
        writeln!(output, "# Variant: {variant}")?;
    }
    let status = if result.message.is_none() {
        "ok"
    } else {
        "not ok"
    };
    write!(output, "1..1\n{status} 1 - {}", entry.name)?;
    if result.message.is_none() {
        if let Some(skip) = &result.skip {
            write!(output, " # SKIP {}", comment_line(skip))?;
        }
    }
    writeln!(output, "\n  ---")?;
    writeln!(output, "  duration_ms: {}", result.duration.as_millis())?;
    if let Some(variant) = entry.variant {
        writeln!(output, "  variant: {}", serde_json::to_string(variant)?)?;
    }
    if let Some(message) = &result.message {
        writeln!(output, "  message: {}", serde_json::to_string(message)?)?;
    }
    if !result.sessions.is_empty() {
        writeln!(output, "  sessions:")?;
    }
    let mut unassigned = 0;
    for session in &result.sessions {
        let role = match session.role {
            Some(role) => role.to_owned(),
            None => {
                unassigned += 1;
                format!("unassigned_{unassigned}")
            }
        };
        let transport = match session.transport {
            Transport::RawQuic => "quic",
            Transport::WebTransport => "webtransport-h3",
        };
        writeln!(output, "    {role}:")?;
        writeln!(
            output,
            "      moqt_version: {}",
            serde_json::to_string(&session.version)?
        )?;
        writeln!(
            output,
            "      transport: {}",
            serde_json::to_string(transport)?
        )?;
        writeln!(
            output,
            "      quic_initial_destination_connection_id: {}",
            serde_json::to_string(&session.connection_id)?
        )?;
    }
    writeln!(output, "  ...")?;
    for comment in &result.comments {
        writeln!(output, "# {}", comment_line(comment))?;
    }
    Ok(output)
}

fn comment_line(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn failure_without_sessions_is_tap_with_a_quoted_yaml_message() {
        let entry = &crate::registry::ENTRIES[0];
        let result = RunResult {
            duration: Duration::ZERO,
            sessions: Vec::new(),
            message: Some("no client session\n\"quoted\" \\ path\r\t".to_owned()),
            skip: None,
            comments: Vec::new(),
        };
        assert_eq!(render(entry, &result).unwrap(), format!("TAP version 14\n# moq-test-relay v{}\n# Test: setup-only\n1..1\nnot ok 1 - setup-only\n  ---\n  duration_ms: 0\n  message: \"no client session\\n\\\"quoted\\\" \\\\ path\\r\\t\"\n  ...\n", env!("CARGO_PKG_VERSION")));
    }
}
