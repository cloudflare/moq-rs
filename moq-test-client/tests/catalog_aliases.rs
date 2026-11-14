// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::process::Command;

#[test]
fn catalog_aliases_parse_from_cli_and_environment_without_changing_list_names() {
    let aliases = [
        ("announce-only", "publish-namespace-only"),
        ("announce-subscribe", "publish-namespace-subscribe"),
        (
            "subscribe-before-announce",
            "subscribe-before-publish-namespace",
        ),
    ];
    for (alias, canonical) in aliases {
        for name in [alias, canonical] {
            for from_env in [false, true] {
                let mut command = Command::new(env!("CARGO_BIN_EXE_moq-test-client"));
                command.arg("--list").env_remove("TESTCASE");
                if from_env {
                    command.env("TESTCASE", name);
                } else {
                    command.args(["--test", name]);
                }
                let output = command.output().unwrap();
                assert!(
                    output.status.success(),
                    "{name}, from_env={from_env}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let list = String::from_utf8(output.stdout).unwrap();
                assert!(list.lines().any(|line| line == canonical));
                assert!(!list.lines().any(|line| line == alias));
            }
        }
    }
}
