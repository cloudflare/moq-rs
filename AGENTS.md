<!--
SPDX-FileCopyrightText: 2026 Cloudflare Inc.
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# Repository Guidance

## Scope And Constraints

- This repository targets MoQT `draft-ietf-moq-transport-18`.
- Core protocol code lives in `moq-transport`; relay behavior lives in `moq-relay-ietf`.
- `moq-test-client` is the interop test harness: it runs TAP-emitting scenarios against a relay. The `moq-test-*` scenarios implement the moq-test protocol (draft-afrind-moq-test) as self-publishing scoreboard tests that need no relay-side moq-test support. The `data-*` scenarios implement moq-interop-runner data-plane specs with the same generator: publisher-first, PUBLISH_NAMESPACE of a `("moq-test", <test-id>, <run-id>)` routing prefix, and a SUBSCRIBE for that prefix plus the scenario's moq-test tuple.
- `moq-relay-ietf` is production critical, so prefer small, tested changes over broad refactors.
- Avoid breaking public APIs, wire behavior, CLI flags, and operator workflows unless explicitly requested.
- Do not add `unsafe` code unless there is a clear performance or FFI need and the safety invariant is documented at the call site.
- Do not commit or directly edit draft spec text files (`docs/draft-*.txt`) unless explicitly requested; they are local protocol reference material.
- Avoid `unwrap()`, `expect()`, `panic!`, `todo!`, `unimplemented!`, and `dbg!` in production paths. Propagate errors or convert lock poisoning to an internal error instead.

## Commands

- Run `cargo fmt --all` after Rust changes.
- Run `cargo test -p moq-transport` for protocol changes.
- Run `cargo test -p moq-relay-ietf` or `cargo build -p moq-relay-ietf` for relay-only changes.
- Run `cargo build --workspace` when shared APIs, relay behavior, or test-client behavior changes.
- Consider `cargo clippy --workspace --all-targets` before larger changes or when review asks for lint coverage.
- List interop scenarios with `cargo run -p moq-test-client -- --list`.
- Run a scenario against a local relay with `cargo run -p moq-test-client -- --relay https://localhost:4443 --tls-disable-verify --test data-subgroup-basic` (any `--list` name works; omit `--test` to run them all).
- Run an ad-hoc moq-test tuple with `cargo run -p moq-test-client -- --relay https://localhost:4443 --tls-disable-verify --moq-test-tuple "moq-test-00/0/0/0/2/4/5/64/32/2/1/1/0/-1/-1/0"` (16 `/`-separated fields; blanks select defaults).
- When tests fail, diagnose from compiler/test output first and keep fixes scoped to the failing behavior.
- Run the fuzz harness from `moq-relay-ietf/` with nightly:
  `cargo +nightly fuzz run decode_setup_tokens fuzz/seeds/decode_setup_tokens -- -max_total_time=300`

## Draft-18 Terminology

- Use `PUBLISH_NAMESPACE` terminology in protocol code and docs; older docs and comments may call this Announce.
- `PublishedNamespace` represents an inbound `PUBLISH_NAMESPACE` received by a subscriber.
- `TrackNamespacePrefix` is for `SUBSCRIBE_NAMESPACE` prefixes and may be empty; `TrackNamespace` is full and non-empty.
- `REQUEST_OK`, `REQUEST_ERROR`, and `REQUEST_UPDATE` are shared request response/update messages.
- Requests (SUBSCRIBE, PUBLISH_NAMESPACE, etc.) are sent on individual bidirectional streams; responses on the same stream omit the Request ID field.
- `MAX_REQUEST_ID` and `REQUESTS_BLOCKED` do not exist in draft-18.

## Token Authorization

Sessions tagged `SessionInterface::Internal` are relay peers and skip token
authorization at setup and on every request. Their SETUP tokens must still be
decoded, with malformed tokens and forbidden alias directives rejected
(draft-18 section 10.2.2). Only an embedder that has authenticated the peer may
tag it internal; a source subnet or listener address does not authenticate a
peer on its own.

## Session Transport Seam

- Native sessions adapt `web_transport_trait::Session` through `session::transport::Session::new(backend, original_url)`. The embedder owns connection establishment and supplies truthful original URL metadata.
- Browser wasm builds retain the separate `session/transport_wasm.rs` adapter; native backend injection does not replace that path.
- External backends can name the transport trait and URL types through `moq_transport::reexports`.

## Outbound Connector Seam

- `moq_native_ietf::client::Connector` establishes a transport before MoQT SETUP.
- Preserve the original URL and its verification name, credentials and path; a direct destination address must not replace that name. Retain the original TLS/trust and bind policy rather than rebuilding it.
- Keep the borrowed connect future cancellation-safe: dropping it must release in-flight resources. Do not add detached drivers, retries or fallback inside the client wrapper.

## External Contributions

- Treat public GitHub mirror and community PRs with heightened scrutiny for protocol correctness, resource bounds, panic-free production paths, and safe peer-facing errors.
- Be careful with compatibility: do not rename public CLI flags or remove behavior solely for terminology cleanup without an explicit migration decision.
