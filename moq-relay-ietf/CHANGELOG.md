# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.10](https://github.com/cloudflare/moq-rs/compare/moq-relay-ietf-v0.7.9...moq-relay-ietf-v0.7.10) - 2026-01-29

### Other

- fix unnecessary_unwrap clippy lint

## [0.7.9](https://github.com/cloudflare/moq-rs/compare/moq-relay-ietf-v0.7.8...moq-relay-ietf-v0.7.9) - 2025-12-19

### Added

- use socket address from coordinator if available to connect
- bypass DNS lookup on relay URL

### Other

- better comment for url in NamespaceOrigin

## [0.7.8](https://github.com/cloudflare/moq-rs/compare/moq-relay-ietf-v0.7.7...moq-relay-ietf-v0.7.8) - 2025-12-18

### Other

- update Cargo.lock dependencies

## [0.7.7](https://github.com/cloudflare/moq-rs/compare/moq-relay-ietf-v0.7.6...moq-relay-ietf-v0.7.7) - 2025-12-18

### Added

- add file-based coordinator and rewrote remote for handling remote streams

### Fixed

- ci
- linter
- seperate RemoteManager rewrite to different PR
- remove once_cell to pass the test
- clippy unused imports
- clippy warnings
- race and proper task shutdown
- if host is IpAddr construct socket addr else resolve dns
- update lookup signature to return owned Client instead of reference
- prevent file truncation when opening for read/write in FileCoordinator
- add lifetime parameter to lookup method signature for proper borrow checking
- return clients on lookup for coordinator and misc fix

### Other

- Merge pull request #118 from itzmanish/feat/multi-relay
- remove track registration from coordinator interface and file implementation
- clarify coordinator file usage in CLI help text and add FIXME for unregister_namespace
- restructure relay into lib/bin and add coordinator interface

## [0.7.6](https://github.com/cloudflare/moq-rs/compare/moq-relay-ietf-v0.7.5...moq-relay-ietf-v0.7.6) - 2025-12-18

### Other

- Use correlation IDs in errors
- cargo fmt
- Add support for nested namespaces
- Revert "Add support for namespace hierachies"
- Address PR feedback
- cargo fmt
- Add support for namespace hierachies
- Wire Up Track Status Handling
- moq-relay-ietf variable renames and comments added
- Update moq-relay-ietf/src/relay.rs
- Print CID for clock sessions
- Add --mlog-serve
- Refactor mlog feature for better layering
- First pass of 'mlog' support
- Allow either CID or CID_server.qlog paths
- Add --qlog-serve
- Wire qlog_dir CLI argument through moq-relay-ietf
- Add --qlog-dir CLI argument to QUIC configuration

## [0.7.5](https://github.com/englishm/moq-rs/compare/moq-relay-ietf-v0.7.4...moq-relay-ietf-v0.7.5) - 2025-09-15

### Other

- cargo fmt
- Start updating control messaging to draft-13 level

## [0.7.4](https://github.com/englishm/moq-rs/compare/moq-relay-ietf-v0.7.3...moq-relay-ietf-v0.7.4) - 2025-02-24

### Other

- updated the following local packages: moq-transport

## [0.7.3](https://github.com/englishm/moq-rs/compare/moq-relay-ietf-v0.7.2...moq-relay-ietf-v0.7.3) - 2025-01-16

### Other

- cargo fmt
- Change type of namespace to tuple

## [0.7.2](https://github.com/englishm/moq-rs/compare/moq-relay-ietf-v0.7.1...moq-relay-ietf-v0.7.2) - 2024-10-31

### Other

- updated the following local packages: moq-transport

## [0.7.1](https://github.com/englishm/moq-rs/compare/moq-relay-ietf-v0.7.0...moq-relay-ietf-v0.7.1) - 2024-10-31

### Other

- release

## [0.7.0](https://github.com/englishm/moq-rs/releases/tag/moq-relay-ietf-v0.7.0) - 2024-10-23

### Other

- Update repository URLs for all crates
- Rename crate

## [0.6.1](https://github.com/kixelated/moq-rs/compare/moq-relay-v0.6.0...moq-relay-v0.6.1) - 2024-10-01

### Other

- update Cargo.lock dependencies

## [0.5.1](https://github.com/kixelated/moq-rs/compare/moq-relay-v0.5.0...moq-relay-v0.5.1) - 2024-07-24

### Other
- update Cargo.lock dependencies
