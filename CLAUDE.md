# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

moq-rs is a Rust implementation of the Media over QUIC Transport (MoQT) protocol, targeting draft-ietf-moq-transport-14. It provides a pub/sub protocol over QUIC designed for live media delivery.

## Build and Development Commands

```bash
# Build entire workspace
cargo build

# Run tests
cargo test --verbose

# Run a single test
cargo test <test_name>

# Linting and formatting (required for PR approval)
cargo clippy --no-deps
cargo fmt --check

# Check for unused dependencies
cargo machete
```

### Local Development Workflow

```bash
# Terminal 1: Start relay server on localhost:4443
./dev/relay

# Terminal 2: Start publisher (downloads Big Buck Bunny if needed)
./dev/pub

# Optional flags for dev scripts:
# - Add --tls-disable-verify to skip certificate verification
# - RUST_LOG=debug (default) for debug logging
# - PORT=4443 (default) for relay port
# - NAME=bbb (default) for broadcast name
```

### Docker Compose (clustering with Redis)

```bash
make run  # Starts Redis, moq-api, relay cluster
```

## Code Architecture

### Workspace Crates

- **moq-transport**: Core protocol library - message encoding/decoding, session management, pub/sub primitives
  - `coding/`: Wire format encoding/decoding
  - `message/`: Protocol message types
  - `session/`: Connection and session handling
  - `serve/`: Publisher/subscriber track serving
  - `data/`: Data structures for groups, objects, streams

- **moq-relay-ietf**: Production relay server
  - `relay.rs`: Main relay logic
  - `producer.rs`/`consumer.rs`: Pub/sub endpoint handling
  - `coordinator.rs`: Multi-relay namespace coordination
  - `local.rs`/`remote.rs`: Local vs federated routing
  - `web.rs`: WebTransport endpoint

- **moq-native-ietf**: QUIC/TLS utilities for native transport

- **moq-pub**: fMP4 publisher client (reads from stdin, publishes to relay)
  - `media.rs`: fMP4 parsing and track extraction

- **moq-sub**: Subscriber client for consuming MoQT streams

- **moq-api**: HTTP API server for origin discovery (Redis-backed)

- **moq-catalog**: Catalog format handling for media tracks

- **moq-clock-ietf**: Demo clock publisher/subscriber

### Key Abstractions

- Publishers connect with `role=publisher` and use PUBLISH_NAMESPACE
- Subscribers connect with `role=subscriber` and use SUBSCRIBE
- Relay deduplicates and caches subscriptions for fanout
- Supports both WebTransport and raw QUIC transport layers
- Delivery modes: stream ("subgroup") and datagram

## Formatting

- Max line width: 100 characters
- Soft tabs (spaces, not hard tabs)
- Format with `cargo fmt`

## Testing with External Players

The moq-js TypeScript player (github.com/video-dev/moq-js) is compatible with moq-pub's fMP4 output. For playback:
1. Run local relay and publisher
2. Access via https://quic.video/watch/bbb?server=localhost:4443

## Protocol References

- Spec: https://datatracker.ietf.org/doc/draft-ietf-moq-transport/14/
- Working group: https://github.com/moq-wg/moq-transport