# moq-rs

An implementation of the Media over QUIC Transport (MoQT) protocol for live media delivery over QUIC, as specified by the IETF MoQ working group.

This codebase was originally created by [Luke Curley (@kixelated)](https://github.com/kixelated). [Mike English (@englishm)](https://github.com/englishm) contributed to early design and has maintained this IETF-aligned fork. The project is now maintained by Cloudflare. The implementation targets [draft-ietf-moq-transport-18](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/18/).

## Protocol Support

This checkout targets **draft-18** of the MoQT specification. Raw QUIC uses the `moqt-18` ALPN; WebTransport uses HTTP/3 with `moqt-18` application-protocol negotiation. Both peers must support the same draft. The `DraftVersion` enum also names later drafts, but this implementation enables only draft-18.

Until `draft-18-dev` merges into `main`, build draft-18 clients and relays from the same `draft-18-dev` revision. Package versions and crates.io documentation still reflect the published release baseline; they do not identify the wire draft of this checkout. For draft-07 compatibility, see the [`draft-ietf-moq-transport-07`](https://github.com/cloudflare/moq-rs/tree/draft-ietf-moq-transport-07) branch.

### What's Included

This repository provides:
- **moq-transport**: A MoQT protocol library
- **moq-relay-ietf**: A relay server with subscription deduplication and fan-out
- **Sample clients**: An fMP4 publisher (moq-pub) and a demonstration clock application (moq-clock-ietf)

### Protocol Feature Support

**Supported:**
- SETUP
- PUBLISH_NAMESPACE
- SUBSCRIBE
- SUBSCRIBE_NAMESPACE
- SUBSCRIBE_TRACKS and TRACK_STATUS
- PUBLISH / REQUEST_OK / PUBLISH_DONE (publisher-initiated tracks)
- Dynamic groups: DYNAMIC_GROUPS advertisement and NEW_GROUP_REQUEST feedback,
  including subscription REQUEST_UPDATE for that parameter
- FETCH, standalone and joining (the relay forwards it to the track's origin)
- WebTransport and raw QUIC transport layers
- Both stream ("subgroup") and datagram delivery modes

**Not Supported:**
- REQUEST_UPDATE parameters other than NEW_GROUP_REQUEST, including FORWARD (so a Joining FETCH can only join a subscription whose Forward State was set by SUBSCRIBE)
- Joining FETCH for a subscription created by PUBLISH (answered with NOT_SUPPORTED)

## Interoperability

Check the draft supported by an interop endpoint before connecting. For a local draft-18 relay, use `https://localhost:4443` after starting `./dev/relay`. Hosts under `relay.example` in the crate READMEs are placeholders, not deployed endpoints.

As an implementation targeting the IETF specification, this codebase should be compatible with other implementations targeting the same draft version. See the [moq-wg/moq-transport wiki](https://github.com/moq-wg/moq-transport/wiki/Interop) for a list of other implementations.

For streaming format compatibility:
- **[video-dev/moq-js](https://github.com/video-dev/moq-js)**: A TypeScript player implementation compatible with moq-pub's fMP4 output. Check draft version compatibility when selecting branches.
- **[gst-moq-pub](https://github.com/rafaelcaricio/gst-moq-pub)**: A GStreamer publisher plugin built on moq-pub, also compatible with moq-js.

## Components

This repository contains several crates:

- **moq-transport**: A media-agnostic library implementing the core MoQT protocol.
  - **moq-native-ietf**: QUIC and TLS utilities for native transport.
- **moq-relay-ietf**: A relay server that forwards content from publishers to subscribers, with caching and deduplication.
  - **moq-api**: An HTTP API server for origin discovery and relay coordination, backed by Redis.
- **moq-pub**: A publisher client that broadcasts fMP4 streams over MoQT.
  - **moq-catalog**: Catalog format handling.
  - **moq-sub**: A subscriber client for consuming MoQT streams.
- **moq-clock-ietf**: A simple time publisher/subscriber demonstrating non-media use cases.
- **moq-cat-mint**: A CLI that mints Common Access Tokens (CAT) for testing relays that require them.

## Development

Typical development workflow:

1. Start a local relay in one terminal:
   ```bash
   ./dev/relay
   ```

2. Start a publisher in another terminal:
   ```bash
   ./dev/pub
   ```
   Add `--tls-disable-verify` if you prefer not to install local certificates.

3. Play the published broadcast in another terminal (requires `ffplay`):
   ```bash
   ./dev/sub
   ```
   Use the same `NAME` and `URL` environment variables for `./dev/pub` and `./dev/sub` when overriding the defaults. Browser players also need a branch compatible with draft-18; matching the media format alone is insufficient.

For a smoke test without a media player or ffmpeg, start `./dev/clock --publish` and `./dev/clock` in separate terminals. Add `--tls-disable-verify` to native clients only when testing with an untrusted local certificate.

See the [dev helper scripts](dev/README.md) for more options and workflows.

## Usage

For detailed usage information, see the README in each crate directory:
- [moq-transport](moq-transport/README.md) - Protocol library documentation
- [moq-relay-ietf](moq-relay-ietf/README.md) - Relay server configuration
- [moq-pub](moq-pub/README.md) - Publisher client usage

The moq-transport crate is also published on [crates.io](https://crates.io/crates/moq-transport) with [API documentation](https://docs.rs/moq-transport/latest/moq_transport/). To read the API for this checkout, run `cargo doc -p moq-transport --no-deps --open`.

## License

Licensed under either:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
