<!-- SPDX-FileCopyrightText: 2026 Cloudflare Inc. -->
<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# Clock history and Joining FETCH

The stream publisher retains the last **8 minute groups**, including the current
minute, and serves both standalone and Joining FETCH. `--history-groups` changes
that window (1–1024 groups). Retention is also limited to 1 MiB of payload and
64 Objects per group; eviction removes whole groups so each retained minute
still has its base Object. Up to 8 FETCH responses run concurrently, each with a
10-second deadline. Datagram publishing retains its existing live-only behavior.

With a draft-18 relay already running, start the publisher:

```sh
cargo run -p moq-clock-ietf -- https://localhost:4443 \
  --tls-disable-verify --publish --history-groups 8
```

Wait at least 65 seconds so a previous minute group exists, then in another
terminal:

```sh
cargo run -p moq-clock-ietf -- https://localhost:4443 \
  --tls-disable-verify --join-groups 1
```

This issues SUBSCRIBE with the Largest Object filter, then a **Relative Joining
FETCH** for one preceding group plus the current group's prefix through the
Joining Location. It prints the fetched timestamps first and then continues live.
The minute base from FETCH is reused to decode live deltas. Overlapping live
Objects at or before the Joining Location are suppressed; a missing Object at
the handoff is an error rather than a silent gap. Logs go to stderr.

A cold relay can initially acknowledge SUBSCRIBE without a Joining Location.
In that case the subscriber waits for one Object to prime the cache, tears down
that warm-up subscription, and sends a new SUBSCRIBE before Joining FETCH. This
warm-up has a 10-second deadline and prints no Objects.

Use a window of at least `N + 1` publisher groups for `--join-groups N`, and let
the publisher accumulate that history first. A request reaching before the
retained window is explicitly rejected with DOES_NOT_EXIST; the subscriber does
not silently shorten the requested history. A request reaching before group ID
zero is clamped to zero. TLS verification is disabled above for local testing
only; use trusted certificates for remote relays.

## Public Rust APIs

`moq_transport::session` exports `Fetch`, `FetchRequested`, `FetchOkInfo` and
`FetchWriter`. `Subscriber::fetch` initiates a standalone range;
`Subscriber::joining_fetch` initiates a relative or absolute join. Publishers call
`accept_fetch`, then handle `fetch_requested` with `resolve`, `respond`,
`write_record`, `write_payload` and `finish` (or `reject`). The transport crate's
Rustdoc contains short requester and responder examples. All these APIs require
`Session::run` to progress concurrently.

The in-process relay regression test exercises a cold pull-through cache,
standalone FETCH, Joining FETCH, the same-minute live handoff, and the next minute:

```sh
cargo test -p moq-clock-ietf relay_joining_fetch_replays_history_then_continues_live_once
```
