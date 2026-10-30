# moq-pub

A command line tool for publishing media via Media over QUIC (MoQ).

Expects to receive fragmented MP4 via standard input and connect to a MOQT relay.

```
ffmpeg ... - | moq-pub --name bbb https://localhost:4443
```

### Invoking `moq-pub`:

Here's how I'm currently testing things, with a local copy of Big Buck Bunny named `bbb_source.mp4`:

```
$ ffmpeg -hide_banner -v quiet -stream_loop -1 -re -i bbb_source.mp4 -an -f mp4 -movflags empty_moov+frag_every_frame+separate_moof+omit_tfhd_offset - | RUST_LOG=moq_pub=info moq-pub --name bbb https://localhost:4443
```

This relies on having `moq-relay` (the relay server) already running locally in another shell.

Add `--publish` to push each created track with PUBLISH after announcing the namespace:

```
$ ffmpeg -hide_banner -v quiet -stream_loop -1 -re -i bbb_source.mp4 -an -f mp4 -movflags empty_moov+frag_every_frame+separate_moof+omit_tfhd_offset - | RUST_LOG=moq_pub=info moq-pub --name bbb --publish https://localhost:4443
```

Note also that we're dropping the audio track (`-an`) above until audio playback is stabilized on the `moq-js` side.

### Retained history and FETCH

Add `--fetch-groups 8` to retain up to eight groups per track and answer
standalone and Joining FETCH requests through a draft-18 relay:

```sh
ffmpeg ... - | moq-pub --name dev --fetch-groups 8 https://localhost:4443
moq-sub --name dev --join-groups 2 https://localhost:4443 | ffplay -
```

History is opt-in. The group limit includes the current group. Each track is
also limited to 16 MiB of retained payloads and 4096 Objects; at most 64 tracks
are retained. Whole-group eviction preserves the start of each retained CMAF
group. A single group exceeding the byte/Object budget fails publishing rather
than silently losing its prefix. Requests for evicted history are rejected.
Up to eight FETCH responses run concurrently, each with a ten-second deadline.
An in-flight response uses an immutable snapshot unaffected by later eviction.

Library callers can use `FetchHistory::new(NonZeroUsize)`, pass a clone to
`Media::new_with_fetch(broadcast, track_tx, Some(history.clone()))`, and run
`serve_fetches(publisher, Some(history))` alongside the session and namespace
publisher. `FetchHistory::with_limits(groups, bytes, objects)` sets all three
per-track limits explicitly. `Media::new` remains live-only. The `reexports`
module exposes the draft-18 `moq_transport` dependency used by these signatures.

### Known issues

-   Expects only one H.264/AVC1-encoded video track (catalog generation doesn't support audio tracks yet)
-   Doesn't yet gracefully handle EOF - workaround: never stop sending it media (`-stream_loop -1`)
-   Probably still full of lots of bugs
-   Various other TODOs you can find in the code
