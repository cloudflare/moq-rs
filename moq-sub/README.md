# moq-sub

A command line tool for subscribing to media via Media over QUIC (MoQ).

Takes a URL to a MoQ relay and a broadcast name via `--name`. It will connect to the relay, subscribe to the broadcast,
and dump the media segments of the first video and first audio track to STDOUT.

```
moq-sub --name dev https://localhost:4443 | ffplay -
```

## FETCH playback (draft-18)

The publisher must retain history, for example with `moq-pub --fetch-groups 8`.
The existing FETCH commands are supported:

```sh
# Replay three groups including the subscription's boundary group, then live.
moq-sub --name dev https://localhost:4443 fetch relative 3 | ffplay -
# Equivalent: two previous groups plus the boundary group.
moq-sub --name dev --join-groups 2 https://localhost:4443 | ffplay -
# Replay from group 4 to the live boundary, then live.
moq-sub --name dev https://localhost:4443 fetch absolute 4 | ffplay -
# Fetch a finite range and exit. End Object 0 includes the whole end group.
moq-sub --name dev https://localhost:4443 fetch standalone 4:0 6:0 > clip.mp4
```

Locations are unsigned decimal `GROUP:OBJECT` values using draft-18's full
64-bit ID range. A nonzero end Object is exclusive. Catalog/init tracks are
downloaded with SUBSCRIBE in every mode. Standalone playback fetches media
tracks only and flushes output before returning.

Joining playback uses a Largest Object SUBSCRIBE and a relative or absolute
Joining FETCH. A cold relay is primed and re-subscribed to obtain an actual
Joining Location. FETCH must cover the requested first Object and end exactly
at that location. The subscriber outputs history before buffered live Objects,
discards overlap through the boundary and checks consecutive media IDs across
the handoff. All selected tracks finish history before any releases live data.

This media profile uses one subgroup (ID 0) per group and consecutive group
and Object IDs, as produced by `moq-pub`. Unavailable history, gaps, unsupported
subgroups, oversized Objects and buffer exhaustion are errors. Per track,
live buffering is bounded to 32 Objects and 16 MiB, and each fetched Object is
limited to 16 MiB. Warm-up and FETCH have ten-second deadlines.

Library callers use
`Media::new_with_fetch(subscriber, tracks, output, request_catalog, Some(mode))`.
`media::FetchMode` retains its `Standalone { start, end }` and `Joining(...)`
variants. **Migration from draft-16:** import `JoiningStart` from
`moq_sub::media` rather than `moq_transport::session`; draft-18 transport uses
`Subscriber::joining_fetch(FetchType, request_id, start, params)` instead.
`JoiningStart::Relative(0)` still means the boundary group, while the CLI's
`fetch relative 1` expresses the same choice as a group count. `Media::new`
continues to select live playback.
