# moq-sub

A command line tool for subscribing to media via Media over QUIC (MoQ).

Takes a URL to a MoQ relay and a broadcast name via `--name`. It will connect to the relay, subscribe to the broadcast,
and dump the media segments of the first video and first audio track to STDOUT.

```sh
moq-sub --name dev https://localhost:4443 | ffplay -
```

With no subcommand, `moq-sub` preserves the live subscription behavior above. The init segment is always written first.

Saved media can be requested with one of the nested `fetch` modes:

```sh
# Fetch groups 10 through 20, then exit. END object 0 includes the whole group.
moq-sub --name dev https://localhost:4443 fetch standalone 10:0 20:0 > media.mp4

# Fetch the last five groups (the boundary group counts as one), then continue live.
moq-sub --name dev https://localhost:4443 fetch relative 5 | ffplay -

# Fetch from absolute group 100 through the live boundary, then continue live.
moq-sub --name dev https://localhost:4443 fetch absolute 100 | ffplay -
```

Locations use decimal `GROUP:OBJECT` syntax. A nonzero END object is exclusive, while END object `0` includes the entire end group; equal nonzero locations request an empty range. Group and object values are limited to QUIC variable-length integers, and relative `GROUPS` must be non-zero. Joining modes buffer complete live objects while all selected-track FETCH requests finish, then release the live stream without interleaving it with fetched output. Fetched and buffered live objects share a 16 MiB byte budget, and the live queue holds at most 32 complete objects; exceeding either limit fails the command.
