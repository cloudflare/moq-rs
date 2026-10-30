# moq-sub

A command line tool for subscribing to media via Media over QUIC (MoQ).

Takes a URL to a MoQ relay and a broadcast name via `--name`. It will connect to the relay, subscribe to the broadcast,
and dump the media segments of the first video and first audio track to STDOUT.

```
moq-sub --name dev https://localhost:4443 | ffplay -
```
