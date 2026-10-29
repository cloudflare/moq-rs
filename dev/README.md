# Local Development

This is a collection of helpful scripts for local development.
Check the source for each script for a list of useful arguments.

Run these helpers from a draft-18 checkout so the relay and clients use matching code. Raw QUIC negotiates `moqt-18`; WebTransport uses HTTP/3 and the `moqt-18` application protocol. Older-draft clients and browser players are not wire-compatible.

## moq-relay-ietf

Hosts a [relay server](../moq-relay-ietf) on port `4443` using locally generated certificates. The default bind address is `[::]:4443`; clients connect to `https://localhost:4443`. `PORT` or `BIND` override the listener, and `CERT`/`KEY` select the TLS files.

Requires:

-   Rust
-   Go (mkcert)

```bash
./dev/relay
```

All native clients below use `localhost:4443` by default. Trust the generated certificate or pass `--tls-disable-verify` to a client for local testing. Browser clients need a draft-18-compatible implementation and browser-accepted TLS certificates.

For a single-relay smoke test, this helper needs no Redis or API server. The checked-in `docker-compose.yml` is historical: it still invokes the removed `moq-dir` binary, so `make run` is not a working setup for this checkout. See the relay README for a standalone Docker command and scope-policy configuration.

## moq-pub

Publish some test footage from disk to the localhost relay using [moq-pub](../moq-pub).
This downloads Big Buck Bunny and publishes a broadcast named `bbb`.

Requires:

-   Rust
-   ffmpeg
-   wget (first download)

```bash
./dev/pub
```

The defaults are `NAME=bbb`, `URL=https://localhost:4443`, and `INPUT=dev/bbb.fmp4`. Use a GStreamer publisher only after checking both its media format and MoQT draft compatibility.

## moq-sub

You can use `moq-sub` to subscribe to media streams from a MoQ relay and pipe them to the standard output.
By piping the command to a video player, e.g. `ffplay` or `mpv`, you can play a MoQ broadcast natively.

Currently, `moq-sub` simply dumps all received segments of the first video and the first audio track 
directly to `stdout`.

The following command subscribes to a stream from a MoQ relay and plays it with `ffplay`.
By default, the URL is `https://localhost:4443` and the broadcast name is `bbb`, matching `dev/pub` and `dev/relay`. Set the same `NAME` and `URL` environment variables on both publisher and subscriber to override them.

```bash
./dev/sub
```

## moq-clock-ietf

To show that MoQ can do more than just media, we made a simple clock.

```bash
./dev/clock --publish
```

And run the subscriber in a separate terminal:

```bash
./dev/clock
```

To test against a scope that requires CAT bearer-token authorization, pass one or more tokens with the `--auth-token` flag:

```bash
./dev/clock --publish --auth-token 1:BASE64URL_ENCODED_TOKEN
./dev/clock --auth-token 1:BASE64URL_ENCODED_TOKEN
```

The format is `TYPE:VALUE`. `TYPE` is the token type as a decimal or `0x`-prefixed integer (1 is a Common Access Token), and `VALUE` is URL-safe base64, padded or not. Each token is sent in SETUP as its own AUTHORIZATION TOKEN option (draft-18 §10.3.1.4) with alias type USE_VALUE. The flag may be repeated. **This flag is for testing only**: the token value is visible in process arguments on the host. See `moq-relay-ietf/README.md` for configuring a relay to require tokens. To generate a test keypair and tokens, use `moq-cat-mint` (see `moq-cat-mint/README.md`).
