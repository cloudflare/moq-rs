# moq-relay-ietf

A server that connects publishing clients to subscribing clients.
Subscriptions are deduplicated and cached, so that a single publisher can serve many subscribers. This checkout implements MoQT draft-18: raw QUIC uses ALPN `moqt-18`, and WebTransport uses HTTP/3 with `moqt-18` application-protocol negotiation. It does not accept older wire drafts.

## Usage

The connection path selects a scope. Publishers and subscribers that exchange tracks must connect to the same scope and use matching track namespaces and names. The path is not the broadcast name: the sample media clients select that separately with `--name`.
Connection paths are normalized and validated: trailing slashes are trimmed, dot segments and percent-encoded characters are rejected, and empty segments are not allowed. Capitalization matters.

For example, `https://relay.example:4443/demo` selects scope `/demo`. This is a placeholder endpoint. WebTransport carries the path in CONNECT; raw QUIC carries it in the SETUP PATH option.

Draft-18 uses a unified SETUP message, with requests on separate bidirectional streams. There is no publisher/subscriber role parameter in SETUP; a session can issue both kinds of requests.

Multiple publishers can share a scope with distinct namespaces. An accepted track subscription ends when its source ends; publishing the name again does not resume the old subscription. Subscribers must request a new subscription.

### Build and run

Build from this checkout rather than installing an older registry release:

```bash
cargo build --locked -p moq-relay-ietf
./target/debug/moq-relay-ietf --bind '[::]:4443' \
  --tls-cert /path/to/cert.pem --tls-key /path/to/key.pem
```

The certificate must be valid for the hostname clients use. Allow UDP on the chosen port for both raw QUIC and WebTransport. `--dev` additionally serves a development HTTPS endpoint over TCP; it is not required for MoQT delivery. For a local certificate and relay, use `./dev/relay` from the repository root.

The binary defaults to `[::]:443`. `--max-request-id` is retained as an ignored compatibility flag: draft-18 has no MAX_REQUEST_ID budget. Run `./target/debug/moq-relay-ietf --help` for the current options.

## Authorization

Per-scope bearer-token authorization is configured via `ScopeConfig.auth`. When a scope has an auth policy, every accepted client session must present a valid token in the SETUP AUTHORIZATION TOKEN option (draft-ietf-moq-transport-18 §10.3.1.4), and the session is closed with UNAUTHORIZED otherwise. Sessions the embedder tags as internal relay peers are exempt; it must authenticate those peers first. Scopes without a policy admit sessions as before, but in every scope a structurally invalid token option is still a protocol error (§10.2.2): an alias directive in SETUP closes with PROTOCOL_VIOLATION, and an undecodable Token with KEY_VALUE_FORMATTING_ERROR. Known gap: relay-to-relay `--announce` forward links are relay-initiated and currently do not present a token; they emit a warning when connecting to an auth-enabled scope.

Requests on an admitted public session on a scope with a policy are then checked against the token before the relay acts on them: PUBLISH_NAMESPACE, PUBLISH, SUBSCRIBE, SUBSCRIBE_NAMESPACE, SUBSCRIBE_TRACKS, TRACK_STATUS and FETCH. A denied request is refused with REQUEST_ERROR UNAUTHORIZED and leaves the session running. SUBSCRIBE_NAMESPACE and SUBSCRIBE_TRACKS withhold namespaces and tracks the token does not cover. The enforcement model, the CAT action mapping, and the fail-closed contract are documented in `src/auth/mod.rs`.

The `auth-cat` crate feature, which provides Common Access Token (CAT, draft-ietf-moq-c4m) verification, is off by default. A relay built without it cannot verify any token, so every scope with a policy refuses all public sessions. Embedders that want token enforcement enable the feature and supply `ScopeAuthConfig` through `Coordinator::get_scope_config`. The standalone binary can load it from a file instead; see below.

**FETCH grant.** FETCH requires the CAT `moqt` claim to include a `Fetch(7)` scope covering the requested namespace and track; tokens that grant only `Subscribe(4)` are denied with UNAUTHORIZED. The decision is made before any routing lookup, so a denial does not reveal whether the track exists. The relay keeps no object history, so a standalone FETCH is forwarded to the session that published the namespace, or to the relay the coordinator names. A Joining FETCH is first resolved against the Joining Location in the relay's own SUBSCRIBE_OK for that session (a lookup of the requester's own subscription only, which reveals nothing about other tracks), then authorized for the resolved track before any routing lookup, and forwarded as the equivalent standalone FETCH. A Joining FETCH that names a track the relay sent by PUBLISH (for example, in answer to SUBSCRIBE_TRACKS) is answered with NOT_SUPPORTED for now. The upstream FETCH_OK is passed on as soon as it arrives and the body is relayed as it streams in; each record is decoded and checked on the way, and a malformed upstream body resets the downstream data stream without closing the requester's session.

### Configuring CAT policy for the standalone relay (`--scope-config`)

The `moq-relay-ietf` binary can load per-scope CAT policy from a JSON file, so token authorization doesn't need a custom coordinator. The relay must be built with `--features auth-cat`, and the root `Dockerfile` already does this. Given `--scope-config` without the feature, the relay refuses to start. The path can also be set with the `MOQ_RELAY_SCOPE_CONFIG` environment variable.

```json
{
  "default": {
    "auth": {
      "keys": [{ "kid": "k1", "alg": "ES256", "pem_file": "issuer.pub.pem" }],
      "issuers": ["https://issuer.example"],
      "audiences": ["my-relay"],
      "clock_skew_secs": 60,
      "ttl_secs": 300
    }
  },
  "scopes": {
    "/open": { "auth": null }
  }
}
```

- **Scopes.** A scope ID is the session's connection path after normalization: the WebTransport URL path (`https://relay.example:4443/demo` → `/demo`), or the SETUP PATH option over raw QUIC. Keys under `scopes` must be written in that form: a leading `/`, no trailing `/`, case-sensitive. A session uses its exact `scopes` entry if one exists, and `default` otherwise. Matching is exact, so `/demo` doesn't cover `/demo/sub`. A session with no path (or `/`) is unscoped and uses `default`. A scope that matches neither has no token policy. `"auth": null`, or an empty scope entry `{}`, opts a scope out of `default`. (`"auth": {}` is an error, because an auth object needs `keys`.)
- **Keys.** These are ES256 (P-256) public keys only, as SubjectPublicKeyInfo PEM (`-----BEGIN PUBLIC KEY-----`). Give each key exactly one of an inline `pem` or a `pem_file` path. Relative paths resolve against the config file's directory. A scope takes 1–5 keys. A private key is rejected: the relay only verifies tokens and never needs the signing key. `kid` is optional. It only controls which key is tried first and never rejects a token.
- **Claims.** `issuers` and `audiences` restrict the token's `iss` and `aud`. Leave either empty or omit it to accept any value. `clock_skew_secs` (capped at 300) and `ttl_secs` default to 60 and 300 when absent or 0. The relay logs a warning for a policy with no `issuers` or no `audiences`, and for a `ttl_secs` above 3600, since the TTL bounds how long a removed key keeps admitting new sessions.
- **Tokens.** A client presents its token in a SETUP AUTHORIZATION TOKEN option (type 0x03). The value is alias type USE_VALUE (0x3), then token type 0x01 (CAT), then the COSE_Sign1 token bytes. For example, `moq-clock-ietf https://relay.example:4443/demo --auth-token 1:BASE64URL_ENCODED_TOKEN` subscribes with one (add `--publish` to publish). Its `moqt` claim must grant `ClientSetup` (0) to connect, plus the actions the client will use: `PublishNamespace` (2), `SubscribeNamespace` (3), `Subscribe` (4), `Publish` (6), `TrackStatus` (8).

**Enforced scopes and multiple relays.** The relay library exempts sessions that the embedder tags as internal relay peers from token authorization. This binary has no way to tag peers yet, so every inbound session is treated as a public client. That includes peers in an `--api` or `--api-url` cluster and `--announce` forward links. Peer relays present no token, so in a scope that enforces tokens they are refused like any client without one, and media published on one relay doesn't reach subscribers on another. Scopes with `"auth": null` are unaffected. The relay logs a warning at startup when `--scope-config` is combined with these options.

**Validation and failures.** The whole file is validated together: unknown fields, a bad key, or a malformed scope key in any entry rejects the entire file. If the file is unusable at startup, the relay refuses to start. If it later goes missing, becomes unreadable, or fails validation, the relay's policy lookup returns an error, which is logged with the file and location (never key material). The relay never treats that error as "no policy", and it doesn't fall back to a scope's cached state either (the binary turns off the relay library's `reuse_scope_config_on_error`):

- A scope whose cached entry is still fresh keeps its cached state until the entry expires: its `ttl_secs` for an enforced policy, 300 s for no policy.
- After that, the scope refuses new sessions with UNAUTHORIZED until the file is fixed. This includes scopes that previously had no policy.
- A failed lookup isn't cached, so once the file is fixed, the next session's lookup picks it up (the file is re-checked at most once a second).

So an edit that fails validation does not take effect, and it shuts every scope to new sessions once their cache entries expire. Watch the error log after editing the file.

**Reloading.** The file and any `pem_file`s are re-checked when the relay looks up a scope's policy (at most once a second), so edits apply without a restart. The relay caches each scope's resolved policy, though, so an edit reaches a scope's new sessions only after that scope's cache entry expires:

| Scope's cached state | Time until an edit applies |
|---|---|
| enforced | its `ttl_secs` (default 300 s) |
| no policy | 300 s (fixed) |

Restart the relay to apply a change immediately. A restart with a broken file fails at startup. Existing sessions keep the policy they were admitted with.

**Docker.** Mount the config and key read-only and pass `--scope-config`. The relay needs a TLS certificate:

```bash
docker build -t moq-relay-ietf .
docker run --rm -p 4443:4443/udp \
  -v "$PWD/scope-config.json:/etc/moq/scope-config.json:ro" \
  -v "$PWD/issuer.pub.pem:/etc/moq/issuer.pub.pem:ro" \
  -v "$PWD/dev/localhost.crt:/etc/tls/cert:ro" \
  -v "$PWD/dev/localhost.key:/etc/tls/key:ro" \
  moq-relay-ietf moq-relay-ietf \
    --bind '[::]:4443' --tls-cert /etc/tls/cert --tls-key /etc/tls/key \
    --scope-config /etc/moq/scope-config.json
```
