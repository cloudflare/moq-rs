# moq-relay

A server that connects publishing clients to subscribing clients.
SUBSCRIBE requests are deduplicated and cached, so that a single publisher can serve many subscribers.
Standalone FETCH requests always create a fresh upstream request and are never cached or deduplicated.
Relative and Absolute Joining FETCH requests are accepted from downstream clients, resolved from an active SUBSCRIBE or established PUBLISH association, and forwarded upstream as fresh Standalone FETCH requests. V1 does not forward Joining FETCH natively between hops or serve FETCH responses from the track cache.

## Usage

The publisher must choose a unique name for their broadcast, sent as the WebTransport path when connecting to the server.
Connection paths are normalized and validated: trailing slashes are trimmed, dot segments and percent-encoded characters are rejected, and empty segments are not allowed. Capitalization matters.

For example: `CONNECT https://relay.quic.video/BigBuckBunny`

The MoqTransport handshake includes a `role` parameter, which must be `publisher` or `subscriber`.
The specification allows a `both` role but you'll get an error.

You can have one publisher and any number of subscribers connected to the same path.
If the publisher disconnects, then all subscribers receive an error and will not get updates, even if a new publisher reuses the path.

## Authorization

Per-scope bearer-token authorization is supported via `ScopeConfig.auth`. When a scope has an auth policy, every accepted client session must present a valid Common Access Token (CAT, draft-ietf-moq-c4m) in the CLIENT_SETUP AUTHORIZATION TOKEN parameter. Sessions the embedder tags as internal relay peers are exempt; it must authenticate those peers first. Known gap: relay-to-relay `--announce` forward links are relay-initiated and currently do not present a token; they emit a warning when connecting to an auth-enabled scope.

The `auth-cat` crate feature is off by default. Embedders that want token enforcement enable it and supply `ScopeAuthConfig` through `Coordinator::get_scope_config`; the standalone binary can load it from a file instead (see below). The enforcement model and fail-closed contract are documented in `src/auth/mod.rs`.

**FETCH grant.** Standalone and joining FETCH both require the CAT `moqt` claim to include a `Fetch(7)` scope covering the requested namespace and track (joining FETCH is resolved to a standalone request before authorization). Tokens that grant only `Subscribe(4)` are denied.

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

- **Scopes.** A scope ID is the session's connection path after normalization: the WebTransport URL path (`https://relay.example:4443/demo` → `/demo`), or the CLIENT_SETUP PATH parameter over raw QUIC. Keys under `scopes` must be written in that form: a leading `/`, no trailing `/`, case-sensitive. A session uses its exact `scopes` entry if one exists, and `default` otherwise. Matching is exact, so `/demo` doesn't cover `/demo/sub`. A session with no path (or `/`) is unscoped and uses `default`. A scope that matches neither has no token policy. `"auth": null`, or an empty scope entry `{}`, opts a scope out of `default`. (`"auth": {}` is an error, because an auth object needs `keys`.)
- **Keys.** These are ES256 (P-256) public keys only, as SubjectPublicKeyInfo PEM (`-----BEGIN PUBLIC KEY-----`). Give each key exactly one of an inline `pem` or a `pem_file` path. Relative paths resolve against the config file's directory. A scope takes 1–5 keys. A private key is rejected: the relay only verifies tokens and never needs the signing key. `kid` is optional. It only controls which key is tried first and never rejects a token.
- **Claims.** `issuers` and `audiences` restrict the token's `iss` and `aud`. Leave either empty or omit it to accept any value. `clock_skew_secs` (capped at 300) and `ttl_secs` default to 60 and 300 when absent or 0. The relay logs a warning for a policy with no `issuers` or no `audiences`, and for a `ttl_secs` above 3600, since the TTL bounds how long a removed key keeps admitting new sessions.
- **Tokens.** A client presents its token in a CLIENT_SETUP AUTHORIZATION TOKEN parameter (key 0x03). The value is alias type USE_VALUE (0x3), then token type 0x01 (CAT), then the COSE_Sign1 token bytes. `moq-clock-ietf --auth-token 1:<base64url token>` sends one. Its `moqt` claim must grant `ClientSetup` (0) to connect, plus the actions the client will use: `PublishNamespace` (2), `SubscribeNamespace` (3), `Subscribe` (4), `Publish` (6), `TrackStatus` (8).

**Single relay only for enforced scopes.** Relay-to-relay sessions present no token. That covers peers in an `--api` or `--api-url` cluster, which connect to each other as ordinary clients, and `--announce` forward links. In a scope that enforces tokens they are refused like any client without one, so media published on one relay never reaches subscribers on another. This holds until the internal-peer exemption ([cloudflare/moq-rs#264](https://github.com/cloudflare/moq-rs/pull/264)) lands and this binary can tag peer relays. Scopes with `"auth": null` are unaffected. The relay logs a warning at startup when `--scope-config` is combined with these options.

**Validation fails closed.** The whole file is validated together: unknown fields, a bad key, or a malformed scope key in any entry rejects the entire file. If the file is unusable at startup, the relay refuses to start. If it later goes missing, becomes unreadable, or fails validation, the relay's policy lookup returns an error, and the error is logged with the file and location (never key material). The relay never treats that error as "no policy". What a new session sees then follows the relay library's rules for a failed lookup: a scope with no usable cached policy refuses the session with UNAUTHORIZED. Depending on the library version, a scope with a cached policy may keep using it for a bounded time. Check `src/auth/scope.rs` for the version you run. Either way, fix the file promptly. Keep an eye on the error log too, because an edit that fails validation does not take effect, and the scope keeps its previous policy, which may be no policy.

**Reloading.** The file and any `pem_file`s are re-checked when the relay looks up a scope's policy (at most once a second), so edits apply without a restart. The relay caches each scope's resolved policy, though, so an edit reaches a scope's new sessions only after that scope's cache entry expires:

| Scope's cached state | Time until an edit applies |
|---|---|
| enforced | its `ttl_secs` (default 300 s) |
| no policy | 300 s (fixed) |
| policy present but unusable (e.g. a key the CAT library rejects) | 30 s |

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
