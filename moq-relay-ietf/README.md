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

Per-scope bearer-token authorization is supported via `ScopeConfig.auth`. When a scope has an auth policy, every session must present a valid Common Access Token (CAT, draft-ietf-moq-c4m) in the CLIENT_SETUP AUTHORIZATION TOKEN parameter.

The `auth-cat` crate feature is off by default. Embedders that want token enforcement enable it and supply `ScopeAuthConfig` through `Coordinator::get_scope_config`. The enforcement model and fail-closed contract are documented in `src/auth/mod.rs`.
