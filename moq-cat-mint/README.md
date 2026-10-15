# moq-cat-mint

Mint Common Access Tokens (CAT, [draft-ietf-moq-c4m]) for testing MoQ relays that
enforce them, such as `moq-relay-ietf` built with the `auth-cat` feature.

**Testing only.** The signing key is a plain file on disk. This is not an issuer service,
and it shouldn't be used with production keys.

```bash
cargo build --release -p moq-cat-mint
M=target/release/moq-cat-mint

$M keygen                       # writes issuer.key (0600) and issuer.pub.pem
# Configure the relay to trust issuer.pub.pem (never the .key) and your issuer.

TOKEN=$($M mint --iss https://issuer.example --aud my-relay --ns clock --publish)
moq-clock-ietf https://localhost:4443/demo --publish --auth-token "1:$TOKEN"
```

`mint` prints the token as base64url, ready for `--auth-token 1:...` (CAT is
AUTHORIZATION TOKEN type 1).

## Grants

Every token grants `ClientSetup` (0), plus the requested actions scoped to the `--ns`
prefix and the `--track` prefix:

| Flag | Actions |
|---|---|
| `--publish` | PublishNamespace (2), Publish (6) |
| `--subscribe` | Subscribe (4), SubscribeNamespace (3), TrackStatus (8), Fetch (7) |
| `--action N` | Action `N` explicitly (repeatable) |

`--ns` takes one namespace tuple field per flag. Repeat it for multi-field prefixes. If you
leave `--ns` or `--track` out, the token covers every namespace or every track.

## Negative tests

- Wrong audience: mint with a different `--aud`.
- Wrong issuer: mint with a different `--iss`.
- Wrong key: run `keygen` into another directory and mint with `--key` pointing there.
- Expired: mint with `--ttl 1` and wait.
- Out of scope: mint for a different `--ns` or `--track`, or with only `--subscribe` for a
  publisher.

## Limitations

- ES256 only.
- No `kid` header. Relays that try every configured key still accept the token.
- Only the claims above (`iss`, `aud`, `exp`, optional `sub`, `moqt`).

[draft-ietf-moq-c4m]: https://datatracker.ietf.org/doc/draft-ietf-moq-c4m/
