# moq-test-relay

A test relay for the eight catalog procedures, using MoQT draft 18. Each run
checks client messages and reports one TAP 14 result. It accepts WebTransport
(`https://`) and raw QUIC (`moqt://`).

```text
setup-only
announce-only
publish-namespace-done
subscribe-namespace-lifecycle
subscribe-error
rendezvous-timeout
announce-subscribe
subscribe-before-announce@hold
subscribe-before-announce@reject
```

```sh
moq-test-relay --list
moq-test-relay --test announce-only --port 15000 --cert cert.pem --key priv.key
moq-test-relay --test subscribe-before-announce --variant hold --port 15000 --cert cert.pem --key priv.key
```

`TESTCASE`, `TEST_VARIANT`, `MOQT_PORT` and `VERBOSE` supply defaults for their
CLI options. Explicit options take precedence. `VERBOSE=1` enables debug logs;
`--verbose=false` overrides it. The port defaults to 4443. Certificate paths
default to `/certs/cert.pem` and `/certs/priv.key`. The `--cert` and `--key`
options use the native library's TLS loader internally.

## Deadlines and shutdown

The first-session wait defaults to 30 seconds. `--first-session-wait SECONDS`
can shorten it for local checks. Test clocks start at the first accepted,
version-negotiated session, before SETUP, except where the catalog names a
request as the clock's origin:

| Entry | Verdict deadline |
| --- | --- |
| `setup-only` | 2 seconds from first session |
| `announce-only`, `publish-namespace-done` | 2 seconds from the first PUBLISH_NAMESPACE; if none arrives, 2 seconds from first session |
| `subscribe-namespace-lifecycle` | 5 seconds from first session, the interface default because its specification gives no timeout |
| `subscribe-error`, `rendezvous-timeout` | 2 seconds from first session |
| `announce-subscribe` | 3 seconds from first session |
| `subscribe-before-announce`, both variants | 3.5 seconds from first session |

The relay reaches a verdict at completion or the test deadline. It keeps
serving until every accepted client session closes or SIGTERM arrives.
`duration_ms` ends at the verdict, excluding that later wait. Late session
errors or resets still fail the result. Missing observations at the deadline
cannot be satisfied retroactively. SIGTERM judges partial evidence and writes
TAP; the relay's own shutdown closes do not count as client errors.

`--post-verdict-wait SECONDS` is the implementation-specific safety cap. Its
default is **60 seconds after the verdict**. At the cap, the relay closes
remaining sessions and records a TAP comment. Run the binary directly as the
container entry point so it receives SIGTERM. Signal handling targets Unix.

The pinned session driver reports a control-stream FIN as `Decode(More)`.
Draft 18 Section 3.3 prohibits closing that stream during the session. The
relay fails a FIN before that session's required role steps, naming its role,
the missing step and the FIN receive time. A FIN after those steps is tolerated
only if a graceful close of that same session is observed before TAP. If
SIGTERM or the safety cap arrives first, or the session instead ends with an
idle timeout, reset or error, the result fails Section 3.3 and names the FIN
time and the lack of a subsequent graceful close. This compatibility policy
applies to every test, including `setup-only`. It does not add a graceful-close
requirement to sessions without observed FIN where the procedure requires none.
Driver termination alone does not mark a transport session closed.

Logs go to stderr. Exit statuses are 0 for `ok` or `ok ... # SKIP`, 1 for
`not ok`, 127 for an unsupported entry, and 2 for an internal failure.
Unsupported entries exit before loading TLS or binding a socket.

## Procedure behavior

- Namespace tests accept PUBLISH_NAMESPACE with REQUEST_OK, following
  Sections 6.2, 10.15 and 10.5. `publish-namespace-done` requires peer
  cancellation of its request stream (Sections 6.2 and 3.3.2). Session
  teardown alone is not withdrawal. The library's additive
  `PublishedNamespace::closed_cause` API distinguishes those events.
- Namespace lifecycle accepts any matching namespace/prefix pair on separate
  publisher and subscriber sessions. It sends REQUEST_OK, then NAMESPACE,
  then NAMESPACE_DONE (Sections 6.1, 6.2 and 10.16-10.18). A publisher that
  arrives first is included in the subscription's current-set snapshot, as
  Sections 6.1 and 10.18 require.
- Error subscriptions name `nonexistent/namespace` or
  `nonexistent/rendezvous`, with track `test-track`. Section 10.2.6 requires
  immediate DOES_NOT_EXIST for absent/zero RENDEZVOUS_TIMEOUT. A nonzero
  timeout is held, then answered with TIMEOUT. Holds are capped 50 ms below
  the test deadline to allow response scheduling. The requested value is
  recorded as a TAP comment. `rendezvous-timeout` requires exactly 500 ms,
  as its catalog procedure specifies. Other values fail the observation but
  receive the same protocol response. Section 10.6.2 defines the error codes.
- Routing tests use `moq-test/interop`, track `test-track`, and separate
  sessions. `announce-subscribe` requires PUBLISH_NAMESPACE before SUBSCRIBE.
  Both `subscribe-before-announce` variants require the reverse order. A
  failure names both application receive times, measured from first session;
  these are not packet timestamps or a total wire order across streams.
- `reject` immediately answers an early SUBSCRIBE: DOES_NOT_EXIST for an
  absent/zero timeout, TIMEOUT for nonzero. Section 10.2.6 permits a shorter
  hold; this variant uses zero. `hold` routes after a matching publisher
  appears, only with a nonzero timeout. Otherwise it immediately answers
  DOES_NOT_EXIST and reports SKIP. Expiry answers TIMEOUT and reports SKIP,
  naming the requested timeout. A required missing publisher still fails.
- Requests outside the procedure are answered through the library and recorded
  as comments after the YAML block, without failing the observation. Required
  requests with wrong names or parameters still fail. The first PUBLISH_NAMESPACE
  and the first SUBSCRIBE take their required procedure steps. Later requests
  of those types are extra requests, even when they name another namespace or
  track. Accepted extra namespaces are routed until withdrawal or session close.
  Section 3.3.2 recommends REQUEST_ERROR when rejecting a request;
  Section 10.6.2 defines the codes. FETCH is observed through
  `Publisher::fetch_requested()` and rejected with REQUEST_ERROR NOT_SUPPORTED.
  Two exceptions get no comment, because the library answers them before the
  application sees them and offers no observation hook. REQUEST_UPDATE always
  gets REQUEST_ERROR NOT_SUPPORTED. A FETCH that fails the library's own
  checks, such as an invalid range, too many FETCHes in flight or an unknown
  joining request, gets the matching REQUEST_ERROR. None of these procedures
  uses either request.

## Known routing limits

The relay forwards SUBSCRIBE parameters, including NEW_GROUP_REQUEST as
Section 10.2.13 requires. It preserves acceptance/rejection, LARGEST_OBJECT
and publisher rejection codes. Other SUBSCRIBE_OK response parameters and
publisher error reason/retry metadata are not preserved by the serving API.

A subscriber that cancels before the publisher answers fails its procedure's
response-wait step. The relay checks cancellation while holding, while waiting
for the publisher, and before recording acceptance. The serving API exposes no
SUBSCRIBE_OK delivery signal. A cancellation or delivery failure between that
last check and downstream SUBSCRIBE_OK delivery can therefore escape observation.

**This is not a claim of general draft-18 relay conformance.** It covers only
the eight catalog procedures. Downstream SUBSCRIBE_OK does carry the
publisher's track properties unchanged, as Sections 9.6 and 2.5 require,
because `Subscribed::serve_inner` sends the set that the upstream SUBSCRIBE_OK
stored on the track.

An additive `Subscribed::serve_with_subscribe_ok(track, subscribe_ok)` API
could preserve upstream response parameters while allocating the downstream
Track Alias locally. That shared library work is separate from this crate.

Protocol references above use
[draft-ietf-moq-transport-18](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html).

## Source and checks

`registry.rs` owns entries and deadlines. `run.rs` owns observations, session
roles and verdict/shutdown handling. `sessions.rs` drives library requests;
`routing.rs` owns routes, bounded holds and namespace fan-out. `tap.rs` renders
TAP/YAML. Add behavior and observations before listing another entry.

```sh
cargo test -p moq-test-relay
cargo clippy -p moq-test-relay --all-targets
```

The scenario fixtures use real local sessions on available UDP ports in
`16000..16100`. Tests are grouped by namespace, subscription, routing and
session behavior under `src/scenarios_tests/`. Optional `MOQ_TEST_RELAY_QLOG`
and `MOQ_TEST_RELAY_TRACE` environment variables enable fixture diagnostics;
they are not relay CLI options. Raw QUIC's synchronous reset-then-close may
send only CONNECTION_CLOSE. Withdrawal tests distinguish that from a
cancellation actually delivered before close.
