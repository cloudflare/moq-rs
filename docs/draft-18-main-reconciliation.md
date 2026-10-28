<!-- SPDX-FileCopyrightText: 2026 Cloudflare Inc. -->
<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# Draft-18/main reconciliation ledger

## Scope and inputs

This integration merges main into draft-18 development, preserving both histories.
It resolves conflicting source in favor of the draft-18 implementation, then carries
the maintenance required to keep that implementation building. It is not a claim
of complete semantic parity or release readiness.

- Draft-18 parent: `8779758892d921bcfccbb6e4d9319c149db28c91`.
- Main parent: `8bc03ed7b14c9a01f9715966a5935f58a24aa43f`.
- Initial ordinary three-way merge: 64 conflicted paths.
- Conflicting generated changelogs use main's published history without manual edits.
- Conflicting manifests retain draft-18 dependencies/features, with main's package
  versions restored as the release baseline. **These are not new release versions.**
  Before publication, choose breaking versions and update all internal dependency
  ranges for the draft-18 wire/API transition.

The merge commit must remain in the history when this is integrated into
`draft-18-dev`: fast-forward it, or preserve it beneath a merge commit. Do not squash
or rebase away the main parent. The main parent is what makes a later merge into
that main revision conflict-free; newer main changes still require revalidation.

## Main-side changes retained now

| Source | Disposition |
|---|---|
| #249 server dependency repair | `axum-server` 0.7.3 replaces `hyper-serve`, including `web.rs` imports. |
| #250 isolated API package build | Preserve explicit serde derive and release-plz configuration. Reassess the historical semver-check exceptions before release. |
| #248 Coordinator Clippy repair | Retain a targeted `double_must_use` allowance on the async-trait-generated methods. |
| Current-toolchain build hygiene | Move existing test modules after production items and replace infallible/factor tests with current idioms; no wire or routing changes. |
| #194 stdout/stderr separation | Preserve `moq-api` diagnostics on stderr. Draft-18 already directs the other binary diagnostics to stderr. |
| Published versions and changelogs | Keep main's package version baseline and generated release history; final draft-18 release bumps remain pending. |
| Bounded strings | Keep `ReasonPhrase::new` and UTF-8 boundary tests, plus the automatically merged TRACK_STATUS error call site. Other dynamic error sites remain in the port queue below. |
| #217 independent error IDs | Keep the standalone `ServeError::error_id` helper and structured error-ID records. Full session correlation is deferred. |
| Public dependency reexports | Keep `moq_transport::reexports::{bytes, uuid}` for public dependency types; does not imply the main FETCH API has been ported. |
| #156/#221 compatible track integrity tests | Retain track round-trip, independent-track and stale-resubscribe tests. Subgroup source/tests use draft-18's different fan-out model. |
| #199 mlog example documentation | Retain main's example-URL clarification in `deploy/MLOG_SETUP.md`; this is not a live endpoint assertion. |

## Semantic ports still wanted from main

These changes were not forgotten by choosing the draft-18 side. Each needs a
draft-18-specific implementation/review and its own test evidence.

| Priority | Source and files | Work remaining / acceptance |
|---|---|---|
| High | #217 (`ded5cff3`), `session/session_id.rs`, transport session/reader/writer/request tasks, relay context, clock | Restore stable session correlation and safe reason logging across the draft-18 bidi architecture. Verify restrictive filters, control-character escaping and upstream/downstream span boundaries. Do not retain an unreferenced SessionId file or half-integrated writer signature. |
| High | `d696a657`, `message/request_error.rs`, `session/{subscribed,subscribed_namespace,published_namespace}.rs` | Use bounded dynamic reason construction throughout, with oversized multi-byte reasons tested on request streams. Do not copy draft-16 namespace cancellation/state-machine rules. |
| Feature parity decision | #246/#247 (`0876bcdf`, `ba4f3e3d`), `moq-pub/src/{fetch,lib,main,media}.rs`, `moq-sub/src/{main,media}.rs` | Port publisher history/public FETCH exports and pub/sub FETCH CLI, or explicitly defer/remove them in the breaking release contract. Main's automatic merges were reverted to draft-18 for these source/README files to avoid importing draft-16 FETCH assumptions. Existing draft-18 relay FETCH is retained. |
| Coverage | #156/#221 (`bf87128a`, `a861742c`), `serve/subgroup.rs` and queue tests | Map remaining main integrity invariants onto draft-18 fan-out, cancellation and explicit object-ID/status semantics. Latest-only subgroup expectations must not replace draft-18 arrival/fan-out behavior. |
| Documentation | #199 (`933a443d`), root/dev/relay READMEs | Refresh protocol/version/deployment examples deliberately. Conflicting docs retain draft-18 text for now. |
| Release gate | Main release commits and `release-plz.toml` | Select breaking versions and matching package ranges, review semver exceptions, and validate fresh-resolution packaging/consuming builds before publishing. |

Until the FETCH surface decision and required ports are complete, this tree retains
draft-18's existing public API/CLI behavior rather than all of main's published
surfaces. The merge only reconciles ancestry and establishes a buildable baseline.

## Already represented by draft-18

- #189: accept-IP metadata and connection tagging.
- #196/#197: idle-cache release, reset-on-drop, upstream-confirmed SUBSCRIBE_OK,
  rejection propagation and draft-18's ordered teardown/rendezvous behavior.
- #220: discovery-only peer namespace forwarding and abandoned readiness cleanup.
- #236/#237: corresponding draft-18 transport/relay standalone and Joining FETCH
  in #272/#273/#274. Keep draft-18 wire/range semantics rather than main's
  JoiningAssociation implementation.
- #235/#258: CAT setup/request enforcement and Fetch(7) authorization, adapted in
  #257/#262 and the draft-18 FETCH work.
- #263/#268/#271: policy-cache fallback (#266), authenticated internal-peer
  admission (#267), and scope-file configuration/strict reload (#269).

Preserve draft-18's bounded scope cache, subgroup fan-out/IDs/status/properties,
datagram semantics, PUBLISH_DONE drain/count handling, SUBSCRIBE_TRACKS, token
presentation/minting and test-client scenarios. Those are not conflicts to undo.

## Related work outside this merge

- Open #204: bounded upstream SUBSCRIBE handshake, separate from the accepted
  subscription lifetime and rendezvous timeout.
- Open #239: main's superseded latest-only subgroup cancellation bug needs a
  draft-18 repro; Cancel propagation alone is not proof of the same failure.
- Open #276: zero-field namespace correctness.
- Open #278: Track Properties forwarding/public API change; #280 builds on it.
- Cross-host longest-prefix routing: compare strict local prefixes with longer
  remote routes, including coordinator/cache semantics and FETCH.
- Existing FETCH limitations: publisher passthrough without relay history cache,
  restricted parameter forwarding, no Joining FETCH for relay-sent PUBLISH,
  whole-response timeout and post-FETCH_OK/no-data waiter cleanup concerns.
- REQUEST_UPDATE now supports NEW_GROUP_REQUEST for dynamic-group feedback.
  Other update parameters, including FORWARD, remain unsupported and are rejected.

These are release-scope decisions and follow-up work, not fixes implicitly supplied
by recording main as an ancestor.

## Local validation

Validated on macOS arm64 with Rust/Cargo 1.99.0; the default laptop toolchain is
older, so commands select `+1.99.0` explicitly. The original workspace check and
tests passed before the small Clippy-only cleanup; the final tree is checked again
before review submission.

- Workspace/all-target compilation check.
- Workspace tests, including transport, relay, test-client and doctests.
- Strict workspace/all-target Clippy (`--no-deps -- -D warnings`).
- Relay tests with `auth-cat` (297 library and 52 binary tests).
- Locked workspace build and unused-dependency check (`cargo machete`).

The integration has not been published, installed from a new registry release, or
deployed. Fresh-resolution package/consumer checks and actual relay/worker interop
remain part of the release gate rather than this ancestry-reconciliation claim.
