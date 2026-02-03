# moq-test-client

A standardized MoQT (Media over QUIC Transport) interoperability test client.

## Overview

This tool enables automated interoperability testing between MoQT implementations. It can run test scenarios against any MoQT relay to verify interoperability with moq-rs-based clients.

**Key Features:**
- Test scenarios for basic MoQT operations
- Human-readable output with timing information
- Machine-parseable result format (`MOQT_TEST_RESULT: SUCCESS/FAILURE`)
- Designed to be implementation-agnostic (interface can be reproduced by other implementations)

## Usage

```bash
# List available test cases
moq-test-client --list

# Run all tests against a relay with proper TLS
moq-test-client --relay https://relay.example.com:4443

# Run a specific test
moq-test-client --relay https://relay.example.com:4443 --test setup-only

# Verbose output
moq-test-client --relay https://relay.example.com:4443 -v
```

### TLS Certificate Verification

By default, TLS certificate verification is enabled (secure). For local development
with self-signed certificates, you can disable verification:

```bash
# Local development only - use --tls-disable-verify for self-signed certs
moq-test-client --relay https://localhost:4443 --tls-disable-verify
```

**Note:** Never disable TLS verification in production environments.

## Test Cases

| Test | Description |
|------|-------------|
| `setup-only` | Connect, complete SETUP exchange, close gracefully |
| `announce-only` | Connect, announce namespace, receive OK, close |
| `subscribe-error` | Subscribe to non-existent track, expect error |
| `announce-subscribe` | Publisher announces, subscriber subscribes, verify handshake |
| `subscribe-before-announce` | Subscriber subscribes before publisher announces |
| `publish-namespace-done` | Announce namespace, send PUBLISH_NAMESPACE_DONE |

## Running with moq-relay

```bash
# Terminal 1: Start the relay
cargo run --bin moq-relay -- --bind 0.0.0.0:4443 --tls-cert dev/localhost.crt --tls-key dev/localhost.key

# Terminal 2: Run tests (with --tls-disable-verify for self-signed dev certs)
cargo run --bin moq-test-client -- --relay https://localhost:4443 --tls-disable-verify
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | All tests passed |
| 1 | One or more tests failed |

## Output Format

The tool outputs human-readable results during execution and ends with a machine-parseable line:

```
MoQT Interop Test Client
========================
Relay: https://localhost:4443

✓ setup-only (42 ms)
✓ announce-only (38 ms)
✓ subscribe-error (51 ms)
✓ announce-subscribe (127 ms)
✓ subscribe-before-announce (89 ms)
✓ publish-namespace-done (45 ms)

Results: 6 passed, 0 failed

MOQT_TEST_RESULT: SUCCESS
```

## Environment Variable Interface

For Docker/containerized testing, the following environment variables are supported:

| Variable | Description |
|----------|-------------|
| `RELAY_URL` | URL of relay (`https://` for WebTransport, `moqt://` for raw QUIC) |
| `TESTCASE` | Test case identifier (optional, runs all if not set) |
| `TLS_DISABLE_VERIFY` | Set to `1` to skip TLS certificate verification |

## Multi-Implementation Testing

This tool implements the test client interface defined by [moq-interop-runner](https://github.com/cloudflare/moq-interop-runner), a framework for testing interoperability between MoQT implementations.

For Docker-based testing against multiple relays (moxygen, libquicr, moqtransport, etc.), see the moq-interop-runner repository.

## Test Case Specifications

The test cases implemented here correspond to the specifications in [moq-interop-runner/docs/tests/TEST-CASES.md](https://github.com/cloudflare/moq-interop-runner/blob/main/docs/tests/TEST-CASES.md):

| Test Case | Protocol References |
|-----------|---------------------|
| `setup-only` | MoQT §3.3, §9.3 |
| `announce-only` | MoQT §6.2, §9.23-9.24 |
| `publish-namespace-done` | MoQT §6.2, §9.26 |
| `subscribe-error` | MoQT §5.1, §9.7, §9.9 |
| `announce-subscribe` | MoQT §5.1, §6.2, §9.7-9.8, §9.23-9.24 |
| `subscribe-before-announce` | MoQT §5.1, §6.2 |

Protocol references are to [draft-ietf-moq-transport-14](https://www.ietf.org/archive/id/draft-ietf-moq-transport-14.html).

## Design Goals

1. **Interoperability**: Test that implementation A works with implementation B
2. **Reproducibility**: Same test, same inputs = same results
3. **Specifiability**: Interface documented precisely enough for independent implementation
4. **Incrementality**: Start simple, evolve to full automation

## Contributing

Contributions welcome! See [moq-rs](https://github.com/cloudflare/moq-rs) for the main project.
