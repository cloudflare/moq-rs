# Repository Guidance

- This repository targets MoQT `draft-ietf-moq-transport-16`.
- Core protocol code lives in `moq-transport`; relay behavior lives in `moq-relay-ietf`.
- `moq-relay-ietf` is production critical, so prefer small, tested changes over broad refactors.
- Run `cargo test -p moq-transport` for protocol changes.
- Run `cargo build --workspace` when shared APIs, relay behavior, or test-client behavior changes.
- Do not commit `docs/draft-16.txt` unless explicitly requested; it is local protocol reference material.
