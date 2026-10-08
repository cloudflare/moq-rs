// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Fuzz target for `moq_transport::setup::decode_setup_tokens`.
//!
//! Exercises the AUTHORIZATION TOKEN SETUP parameter parser against arbitrary
//! peer-supplied bytes. The production function lives in `moq-transport` (no
//! C build deps) so the fuzz harness can depend on it directly without
//! pulling in the relay's TLS/QUIC dependency tree.
//!
//! # Invariants asserted
//!
//! * `tokens.len() <= MAX_SETUP_TOKENS`
//! * Accepted token values are non-empty
//! * `counted <= n_params` (scan limit is correctly bounded)
//! * `any_present()` iff `tokens.is_empty() == false || skipped > 0`
//!
//! # Run
//!
//! ```sh
//! cargo +nightly fuzz run decode_setup_tokens fuzz/seeds/decode_setup_tokens \
//!     -- -max_total_time=300
//! ```

#![no_main]

use libfuzzer_sys::fuzz_target;
use moq_transport::coding::{KeyValuePair, KeyValuePairs};
use moq_transport::setup::{decode_setup_tokens, AliasType, ParameterType, Setup, MAX_SETUP_TOKENS};

fuzz_target!(|data: &[u8]| {
    // Interpret input as:
    //   byte[0]  = number of AUTHORIZATION TOKEN copies to include (0-7)
    //   byte[1..] = the raw parameter payload (shared by all copies)
    //
    // This structure lets the engine explore:
    //   - 0 params (empty Setup)
    //   - 1-4 params (at/below MAX_SETUP_TOKENS)
    //   - 5-7 params (above limit, exercises scan-stop path)
    //   - arbitrary payload bytes

    let (count_byte, payload) = match data.split_first() {
        Some((b, rest)) => (*b, rest),
        None => {
            // Empty input → empty Setup — verify absent case.
            let result = decode_setup_tokens(&Setup {
                params: KeyValuePairs::default(),
            });
            assert_eq!(result.tokens.len(), 0);
            assert_eq!(result.skipped, 0);
            assert!(!result.any_present());
            return;
        }
    };

    let n_params = (count_byte as usize) % 8;
    let auth_key: u64 = ParameterType::AuthorizationToken.into();

    let mut kvps = KeyValuePairs::default();
    for _ in 0..n_params {
        kvps.0.push(KeyValuePair::new_bytes(auth_key, payload.to_vec()));
    }
    let setup = Setup { params: kvps };

    // Call the production function — not a copy.
    let result = decode_setup_tokens(&setup);

    // Invariants that must hold for any input.
    assert!(
        result.tokens.len() <= MAX_SETUP_TOKENS,
        "accepted tokens must not exceed MAX_SETUP_TOKENS"
    );
    assert_eq!(
        result.any_present(),
        !result.tokens.is_empty() || result.skipped > 0,
        "any_present() must match tokens+skipped state"
    );
    for token in &result.tokens {
        assert!(!token.value.is_empty(), "accepted token value must be non-empty");
    }
    if n_params > 0 {
        let counted = result.tokens.len() + result.skipped;
        assert!(
            counted <= n_params,
            "counted ({counted}) cannot exceed n_params ({n_params})"
        );
    }

    // Verify UseValue alias type constant is stable (regression: do not redefine).
    assert_eq!(AliasType::UseValue as u64, 0x01);
});
