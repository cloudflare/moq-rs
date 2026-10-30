// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Fuzz target for `moq_transport::setup::decode_setup_tokens`.
//!
//! Exercises the AUTHORIZATION TOKEN setup-option parser against arbitrary
//! peer-supplied bytes. The production function lives in `moq-transport` (no
//! C build deps) so the fuzz harness can depend on it directly without
//! pulling in the relay's TLS/QUIC dependency tree.
//!
//! # Invariants asserted
//!
//! * Never panics.
//! * `Ok(tokens)` implies `tokens.len() <= MAX_SETUP_TOKENS` and
//!   `n_params <= MAX_SETUP_TOKENS`.
//! * More than `MAX_SETUP_TOKENS` options always errors.
//! * A decoded token's value is a suffix of the input payload (the value is
//!   not length-prefixed and the parser must not copy or truncate it).
//! * Every error maps to a draft-18 §15.10.1 session close code.
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
use moq_transport::setup::{decode_setup_tokens, AliasType, ParameterType, MAX_SETUP_TOKENS};

fuzz_target!(|data: &[u8]| {
    // Interpret input as:
    //   byte[0]  = number of AUTHORIZATION TOKEN copies to include (0-7)
    //   byte[1..] = the raw option payload (shared by all copies)
    //
    // This structure lets the engine explore:
    //   - 0 options (empty SETUP)
    //   - 1-4 options (at/below MAX_SETUP_TOKENS)
    //   - 5-7 options (above the limit)
    //   - arbitrary payload bytes
    let (count_byte, payload) = match data.split_first() {
        Some((b, rest)) => (*b, rest),
        None => {
            // Empty input → empty SETUP — verify the absent case.
            let empty = KeyValuePairs::default();
            let tokens = decode_setup_tokens(&empty).expect("absent is ok");
            assert!(tokens.is_empty());
            return;
        }
    };

    let n_params = (count_byte as usize) % 8;
    let auth_key: u64 = ParameterType::AuthorizationToken.into();

    let mut kvps = KeyValuePairs::default();
    for _ in 0..n_params {
        kvps.0.push(KeyValuePair::new_bytes(auth_key, payload.to_vec()));
    }

    // Call the production function — not a copy.
    match decode_setup_tokens(&kvps) {
        Ok(tokens) => {
            assert!(tokens.len() <= MAX_SETUP_TOKENS);
            assert!(n_params <= MAX_SETUP_TOKENS, "over-limit input must error");
            assert_eq!(tokens.len(), n_params);
            for token in &tokens {
                assert!(
                    payload.ends_with(token.value),
                    "token value must be the unmodified tail of the option"
                );
            }
        }
        Err(err) => {
            assert!(
                matches!(err.session_error_code(), 0x2 | 0x3 | 0x6),
                "unexpected session close code {:#x}",
                err.session_error_code()
            );
        }
    }

    // Draft-18 §15.5 Table 11 code points (regression: an earlier port swapped
    // REGISTER and USE_VALUE).
    assert_eq!(AliasType::Register as u64, 0x01);
    assert_eq!(AliasType::UseValue as u64, 0x03);
});
