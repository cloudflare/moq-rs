// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! AUTHORIZATION TOKEN decoding for the relay (draft-ietf-moq-transport-18
//! §10.2.2, §10.3.1.4).
//!
//! The wire parser lives in [`moq_transport::setup::decode_setup_tokens`] so
//! that it can be fuzzed without the relay's dependency tree. This module
//! takes the values it borrows and moves each into an [`AuthToken`], whose
//! buffer zeroizes on drop, before any hook sees them.

use moq_transport::coding::KeyValuePairs;

pub use moq_transport::setup::{TokenError, CAT_TOKEN_TYPE, MAX_SETUP_TOKENS};

use super::AuthToken;

/// Decode every AUTHORIZATION TOKEN option from a SETUP parameter set.
///
/// Returns tokens in the order they appeared, with aliases resolved. An absent
/// option yields an empty vector; whether that is acceptable is the caller's
/// policy decision. See [`moq_transport::setup::decode_setup_tokens`] for the
/// decoding rules and [`TokenError`] for the session close code each failure
/// maps to.
pub fn decode_setup_tokens(params: &KeyValuePairs) -> Result<Vec<AuthToken>, TokenError> {
    Ok(moq_transport::setup::decode_setup_tokens(params)?
        .into_iter()
        .map(|token| AuthToken::new(token.token_type, token.value.to_vec()))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use moq_transport::coding::{Encode, KeyValuePair, VarInt};
    use moq_transport::setup::ParameterType;

    fn varint(value: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        VarInt::from(value).encode(&mut buf).unwrap();
        buf
    }

    fn params(values: Vec<Vec<u8>>) -> KeyValuePairs {
        let key = u64::from(ParameterType::AuthorizationToken);
        KeyValuePairs(
            values
                .into_iter()
                .map(|value| KeyValuePair::new_bytes(key, value))
                .collect(),
        )
    }

    /// A conformant draft-18 client sends USE_VALUE as 0x3 (§15.5 Table 11).
    #[test]
    fn decodes_a_draft18_use_value_token() {
        let mut value = varint(0x3);
        value.extend_from_slice(&varint(CAT_TOKEN_TYPE));
        value.extend_from_slice(b"payload");

        let decoded = decode_setup_tokens(&params(vec![value])).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].token_type, CAT_TOKEN_TYPE);
        assert_eq!(decoded[0].expose_value(), b"payload");
    }

    #[test]
    fn decode_errors_pass_through() {
        let err = decode_setup_tokens(&params(vec![varint(0x2)])).unwrap_err();
        assert_eq!(err, TokenError::AliasInSetup(0x2));
        assert_eq!(err.session_error_code(), 0x3);
    }
}
