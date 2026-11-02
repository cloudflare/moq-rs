// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Outbound AUTHORIZATION TOKEN setup options (draft-ietf-moq-transport-18
//! §10.3.1.4). The receiving side is in [`super::decode_setup_tokens`].

use std::fmt;

use crate::coding::{Encode, EncodeError, KeyValuePair, VarInt};

use super::{AliasType, ParameterType};

/// An opaque authorization token a client presents inline in SETUP.
///
/// The token type identifies how the receiver interprets the value; this type
/// deliberately knows nothing about CAT or any other token format.
///
/// This type always sends alias type USE_VALUE. That is a policy of this API,
/// not a protocol rule: SETUP also permits REGISTER, which a receiver treats
/// as USE_VALUE when it exceeds its token cache size (§10.3.1.4). Only DELETE
/// and USE_ALIAS are invalid in SETUP (§10.2.2).
pub struct AuthorizationToken {
    token_type: VarInt,
    value: Vec<u8>,
}

impl AuthorizationToken {
    /// Create a token from a MoQT token type and its opaque value.
    ///
    /// Infallible: draft-18's vi64 encoding (§1.4.1) represents every `u64`,
    /// unlike the 62-bit QUIC varint of earlier drafts. The value is not
    /// validated: it may be empty, and token type 0 (reserved for a type
    /// agreed out of band) is accepted.
    pub fn new(token_type: u64, value: Vec<u8>) -> Self {
        Self {
            token_type: VarInt::from(token_type),
            value,
        }
    }

    /// The option value: `USE_VALUE (vi64), Token Type (vi64), Token Value
    /// (..)`. The value has no length of its own; the option's length
    /// delimits it.
    pub(crate) fn encode_value(&self) -> Result<Vec<u8>, EncodeError> {
        let mut encoded = Vec::with_capacity(self.value.len() + 9);
        VarInt::from(AliasType::UseValue as u64).encode(&mut encoded)?;
        self.token_type.encode(&mut encoded)?;
        encoded.extend_from_slice(&self.value);
        Ok(encoded)
    }
}

/// Encode tokens as SETUP options, dropping exact duplicates.
///
/// §10.2.2 allows the AUTHORIZATION TOKEN parameter to repeat only while each
/// (Token Type, Token Value) pair is unique, and §10.3.1.4 applies that to
/// SETUP. An exact duplicate is dropped rather than rejected, because it
/// carries no information the first copy did not. Tokens that share only a
/// type or only a value are distinct and all sent, in order.
///
/// No count limit is applied here: how many tokens to accept is the
/// receiver's policy, and receivers differ.
pub(crate) fn encode_setup_options(
    tokens: &[AuthorizationToken],
) -> Result<Vec<KeyValuePair>, EncodeError> {
    let mut seen: Vec<Vec<u8>> = Vec::with_capacity(tokens.len());
    for token in tokens {
        let encoded = token.encode_value()?;
        // The encoding is injective in (type, value), so comparing encodings
        // compares pairs. Token lists are tiny; a linear scan is fine.
        if !seen.contains(&encoded) {
            seen.push(encoded);
        }
    }
    Ok(seen
        .into_iter()
        .map(|value| KeyValuePair::new_bytes(ParameterType::AuthorizationToken.into(), value))
        .collect())
}

impl fmt::Debug for AuthorizationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthorizationToken")
            .field("token_type", &self.token_type)
            .field(
                "value",
                &format_args!("<redacted, {} bytes>", self.value.len()),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::KeyValuePairs;
    use crate::setup::{decode_setup_tokens, CAT_TOKEN_TYPE};

    #[test]
    fn encodes_an_inline_token_without_a_value_length() {
        let token = AuthorizationToken::new(1, vec![0xaa, 0xbb]);
        assert_eq!(token.encode_value().unwrap(), [0x03, 0x01, 0xaa, 0xbb]);
    }

    #[test]
    fn multi_byte_token_types_are_vi64_encoded() {
        let token = AuthorizationToken::new(0x63346d, b"x".to_vec());
        let mut expected = vec![0x03];
        VarInt::from(0x63346du64).encode(&mut expected).unwrap();
        expected.push(b'x');
        assert_eq!(token.encode_value().unwrap(), expected);
        // 0x63346d is 23 bits: the 4-byte vi64 form (leading 1110).
        assert_eq!(expected[1] & 0xf0, 0xe0);
    }

    /// vi64 covers all of u64, so even the largest type is encodable and
    /// survives the decoder.
    #[test]
    fn the_full_u64_range_is_accepted() {
        let token = AuthorizationToken::new(u64::MAX, b"v".to_vec());
        let params = KeyValuePairs(vec![KeyValuePair::new_bytes(
            ParameterType::AuthorizationToken.into(),
            token.encode_value().unwrap(),
        )]);
        let decoded = decode_setup_tokens(&params).unwrap();
        assert_eq!(decoded[0].token_type, u64::MAX);
        assert_eq!(decoded[0].value, b"v");
    }

    /// What a client sends must be what the relay's decoder accepts.
    #[test]
    fn round_trips_through_the_setup_decoder() {
        let tokens = [
            AuthorizationToken::new(CAT_TOKEN_TYPE, b"first".to_vec()),
            AuthorizationToken::new(0x63346d, b"second".to_vec()),
        ];
        let params = KeyValuePairs(
            tokens
                .iter()
                .map(|token| {
                    KeyValuePair::new_bytes(
                        ParameterType::AuthorizationToken.into(),
                        token.encode_value().unwrap(),
                    )
                })
                .collect(),
        );

        let decoded = decode_setup_tokens(&params).expect("well-formed");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].token_type, CAT_TOKEN_TYPE);
        assert_eq!(decoded[0].value, b"first");
        assert_eq!(decoded[1].token_type, 0x63346d);
        assert_eq!(decoded[1].value, b"second");
    }

    #[test]
    fn exact_duplicates_are_dropped_and_partial_matches_kept() {
        let tokens = [
            AuthorizationToken::new(1, vec![1, 2]),
            AuthorizationToken::new(1, vec![1, 2]), // exact duplicate
            AuthorizationToken::new(1, vec![3]),    // same type, other value
            AuthorizationToken::new(2, vec![1, 2]), // other type, same value
        ];
        let options = encode_setup_options(&tokens).unwrap();
        assert_eq!(options.len(), 3);

        let params = KeyValuePairs(options);
        let decoded = decode_setup_tokens(&params).unwrap();
        let pairs: Vec<(u64, &[u8])> = decoded.iter().map(|t| (t.token_type, t.value)).collect();
        assert_eq!(pairs, [(1, &[1u8, 2][..]), (1, &[3][..]), (2, &[1, 2][..])]);
    }

    #[test]
    fn no_tokens_means_no_options() {
        assert!(encode_setup_options(&[]).unwrap().is_empty());
    }

    #[test]
    fn debug_redacts_the_token_value() {
        let token = AuthorizationToken::new(1, b"secret".to_vec());
        let debug = format!("{token:?}");
        assert!(debug.contains("<redacted, 6 bytes>"));
        assert!(!debug.contains("secret"));
    }
}
