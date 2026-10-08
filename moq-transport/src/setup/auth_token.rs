// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! AUTHORIZATION TOKEN wire-format types and SETUP parsing.
//!
//! This module lives in `moq-transport` so that fuzz harnesses and other
//! dependents can exercise the parser without pulling in the relay's full
//! dependency tree (TLS, QUIC, HTTP server).
//!
//! `moq-relay-ietf::auth` re-exports these types and the parsing function.
//!
//! # Wire format (draft-18 §10.3.1.4)
//!
//! ```text
//! AUTHORIZATION TOKEN Option {
//!   Alias Type (vi),     ; USE_VALUE = 1 (draft-18; was 0x3 in draft-16)
//!   Token Type (vi),     ; e.g. 0x01 for CAT
//!   Token Value (..),    ; remainder of the parameter value
//! }
//! ```

use crate::coding::{Decode, Value};
use crate::setup::{ParameterType, Setup};

/// AUTHORIZATION TOKEN alias-operation codes (draft-18 §10.3.1.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub enum AliasType {
    /// `DELETE (0x0)` — retire a previously registered alias.
    Delete = 0x0,
    /// `USE_VALUE (0x1)` — inline token bytes.
    ///
    /// Note: draft-18 renumbers USE_VALUE to 0x1. The [`CAT_TOKEN_TYPE`]
    /// constant 0x01 (IANA token-type for CAT) shares the same integer value
    /// but lives in a separate wire field.
    UseValue = 0x1,
    /// `USE_ALIAS (0x2)` — reference a previously registered alias.
    UseAlias = 0x2,
    /// `REGISTER (0x3)` — register a new alias.
    Register = 0x3,
}

impl TryFrom<u64> for AliasType {
    type Error = crate::coding::DecodeError;

    fn try_from(v: u64) -> Result<Self, Self::Error> {
        match v {
            0x0 => Ok(Self::Delete),
            0x1 => Ok(Self::UseValue),
            0x2 => Ok(Self::UseAlias),
            0x3 => Ok(Self::Register),
            _ => Err(crate::coding::DecodeError::InvalidValue),
        }
    }
}

/// IANA-registered token type for CAT (Common Access Token, draft-ietf-moq-c4m §7.1).
pub const CAT_TOKEN_TYPE: u64 = 0x01;

/// Maximum accepted AUTHORIZATION TOKEN parameters per session.
///
/// Scanning stops once this many tokens have been accepted, regardless of
/// how many parameters remain.
pub const MAX_SETUP_TOKENS: usize = 4;

/// A decoded AUTHORIZATION TOKEN parameter.
#[derive(Clone, Debug)]
pub struct AuthToken {
    /// IANA token-type registry value (e.g. [`CAT_TOKEN_TYPE`]).
    pub token_type: u64,
    /// Raw token bytes (COSE_Sign1 for CAT).
    pub value: Vec<u8>,
}

impl AuthToken {
    /// Construct a new token.
    pub fn new(token_type: u64, value: Vec<u8>) -> Self {
        Self { token_type, value }
    }
}

/// Structured result from [`decode_setup_tokens`].
///
/// Distinguishes token absence from malformed tokens so callers can fail
/// closed appropriately.
///
/// | `tokens` | `skipped` | Meaning |
/// |---|---|---|
/// | non-empty | any | Valid tokens found |
/// | empty | 0 | No auth params present |
/// | empty | >0 | Auth params present but unusable — fail closed |
#[derive(Debug, Default)]
pub struct SetupTokens {
    /// Successfully decoded USE_VALUE tokens, up to [`MAX_SETUP_TOKENS`].
    pub tokens: Vec<AuthToken>,
    /// Count of AUTHORIZATION TOKEN parameters that were present but could
    /// not be used (wrong encoding, non-USE_VALUE alias, varint decode
    /// failure, or empty token value).
    pub skipped: usize,
}

impl SetupTokens {
    /// Whether any auth parameter was present (accepted or skipped).
    pub fn any_present(&self) -> bool {
        !self.tokens.is_empty() || self.skipped > 0
    }

    /// Whether all *scanned* auth parameters were accepted (none skipped).
    ///
    /// Note: scanning stops at [`MAX_SETUP_TOKENS`] accepted tokens, so
    /// parameters beyond that point are neither accepted nor skipped.
    pub fn all_accepted(&self) -> bool {
        self.skipped == 0
    }
}

/// Extract AUTHORIZATION TOKEN parameters from a SETUP message.
///
/// Accepts up to [`MAX_SETUP_TOKENS`] USE_VALUE tokens. Parameters with other
/// alias types, non-bytes encoding, decode failures, or empty token values are
/// counted in [`SetupTokens::skipped`].
///
/// Scanning stops as soon as [`MAX_SETUP_TOKENS`] tokens have been accepted.
pub fn decode_setup_tokens(setup: &Setup) -> SetupTokens {
    let auth_key: u64 = ParameterType::AuthorizationToken.into();
    let mut result = SetupTokens::default();

    for kvp in &setup.params.0 {
        if result.tokens.len() >= MAX_SETUP_TOKENS {
            break;
        }
        if kvp.key != auth_key {
            continue;
        }

        let bytes = match &kvp.value {
            Value::BytesValue(b) => b,
            _ => {
                result.skipped += 1;
                continue;
            }
        };

        let mut cursor = bytes.as_slice();

        let alias_raw = match u64::decode(&mut cursor) {
            Ok(v) => v,
            Err(_) => {
                result.skipped += 1;
                continue;
            }
        };

        match AliasType::try_from(alias_raw) {
            Ok(AliasType::UseValue) => {}
            _ => {
                result.skipped += 1;
                continue;
            }
        }

        let token_type = match u64::decode(&mut cursor) {
            Ok(v) => v,
            Err(_) => {
                result.skipped += 1;
                continue;
            }
        };

        let token_value = cursor.to_vec();
        if token_value.is_empty() {
            result.skipped += 1;
            continue;
        }

        result.tokens.push(AuthToken::new(token_type, token_value));
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::{KeyValuePair, KeyValuePairs};
    use crate::setup::Setup;

    fn setup_with_token(token_type: u64, token_bytes: &[u8]) -> Setup {
        let mut params = KeyValuePairs::default();
        let mut payload = Vec::new();
        encode_varint(AliasType::UseValue as u64, &mut payload);
        encode_varint(token_type, &mut payload);
        payload.extend_from_slice(token_bytes);
        params.set_bytesvalue(ParameterType::AuthorizationToken.into(), payload);
        Setup { params }
    }

    fn encode_varint(v: u64, buf: &mut Vec<u8>) {
        if v < 64 {
            buf.push(v as u8);
        } else if v < 16_384 {
            buf.push(((v >> 8) as u8) | 0x40);
            buf.push((v & 0xff) as u8);
        } else if v < 1_073_741_824 {
            buf.push(((v >> 24) as u8) | 0x80);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push((v & 0xff) as u8);
        } else {
            buf.push(((v >> 56) as u8) | 0xc0);
            buf.push((v >> 48) as u8);
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push((v & 0xff) as u8);
        }
    }

    #[test]
    fn no_auth_params_absent() {
        let setup = Setup {
            params: KeyValuePairs::default(),
        };
        let r = decode_setup_tokens(&setup);
        assert!(r.tokens.is_empty());
        assert_eq!(r.skipped, 0);
        assert!(!r.any_present());
    }

    #[test]
    fn valid_cat_token() {
        let setup = setup_with_token(CAT_TOKEN_TYPE, b"cose-payload");
        let r = decode_setup_tokens(&setup);
        assert_eq!(r.tokens.len(), 1);
        assert_eq!(r.skipped, 0);
        assert_eq!(r.tokens[0].token_type, CAT_TOKEN_TYPE);
        assert_eq!(r.tokens[0].value, b"cose-payload");
    }

    #[test]
    fn non_bytes_param_counted_as_skipped() {
        let mut params = KeyValuePairs::default();
        params.set_intvalue(ParameterType::AuthorizationToken.into(), 42);
        let r = decode_setup_tokens(&Setup { params });
        assert!(r.tokens.is_empty());
        assert_eq!(r.skipped, 1);
        assert!(r.any_present());
        assert!(!r.all_accepted());
    }

    #[test]
    fn empty_token_value_skipped() {
        let mut params = KeyValuePairs::default();
        let mut payload = Vec::new();
        encode_varint(AliasType::UseValue as u64, &mut payload);
        encode_varint(CAT_TOKEN_TYPE, &mut payload);
        // no bytes follow
        params.set_bytesvalue(ParameterType::AuthorizationToken.into(), payload);
        let r = decode_setup_tokens(&Setup { params });
        assert_eq!(r.skipped, 1);
    }

    #[test]
    fn max_tokens_accepted() {
        let auth_key: u64 = ParameterType::AuthorizationToken.into();
        let mut params = KeyValuePairs::default();
        for i in 0..(MAX_SETUP_TOKENS + 2) as u64 {
            let mut payload = Vec::new();
            encode_varint(AliasType::UseValue as u64, &mut payload);
            encode_varint(CAT_TOKEN_TYPE, &mut payload);
            payload.push(i as u8);
            params.0.push(KeyValuePair::new_bytes(auth_key, payload));
        }
        let r = decode_setup_tokens(&Setup { params });
        assert_eq!(r.tokens.len(), MAX_SETUP_TOKENS);
    }

    #[test]
    fn cat_token_type_is_one() {
        assert_eq!(CAT_TOKEN_TYPE, 0x01);
    }

    #[test]
    fn alias_use_value_is_one() {
        assert_eq!(AliasType::UseValue as u64, 0x01);
    }
}
