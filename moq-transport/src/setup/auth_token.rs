// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! AUTHORIZATION TOKEN setup-option decoding (draft-ietf-moq-transport-18
//! §10.2.2, §10.3.1.4).
//!
//! This module lives in `moq-transport` so that fuzz harnesses can exercise the
//! parser without the relay's dependency tree (TLS, QUIC, HTTP server).
//! `moq-relay-ietf` wraps the borrowed values it returns in a zeroizing
//! credential type before handing them to an authorization hook.
//!
//! The option value is a single Token structure (§10.2.2, Figure 5):
//!
//! ```text
//! Token {
//!   Alias Type (vi64),
//!   [Token Alias (vi64),]
//!   [Token Type (vi64),]
//!   [Token Value (..)]
//! }
//! ```
//!
//! Two properties of this format are easy to get wrong and are both load
//! bearing here:
//!
//! * **Token Value is not length-prefixed.** `(..)` runs to the end of the
//!   option value, so its length comes from the enclosing key-value pair.
//! * **One Token per option.** §10.3.1.4 lets an endpoint "specify one or more
//!   tokens in SETUP", so multiple tokens arrive as repeated key-value pairs
//!   rather than as a sequence packed into one value.

use crate::coding::{Decode, KeyValuePairs, Value, VarInt};
use crate::setup::ParameterType;

/// AUTHORIZATION TOKEN Alias Type code points (draft-18 §10.2.2, §15.5
/// Table 11). Unchanged from draft-16.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub enum AliasType {
    /// Retire an alias. Carries an alias but no type or value.
    Delete = 0x0,
    /// Bind an alias to a type and value for the rest of the session.
    Register = 0x1,
    /// Reference a previously registered alias. No type or value.
    UseAlias = 0x2,
    /// Inline type and value, no alias.
    UseValue = 0x3,
}

impl TryFrom<u64> for AliasType {
    type Error = u64;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0x0 => Ok(Self::Delete),
            0x1 => Ok(Self::Register),
            0x2 => Ok(Self::UseAlias),
            0x3 => Ok(Self::UseValue),
            other => Err(other),
        }
    }
}

/// Token type for a Common Access Token (draft-ietf-moq-c4m §7.1).
pub const CAT_TOKEN_TYPE: u64 = 0x01;

/// Upper bound on tokens accepted from a single SETUP.
///
/// §10.3.1.4 lets the option repeat without stating a limit, but each token
/// costs signature verifications at setup, so a receiver has to bound it. Four
/// is well above any honest case — a client holding credentials for several
/// relays presents one or two — while the real protection against a peer
/// inflating admission cost is the verification budget, since a single token
/// can already be tried against every configured key.
pub const MAX_SETUP_TOKENS: usize = 4;

/// One AUTHORIZATION TOKEN decoded from SETUP, with aliases resolved.
///
/// Borrows the option value rather than copying it: the value is a bearer
/// credential, and every copy is one more thing that nothing zeroizes.
/// Deliberately not `Debug`, so that it cannot be logged by accident.
pub struct SetupToken<'a> {
    /// Token type from the IANA "MOQT Auth Token Type" registry.
    pub token_type: u64,
    /// The token payload. Its serialization is defined by `token_type`.
    pub value: &'a [u8],
}

/// A failure to decode an AUTHORIZATION TOKEN setup option.
///
/// Each variant maps to the session termination code draft-18 mandates for
/// that condition; see [`session_error_code`](TokenError::session_error_code).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    /// The Token structure could not be decoded.
    #[error("malformed token structure: {0}")]
    Malformed(&'static str),

    /// DELETE or USE_ALIAS appeared in SETUP. §10.2.2 requires the server to
    /// close the session with PROTOCOL_VIOLATION, since no alias can have been
    /// registered before the first message of the session.
    #[error("alias type {0:#x} is not permitted in SETUP")]
    AliasInSetup(u64),

    /// The option was present but not bytes-valued.
    #[error("AUTHORIZATION TOKEN option must be bytes-encoded")]
    NotBytes,

    /// More tokens than [`MAX_SETUP_TOKENS`] were presented.
    #[error("too many authorization tokens: {0}")]
    TooMany(usize),
}

impl TokenError {
    /// The session termination code for this failure (draft-18 §15.10.1).
    pub fn session_error_code(&self) -> u32 {
        match self {
            // §10.2.2: "If a server receives Alias Type DELETE (0x0) or
            // USE_ALIAS (0x2) in a SETUP message, it MUST close the session
            // with a PROTOCOL_VIOLATION."
            Self::AliasInSetup(_) => 0x3,
            // §10.2.2: "If the Token structure cannot be decoded, the receiver
            // MUST close the Session with KEY_VALUE_FORMATTING_ERROR."
            Self::Malformed(_) | Self::NotBytes => 0x6,
            // Not a decoding failure; a resource bound. UNAUTHORIZED.
            Self::TooMany(_) => 0x2,
        }
    }

    /// The reason phrase sent to the peer. Fixed text, never validation detail.
    pub fn reason_phrase(&self) -> &'static str {
        match self {
            Self::AliasInSetup(_) => "token alias not permitted in setup",
            Self::Malformed(_) | Self::NotBytes => "malformed authorization token",
            Self::TooMany(_) => "too many authorization tokens",
        }
    }

    /// A stable, low-cardinality label for metrics.
    pub fn metric_label(&self) -> &'static str {
        match self {
            Self::AliasInSetup(_) => "alias_in_setup",
            Self::Malformed(_) | Self::NotBytes => "token_malformed",
            Self::TooMany(_) => "too_many_tokens",
        }
    }
}

/// Decode every AUTHORIZATION TOKEN option from a SETUP parameter set.
///
/// Iterates the options directly rather than using [`KeyValuePairs::get`],
/// which returns only the first match and would silently drop the repetitions
/// §10.3.1.4 permits.
///
/// Returns tokens in the order they appeared. An absent option yields an empty
/// vector; whether that is acceptable is the caller's policy decision.
pub fn decode_setup_tokens(params: &KeyValuePairs) -> Result<Vec<SetupToken<'_>>, TokenError> {
    let key = u64::from(ParameterType::AuthorizationToken);
    let mut tokens = Vec::new();

    let present = params.0.iter().filter(|kvp| kvp.key == key).count();

    for kvp in params.0.iter().filter(|kvp| kvp.key == key) {
        let Value::BytesValue(bytes) = &kvp.value else {
            return Err(TokenError::NotBytes);
        };

        // Decode every option even once the bound is exceeded, so that a
        // structural violation is still reported as one. §10.2.2's "MUST close
        // with PROTOCOL_VIOLATION" for an alias directive in SETUP does not
        // stop applying because the peer also sent too many tokens.
        let token = decode_token(bytes)?;
        if tokens.len() < MAX_SETUP_TOKENS {
            tokens.push(token);
        }
    }

    if present > MAX_SETUP_TOKENS {
        return Err(TokenError::TooMany(present));
    }

    Ok(tokens)
}

/// Decode one Token structure from an option value.
fn decode_token(value: &[u8]) -> Result<SetupToken<'_>, TokenError> {
    let mut buf = value;

    let alias_type = VarInt::decode(&mut buf)
        .map_err(|_| TokenError::Malformed("missing alias type"))?
        .into_inner();

    match AliasType::try_from(alias_type) {
        // Both reference an alias that cannot exist yet: SETUP is the first
        // message of the session, so nothing has been registered.
        Ok(AliasType::Delete | AliasType::UseAlias) => Err(TokenError::AliasInSetup(alias_type)),

        // §10.3.1.4: a REGISTER in SETUP "that exceeds its
        // MAX_AUTH_TOKEN_CACHE_SIZE ... MUST NOT fail the session with
        // AUTH_TOKEN_CACHE_OVERFLOW. Instead, it MUST treat the option as
        // Alias Type USE_VALUE." This crate never advertises
        // MAX_AUTH_TOKEN_CACHE_SIZE, so its default of 0 applies and every
        // registration is handled as an inline value. The alias is consumed
        // and discarded.
        Ok(AliasType::Register) => {
            VarInt::decode(&mut buf)
                .map_err(|_| TokenError::Malformed("REGISTER missing token alias"))?;
            decode_type_and_value(buf)
        }

        Ok(AliasType::UseValue) => decode_type_and_value(buf),

        // An unrecognized alias type means the rest of the structure cannot be
        // interpreted, which §10.2.2 treats as a decode failure.
        Err(_) => Err(TokenError::Malformed("unknown alias type")),
    }
}

/// Decode the trailing `Token Type (vi64)` and `Token Value (..)`.
///
/// The value is whatever remains: it carries no length prefix of its own.
fn decode_type_and_value(mut buf: &[u8]) -> Result<SetupToken<'_>, TokenError> {
    let token_type = VarInt::decode(&mut buf)
        .map_err(|_| TokenError::Malformed("missing token type"))?
        .into_inner();

    Ok(SetupToken {
        token_type,
        value: buf,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::{Encode, KeyValuePair};

    /// Build a parameter set from raw AUTHORIZATION TOKEN values, preserving
    /// repeats. `KeyValuePairs::set_bytesvalue` deduplicates by key, so the
    /// repeated-key case has to be constructed directly.
    fn params(values: Vec<Vec<u8>>) -> KeyValuePairs {
        let key = u64::from(ParameterType::AuthorizationToken);
        KeyValuePairs(
            values
                .into_iter()
                .map(|value| KeyValuePair::new_bytes(key, value))
                .collect(),
        )
    }

    fn varint(value: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        VarInt::from(value).encode(&mut buf).unwrap();
        buf
    }

    /// A USE_VALUE token, encoded the way a conformant peer would.
    fn use_value(token_type: u64, value: &[u8]) -> Vec<u8> {
        let mut buf = varint(AliasType::UseValue as u64);
        buf.extend_from_slice(&varint(token_type));
        buf.extend_from_slice(value);
        buf
    }

    #[test]
    fn decodes_a_use_value_token() {
        let p = params(vec![use_value(CAT_TOKEN_TYPE, b"payload")]);
        let decoded = decode_setup_tokens(&p).expect("well-formed");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].token_type, CAT_TOKEN_TYPE);
        assert_eq!(decoded[0].value, b"payload");
    }

    /// §15.5 Table 11. An earlier draft-18 port swapped REGISTER and USE_VALUE;
    /// this pins the registered code points.
    #[test]
    fn alias_type_code_points_match_draft18_table_11() {
        assert_eq!(AliasType::Delete as u64, 0x0);
        assert_eq!(AliasType::Register as u64, 0x1);
        assert_eq!(AliasType::UseAlias as u64, 0x2);
        assert_eq!(AliasType::UseValue as u64, 0x3);
        for code in 0..4 {
            assert_eq!(AliasType::try_from(code).unwrap() as u64, code);
        }
        assert_eq!(AliasType::try_from(4), Err(4));
    }

    /// 0x1 is REGISTER, which carries a Token Alias before the type. Reading
    /// it as USE_VALUE would take the alias for the token type.
    #[test]
    fn alias_type_one_is_register_not_use_value() {
        let mut register = varint(0x1);
        register.extend_from_slice(&varint(42)); // Token Alias
        register.extend_from_slice(&varint(CAT_TOKEN_TYPE));
        register.extend_from_slice(b"payload");

        let p = params(vec![register]);
        let decoded = decode_setup_tokens(&p).unwrap();
        assert_eq!(decoded[0].token_type, CAT_TOKEN_TYPE);
        assert_eq!(decoded[0].value, b"payload");
    }

    #[test]
    fn delete_in_setup_is_a_protocol_violation() {
        let mut delete = varint(AliasType::Delete as u64);
        delete.extend_from_slice(&varint(7));

        let err = decode_setup_tokens(&params(vec![delete])).err().unwrap();
        assert_eq!(err, TokenError::AliasInSetup(0x0));
        // §10.2.2 mandates PROTOCOL_VIOLATION (0x3) specifically.
        assert_eq!(err.session_error_code(), 0x3);
    }

    #[test]
    fn use_alias_in_setup_is_a_protocol_violation() {
        let mut use_alias = varint(AliasType::UseAlias as u64);
        use_alias.extend_from_slice(&varint(3));

        let err = decode_setup_tokens(&params(vec![use_alias])).err().unwrap();
        assert_eq!(err, TokenError::AliasInSetup(0x2));
        assert_eq!(err.session_error_code(), 0x3);
    }

    /// The value runs to the end of the option. A payload that begins with
    /// bytes resembling a varint length must survive intact — this is exactly
    /// what a length-prefixed parser corrupts.
    #[test]
    fn token_value_is_not_length_prefixed() {
        let payload = b"\x07\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4a\x4b";
        let p = params(vec![use_value(CAT_TOKEN_TYPE, payload)]);
        let decoded = decode_setup_tokens(&p).unwrap();

        assert_eq!(decoded[0].value, payload);
        assert_eq!(decoded[0].value.len(), 12);
    }

    #[test]
    fn empty_token_value_is_accepted_by_the_decoder() {
        // Structurally valid: Token Value is optional in the grammar. Whether
        // an empty CAT payload is acceptable is the hook's decision.
        let p = params(vec![use_value(CAT_TOKEN_TYPE, b"")]);
        let decoded = decode_setup_tokens(&p).unwrap();

        assert_eq!(decoded.len(), 1);
        assert!(decoded[0].value.is_empty());
    }

    #[test]
    fn repeated_options_all_decode() {
        let p = params(vec![
            use_value(CAT_TOKEN_TYPE, b"first"),
            use_value(0, b"second"),
            use_value(CAT_TOKEN_TYPE, b"third"),
        ]);
        let decoded = decode_setup_tokens(&p).unwrap();

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].value, b"first");
        assert_eq!(decoded[1].token_type, 0);
        assert_eq!(decoded[2].value, b"third");
    }

    #[test]
    fn absent_option_yields_no_tokens() {
        assert!(decode_setup_tokens(&KeyValuePairs::default())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn other_options_are_ignored() {
        let mut p = KeyValuePairs::default();
        p.set_bytesvalue(u64::from(ParameterType::Path), b"/tenant".to_vec());
        p.set_bytesvalue(u64::from(ParameterType::Authority), b"relay".to_vec());

        assert!(decode_setup_tokens(&p).unwrap().is_empty());
    }

    #[test]
    fn empty_option_value_is_malformed() {
        let err = decode_setup_tokens(&params(vec![vec![]])).err().unwrap();
        assert_eq!(err, TokenError::Malformed("missing alias type"));
        assert_eq!(err.session_error_code(), 0x6);
    }

    #[test]
    fn use_value_without_token_type_is_malformed() {
        let p = params(vec![varint(AliasType::UseValue as u64)]);
        let err = decode_setup_tokens(&p).err().unwrap();
        assert_eq!(err, TokenError::Malformed("missing token type"));
        assert_eq!(err.session_error_code(), 0x6);
    }

    #[test]
    fn register_without_alias_is_malformed() {
        let p = params(vec![varint(AliasType::Register as u64)]);
        let err = decode_setup_tokens(&p).err().unwrap();
        assert_eq!(err, TokenError::Malformed("REGISTER missing token alias"));
    }

    #[test]
    fn unknown_alias_type_is_malformed() {
        let err = decode_setup_tokens(&params(vec![varint(0x9)]))
            .err()
            .unwrap();
        assert_eq!(err, TokenError::Malformed("unknown alias type"));
        assert_eq!(err.session_error_code(), 0x6);
    }

    #[test]
    fn truncated_varint_is_malformed() {
        // 0x80 opens a multi-byte varint whose continuation never arrives.
        let err = decode_setup_tokens(&params(vec![vec![0x80]]))
            .err()
            .unwrap();
        assert_eq!(err, TokenError::Malformed("missing alias type"));
    }

    #[test]
    fn int_valued_option_is_rejected() {
        let key = u64::from(ParameterType::AuthorizationToken);
        let p = KeyValuePairs(vec![KeyValuePair::new_int(key, 5)]);

        let err = decode_setup_tokens(&p).err().unwrap();
        assert_eq!(err, TokenError::NotBytes);
        assert_eq!(err.session_error_code(), 0x6);
    }

    #[test]
    fn token_count_is_bounded() {
        let values = (0..MAX_SETUP_TOKENS + 1)
            .map(|_| use_value(CAT_TOKEN_TYPE, b"payload"))
            .collect();
        let p = params(values);
        let err = decode_setup_tokens(&p).err().unwrap();
        assert!(matches!(err, TokenError::TooMany(_)));
        assert_eq!(err.session_error_code(), 0x2);

        // Exactly at the bound is still accepted.
        let at_limit = params(
            (0..MAX_SETUP_TOKENS)
                .map(|_| use_value(CAT_TOKEN_TYPE, b"payload"))
                .collect(),
        );
        assert_eq!(
            decode_setup_tokens(&at_limit).unwrap().len(),
            MAX_SETUP_TOKENS
        );
    }

    /// An alias directive past the bound is still reported as the protocol
    /// violation it is, not masked by the count check.
    #[test]
    fn alias_violation_past_the_bound_is_still_reported() {
        let mut values: Vec<Vec<u8>> = (0..MAX_SETUP_TOKENS)
            .map(|_| use_value(CAT_TOKEN_TYPE, b"payload"))
            .collect();
        let mut use_alias = varint(AliasType::UseAlias as u64);
        use_alias.extend_from_slice(&varint(1));
        values.push(use_alias);

        let p = params(values);
        assert_eq!(
            decode_setup_tokens(&p).err().unwrap(),
            TokenError::AliasInSetup(0x2)
        );
    }

    #[test]
    fn multi_byte_varint_token_type_round_trips() {
        // 0x63346d needs a multi-byte varint, exercising the non-trivial path.
        let p = params(vec![use_value(0x63346d, b"payload")]);
        let decoded = decode_setup_tokens(&p).unwrap();

        assert_eq!(decoded[0].token_type, 0x63346d);
        assert_eq!(decoded[0].value, b"payload");
    }
}
