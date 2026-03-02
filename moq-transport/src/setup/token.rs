//! Token structure encoding for MoQ Transport SETUP parameters.
//!
//! Per draft-ietf-moq-transport Section 9.2.1.1, the Token wire format is:
//! ```text
//! Token {
//!   Alias Type (varint)
//!   Token Type (varint)
//!   Token Value (remaining bytes)
//! }
//! ```
//!
//! This module provides builders that produce the complete byte sequence
//! to be set as the AUTHORIZATION_TOKEN (0x03) parameter value.

use bytes::BytesMut;

use crate::coding::{Encode, VarInt};

/// Alias Type: USE_VALUE (0x03) - the token value is sent inline.
const ALIAS_USE_VALUE: u64 = 0x03;

/// Token Type: OUT_OF_BAND (0x00) - simple shared-secret token.
const TOKEN_TYPE_OUT_OF_BAND: u64 = 0x00;

/// Token Type: CAT (0x10) - Common Access Token (unsigned CWT).
const TOKEN_TYPE_CAT: u64 = 0x10;

/// Build a simple OUT_OF_BAND token (Type 0x00) with the Token structure wrapper.
///
/// The `value` is typically a shared secret string encoded as UTF-8 bytes.
pub fn build_simple_token(value: &[u8]) -> Vec<u8> {
    build_token(TOKEN_TYPE_OUT_OF_BAND, value)
}

/// Build a CAT token (Type 0x10) with the Token structure wrapper.
///
/// The `cwt_bytes` should be a CBOR-encoded CWT claims map (unsigned).
pub fn build_cat_token(cwt_bytes: &[u8]) -> Vec<u8> {
    build_token(TOKEN_TYPE_CAT, cwt_bytes)
}

/// Internal helper: encode alias type + token type as varints, then append value bytes.
fn build_token(token_type: u64, value: &[u8]) -> Vec<u8> {
    let mut buf = BytesMut::new();
    // Alias Type
    VarInt::try_from(ALIAS_USE_VALUE)
        .expect("alias type fits in varint")
        .encode(&mut buf)
        .expect("encode alias type");
    // Token Type
    VarInt::try_from(token_type)
        .expect("token type fits in varint")
        .encode(&mut buf)
        .expect("encode token type");
    // Token Value (raw bytes, no length prefix — remainder of parameter value)
    buf.extend_from_slice(value);
    buf.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_token_structure() {
        let value = b"my-secret";
        let token = build_simple_token(value);
        // Alias 0x03 encodes as single-byte varint 0x03
        assert_eq!(token[0], 0x03);
        // Token type 0x00 encodes as single-byte varint 0x00
        assert_eq!(token[1], 0x00);
        // Remainder is the raw value
        assert_eq!(&token[2..], value);
    }

    #[test]
    fn cat_token_structure() {
        let cwt = b"\xa3\x02\x65hello";
        let token = build_cat_token(cwt);
        // Alias 0x03
        assert_eq!(token[0], 0x03);
        // Token type 0x10 encodes as single-byte varint 0x10
        assert_eq!(token[1], 0x10);
        // Remainder is the CWT bytes
        assert_eq!(&token[2..], cwt.as_slice());
    }
}
