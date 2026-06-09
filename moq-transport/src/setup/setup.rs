// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Unified SETUP message (draft-ietf-moq-transport-18 §10.3).
//!
//! In draft-18 both peers send the same SETUP message on their respective
//! unidirectional control streams. The message type (`0x2F00`) doubles as
//! the stream type identifier. Setup Options are length-bounded KVPs
//! (no count prefix).
//!
//! ```text
//! SETUP Message {
//!   Type (vi64) = 0x2F00,
//!   Length (16),
//!   Setup Options (..) ...,
//! }
//! ```

use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};

/// The SETUP message type, which also serves as the control stream type.
pub const SETUP_TYPE: u64 = 0x2F00;

/// Sent by both peers to establish the session (draft-18).
///
/// Replaces the separate CLIENT_SETUP (0x20) and SERVER_SETUP (0x21)
/// from earlier drafts. Version negotiation is handled entirely by ALPN;
/// this message carries only Setup Options (PATH, AUTHORITY, etc.).
#[derive(Debug)]
pub struct Setup {
    /// Setup Options encoded as length-bounded KVPs.
    pub params: KeyValuePairs,
}

impl Setup {
    /// Decode a SETUP message, assuming the stream type / message type
    /// varint has already been read and matched against `SETUP_TYPE`.
    ///
    /// This is the typical path when accepting a control stream: the
    /// caller reads the stream type varint first to dispatch, then calls
    /// this to parse the remainder.
    pub fn decode_after_type<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let len = u16::decode(r)? as usize;
        let params = KeyValuePairs::decode_bounded(r, len)?;
        Ok(Self { params })
    }

    /// Encode the full SETUP message including the type prefix.
    ///
    /// This writes the complete message: type varint + u16 length + KVP payload.
    /// Used when opening a control stream (the type varint doubles as the
    /// stream type identifier).
    pub fn encode_full<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        // Type / stream-type prefix
        SETUP_TYPE.encode(w)?;

        // Encode options into a temporary buffer to measure length
        let payload = self.params.encode_bounded()?;

        if payload.len() > u16::MAX as usize {
            return Err(EncodeError::MsgBoundsExceeded);
        }
        (payload.len() as u16).encode(w)?;
        Self::encode_remaining(w, payload.len())?;
        w.put_slice(&payload);

        Ok(())
    }
}

// Standard Decode reads the type prefix then delegates.
impl Decode for Setup {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let typ = u64::decode(r)?;
        if typ != SETUP_TYPE {
            return Err(DecodeError::InvalidMessage(typ));
        }
        Self::decode_after_type(r)
    }
}

// Standard Encode writes the full message.
impl Encode for Setup {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.encode_full(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::ParameterType;
    use bytes::Buf as _;
    use bytes::BytesMut;

    #[test]
    fn encode_decode_with_path() {
        let mut buf = BytesMut::new();

        let mut params = KeyValuePairs::default();
        params.set_bytesvalue(ParameterType::Path.into(), b"/moq".to_vec());

        let setup = Setup { params };
        setup.encode(&mut buf).unwrap();

        // Verify wire layout:
        // Type: 0x2F00 as varint = [0xAF, 0x00] (2 bytes)
        // Length: u16
        // Options: delta=1 (PATH), length=4, "/moq"
        assert_eq!(buf[0], 0xAF); // first byte of varint 0x2F00
        assert_eq!(buf[1], 0x00);

        let decoded = Setup::decode(&mut buf).unwrap();
        assert_eq!(decoded.params, setup.params);
    }

    #[test]
    fn encode_decode_empty() {
        let mut buf = BytesMut::new();
        let setup = Setup {
            params: KeyValuePairs::default(),
        };
        setup.encode(&mut buf).unwrap();

        // Type (2B) + Length (2B, value=0) + no options
        assert_eq!(buf.len(), 4);
        assert_eq!(buf[2], 0x00); // length high byte
        assert_eq!(buf[3], 0x00); // length low byte

        let decoded = Setup::decode(&mut buf).unwrap();
        assert!(decoded.params.0.is_empty());
    }

    #[test]
    fn decode_after_type() {
        let mut buf = BytesMut::new();

        let mut params = KeyValuePairs::default();
        params.set_bytesvalue(ParameterType::Path.into(), b"/test".to_vec());
        params.set_intvalue(ParameterType::MaxRequestId.into(), 42);

        let setup = Setup { params };
        setup.encode(&mut buf).unwrap();

        // Skip the type varint (0x2F00 = 2 bytes)
        buf.advance(2);
        let decoded = Setup::decode_after_type(&mut buf).unwrap();
        assert_eq!(decoded.params, setup.params);
    }

    #[test]
    fn decode_rejects_wrong_type() {
        let mut buf = BytesMut::new();
        (0x20_u64).encode(&mut buf).unwrap(); // old CLIENT_SETUP type
        (0_u16).encode(&mut buf).unwrap();

        assert!(matches!(
            Setup::decode(&mut buf).unwrap_err(),
            DecodeError::InvalidMessage(0x20)
        ));
    }

    #[test]
    fn round_trip_multiple_options() {
        let mut buf = BytesMut::new();

        let mut params = KeyValuePairs::default();
        params.set_bytesvalue(ParameterType::Path.into(), b"/live/stream".to_vec());
        params.set_intvalue(ParameterType::MaxRequestId.into(), 1000);
        params.set_bytesvalue(ParameterType::Authority.into(), b"relay.example.com".to_vec());

        let setup = Setup { params };
        setup.encode(&mut buf).unwrap();
        let decoded = Setup::decode(&mut buf).unwrap();

        assert_eq!(decoded.params.0.len(), 3);
        assert_eq!(decoded.params, setup.params);
    }
}
