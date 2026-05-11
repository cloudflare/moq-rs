// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SERVER_SETUP message (draft-ietf-moq-transport-16 §9.3).
//!
//! From draft-16, version negotiation is performed via ALPN only.  The
//! SERVER_SETUP payload carries setup parameters only; it no longer echoes
//! the selected version.

use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};

/// Sent by the server in response to CLIENT_SETUP.
///
/// Message Type = 0x21 (unchanged from draft-11+).
/// The payload contains only setup parameters; version is agreed via ALPN.
#[derive(Debug)]
pub struct Server {
    /// Setup parameters (MAX_REQUEST_ID, MAX_AUTH_TOKEN_CACHE_SIZE,
    /// AUTHORIZATION_TOKEN, MOQT_IMPLEMENTATION, …).
    pub params: KeyValuePairs,
}

impl Decode for Server {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let typ = u64::decode(r)?;
        if typ != 0x21 {
            return Err(DecodeError::InvalidMessage(typ));
        }

        let _len = u16::decode(r)?;
        // TODO(itzmanish): enforce message length.

        let params = KeyValuePairs::decode(r)?;

        Ok(Self { params })
    }
}

impl Encode for Server {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        (0x21_u64).encode(w)?;

        let mut buf = Vec::new();
        self.params.encode(&mut buf)?;

        if buf.len() > u16::MAX as usize {
            return Err(EncodeError::MsgBoundsExceeded);
        }
        (buf.len() as u16).encode(w)?;
        Self::encode_remaining(w, buf.len())?;
        w.put_slice(&buf);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::ParameterType;
    use bytes::BytesMut;

    #[test]
    fn encode_decode_params_only() {
        let mut buf = BytesMut::new();

        let mut params = KeyValuePairs::default();
        // MAX_REQUEST_ID is even key (0x02) → int value; delta from 0 = 2
        params.set_intvalue(ParameterType::MaxRequestId.into(), 1000);

        let server = Server { params };
        server.encode(&mut buf).unwrap();

        // Wire layout:
        //   0x21          type
        //   len (2B)      16-bit length of payload
        //   payload:
        //     0x01        count = 1 param
        //     0x02        delta=2 → abs_type=2 (MAX_REQUEST_ID, even→int)
        //     0x43 0xe8   value=1000 (2-byte varint)
        let bytes = buf.to_vec();
        assert_eq!(bytes[0], 0x21);
        let payload_len = u16::from_be_bytes([bytes[1], bytes[2]]) as usize;
        assert_eq!(bytes.len(), 3 + payload_len);

        let decoded = Server::decode(&mut buf).unwrap();
        assert_eq!(decoded.params, server.params);
    }

    #[test]
    fn encode_decode_no_params() {
        let mut buf = BytesMut::new();
        let server = Server {
            params: KeyValuePairs::default(),
        };
        server.encode(&mut buf).unwrap();

        assert_eq!(buf[0], 0x21);
        let payload_len = u16::from_be_bytes([buf[1], buf[2]]) as usize;
        assert_eq!(payload_len, 1); // just count=0

        let decoded = Server::decode(&mut buf).unwrap();
        assert!(decoded.params.0.is_empty());
    }
}
