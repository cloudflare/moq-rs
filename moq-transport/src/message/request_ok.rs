// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! REQUEST_OK message (draft-ietf-moq-transport-16 §9.7).
//!
//! Sent in response to REQUEST_UPDATE, TRACK_STATUS, SUBSCRIBE_NAMESPACE,
//! and PUBLISH_NAMESPACE requests.  The Request ID identifies which request
//! this acknowledgement is for.

use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};

/// Sent to acknowledge a successful request update or status query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestOk {
    /// The Request ID of the message this is replying to.
    pub id: u64,

    /// Optional parameters (e.g. LARGEST_OBJECT for TRACK_STATUS responses).
    pub params: KeyValuePairs,
}

impl Decode for RequestOk {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let params = KeyValuePairs::decode(r)?;
        Ok(Self { id, params })
    }
}

impl Encode for RequestOk {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.params.encode(w)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_decode_no_params() {
        let mut buf = BytesMut::new();
        let msg = RequestOk {
            id: 42,
            params: KeyValuePairs::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = RequestOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_with_params() {
        let mut buf = BytesMut::new();
        let mut params = KeyValuePairs::new();
        params.set_intvalue(0x08, 3600); // EXPIRES example
        let msg = RequestOk { id: 100, params };
        msg.encode(&mut buf).unwrap();
        let decoded = RequestOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
