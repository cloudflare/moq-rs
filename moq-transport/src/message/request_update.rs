// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! REQUEST_UPDATE message (draft-ietf-moq-transport-16 §9.11).
//!
//! The sender of a request (SUBSCRIBE, PUBLISH, FETCH, TRACK_STATUS,
//! PUBLISH_NAMESPACE, SUBSCRIBE_NAMESPACE) sends REQUEST_UPDATE to modify
//! it.  The receiver responds with exactly one REQUEST_OK or REQUEST_ERROR.

use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};

/// Sent to modify an existing request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestUpdate {
    /// New Request ID for this update message.
    pub id: u64,

    /// The Request ID of the request being modified.
    pub existing_request_id: u64,

    /// Parameters to update. Absent parameters retain their current values.
    pub params: KeyValuePairs,
}

impl Decode for RequestUpdate {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let existing_request_id = u64::decode(r)?;
        let params = KeyValuePairs::decode(r)?;
        Ok(Self {
            id,
            existing_request_id,
            params,
        })
    }
}

impl Encode for RequestUpdate {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.existing_request_id.encode(w)?;
        self.params.encode(w)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();
        let mut params = KeyValuePairs::new();
        params.set_intvalue(0x10, 1); // FORWARD=1
        let msg = RequestUpdate {
            id: 4,
            existing_request_id: 2,
            params,
        };
        msg.encode(&mut buf).unwrap();
        let decoded = RequestUpdate::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_no_params() {
        let mut buf = BytesMut::new();
        let msg = RequestUpdate {
            id: 6,
            existing_request_id: 4,
            params: KeyValuePairs::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = RequestUpdate::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
