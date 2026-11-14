// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! REQUEST_OK acknowledges a successful request (draft-18 Section 10.5).
//! The request stream supplies the Request ID on the wire.

use super::TrackExtensions;
use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};

/// Sent to acknowledge a successful request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestOk {
    /// The Request ID of the message this is replying to.
    pub id: u64,

    /// Optional parameters (e.g. LARGEST_OBJECT for TRACK_STATUS responses).
    pub params: KeyValuePairs,

    /// Track Properties, populated only for TRACK_STATUS_OK (draft-18 Section 10.5).
    pub track_extensions: TrackExtensions,
}

impl RequestOk {
    /// Create an acknowledgement with no Track Properties.
    pub fn new(id: u64, params: KeyValuePairs) -> Self {
        Self {
            id,
            params,
            track_extensions: TrackExtensions::default(),
        }
    }
}

impl Decode for RequestOk {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let params = KeyValuePairs::decode_message_params(r)?;
        let track_extensions = TrackExtensions::decode(r)?;
        Ok(Self {
            id,
            params,
            track_extensions,
        })
    }
}

impl Encode for RequestOk {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.params.encode_message_params(w)?;
        self.track_extensions.encode(w)?;
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
        let msg = RequestOk::new(42, KeyValuePairs::default());
        msg.encode(&mut buf).unwrap();
        assert_eq!(&buf[..], &[42, 0]);
        let decoded = RequestOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_with_params() {
        let mut buf = BytesMut::new();
        let mut params = KeyValuePairs::new();
        params.set_intvalue(0x08, 3600); // EXPIRES example
        let msg = RequestOk::new(100, params);
        msg.encode(&mut buf).unwrap();
        let decoded = RequestOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn request_ok_preserves_track_extensions_and_empty_wire_output() {
        use crate::session::test_support::TRACK_PROPERTY_BYTES;

        for property_bytes in [TRACK_PROPERTY_BYTES, &[][..]] {
            let mut input = property_bytes;
            let mut msg = RequestOk::new(42, KeyValuePairs::default());
            msg.track_extensions = TrackExtensions::decode(&mut input).unwrap();
            let mut encoded = Vec::new();
            msg.encode(&mut encoded).unwrap();
            let mut expected = vec![42, 0];
            expected.extend_from_slice(property_bytes);
            assert_eq!(encoded, expected);

            let mut input = expected.as_slice();
            let decoded = RequestOk::decode(&mut input).unwrap();
            assert!(input.is_empty());
            assert_eq!(decoded, msg);
            let mut forwarded = Vec::new();
            decoded.track_extensions.encode(&mut forwarded).unwrap();
            assert_eq!(forwarded, property_bytes);
        }
    }
}
