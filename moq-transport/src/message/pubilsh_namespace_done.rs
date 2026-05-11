// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! PUBLISH_NAMESPACE_DONE message (draft-ietf-moq-transport-16 §9.22).
//!
//! Sent by the publisher to stop serving new subscriptions for a namespace.
//! Carries the Request ID of the corresponding PUBLISH_NAMESPACE.

use crate::coding::{Decode, DecodeError, Encode, EncodeError};

/// Sent by the publisher to terminate a PUBLISH_NAMESPACE.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishNamespaceDone {
    /// The Request ID of the PUBLISH_NAMESPACE being terminated.
    pub id: u64,
}

impl Decode for PublishNamespaceDone {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        Ok(Self { id })
    }
}

impl Encode for PublishNamespaceDone {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();
        let msg = PublishNamespaceDone { id: 12345 };
        msg.encode(&mut buf).unwrap();
        let decoded = PublishNamespaceDone::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn round_trips_id_zero() {
        let mut buf = BytesMut::new();
        let msg = PublishNamespaceDone { id: 0 };
        msg.encode(&mut buf).unwrap();
        assert_eq!(PublishNamespaceDone::decode(&mut buf).unwrap(), msg);
    }
}
