// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! PUBLISH_NAMESPACE_CANCEL message (draft-ietf-moq-transport-16 §9.24).
//!
//! Sent by the subscriber to revoke acceptance of a PUBLISH_NAMESPACE, for
//! example when authorization credentials expire.  Carries the Request ID of
//! the corresponding PUBLISH_NAMESPACE rather than the namespace itself.

use crate::coding::{Decode, DecodeError, Encode, EncodeError, ReasonPhrase};

/// Sent by the subscriber to revoke acceptance of a PUBLISH_NAMESPACE.
///
/// The publisher may re-send PUBLISH_NAMESPACE with refreshed credentials or
/// discard the associated state.  After receiving this, the publisher does NOT
/// send PUBLISH_NAMESPACE_DONE for the same request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishNamespaceCancel {
    /// The Request ID of the PUBLISH_NAMESPACE being cancelled.
    pub id: u64,

    /// Error code explaining why the acceptance was revoked.
    pub error_code: u64,

    /// Human-readable reason (max 1024 bytes, internal only — not shown to end users).
    pub reason_phrase: ReasonPhrase,
}

impl Decode for PublishNamespaceCancel {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let error_code = u64::decode(r)?;
        let reason_phrase = ReasonPhrase::decode(r)?;
        Ok(Self {
            id,
            error_code,
            reason_phrase,
        })
    }
}

impl Encode for PublishNamespaceCancel {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.error_code.encode(w)?;
        self.reason_phrase.encode(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();
        let msg = PublishNamespaceCancel {
            id: 42,
            error_code: 0x1,
            reason_phrase: ReasonPhrase("credentials expired".to_string()),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = PublishNamespaceCancel::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_empty_reason() {
        let mut buf = BytesMut::new();
        let msg = PublishNamespaceCancel {
            id: 0,
            error_code: 0,
            reason_phrase: ReasonPhrase(String::new()),
        };
        msg.encode(&mut buf).unwrap();
        assert_eq!(PublishNamespaceCancel::decode(&mut buf).unwrap(), msg);
    }
}
