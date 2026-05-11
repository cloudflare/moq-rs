// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError, ReasonPhrase};

/// Draft-16 §13.4.3 PUBLISH_DONE codes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u64)]
pub enum PublishDoneCode {
    InternalError = 0x0,
    Unauthorized = 0x1,
    TrackEnded = 0x2,
    SubscriptionEnded = 0x3,
    GoingAway = 0x4,
    Expired = 0x5,
    TooFarBehind = 0x6,
    UpdateFailed = 0x8,
    MalformedTrack = 0x12,
}

impl From<PublishDoneCode> for u64 {
    fn from(value: PublishDoneCode) -> Self {
        value as u64
    }
}

/// Sent by the publisher to cleanly terminate a Subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishDone {
    // The request ID of the subscription being terminated.
    pub id: u64,

    /// The status code indicating why the subscription ended.
    pub status_code: u64,

    /// The number of data streams the publisher opened for this subscription.
    pub stream_count: u64,

    /// Provides the reason for the subscription error.
    pub reason: ReasonPhrase,
}

impl Decode for PublishDone {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let status_code = u64::decode(r)?;
        let stream_count = u64::decode(r)?;
        let reason = ReasonPhrase::decode(r)?;

        Ok(Self {
            id,
            status_code,
            stream_count,
            reason,
        })
    }
}

impl Encode for PublishDone {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.status_code.encode(w)?;
        self.stream_count.encode(w)?;
        self.reason.encode(w)?;

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

        let msg = PublishDone {
            id: 12345,
            status_code: 0x02,
            stream_count: 2,
            reason: ReasonPhrase("Track Ended".to_string()),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = PublishDone::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

}
