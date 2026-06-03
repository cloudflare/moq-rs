use crate::coding::{Decode, DecodeError, Encode, EncodeError, ReasonPhrase};

// TODO SLG - add an enum for status_codes

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
