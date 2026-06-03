use crate::coding::{Decode, DecodeError, Encode, EncodeError};

/// Sent by the publisher to update the max allowed subscription ID for the session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestsBlocked {
    /// The max allowed request ID
    pub max_request_id: u64,
}

impl Decode for RequestsBlocked {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let max_request_id = u64::decode(r)?;

        Ok(Self { max_request_id })
    }
}

impl Encode for RequestsBlocked {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.max_request_id.encode(w)?;

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

        let msg = RequestsBlocked {
            max_request_id: 12345,
        };
        msg.encode(&mut buf).unwrap();
        let decoded = RequestsBlocked::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
