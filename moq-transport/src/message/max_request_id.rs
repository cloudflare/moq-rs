use crate::coding::{Decode, DecodeError, Encode, EncodeError};

/// Sent by the publisher to update the max allowed subscription ID for the session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaxRequestId {
    /// The max allowed request ID
    pub request_id: u64,
}

impl Decode for MaxRequestId {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let request_id = u64::decode(r)?;

        Ok(Self { request_id })
    }
}

impl Encode for MaxRequestId {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.request_id.encode(w)?;

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

        let msg = MaxRequestId { request_id: 12345 };
        msg.encode(&mut buf).unwrap();
        let decoded = MaxRequestId::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
