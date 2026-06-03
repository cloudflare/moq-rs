use crate::coding::{Decode, DecodeError, Encode, EncodeError};

/// Sent by the subscriber to terminate a Subscribe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Unsubscribe {
    // The request ID of the subscription being terminated.
    pub id: u64,
}

impl Decode for Unsubscribe {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        Ok(Self { id })
    }
}

impl Encode for Unsubscribe {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
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

        let msg = Unsubscribe { id: 12345 };
        msg.encode(&mut buf).unwrap();
        let decoded = Unsubscribe::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
