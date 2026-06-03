use crate::coding::{Decode, DecodeError, Encode, EncodeError};

/// A subscriber issues a FETCH_CANCEL message to a publisher indicating it is
/// no longer interested in receiving Objects for the fetch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchCancel {
    /// The request ID of the Fetch this message is cancelling.
    pub id: u64,
}

impl Decode for FetchCancel {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        Ok(Self { id })
    }
}

impl Encode for FetchCancel {
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

        let msg = FetchCancel { id: 12345 };
        msg.encode(&mut buf).unwrap();
        let decoded = FetchCancel::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
