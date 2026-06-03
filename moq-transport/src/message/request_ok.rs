use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};

/// Reqeust Ok
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestOk {
    /// The SubscribeNamespace/PublishNamespace request ID this message is replying to.
    pub id: u64,

    /// Optional parameters
    pub params: KeyValuePairs,
}

impl Decode for RequestOk {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let params = KeyValuePairs::decode(r)?;
        Ok(Self { id, params })
    }
}

impl Encode for RequestOk {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.params.encode(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();

        let msg = RequestOk {
            id: 12345,
            params: KeyValuePairs::new(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = RequestOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
