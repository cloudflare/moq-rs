use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestUpdate {
    pub id: u64,
    pub existing_request_id: u64,
    pub params: KeyValuePairs,
}

impl Decode for RequestUpdate {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let existing_request_id = u64::decode(r)?;
        let params = KeyValuePairs::decode(r)?;

        Ok(Self {
            id,
            existing_request_id,
            params,
        })
    }
}

impl Encode for RequestUpdate {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.existing_request_id.encode(w)?;
        self.params.encode(w)?;

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

        let mut kvps = KeyValuePairs::new();
        kvps.set_intvalue(124, 456);

        let msg = RequestUpdate {
            id: 1000,
            existing_request_id: 924,
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = RequestUpdate::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
