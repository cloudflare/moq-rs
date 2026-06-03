use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};

/// REQUEST_UPDATE message (draft-16 Section 9.11).
///
/// Sent to modify an existing request (SUBSCRIBE, PUBLISH, FETCH, etc.).
/// Parameters previously set that are not present in the update remain unchanged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscribeUpdate {
    /// The request ID of this REQUEST_UPDATE
    pub id: u64,

    /// The request ID of the existing request this message is updating.
    pub existing_request_id: u64,

    /// Parameters to update (draft-16 Section 9.2.2).
    /// Parameters not present remain unchanged from the original request.
    pub params: KeyValuePairs,
}

impl Decode for SubscribeUpdate {
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

impl Encode for SubscribeUpdate {
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

        let msg = SubscribeUpdate {
            id: 1000,
            existing_request_id: 924,
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = SubscribeUpdate::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_empty_params() {
        let mut buf = BytesMut::new();

        let msg = SubscribeUpdate {
            id: 5,
            existing_request_id: 3,
            params: KeyValuePairs::new(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = SubscribeUpdate::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
