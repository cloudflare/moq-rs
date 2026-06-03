use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};

/// Sent by the subscriber to acknowledge a PUBLISH message and establish a subscription.
///
/// Draft-16: All subscription properties (forward, subscriber_priority, group_order,
/// filter_type, etc.) are now in Parameters (Section 9.2.2).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishOk {
    /// The request ID of the Publish this message is replying to.
    pub id: u64,

    /// Parameters (may contain Forward, SubscriberPriority, GroupOrder, SubscriptionFilter, etc.)
    pub params: KeyValuePairs,
}

impl Decode for PublishOk {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let params = KeyValuePairs::decode(r)?;

        Ok(Self { id, params })
    }
}

impl Encode for PublishOk {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
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
        kvps.set_bytesvalue(123, vec![0x00, 0x01, 0x02, 0x03]);

        let msg = PublishOk {
            id: 12345,
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = PublishOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_no_params() {
        let mut buf = BytesMut::new();

        let msg = PublishOk {
            id: 12345,
            params: Default::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = PublishOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
