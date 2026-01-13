use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};

/// Sent by the publisher to accept a Subscribe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscribeOk {
    /// The request ID of the SUBSCRIBE this message is replying to
    pub id: u64,

    /// The identifier used for this track in Subgroups or Datagrams.
    pub track_alias: u64,

    /// Subscribe Parameters
    pub params: KeyValuePairs,

    /// Track extensions
    pub track_extensions: KeyValuePairs,
}

impl Decode for SubscribeOk {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let track_alias = u64::decode(r)?;
        let params = KeyValuePairs::decode(r)?;
        let track_extensions = KeyValuePairs::decode(r)?;

        Ok(Self {
            id,
            track_alias,
            params,
            track_extensions,
        })
    }
}

impl Encode for SubscribeOk {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.track_alias.encode(w)?;
        self.params.encode(w)?;
        self.track_extensions.encode(w)?;

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

        // One parameter for testing
        let mut kvps = KeyValuePairs::new();
        kvps.set_bytesvalue(123, vec![0x00, 0x01, 0x02, 0x03]);

        let msg = SubscribeOk {
            id: 12345,
            track_alias: 100,
            params: kvps.clone(),
            track_extensions: kvps,
        };
        msg.encode(&mut buf).unwrap();
        let decoded = SubscribeOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_missing_fields() {
        let mut buf = BytesMut::new();

        let msg = SubscribeOk {
            id: 12345,
            track_alias: 100,
            params: Default::default(),
            track_extensions: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));
    }
}
