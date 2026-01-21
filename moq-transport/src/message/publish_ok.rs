use crate::coding::{Decode, DecodeError, Encode, EncodeError};
use crate::data::Parameters;

/// Sent by the subscriber to request all future objects for the given track.
///
/// Objects will use the provided ID instead of the full track name, to save bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishOk {
    /// The request ID of the Publish this message is replying to.
    pub id: u64,

    /// Optional parameters
    pub params: Parameters,
}

impl Decode for PublishOk {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;

        let params = Parameters::decode(r)?;

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

        // One parameter for testing
        let mut kvps = Parameters::new();
        kvps.set_bytesvalue(123, vec![0x00, 0x01, 0x02, 0x03]);

        // FilterType = NextGroupStart
        let msg = PublishOk {
            id: 12345,
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = PublishOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // FilterType = AbsoluteStart
        let msg = PublishOk {
            id: 12345,
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = PublishOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // FilterType = AbsoluteRange
        let msg = PublishOk {
            id: 12345,
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = PublishOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_missing_fields() {
        let mut buf = BytesMut::new();

        // FilterType = AbsoluteStart - missing start_location
        let msg = PublishOk {
            id: 12345,
            params: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));

        // FilterType = AbsoluteRange - missing start_location
        let msg = PublishOk {
            id: 12345,
            params: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));

        // FilterType = AbsoluteRange - missing end_group_id
        let msg = PublishOk {
            id: 12345,
            params: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));
    }
}
