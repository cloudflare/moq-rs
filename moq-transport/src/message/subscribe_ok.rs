use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs, TrackExtensions};

/// Sent by the publisher to accept a Subscribe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscribeOk {
    /// The request ID of the SUBSCRIBE this message is replying to
    pub id: u64,

    /// The identifier used for this track in Subgroups or Datagrams.
    pub track_alias: u64,

    /// Subscribe Parameters (has count prefix per spec)
    pub params: KeyValuePairs,

    /// Track extensions (NO prefix per draft-16 Section 9.10 - reads until end of message)
    pub track_extensions: TrackExtensions,
}

impl Decode for SubscribeOk {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let track_alias = u64::decode(r)?;
        let params = KeyValuePairs::decode(r)?;
        // Track extensions have NO prefix - read until end of message
        let track_extensions = TrackExtensions::decode(r)?;

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

        // Track extensions (no prefix)
        let mut ext = TrackExtensions::new();
        ext.set_intvalue(2, 42);

        let msg = SubscribeOk {
            id: 12345,
            track_alias: 100,
            params: kvps,
            track_extensions: ext,
        };
        msg.encode(&mut buf).unwrap();
        let decoded = SubscribeOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_empty_extensions() {
        let mut buf = BytesMut::new();

        let msg = SubscribeOk {
            id: 0,
            track_alias: 0,
            params: KeyValuePairs::new(),
            track_extensions: TrackExtensions::new(),
        };
        msg.encode(&mut buf).unwrap();
        // Expected: id=0 (1 byte), track_alias=0 (1 byte), params_count=0 (1 byte), NO track_extensions bytes
        assert_eq!(buf.to_vec(), vec![0x00, 0x00, 0x00]);
        let decoded = SubscribeOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    // Note: encode_missing_fields test removed — content_exists was removed
    // from the struct in draft-16; no fields to validate at encode time.
}
