use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs, TrackNamespace};

/// Sent by the subscriber to request all future objects for the given track.
///
/// Objects will use the provided ID instead of the full track name, to save bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Subscribe {
    /// The subscription request ID
    pub id: u64,

    /// Track properties
    pub track_namespace: TrackNamespace,
    pub track_name: String, // TODO SLG - consider making a FullTrackName base struct (total size limit of 4096)

    /// Optional parameters
    /// NOTE(itzmanish): since the forward and other fields are moved to parameters
    /// we need to validate them on publisher logic
    pub params: KeyValuePairs,
}

impl Decode for Subscribe {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;

        let track_namespace = TrackNamespace::decode(r)?;
        let track_name = String::decode(r)?;

        let params = KeyValuePairs::decode(r)?;

        Ok(Self {
            id,
            track_namespace,
            track_name,
            params,
        })
    }
}

impl Encode for Subscribe {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;

        self.track_namespace.encode(w)?;
        self.track_name.encode(w)?;
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
        let mut kvps = KeyValuePairs::new();
        kvps.set_bytesvalue(123, vec![0x00, 0x01, 0x02, 0x03]);

        // FilterType = NextGroupStart
        let msg = Subscribe {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Subscribe::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // FilterType = AbsoluteStart
        let msg = Subscribe {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Subscribe::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // FilterType = AbsoluteRange
        let msg = Subscribe {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Subscribe::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_missing_fields() {
        let mut buf = BytesMut::new();

        // FilterType = AbsoluteStart - missing start_location
        let msg = Subscribe {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            params: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));

        // FilterType = AbsoluteRange - missing start_location
        let msg = Subscribe {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            params: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));

        // FilterType = AbsoluteRange - missing end_group_id
        let msg = Subscribe {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            params: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));
    }
}
