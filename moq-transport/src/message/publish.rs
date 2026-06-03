use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs, TrackNamespace};
use crate::data::ExtensionHeaders;

/// Sent by publisher to initiate a subscription to a track.
///
/// Draft-16: Fields like group_order, content_exists, largest_location, forward
/// have been moved to Parameters (Section 9.2.2).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Publish {
    /// The publish request ID
    pub id: u64,

    /// Track properties
    pub track_namespace: TrackNamespace,
    pub track_name: String, // TODO SLG - consider making a FullTrackName base struct (total size limit of 4096)
    pub track_alias: u64,

    /// Optional parameters (may contain Forward, GroupOrder, LargestObject, PublisherPriority, etc.)
    pub params: KeyValuePairs,

    /// Track extensions
    pub track_extensions: ExtensionHeaders,
}

impl Decode for Publish {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;

        let track_namespace = TrackNamespace::decode(r)?;
        let track_name = String::decode(r)?;
        let track_alias = u64::decode(r)?;

        let params = KeyValuePairs::decode(r)?;

        // Track Extensions use remaining bytes (no length prefix per draft-16)
        let track_extensions = ExtensionHeaders::decode_remaining_bytes(r)?;

        Ok(Self {
            id,
            track_namespace,
            track_name,
            track_alias,
            params,
            track_extensions,
        })
    }
}

impl Encode for Publish {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;

        self.track_namespace.encode(w)?;
        self.track_name.encode(w)?;
        self.track_alias.encode(w)?;

        self.params.encode(w)?;
        // Track Extensions use remaining bytes (no length prefix per draft-16)
        self.track_extensions.encode_without_length(w)?;

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

        let msg = Publish {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            track_alias: 212,
            params: kvps.clone(),
            track_extensions: Default::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Publish::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_no_params() {
        let mut buf = BytesMut::new();

        let msg = Publish {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            track_alias: 212,
            params: Default::default(),
            track_extensions: Default::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Publish::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_with_track_extensions() {
        let mut buf = BytesMut::new();

        let mut track_ext = ExtensionHeaders::new();
        track_ext.set_intvalue(0x12, 50); // AUDIO_LEVEL_EXT = 0x12 (18), value = 50

        let msg = Publish {
            id: 1,
            track_namespace: TrackNamespace::from_utf8_path("topn-test/speaker-0"),
            track_name: "audio".to_string(),
            track_alias: 0,
            params: Default::default(),
            track_extensions: track_ext,
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Publish::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
        // Verify the track extension was decoded correctly
        let kvp = decoded.track_extensions.get(0x12).unwrap();
        assert_eq!(kvp.key, 0x12);
        match &kvp.value {
            crate::coding::Value::IntValue(v) => assert_eq!(*v, 50),
            _ => panic!("Expected int value"),
        }
    }
}
