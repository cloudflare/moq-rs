use crate::coding::{
    Decode, DecodeError, Encode, EncodeError, KeyValuePairs, Location, TrackNamespace,
};
use crate::data::ExtensionHeaders;
use crate::message::GroupOrder;

/// Sent by publisher to initiate a subscription to a track.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Publish {
    /// The publish request ID
    pub id: u64,

    /// Track properties
    pub track_namespace: TrackNamespace,
    pub track_name: String, // TODO SLG - consider making a FullTrackName base struct (total size limit of 4096)
    pub track_alias: u64,

    pub group_order: GroupOrder,
    pub content_exists: bool,
    // The largest object available for this track, if content exists.
    pub largest_location: Option<Location>,
    pub forward: bool,

    /// Optional parameters
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

        let group_order = GroupOrder::decode(r)?;
        // GroupOrder enum has Publisher in it, but it's not allowed to be used in this
        // publish message, so validate it now so we can return a protocol error.
        if group_order == GroupOrder::Publisher {
            return Err(DecodeError::InvalidGroupOrder);
        }
        let content_exists = bool::decode(r)?;
        let largest_location = match content_exists {
            true => Some(Location::decode(r)?),
            false => None,
        };
        let forward = bool::decode(r)?;

        let params = KeyValuePairs::decode(r)?;
        let track_extensions = ExtensionHeaders::decode(r)?;

        Ok(Self {
            id,
            track_namespace,
            track_name,
            track_alias,
            group_order,
            content_exists,
            largest_location,
            forward,
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

        // GroupOrder enum has Publisher in it, but it's not allowed to be used in this
        // publish message.
        if self.group_order == GroupOrder::Publisher {
            return Err(EncodeError::InvalidValue);
        }
        self.group_order.encode(w)?;
        self.content_exists.encode(w)?;
        if self.content_exists {
            if let Some(largest) = &self.largest_location {
                largest.encode(w)?;
            } else {
                return Err(EncodeError::MissingField("LargestLocation".to_string()));
            }
        }
        self.forward.encode(w)?;
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

        // Content exists = true
        let msg = Publish {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            track_alias: 212,
            group_order: GroupOrder::Ascending,
            content_exists: true,
            largest_location: Some(Location::new(2, 3)),
            forward: true,
            params: kvps.clone(),
            track_extensions: Default::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Publish::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // Content exists = false
        let msg = Publish {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            track_alias: 212,
            group_order: GroupOrder::Ascending,
            content_exists: false,
            largest_location: None,
            forward: true,
            params: kvps.clone(),
            track_extensions: Default::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Publish::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_missing_fields() {
        let mut buf = BytesMut::new();

        let msg = Publish {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            track_alias: 212,
            group_order: GroupOrder::Ascending,
            content_exists: true,
            largest_location: None,
            forward: true,
            params: Default::default(),
            track_extensions: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));
    }

    #[test]
    fn encode_bad_group_order() {
        let mut buf = BytesMut::new();

        let msg = Publish {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            track_alias: 212,
            group_order: GroupOrder::Publisher,
            content_exists: false,
            largest_location: None,
            forward: true,
            params: Default::default(),
            track_extensions: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::InvalidValue));
    }
}
