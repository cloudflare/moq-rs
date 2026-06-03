use crate::coding::{
    Decode, DecodeError, Encode, EncodeError, KeyValuePairs, Location, TrackNamespace,
};
use crate::message::FilterType;
use crate::message::GroupOrder;

/// A potential subscriber sends a TrackStatus message to obtain information about
/// the current status of a given track.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrackStatus {
    /// The subscription request ID
    pub id: u64,

    /// Track properties
    pub track_namespace: TrackNamespace,
    pub track_name: String, // TODO SLG - consider making a FullTrackName base struct (total size limit of 4096)

    /// Subscriber Priority
    pub subscriber_priority: u8,
    pub group_order: GroupOrder,

    /// Forward Flag
    pub forward: bool,

    /// Filter type
    pub filter_type: FilterType,

    /// The starting location for this subscription. Only present for "AbsoluteStart" and "AbsoluteRange" filter types.
    pub start_location: Option<Location>,
    /// End group id, inclusive, for the subscription, if applicable. Only present for "AbsoluteRange" filter type.
    pub end_group_id: Option<u64>,

    /// Optional parameters
    pub params: KeyValuePairs,
}

impl Decode for TrackStatus {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;

        let track_namespace = TrackNamespace::decode(r)?;
        let track_name = String::decode(r)?;

        let subscriber_priority = u8::decode(r)?;
        let group_order = GroupOrder::decode(r)?;

        let forward = bool::decode(r)?;

        let filter_type = FilterType::decode(r)?;
        let start_location: Option<Location>;
        let end_group_id: Option<u64>;
        match filter_type {
            FilterType::AbsoluteStart => {
                start_location = Some(Location::decode(r)?);
                end_group_id = None;
            }
            FilterType::AbsoluteRange => {
                start_location = Some(Location::decode(r)?);
                end_group_id = Some(u64::decode(r)?);
            }
            _ => {
                start_location = None;
                end_group_id = None;
            }
        }

        let params = KeyValuePairs::decode(r)?;

        Ok(Self {
            id,
            track_namespace,
            track_name,
            subscriber_priority,
            group_order,
            forward,
            filter_type,
            start_location,
            end_group_id,
            params,
        })
    }
}

impl Encode for TrackStatus {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;

        self.track_namespace.encode(w)?;
        self.track_name.encode(w)?;

        self.subscriber_priority.encode(w)?;
        self.group_order.encode(w)?;

        self.forward.encode(w)?;

        self.filter_type.encode(w)?;
        match self.filter_type {
            FilterType::AbsoluteStart => {
                if let Some(start) = &self.start_location {
                    start.encode(w)?;
                } else {
                    return Err(EncodeError::MissingField("LargestLocation".to_string()));
                }
                // Just ignore end_group_id if it happens to be set
            }
            FilterType::AbsoluteRange => {
                if let Some(start) = &self.start_location {
                    start.encode(w)?;
                } else {
                    return Err(EncodeError::MissingField("LargestLocation".to_string()));
                }
                if let Some(end) = self.end_group_id {
                    end.encode(w)?;
                } else {
                    return Err(EncodeError::MissingField("EndGroupId".to_string()));
                }
            }
            _ => {}
        }

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
        let msg = TrackStatus {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            subscriber_priority: 127,
            group_order: GroupOrder::Publisher,
            forward: true,
            filter_type: FilterType::NextGroupStart,
            start_location: None,
            end_group_id: None,
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = TrackStatus::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // FilterType = AbsoluteStart
        let msg = TrackStatus {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            subscriber_priority: 127,
            group_order: GroupOrder::Publisher,
            forward: true,
            filter_type: FilterType::AbsoluteStart,
            start_location: Some(Location::new(12345, 67890)),
            end_group_id: None,
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = TrackStatus::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // FilterType = AbsoluteRange
        let msg = TrackStatus {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            subscriber_priority: 127,
            group_order: GroupOrder::Publisher,
            forward: true,
            filter_type: FilterType::AbsoluteRange,
            start_location: Some(Location::new(12345, 67890)),
            end_group_id: Some(23456),
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = TrackStatus::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_missing_fields() {
        let mut buf = BytesMut::new();

        // FilterType = AbsoluteStart - missing start_location
        let msg = TrackStatus {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            subscriber_priority: 127,
            group_order: GroupOrder::Publisher,
            forward: true,
            filter_type: FilterType::AbsoluteStart,
            start_location: None,
            end_group_id: None,
            params: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));

        // FilterType = AbsoluteRange - missing start_location
        let msg = TrackStatus {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            subscriber_priority: 127,
            group_order: GroupOrder::Publisher,
            forward: true,
            filter_type: FilterType::AbsoluteRange,
            start_location: None,
            end_group_id: None,
            params: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));

        // FilterType = AbsoluteRange - missing end_group_id
        let msg = TrackStatus {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            subscriber_priority: 127,
            group_order: GroupOrder::Publisher,
            forward: true,
            filter_type: FilterType::AbsoluteRange,
            start_location: Some(Location::new(12345, 67890)),
            end_group_id: None,
            params: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));
    }
}
