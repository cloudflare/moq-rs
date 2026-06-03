use crate::coding::{
    Decode, DecodeError, Encode, EncodeError, KeyValuePairs, Location, TrackNamespace,
};
use crate::message::{FetchType, GroupOrder};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StandaloneFetch {
    pub track_namespace: TrackNamespace,
    pub track_name: String,
    pub start_location: Location,
    pub end_location: Location,
}

impl Decode for StandaloneFetch {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let track_namespace = TrackNamespace::decode(r)?;
        let track_name = String::decode(r)?;
        let start_location = Location::decode(r)?;
        let end_location = Location::decode(r)?;

        Ok(Self {
            track_namespace,
            track_name,
            start_location,
            end_location,
        })
    }
}

impl Encode for StandaloneFetch {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.track_namespace.encode(w)?;
        self.track_name.encode(w)?;
        self.start_location.encode(w)?;
        self.end_location.encode(w)?;

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoiningFetch {
    /// The request ID of the existing subscription to be joined.
    pub joining_request_id: u64,
    pub joining_start: u64,
}

impl Decode for JoiningFetch {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let joining_request_id = u64::decode(r)?;
        let joining_start = u64::decode(r)?;

        Ok(Self {
            joining_request_id,
            joining_start,
        })
    }
}

impl Encode for JoiningFetch {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.joining_request_id.encode(w)?;
        self.joining_start.encode(w)?;

        Ok(())
    }
}

/// Sent by the subscriber to request to request a range
/// of already published objects within a track.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Fetch {
    /// The fetch request ID
    pub id: u64,

    /// Subscriber Priority
    pub subscriber_priority: u8,

    /// Object delivery order
    pub group_order: GroupOrder,

    /// Standalone fetch vs Relative Joining fetch vs Absolute Joining fetch
    pub fetch_type: FetchType,

    /// Track properties for Standalone fetch
    pub standalone_fetch: Option<StandaloneFetch>,

    /// Joining properties for Relative Joining or Absolute Joining fetches.
    pub joining_fetch: Option<JoiningFetch>,

    /// Optional parameters
    pub params: KeyValuePairs,
}

impl Decode for Fetch {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;

        let subscriber_priority = u8::decode(r)?;
        let group_order = GroupOrder::decode(r)?;

        let fetch_type = FetchType::decode(r)?;

        let standalone_fetch: Option<StandaloneFetch>;
        let joining_fetch: Option<JoiningFetch>;
        match fetch_type {
            FetchType::Standalone => {
                standalone_fetch = Some(StandaloneFetch::decode(r)?);
                joining_fetch = None;
            }
            FetchType::RelativeJoining | FetchType::AbsoluteJoining => {
                standalone_fetch = None;
                joining_fetch = Some(JoiningFetch::decode(r)?);
            }
        };

        let params = KeyValuePairs::decode(r)?;

        Ok(Self {
            id,
            subscriber_priority,
            group_order,
            fetch_type,
            standalone_fetch,
            joining_fetch,
            params,
        })
    }
}

impl Encode for Fetch {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;

        self.subscriber_priority.encode(w)?;
        self.group_order.encode(w)?;

        self.fetch_type.encode(w)?;

        match self.fetch_type {
            FetchType::Standalone => {
                if let Some(standalone_fetch) = &self.standalone_fetch {
                    standalone_fetch.encode(w)?;
                } else {
                    return Err(EncodeError::MissingField(
                        "StandaloneFetch info".to_string(),
                    ));
                }
            }
            FetchType::RelativeJoining | FetchType::AbsoluteJoining => {
                if let Some(joining_fetch) = &self.joining_fetch {
                    joining_fetch.encode(w)?;
                } else {
                    return Err(EncodeError::MissingField("JoiningFetch info".to_string()));
                }
            }
        };

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

        // FetchType = Standlone
        let msg = Fetch {
            id: 12345,
            subscriber_priority: 127,
            group_order: GroupOrder::Publisher,
            fetch_type: FetchType::Standalone,
            standalone_fetch: Some(StandaloneFetch {
                track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
                track_name: "audiotrack".to_string(),
                start_location: Location::new(34, 53),
                end_location: Location::new(34, 53),
            }),
            joining_fetch: None,
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Fetch::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // FetchType = RelativeJoining
        let msg = Fetch {
            id: 12345,
            subscriber_priority: 127,
            group_order: GroupOrder::Publisher,
            fetch_type: FetchType::RelativeJoining,
            standalone_fetch: None,
            joining_fetch: Some(JoiningFetch {
                joining_request_id: 382,
                joining_start: 3463,
            }),
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Fetch::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // FetchType = AbsoluteJoining
        let msg = Fetch {
            id: 12345,
            subscriber_priority: 127,
            group_order: GroupOrder::Publisher,
            fetch_type: FetchType::AbsoluteJoining,
            standalone_fetch: None,
            joining_fetch: Some(JoiningFetch {
                joining_request_id: 382,
                joining_start: 3463,
            }),
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Fetch::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_missing_fields() {
        let mut buf = BytesMut::new();

        // FetchType = Standlone - missing standalone_fetch
        let msg = Fetch {
            id: 12345,
            subscriber_priority: 127,
            group_order: GroupOrder::Publisher,
            fetch_type: FetchType::Standalone,
            standalone_fetch: None,
            joining_fetch: None,
            params: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));

        // FetchType = AbsoluteJoining - missing joining_fetch
        let msg = Fetch {
            id: 12345,
            subscriber_priority: 127,
            group_order: GroupOrder::Publisher,
            fetch_type: FetchType::AbsoluteJoining,
            standalone_fetch: None,
            joining_fetch: None,
            params: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));
    }
}
