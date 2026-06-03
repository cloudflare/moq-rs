use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs, Location};
use crate::message::GroupOrder;

/// Sent by the publisher to accept a Subscribe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrackStatusOk {
    /// The request ID of the TRACK_STATUS this message is replying to
    pub id: u64,

    /// The identifier used for this track in Subgroups or Datagrams.
    pub track_alias: u64,

    /// The time in milliseconds after which the subscription is not longer valid.
    pub expires: u64,

    /// Order groups will be delivered in
    pub group_order: GroupOrder,

    /// If content_exists, then largest_location is the location of the largest
    /// object available for this track
    pub content_exists: bool,
    pub largest_location: Option<Location>, // Only provided if content_exists is 1/true

    /// Subscribe Parameters
    pub params: KeyValuePairs,
}

impl Decode for TrackStatusOk {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let track_alias = u64::decode(r)?;
        let expires = u64::decode(r)?;
        let group_order = GroupOrder::decode(r)?;
        let content_exists = bool::decode(r)?;
        let largest_location = match content_exists {
            true => Some(Location::decode(r)?),
            false => None,
        };
        let params = KeyValuePairs::decode(r)?;

        Ok(Self {
            id,
            track_alias,
            expires,
            group_order,
            content_exists,
            largest_location,
            params,
        })
    }
}

impl Encode for TrackStatusOk {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.track_alias.encode(w)?;
        self.expires.encode(w)?;
        self.group_order.encode(w)?;
        self.content_exists.encode(w)?;
        if self.content_exists {
            if let Some(largest) = &self.largest_location {
                largest.encode(w)?;
            } else {
                return Err(EncodeError::MissingField("LargestLocation".to_string()));
            }
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

        let msg = TrackStatusOk {
            id: 12345,
            track_alias: 100,
            expires: 3600,
            group_order: GroupOrder::Publisher,
            content_exists: true,
            largest_location: Some(Location::new(2, 3)),
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = TrackStatusOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_missing_fields() {
        let mut buf = BytesMut::new();

        let msg = TrackStatusOk {
            id: 12345,
            track_alias: 100,
            expires: 3600,
            group_order: GroupOrder::Publisher,
            content_exists: true,
            largest_location: None,
            params: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));
    }
}
