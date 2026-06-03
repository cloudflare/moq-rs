use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs, Location};
use crate::data::ExtensionHeaders;
use crate::message::GroupOrder;

/// A publisher sends a FETCH_OK control message in response to successful fetches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchOk {
    /// The Fetch request ID of the Fetch this message is replying to.
    pub id: u64,

    /// Order groups will be delivered in
    pub group_order: GroupOrder,

    /// True if all objects have been published on this track
    pub end_of_track: bool,

    /// The largest object covered by the fetch response
    pub end_location: Location,

    /// Optional parameters
    pub params: KeyValuePairs,

    /// Track extensions
    pub track_extensions: ExtensionHeaders,
}

impl Decode for FetchOk {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;

        let group_order = GroupOrder::decode(r)?;
        // GroupOrder enum has Publisher in it, but it's not allowed to be used in this
        // FetchOk message, so validate it now so we can return a protocol error.
        if group_order == GroupOrder::Publisher {
            return Err(DecodeError::InvalidGroupOrder);
        }
        let end_of_track = bool::decode(r)?;
        let end_location = Location::decode(r)?;
        let params = KeyValuePairs::decode(r)?;
        let track_extensions = ExtensionHeaders::decode(r)?;

        Ok(Self {
            id,
            group_order,
            end_of_track,
            end_location,
            params,
            track_extensions,
        })
    }
}

impl Encode for FetchOk {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;

        // GroupOrder enum has Publisher in it, but it's not allowed to be used in this
        // FetchOk message.
        if self.group_order == GroupOrder::Publisher {
            return Err(EncodeError::InvalidValue);
        }
        self.group_order.encode(w)?;
        self.end_of_track.encode(w)?;
        self.end_location.encode(w)?;
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

        let msg = FetchOk {
            id: 12345,
            group_order: GroupOrder::Descending,
            end_of_track: true,
            end_location: Location::new(2, 3),
            params: kvps.clone(),
            track_extensions: Default::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = FetchOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_bad_group_order() {
        let mut buf = BytesMut::new();

        let msg = FetchOk {
            id: 12345,
            group_order: GroupOrder::Publisher,
            end_of_track: true,
            end_location: Location::new(2, 3),
            params: Default::default(),
            track_extensions: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::InvalidValue));
    }
}
