// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs, Location};
use crate::message::GroupOrder;

/// Sent by the publisher to accept a Subscribe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscribeOk {
    /// The request ID of the SUBSCRIBE this message is replying to
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

impl Decode for SubscribeOk {
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

impl Encode for SubscribeOk {
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

        let msg = SubscribeOk {
            id: 12345,
            track_alias: 100,
            expires: 3600,
            group_order: GroupOrder::Publisher,
            content_exists: true,
            largest_location: Some(Location::new(2, 3)),
            params: kvps.clone(),
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
            expires: 3600,
            group_order: GroupOrder::Publisher,
            content_exists: true,
            largest_location: None,
            params: Default::default(),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));
    }

    #[test]
    fn track_alias_independent_of_request_id() {
        // track_alias can differ from the request id — it is chosen by the publisher.
        let mut buf = BytesMut::new();
        let msg = SubscribeOk {
            id: 10,
            track_alias: 42,
            expires: 0,
            group_order: GroupOrder::Ascending,
            content_exists: false,
            largest_location: None,
            params: KeyValuePairs::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = SubscribeOk::decode(&mut buf).unwrap();
        assert_eq!(decoded.id, 10);
        assert_eq!(decoded.track_alias, 42);
    }

    #[test]
    fn encode_decode_no_content() {
        let mut buf = BytesMut::new();
        let msg = SubscribeOk {
            id: 0,
            track_alias: 0,
            expires: 0,
            group_order: GroupOrder::Descending,
            content_exists: false,
            largest_location: None,
            params: KeyValuePairs::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = SubscribeOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
        assert!(!decoded.content_exists);
        assert!(decoded.largest_location.is_none());
    }
}
