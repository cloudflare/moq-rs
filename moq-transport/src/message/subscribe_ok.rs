// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};
use crate::message::TrackExtensions;

/// Sent by the publisher to accept a Subscribe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscribeOk {
    /// The request ID of the SUBSCRIBE this message is replying to
    pub id: u64,

    /// The identifier used for this track in Subgroups or Datagrams.
    pub track_alias: u64,

    /// Subscribe Parameters
    pub params: KeyValuePairs,

    /// Track extension headers.
    pub track_extensions: TrackExtensions,
}

impl Decode for SubscribeOk {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let track_alias = u64::decode(r)?;
        let params = KeyValuePairs::decode(r)?;
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

        let msg = SubscribeOk {
            id: 12345,
            track_alias: 100,
            params: kvps.clone(),
            track_extensions: TrackExtensions::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = SubscribeOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn track_alias_independent_of_request_id() {
        // track_alias can differ from the request id — it is chosen by the publisher.
        let mut buf = BytesMut::new();
        let msg = SubscribeOk {
            id: 10,
            track_alias: 42,
            params: KeyValuePairs::default(),
            track_extensions: TrackExtensions::default(),
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
            params: KeyValuePairs::default(),
            track_extensions: TrackExtensions::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = SubscribeOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
