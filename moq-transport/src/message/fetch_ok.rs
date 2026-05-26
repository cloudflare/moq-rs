// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs, Location};
use crate::message::TrackExtensions;

/// A publisher sends a FETCH_OK control message in response to successful fetches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchOk {
    /// The Fetch request ID of the Fetch this message is replying to.
    pub id: u64,

    /// True if all objects have been published on this track
    pub end_of_track: bool,

    /// The largest object covered by the fetch response
    pub end_location: Location,

    /// Optional parameters
    pub params: KeyValuePairs,

    /// Track extension headers.
    pub track_extensions: TrackExtensions,
}

impl Decode for FetchOk {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let end_of_track = bool::decode(r)?;
        let end_location = Location::decode(r)?;
        let params = KeyValuePairs::decode(r)?;
        let track_extensions = TrackExtensions::decode(r)?;

        Ok(Self {
            id,
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
            end_of_track: true,
            end_location: Location::new(2, 3),
            params: kvps.clone(),
            track_extensions: TrackExtensions::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = FetchOk::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
