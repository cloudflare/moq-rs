// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{
    validate_full_track_name, Decode, DecodeError, Encode, EncodeError, KeyValuePairs, TrackName,
    TrackNamespace,
};
use crate::message::TrackExtensions;

/// Sent by publisher to initiate a subscription to a track.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Publish {
    /// The publish request ID
    pub id: u64,

    /// Track properties
    pub track_namespace: TrackNamespace,
    pub track_name: TrackName, // TODO SLG - consider making a FullTrackName base struct (total size limit of 4096)
    pub track_alias: u64,

    /// Optional parameters
    pub params: KeyValuePairs,

    /// Track extension headers.
    pub track_extensions: TrackExtensions,
}

impl Decode for Publish {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;

        let track_namespace = TrackNamespace::decode(r)?;
        let track_name = TrackName::decode(r)?;
        validate_full_track_name(&track_namespace, track_name.as_bytes())?;
        let track_alias = u64::decode(r)?;
        let params = KeyValuePairs::decode(r)?;
        let track_extensions = TrackExtensions::decode(r)?;

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

        let msg = Publish {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".into(),
            track_alias: 212,
            params: kvps.clone(),
            track_extensions: TrackExtensions::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Publish::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
