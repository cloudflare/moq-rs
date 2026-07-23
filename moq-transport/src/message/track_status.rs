// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{
    validate_full_track_name, Decode, DecodeError, Encode, EncodeError, KeyValuePairs, TrackName,
    TrackNamespace,
};

/// A potential subscriber sends a TrackStatus message to obtain information about
/// the current status of a given track.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrackStatus {
    /// The subscription request ID
    pub id: u64,

    /// Track properties
    pub track_namespace: TrackNamespace,
    pub track_name: TrackName, // TODO SLG - consider making a FullTrackName base struct (total size limit of 4096)

    /// Optional parameters
    pub params: KeyValuePairs,
}

impl Decode for TrackStatus {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;

        let track_namespace = TrackNamespace::decode(r)?;
        let track_name = TrackName::decode(r)?;
        validate_full_track_name(&track_namespace, track_name.as_bytes())?;

        let params = KeyValuePairs::decode_message_params(r)?;

        Ok(Self {
            id,
            track_namespace,
            track_name,
            params,
        })
    }
}

impl Encode for TrackStatus {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;

        self.track_namespace.encode(w)?;
        self.track_name.encode(w)?;

        self.params.encode_message_params(w)?;

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

        let msg = TrackStatus {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".into(),
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = TrackStatus::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
