// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{
    validate_full_track_name, Decode, DecodeError, Encode, EncodeError, KeyValuePairs, TrackName,
    TrackNamespace,
};

/// Sent by the subscriber to request all future objects for the given track.
///
/// Objects will use the provided ID instead of the full track name, to save bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Subscribe {
    /// The subscription request ID
    pub id: u64,

    /// Track properties
    pub track_namespace: TrackNamespace,
    pub track_name: TrackName, // TODO SLG - consider making a FullTrackName base struct (total size limit of 4096)

    /// Optional parameters
    pub params: KeyValuePairs,
}

impl Decode for Subscribe {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;

        let track_namespace = TrackNamespace::decode(r)?;
        let track_name = TrackName::decode(r)?;
        validate_full_track_name(&track_namespace, track_name.as_bytes())?;

        let params = KeyValuePairs::decode(r)?;

        Ok(Self {
            id,
            track_namespace,
            track_name,
            params,
        })
    }
}

impl Encode for Subscribe {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;

        self.track_namespace.encode(w)?;
        self.track_name.encode(w)?;

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

        let mut kvps = KeyValuePairs::new();
        kvps.set_bytesvalue(123, vec![0x00, 0x01, 0x02, 0x03]);

        let msg = Subscribe {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".into(),
            params: kvps.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Subscribe::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn default_params_roundtrip() {
        // Verify a minimal SUBSCRIBE with no params still round-trips cleanly.
        let mut buf = BytesMut::new();
        let msg = Subscribe {
            id: 0,
            track_namespace: TrackNamespace::from_utf8_path("a/b"),
            track_name: "t".into(),
            params: KeyValuePairs::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Subscribe::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn decode_rejects_full_track_name_over_limit() {
        let mut buf = BytesMut::new();
        let msg = Subscribe {
            id: 0,
            track_namespace: TrackNamespace {
                fields: vec![crate::coding::TupleField {
                    value: vec![b'a'; crate::coding::MAX_FULL_TRACK_NAME_LEN],
                }],
            },
            track_name: "x".into(),
            params: KeyValuePairs::default(),
        };

        msg.encode(&mut buf).unwrap();
        let err = Subscribe::decode(&mut buf).unwrap_err();
        assert!(matches!(err, DecodeError::TrackNameTooLong));
    }

    #[test]
    fn minimal_wire_format_has_no_fixed_subscription_fields() {
        let mut buf = BytesMut::new();
        let msg = Subscribe {
            id: 2,
            track_namespace: TrackNamespace::from_utf8_path("ns/v"),
            track_name: "track".into(),
            params: KeyValuePairs::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Subscribe::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
