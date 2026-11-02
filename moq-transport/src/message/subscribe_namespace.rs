// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{
    Decode, DecodeError, Encode, EncodeError, KeyValuePairs, TrackNamespacePrefix,
};

/// Subscribe Namespace (draft-18 §10.18)
///
/// Requests the current set of published namespaces matching the given prefix,
/// plus future updates (NAMESPACE / NAMESPACE_DONE). This message covers
/// namespace discovery only; requesting PUBLISH messages for tracks in those
/// namespaces is the separate SUBSCRIBE_TRACKS message (§10.19).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscribeNamespace {
    /// The subscription request ID
    pub id: u64,

    /// The track namespace prefix
    pub track_namespace_prefix: TrackNamespacePrefix,

    /// Optional parameters
    pub params: KeyValuePairs,
}

impl Decode for SubscribeNamespace {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let track_namespace_prefix = TrackNamespacePrefix::decode(r)?;
        let params = KeyValuePairs::decode_message_params(r)?;

        Ok(Self {
            id,
            track_namespace_prefix,
            params,
        })
    }
}

impl Encode for SubscribeNamespace {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.track_namespace_prefix.encode(w)?;
        self.params.encode_message_params(w)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Bytes, BytesMut};

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();

        let mut kvps = KeyValuePairs::new();
        kvps.set_bytesvalue(123, vec![0x00, 0x01, 0x02, 0x03]);

        let msg = SubscribeNamespace {
            id: 12345,
            track_namespace_prefix: TrackNamespacePrefix::from_utf8_path("path/prefix"),
            params: kvps,
        };
        msg.encode(&mut buf).unwrap();
        let decoded = SubscribeNamespace::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    /// A conformant draft-18 peer sends SUBSCRIBE_NAMESPACE as:
    /// Request ID, Track Namespace Prefix, Parameters — no Subscribe Options.
    /// This payload must decode without error and consume all bytes exactly.
    #[test]
    fn decodes_draft18_wire_without_subscribe_options() {
        // Payload (no framing): id=1 (0x01), prefix="ns" (pfc=0x01, len=0x02, 'n','s'),
        // params_count=0 (0x00). Six bytes total.
        let payload: &[u8] = &[0x01, 0x01, 0x02, b'n', b's', 0x00];
        let mut buf = Bytes::copy_from_slice(payload);
        let decoded = SubscribeNamespace::decode(&mut buf).unwrap();
        assert_eq!(decoded.id, 1);
        assert_eq!(decoded.track_namespace_prefix.fields.len(), 1);
        assert_eq!(&decoded.track_namespace_prefix.fields[0].value[..], b"ns");
        assert!(decoded.params.0.is_empty());
        assert!(buf.is_empty(), "decoder must consume all 6 payload bytes");
    }
}
