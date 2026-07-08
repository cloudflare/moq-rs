// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{
    Decode, DecodeError, Encode, EncodeError, KeyValuePairs, TrackNamespacePrefix,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u64)]
pub enum SubscribeOptions {
    Publish = 0x00,
    Namespace = 0x01,
    Both = 0x02,
}

impl Decode for SubscribeOptions {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let options = u64::decode(r)?;
        match options {
            0x00 => Ok(SubscribeOptions::Publish),
            0x01 => Ok(SubscribeOptions::Namespace),
            0x02 => Ok(SubscribeOptions::Both),
            _ => Err(DecodeError::InvalidSubscribeOptions(options)),
        }
    }
}

impl Encode for SubscribeOptions {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        match self {
            SubscribeOptions::Publish => 0x00u64.encode(w),
            SubscribeOptions::Namespace => 0x01u64.encode(w),
            SubscribeOptions::Both => 0x02u64.encode(w),
        }
    }
}

/// Subscribe Namespace
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscribeNamespace {
    /// The subscription request ID
    pub id: u64,

    /// The track namespace prefix
    pub track_namespace_prefix: TrackNamespacePrefix,

    pub subscribe_options: SubscribeOptions,

    /// Optional parameters
    pub params: KeyValuePairs,
}

impl Decode for SubscribeNamespace {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let track_namespace_prefix = TrackNamespacePrefix::decode(r)?;
        let subscribe_options = SubscribeOptions::decode(r)?;
        let params = KeyValuePairs::decode(r)?;

        Ok(Self {
            id,
            track_namespace_prefix,
            subscribe_options,
            params,
        })
    }
}

impl Encode for SubscribeNamespace {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.track_namespace_prefix.encode(w)?;
        self.subscribe_options.encode(w)?;
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

        let msg = SubscribeNamespace {
            id: 12345,
            track_namespace_prefix: TrackNamespacePrefix::from_utf8_path("path/prefix"),
            subscribe_options: SubscribeOptions::Publish,
            params: kvps,
        };
        msg.encode(&mut buf).unwrap();
        let decoded = SubscribeNamespace::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
