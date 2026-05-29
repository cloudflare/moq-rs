// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError, TrackNamespacePrefix};

/// NAMESPACE message sent on a SUBSCRIBE_NAMESPACE response stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Namespace {
    pub track_namespace_suffix: TrackNamespacePrefix,
}

impl Decode for Namespace {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let track_namespace_suffix = TrackNamespacePrefix::decode(r)?;
        Ok(Self {
            track_namespace_suffix,
        })
    }
}

impl Encode for Namespace {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.track_namespace_suffix.encode(w)
    }
}

/// NAMESPACE_DONE message sent on a SUBSCRIBE_NAMESPACE response stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceDone {
    pub track_namespace_suffix: TrackNamespacePrefix,
}

impl Decode for NamespaceDone {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let track_namespace_suffix = TrackNamespacePrefix::decode(r)?;
        Ok(Self {
            track_namespace_suffix,
        })
    }
}

impl Encode for NamespaceDone {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.track_namespace_suffix.encode(w)
    }
}
