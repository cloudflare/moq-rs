// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::BoundsExceeded;
use std::{io, string::FromUtf8Error, sync};
use thiserror::Error;

pub trait Decode: Sized {
    fn decode<B: bytes::Buf>(buf: &mut B) -> Result<Self, DecodeError>;

    // Helper function to make sure we have enough bytes to decode
    fn decode_remaining<B: bytes::Buf>(buf: &mut B, required: usize) -> Result<(), DecodeError> {
        let needed = required.saturating_sub(buf.remaining());
        if needed > 0 {
            Err(DecodeError::More(needed))
        } else {
            Ok(())
        }
    }
}

/// A decode error.
#[derive(Error, Debug, Clone)]
pub enum DecodeError {
    #[error("fill buffer")]
    More(usize),

    #[error("invalid payload length {0} got {1}")]
    InvalidLength(usize, usize),

    #[error("invalid string")]
    InvalidString(#[from] FromUtf8Error),

    #[error("invalid message: {0:?}")]
    InvalidMessage(u64),

    #[error("invalid subscribe location")]
    InvalidSubscribeLocation,

    #[error("invalid filter type")]
    InvalidFilterType,

    #[error("invalid fetch type")]
    InvalidFetchType,

    #[error("invalid group order")]
    InvalidGroupOrder,

    #[error("invalid object status")]
    InvalidObjectStatus,

    #[error("invalid header type")]
    InvalidHeaderType,

    #[error("invalid value")]
    InvalidValue,

    #[error("varint bounds exceeded")]
    BoundsExceeded(#[from] BoundsExceeded),

    // TODO move these to ParamError
    #[error("duplicate parameter: {0:?}")]
    DuplicateParameter(u64),

    #[error("missing parameter")]
    MissingParameter,

    #[error("invalid parameter")]
    InvalidParameter,

    #[error("io error: {0}")]
    Io(sync::Arc<io::Error>),

    #[error("key-value-pair length exceeded")]
    KeyValuePairLengthExceeded(),

    /// Delta-encoded KVP type would overflow u64 (draft-16 §1.4.2 PROTOCOL_VIOLATION).
    #[error("key-value-pair type delta overflow")]
    KvpTypeOverflow,

    #[error("field '{0}' too large")]
    FieldBoundsExceeded(String),

    /// A namespace field had zero length (draft-16 §2.4.1 PROTOCOL_VIOLATION).
    #[error("namespace field must not be empty")]
    EmptyNamespaceField,

    /// A full track name exceeded 4096 bytes (draft-16 §2.4.1 PROTOCOL_VIOLATION).
    #[error("full track name exceeds 4096 bytes")]
    TrackNameTooLong,

    #[error("invalid datagram type")]
    InvalidDatagramType,
}

impl From<io::Error> for DecodeError {
    fn from(err: io::Error) -> Self {
        Self::Io(sync::Arc::new(err))
    }
}
