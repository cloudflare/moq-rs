// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{io, sync};

use super::BoundsExceeded;

pub trait Encode: Sized {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError>;

    // Helper function to make sure we have enough bytes to encode
    fn encode_remaining<W: bytes::BufMut>(buf: &mut W, required: usize) -> Result<(), EncodeError> {
        let needed = required.saturating_sub(buf.remaining_mut());
        if needed > 0 {
            Err(EncodeError::More(needed))
        } else {
            Ok(())
        }
    }
}

/// An encode error.
#[derive(thiserror::Error, Debug, Clone)]
pub enum EncodeError {
    #[error("short buffer")]
    More(usize),

    #[error("varint too large")]
    BoundsExceeded(#[from] BoundsExceeded),

    #[error("invalid value")]
    InvalidValue,

    #[error("field '{0}' missing")]
    MissingField(String),

    #[error("i/o error: {0}")]
    Io(sync::Arc<io::Error>),

    #[error("message too large")]
    MsgBoundsExceeded,

    #[error("field '{0}' too large")]
    FieldBoundsExceeded(String),

    /// KVP keys must be in non-decreasing order during encode.
    #[error("key-value-pair keys must be non-decreasing")]
    KvpKeyOrder,

    /// Bytes-typed KVP value exceeds maximum length of 2^16-1.
    #[error("key-value-pair bytes value too long")]
    KeyValuePairLengthExceeded,

    /// A namespace field had zero length (draft-16 §2.4.1 PROTOCOL_VIOLATION).
    #[error("namespace field must not be empty")]
    EmptyNamespaceField,
}

impl From<io::Error> for EncodeError {
    fn from(err: io::Error) -> Self {
        Self::Io(sync::Arc::new(err))
    }
}
