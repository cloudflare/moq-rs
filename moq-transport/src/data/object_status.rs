// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError};

/// Object status values per draft-ietf-moq-transport-16 §10.2.1.1.
///
/// Note: value 0x1 (`ObjectDoesNotExist`) was present in earlier drafts but
/// was removed in draft-16.  Any received value other than 0x0, 0x3, or 0x4
/// is treated as a protocol error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectStatus {
    /// 0x0 — Normal object with payload.  For non-zero length objects this
    /// status is implicit; a zero-length object must encode it explicitly.
    NormalObject = 0x0,
    /// 0x3 — End of Group.  No objects with this Group ID and an Object ID
    /// greater than or equal to the one specified will exist.
    EndOfGroup = 0x3,
    /// 0x4 — End of Track.  No objects at or beyond this location exist.
    EndOfTrack = 0x4,
}

impl Decode for ObjectStatus {
    fn decode<B: bytes::Buf>(r: &mut B) -> Result<Self, DecodeError> {
        match u64::decode(r)? {
            0x0 => Ok(Self::NormalObject),
            0x3 => Ok(Self::EndOfGroup),
            0x4 => Ok(Self::EndOfTrack),
            _ => Err(DecodeError::InvalidObjectStatus),
        }
    }
}

impl Encode for ObjectStatus {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        let val = *self as u64;
        val.encode(w)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Bytes, BytesMut};

    #[test]
    fn encode_decode_valid_statuses() {
        let mut buf = BytesMut::new();

        for status in [
            ObjectStatus::NormalObject,
            ObjectStatus::EndOfGroup,
            ObjectStatus::EndOfTrack,
        ] {
            status.encode(&mut buf).unwrap();
            let decoded = ObjectStatus::decode(&mut buf).unwrap();
            assert_eq!(decoded, status);
        }
    }

    #[test]
    fn decode_rejects_removed_does_not_exist_value() {
        // 0x1 was ObjectDoesNotExist in pre-draft-16 but is no longer valid.
        let data = vec![0x01u8];
        let mut buf: Bytes = data.into();
        assert!(matches!(
            ObjectStatus::decode(&mut buf).unwrap_err(),
            crate::coding::DecodeError::InvalidObjectStatus
        ));
    }

    #[test]
    fn decode_rejects_unknown_status() {
        let data = vec![0x02u8];
        let mut buf: Bytes = data.into();
        assert!(matches!(
            ObjectStatus::decode(&mut buf).unwrap_err(),
            crate::coding::DecodeError::InvalidObjectStatus
        ));
    }

    #[test]
    fn normal_object_wire_value_is_zero() {
        // Objects with payload use NormalObject = 0x0.
        assert_eq!(ObjectStatus::NormalObject as u64, 0x0);
    }

    #[test]
    fn end_of_group_wire_value_is_three() {
        assert_eq!(ObjectStatus::EndOfGroup as u64, 0x3);
    }

    #[test]
    fn end_of_track_wire_value_is_four() {
        assert_eq!(ObjectStatus::EndOfTrack as u64, 0x4);
    }
}
