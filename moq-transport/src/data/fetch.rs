// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};
use crate::data::{ObjectStatus, StreamHeaderType};

/// Stream header for a FETCH data stream (§11.5).
///
/// A publisher opens a **unidirectional** stream per FETCH response.  The
/// stream begins with this header, which ties it back to the originating FETCH
/// request via [`request_id`].  All subsequent bytes are zero or more
/// [`FetchObject`] frames (header + payload), each read independently.
///
/// # Wire format
///
/// ```text
/// FETCH Stream Header {
///   Stream Header Type (i)  = 0x05,
///   Request ID (i),
/// }
/// ```
///
/// The `header_type` is decoded by the frame dispatcher *before* this struct
/// is constructed (so the dispatcher can choose the right struct), which is
/// why [`FetchHeader::decode`] takes the already-decoded type as a parameter
/// rather than re-reading it from the buffer.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FetchHeader {
    /// Stream type — always [`StreamHeaderType::Fetch`] (`0x05`).
    pub header_type: StreamHeaderType,
    /// The request ID of the FETCH that this stream carries data for.
    pub request_id: u64,
}

impl FetchHeader {
    /// Decode a `FetchHeader` given the stream type already read from the wire.
    pub fn decode<R: bytes::Buf>(
        header_type: StreamHeaderType,
        r: &mut R,
    ) -> Result<Self, DecodeError> {
        let request_id = u64::decode(r)?;
        Ok(Self {
            header_type,
            request_id,
        })
    }
}

impl Encode for FetchHeader {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.header_type.encode(w)?;
        self.request_id.encode(w)
    }
}

/// One object frame on a FETCH data stream (§11.5).
///
/// # Payload contract — callers must consume payload bytes
///
/// `FetchObject` encodes only the *header* fields.  The object payload
/// (`payload_length` raw bytes) immediately follows the encoded header on the
/// wire and is **not** consumed by [`Decode`] or produced by [`Encode`].
/// Callers are responsible for reading or skipping exactly `payload_length`
/// bytes after each successful [`FetchObject::decode`] call before decoding
/// the next object.  Failing to do so corrupts stream synchronisation for
/// every subsequent object.
///
/// For status-only objects (`payload_length == 0`) the next field is the
/// single-byte [`ObjectStatus`] code; that IS consumed by `decode`.
///
/// # Wire format (per object)
///
/// ```text
/// FETCH Object {
///   Group ID (i),
///   Subgroup ID (i),
///   Object ID (i),
///   Publisher Priority (8),
///   Extension Headers (..),
///   Payload Length (i),
///   [Object Status (8)]  -- only when Payload Length == 0
///   [payload bytes]      -- Payload Length raw bytes, read/written by caller
/// }
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FetchObject {
    /// Group sequence number.
    pub group_id: u64,
    /// Subgroup sequence number within the group.
    pub subgroup_id: u64,
    /// Object sequence number within the subgroup.
    pub object_id: u64,
    /// Publisher priority; smaller values are sent first.
    pub publisher_priority: u8,
    /// Extension headers (may be empty).
    pub extension_headers: KeyValuePairs,
    /// Byte length of the object payload that follows this header on the wire.
    /// Zero indicates a status-only object; the `status` field is then present.
    pub payload_length: usize,
    /// Object status, present only when `payload_length == 0`.
    pub status: Option<ObjectStatus>,
}

impl Decode for FetchObject {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let group_id = u64::decode(r)?;
        let subgroup_id = u64::decode(r)?;
        let object_id = u64::decode(r)?;
        let publisher_priority = u8::decode(r)?;
        let extension_headers = KeyValuePairs::decode(r)?;
        let payload_length = usize::decode(r)?;
        let status = match payload_length {
            0 => Some(ObjectStatus::decode(r)?),
            _ => None,
        };
        Ok(Self {
            group_id,
            subgroup_id,
            object_id,
            publisher_priority,
            extension_headers,
            payload_length,
            status,
        })
    }
}

impl Encode for FetchObject {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.group_id.encode(w)?;
        self.subgroup_id.encode(w)?;
        self.object_id.encode(w)?;
        self.publisher_priority.encode(w)?;
        self.extension_headers.encode(w)?;
        self.payload_length.encode(w)?;
        if self.payload_length == 0 {
            self.status
                .ok_or(EncodeError::MissingField("Status".to_string()))?
                .encode(w)?;
        }
        // Payload bytes are NOT encoded here; the caller writes them
        // immediately after this call.  See the struct-level doc comment.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::{Decode, Encode};

    // ── FetchHeader ──────────────────────────────────────────────────────────

    #[test]
    fn fetch_header_encode_decode_roundtrip() {
        let header = FetchHeader {
            header_type: StreamHeaderType::Fetch,
            request_id: 42,
        };
        let mut buf = bytes::BytesMut::new();
        header.encode(&mut buf).unwrap();

        // Type byte 0x05 + varint 42 (one byte, since 42 < 64)
        assert_eq!(buf[0], 0x05);

        // FetchHeader::decode takes the type as a separate argument (already
        // dispatched before this struct is constructed).
        let mut remaining = buf.freeze();
        let header_type = StreamHeaderType::decode(&mut remaining).unwrap();
        assert_eq!(header_type, StreamHeaderType::Fetch);
        let decoded = FetchHeader::decode(header_type, &mut remaining).unwrap();
        assert_eq!(decoded, header);
        assert!(remaining.is_empty(), "no bytes left after decode");
    }

    #[test]
    fn fetch_header_request_id_zero() {
        let header = FetchHeader {
            header_type: StreamHeaderType::Fetch,
            request_id: 0,
        };
        let mut buf = bytes::BytesMut::new();
        header.encode(&mut buf).unwrap();
        let mut remaining = buf.freeze();
        StreamHeaderType::decode(&mut remaining).unwrap();
        let decoded = FetchHeader::decode(StreamHeaderType::Fetch, &mut remaining).unwrap();
        assert_eq!(decoded.request_id, 0);
        assert!(remaining.is_empty());
    }

    #[test]
    fn fetch_header_large_request_id() {
        let header = FetchHeader {
            header_type: StreamHeaderType::Fetch,
            request_id: u64::from(u32::MAX) + 1,
        };
        let mut buf = bytes::BytesMut::new();
        header.encode(&mut buf).unwrap();
        let mut remaining = buf.freeze();
        StreamHeaderType::decode(&mut remaining).unwrap();
        let decoded = FetchHeader::decode(StreamHeaderType::Fetch, &mut remaining).unwrap();
        assert_eq!(decoded.request_id, header.request_id);
        assert!(remaining.is_empty());
    }

    // ── FetchObject — status / zero-payload objects ──────────────────────────

    #[test]
    fn fetch_object_zero_payload_roundtrip() {
        let obj = FetchObject {
            group_id: 1,
            subgroup_id: 2,
            object_id: 3,
            publisher_priority: 128,
            extension_headers: KeyValuePairs::default(),
            payload_length: 0,
            status: Some(ObjectStatus::EndOfGroup),
        };
        let mut buf = bytes::BytesMut::new();
        obj.encode(&mut buf).unwrap();
        let decoded = FetchObject::decode(&mut buf).unwrap();
        assert_eq!(decoded, obj);
        assert!(
            buf.is_empty(),
            "no bytes left; status is part of the header"
        );
    }

    #[test]
    fn fetch_object_encode_requires_status_when_payload_is_zero() {
        let obj = FetchObject {
            group_id: 0,
            subgroup_id: 0,
            object_id: 0,
            publisher_priority: 0,
            extension_headers: KeyValuePairs::default(),
            payload_length: 0,
            status: None, // missing — must be an error
        };
        let mut buf = bytes::BytesMut::new();
        assert!(
            obj.encode(&mut buf).is_err(),
            "missing status on zero-payload object must be an encode error"
        );
    }

    // ── FetchObject — payload contract ───────────────────────────────────────

    /// Demonstrates the payload-consumption contract: after decoding a
    /// FetchObject with `payload_length > 0` the caller must advance the
    /// reader by exactly `payload_length` bytes before decoding the next
    /// object.  If it does so, sequential objects decode correctly.
    #[test]
    fn fetch_object_payload_contract_sequential_objects() {
        let obj_a = FetchObject {
            group_id: 7,
            subgroup_id: 0,
            object_id: 0,
            publisher_priority: 128,
            extension_headers: KeyValuePairs::default(),
            payload_length: 5,
            status: None,
        };
        let payload_a: &[u8] = b"hello";

        let obj_b = FetchObject {
            group_id: 7,
            subgroup_id: 0,
            object_id: 1,
            publisher_priority: 128,
            extension_headers: KeyValuePairs::default(),
            payload_length: 0,
            status: Some(ObjectStatus::NormalObject),
        };

        // Encode header_a + payload_a + header_b
        let mut buf = bytes::BytesMut::new();
        obj_a.encode(&mut buf).unwrap();
        buf.extend_from_slice(payload_a);
        obj_b.encode(&mut buf).unwrap();

        // Decode: header_a → skip payload_a → header_b
        let decoded_a = FetchObject::decode(&mut buf).unwrap();
        assert_eq!(decoded_a, obj_a);
        // After decoding header_a, the buffer still holds payload_a bytes plus
        // the encoded obj_b header.
        assert!(
            buf.len() > decoded_a.payload_length,
            "buffer must still contain payload bytes and obj_b header"
        );

        // Caller responsibility: consume payload bytes before next decode.
        bytes::Buf::advance(&mut buf, decoded_a.payload_length);

        let decoded_b = FetchObject::decode(&mut buf).unwrap();
        assert_eq!(decoded_b, obj_b);
        assert!(buf.is_empty(), "all bytes consumed");
    }

    /// Shows what happens when a caller forgets to consume payload bytes:
    /// the next decode reads payload bytes as field data, corrupting the stream.
    #[test]
    fn fetch_object_skipping_payload_corrupts_next_decode() {
        let obj_a = FetchObject {
            group_id: 1,
            subgroup_id: 0,
            object_id: 0,
            publisher_priority: 0,
            extension_headers: KeyValuePairs::default(),
            payload_length: 3,
            status: None,
        };
        let payload_a: &[u8] = b"\xc0\x01\x00"; // varint bytes, deliberately ambiguous
        let obj_b = FetchObject {
            group_id: 2,
            subgroup_id: 0,
            object_id: 0,
            publisher_priority: 0,
            extension_headers: KeyValuePairs::default(),
            payload_length: 0,
            status: Some(ObjectStatus::NormalObject),
        };

        let mut buf = bytes::BytesMut::new();
        obj_a.encode(&mut buf).unwrap();
        buf.extend_from_slice(payload_a);
        obj_b.encode(&mut buf).unwrap();

        // Decode header_a correctly.
        let decoded_a = FetchObject::decode(&mut buf).unwrap();
        assert_eq!(decoded_a.group_id, 1);

        // If the caller forgets to consume payload_a (3 bytes) before decoding
        // obj_b, the decoder reads payload bytes as the group_id of obj_b.
        // Either the decode produces a wrong value or returns an error — either
        // way the stream is desynced.
        let decoded_b_wrong = FetchObject::decode(&mut buf).unwrap_or(FetchObject {
            group_id: u64::MAX, // sentinel: marks decode failure
            subgroup_id: 0,
            object_id: 0,
            publisher_priority: 0,
            extension_headers: KeyValuePairs::default(),
            payload_length: 0,
            status: Some(ObjectStatus::NormalObject),
        });
        // The group_id must NOT equal obj_b.group_id (2) because payload bytes
        // were read as the varint instead of the actual group_id.
        assert_ne!(
            decoded_b_wrong.group_id, 2,
            "consuming payload bytes before decode is required for correct stream sync"
        );
    }

    #[test]
    fn fetch_object_non_zero_payload_no_status() {
        let obj = FetchObject {
            group_id: 0,
            subgroup_id: 0,
            object_id: 0,
            publisher_priority: 0,
            extension_headers: KeyValuePairs::default(),
            payload_length: 100,
            status: None,
        };
        let mut buf = bytes::BytesMut::new();
        obj.encode(&mut buf).unwrap();
        let decoded = FetchObject::decode(&mut buf).unwrap();
        assert_eq!(decoded, obj);
        assert!(
            buf.is_empty(),
            "payload bytes are not encoded/decoded by this struct"
        );
    }
}
