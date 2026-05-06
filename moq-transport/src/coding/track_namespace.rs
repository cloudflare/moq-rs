// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Track namespace and track name encoding per draft-ietf-moq-transport-16 §2.4.1.
//!
//! Rules enforced here:
//!
//! - A **full** `TrackNamespace` must have 1–32 fields; each field must be ≥ 1 byte.
//! - A **prefix** `TrackNamespacePrefix` (used in `SUBSCRIBE_NAMESPACE`) may have
//!   0–32 fields; each non-empty field must also be ≥ 1 byte.
//! - The total length of a Full Track Name (sum of all namespace field lengths +
//!   track name length) MUST NOT exceed 4096 bytes.  Validated by callers via
//!   [`full_track_name_len`].
//! - Track names are arbitrary bytes and may be empty.

use super::{Decode, DecodeError, Encode, EncodeError, TupleField};
use core::hash::{Hash, Hasher};
use std::convert::TryFrom;
use std::fmt;
use thiserror::Error;

/// Maximum total length of a Full Track Name (namespace fields + track name).
pub const MAX_FULL_TRACK_NAME_LEN: usize = 4096;

// ─── Errors ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum TrackNamespaceError {
    #[error("too many fields: {0} exceeds maximum of {1}")]
    TooManyFields(usize, usize),

    #[error("too few fields: full track namespace requires at least 1 field")]
    TooFewFields,

    #[error("field too large: {0} bytes exceeds maximum of {1}")]
    FieldTooLarge(usize, usize),

    #[error("empty field: namespace fields must be at least 1 byte")]
    EmptyField,
}

// ─── TrackNamespace (full, 1–32 non-empty fields) ─────────────────────────────

/// A full Track Namespace: 1–32 non-empty byte fields.
///
/// Used in `SUBSCRIBE`, `PUBLISH`, `PUBLISH_NAMESPACE`, `TRACK_STATUS`, etc.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct TrackNamespace {
    pub fields: Vec<TupleField>,
}

impl TrackNamespace {
    pub const MAX_FIELDS: usize = 32;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, field: TupleField) {
        self.fields.push(field);
    }

    pub fn clear(&mut self) {
        self.fields.clear();
    }

    /// Build from a `/`-separated UTF-8 path (each segment becomes a field).
    /// Empty segments (e.g. leading `/`) are included as empty fields and will
    /// fail validation; callers producing full namespaces should ensure no
    /// empty segments.
    pub fn from_utf8_path(path: &str) -> Self {
        let mut ns = TrackNamespace::new();
        for part in path.split('/') {
            ns.add(TupleField::from_utf8(part));
        }
        ns
    }

    pub fn to_utf8_path(&self) -> String {
        let mut path = String::new();
        for field in &self.fields {
            path.push('/');
            path.push_str(&String::from_utf8_lossy(&field.value));
        }
        path
    }

    /// Sum of all field lengths. Used for full-track-name limit calculation.
    pub fn namespace_byte_len(&self) -> usize {
        self.fields.iter().map(|f| f.value.len()).sum()
    }
}

impl Hash for TrackNamespace {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fields.hash(state);
    }
}

impl Decode for TrackNamespace {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let count = usize::decode(r)?;

        // Draft-16 §2.4.1: 0 or >32 fields → PROTOCOL_VIOLATION.
        if count == 0 || count > Self::MAX_FIELDS {
            return Err(DecodeError::FieldBoundsExceeded(format!(
                "TrackNamespace must have 1–{} fields, got {}",
                Self::MAX_FIELDS,
                count
            )));
        }

        let mut fields = Vec::with_capacity(count);
        for _ in 0..count {
            let field = TupleField::decode(r)?;
            // Draft-16 §2.4.1: each field must be ≥ 1 byte.
            if field.value.is_empty() {
                return Err(DecodeError::EmptyNamespaceField);
            }
            fields.push(field);
        }
        Ok(Self { fields })
    }
}

impl Encode for TrackNamespace {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        if self.fields.is_empty() || self.fields.len() > Self::MAX_FIELDS {
            return Err(EncodeError::FieldBoundsExceeded(format!(
                "TrackNamespace must have 1–{} fields",
                Self::MAX_FIELDS
            )));
        }
        self.fields.len().encode(w)?;
        for field in &self.fields {
            if field.value.is_empty() {
                return Err(EncodeError::EmptyNamespaceField);
            }
            field.encode(w)?;
        }
        Ok(())
    }
}

impl fmt::Debug for TrackNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for TrackNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{0}", self.to_utf8_path())
    }
}

impl TryFrom<Vec<TupleField>> for TrackNamespace {
    type Error = TrackNamespaceError;

    fn try_from(fields: Vec<TupleField>) -> Result<Self, Self::Error> {
        if fields.is_empty() {
            return Err(TrackNamespaceError::TooFewFields);
        }
        if fields.len() > Self::MAX_FIELDS {
            return Err(TrackNamespaceError::TooManyFields(
                fields.len(),
                Self::MAX_FIELDS,
            ));
        }
        for field in &fields {
            if field.value.is_empty() {
                return Err(TrackNamespaceError::EmptyField);
            }
            if field.value.len() > TupleField::MAX_VALUE_SIZE {
                return Err(TrackNamespaceError::FieldTooLarge(
                    field.value.len(),
                    TupleField::MAX_VALUE_SIZE,
                ));
            }
        }
        Ok(Self { fields })
    }
}

impl TryFrom<&str> for TrackNamespace {
    type Error = TrackNamespaceError;

    fn try_from(path: &str) -> Result<Self, Self::Error> {
        let fields: Vec<TupleField> = path
            .split('/')
            .filter(|s| !s.is_empty())
            .map(TupleField::from_utf8)
            .collect();
        Self::try_from(fields)
    }
}

impl TryFrom<String> for TrackNamespace {
    type Error = TrackNamespaceError;

    fn try_from(path: String) -> Result<Self, Self::Error> {
        Self::try_from(path.as_str())
    }
}

impl TryFrom<Vec<&str>> for TrackNamespace {
    type Error = TrackNamespaceError;

    fn try_from(parts: Vec<&str>) -> Result<Self, Self::Error> {
        let fields: Vec<TupleField> = parts.into_iter().map(TupleField::from_utf8).collect();
        Self::try_from(fields)
    }
}

impl TryFrom<Vec<String>> for TrackNamespace {
    type Error = TrackNamespaceError;

    fn try_from(parts: Vec<String>) -> Result<Self, Self::Error> {
        let fields: Vec<TupleField> = parts.iter().map(|s| TupleField::from_utf8(s)).collect();
        Self::try_from(fields)
    }
}

// ─── TrackNamespacePrefix (0–32 fields, for SUBSCRIBE_NAMESPACE) ──────────────

/// A Track Namespace Prefix used in `SUBSCRIBE_NAMESPACE`.
///
/// Unlike [`TrackNamespace`], a prefix is allowed to have 0 fields (matching
/// all namespaces).  Fields that are present must still be non-empty.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct TrackNamespacePrefix {
    pub fields: Vec<TupleField>,
}

impl TrackNamespacePrefix {
    pub const MAX_FIELDS: usize = 32;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_utf8_path(path: &str) -> Self {
        let mut prefix = TrackNamespacePrefix::new();
        for part in path.split('/').filter(|s| !s.is_empty()) {
            prefix.fields.push(TupleField::from_utf8(part));
        }
        prefix
    }

    pub fn to_utf8_path(&self) -> String {
        let mut path = String::new();
        for field in &self.fields {
            path.push('/');
            path.push_str(&String::from_utf8_lossy(&field.value));
        }
        path
    }
}

impl Hash for TrackNamespacePrefix {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fields.hash(state);
    }
}

impl Decode for TrackNamespacePrefix {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let count = usize::decode(r)?;

        // Draft-16 §9.25: 0–32 fields allowed for prefix.
        if count > Self::MAX_FIELDS {
            return Err(DecodeError::FieldBoundsExceeded(format!(
                "TrackNamespacePrefix must have 0–{} fields, got {}",
                Self::MAX_FIELDS,
                count
            )));
        }

        let mut fields = Vec::with_capacity(count);
        for _ in 0..count {
            let field = TupleField::decode(r)?;
            if field.value.is_empty() {
                return Err(DecodeError::EmptyNamespaceField);
            }
            fields.push(field);
        }
        Ok(Self { fields })
    }
}

impl Encode for TrackNamespacePrefix {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        if self.fields.len() > Self::MAX_FIELDS {
            return Err(EncodeError::FieldBoundsExceeded(format!(
                "TrackNamespacePrefix must have 0–{} fields",
                Self::MAX_FIELDS
            )));
        }
        self.fields.len().encode(w)?;
        for field in &self.fields {
            if field.value.is_empty() {
                return Err(EncodeError::EmptyNamespaceField);
            }
            field.encode(w)?;
        }
        Ok(())
    }
}

impl fmt::Debug for TrackNamespacePrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for TrackNamespacePrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{0}", self.to_utf8_path())
    }
}

// ─── Full track name length helper ───────────────────────────────────────────

/// Compute the wire-encoded byte length of a Full Track Name.
///
/// This is the sum of each namespace field's length plus the track name length.
/// If the result exceeds [`MAX_FULL_TRACK_NAME_LEN`] the caller should close
/// the session with PROTOCOL_VIOLATION.
pub fn full_track_name_len(namespace: &TrackNamespace, track_name: &[u8]) -> usize {
    namespace.namespace_byte_len() + track_name.len()
}

// ─── Add missing EncodeError variant ─────────────────────────────────────────
// (defined here to keep it adjacent to the validation logic)

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Bytes, BytesMut};
    use std::convert::TryInto;

    // ── TrackNamespace encode/decode ──────────────────────────────────────────

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();

        let t = TrackNamespace::from_utf8_path("test/path/to/resource");
        t.encode(&mut buf).unwrap();
        #[rustfmt::skip]
        assert_eq!(
            buf.to_vec(),
            vec![
                0x04, // 4 tuple fields
                0x04, 0x74, 0x65, 0x73, 0x74,       // "test"
                0x04, 0x70, 0x61, 0x74, 0x68,       // "path"
                0x02, 0x74, 0x6f,                   // "to"
                0x08, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, // "resource"
            ]
        );
        let decoded = TrackNamespace::decode(&mut buf).unwrap();
        assert_eq!(decoded, t);
    }

    #[test]
    fn encode_single_field() {
        let mut buf = BytesMut::new();
        let mut t = TrackNamespace::new();
        t.add(TupleField::from_utf8("test"));
        t.encode(&mut buf).unwrap();
        assert_eq!(
            buf.to_vec(),
            vec![0x01, 0x04, 0x74, 0x65, 0x73, 0x74]
        );
        let decoded = TrackNamespace::decode(&mut buf).unwrap();
        assert_eq!(decoded, t);
    }

    // ── 0-field rejection ─────────────────────────────────────────────────────

    #[test]
    fn decode_zero_fields_is_error() {
        // wire: count = 0
        let data: Vec<u8> = vec![0x00];
        let mut buf: Bytes = data.into();
        let err = TrackNamespace::decode(&mut buf).unwrap_err();
        assert!(
            matches!(err, DecodeError::FieldBoundsExceeded(_)),
            "expected FieldBoundsExceeded, got {:?}",
            err
        );
    }

    #[test]
    fn encode_zero_fields_is_error() {
        let t = TrackNamespace::new(); // empty
        let mut buf = BytesMut::new();
        assert!(matches!(
            t.encode(&mut buf).unwrap_err(),
            EncodeError::FieldBoundsExceeded(_)
        ));
    }

    // ── >32 fields rejection ──────────────────────────────────────────────────

    #[test]
    fn encode_too_large() {
        let mut buf = BytesMut::new();
        let mut t = TrackNamespace::new();
        for i in 0..=TrackNamespace::MAX_FIELDS {
            t.add(TupleField::from_utf8(&format!("f{}", i)));
        }
        assert!(matches!(
            t.encode(&mut buf).unwrap_err(),
            EncodeError::FieldBoundsExceeded(_)
        ));
    }

    #[test]
    fn decode_too_large() {
        let mut data: Vec<u8> = vec![0x00; 256];
        data[0] = (TrackNamespace::MAX_FIELDS + 1) as u8; // count = 33
        let mut buf: Bytes = data.into();
        assert!(matches!(
            TrackNamespace::decode(&mut buf).unwrap_err(),
            DecodeError::FieldBoundsExceeded(_)
        ));
    }

    // ── empty field rejection ─────────────────────────────────────────────────

    #[test]
    fn decode_empty_field_is_error() {
        // wire: count=1, field length=0
        let data: Vec<u8> = vec![0x01, 0x00]; // count=1, field_len=0
        let mut buf: Bytes = data.into();
        assert!(matches!(
            TrackNamespace::decode(&mut buf).unwrap_err(),
            DecodeError::EmptyNamespaceField
        ));
    }

    #[test]
    fn encode_empty_field_is_error() {
        let mut t = TrackNamespace::new();
        t.add(TupleField { value: vec![] }); // empty field
        let mut buf = BytesMut::new();
        assert!(matches!(
            t.encode(&mut buf).unwrap_err(),
            EncodeError::EmptyNamespaceField
        ));
    }

    // ── TryFrom conversions ───────────────────────────────────────────────────

    #[test]
    fn try_from_str() {
        let ns: TrackNamespace = "test/path/to/resource".try_into().unwrap();
        assert_eq!(ns.fields.len(), 4);
        assert_eq!(ns.to_utf8_path(), "/test/path/to/resource");
    }

    #[test]
    fn try_from_string() {
        let path = String::from("test/path");
        let ns: TrackNamespace = path.try_into().unwrap();
        assert_eq!(ns.fields.len(), 2);
        assert_eq!(ns.to_utf8_path(), "/test/path");
    }

    #[test]
    fn try_from_vec_str() {
        let parts = vec!["test", "path", "to", "resource"];
        let ns: TrackNamespace = parts.try_into().unwrap();
        assert_eq!(ns.fields.len(), 4);
        assert_eq!(ns.to_utf8_path(), "/test/path/to/resource");
    }

    #[test]
    fn try_from_vec_string() {
        let parts = vec![String::from("test"), String::from("path")];
        let ns: TrackNamespace = parts.try_into().unwrap();
        assert_eq!(ns.fields.len(), 2);
        assert_eq!(ns.to_utf8_path(), "/test/path");
    }

    #[test]
    fn try_from_vec_tuple_field() {
        let fields = vec![TupleField::from_utf8("test"), TupleField::from_utf8("path")];
        let ns: TrackNamespace = fields.try_into().unwrap();
        assert_eq!(ns.fields.len(), 2);
        assert_eq!(ns.to_utf8_path(), "/test/path");
    }

    #[test]
    fn try_from_empty_vec_is_error() {
        let fields: Vec<TupleField> = vec![];
        let result: Result<TrackNamespace, _> = fields.try_into();
        assert!(matches!(result.unwrap_err(), TrackNamespaceError::TooFewFields));
    }

    #[test]
    fn try_from_too_many_fields() {
        let fields: Vec<TupleField> = (0..=TrackNamespace::MAX_FIELDS)
            .map(|i| TupleField::from_utf8(&format!("f{}", i)))
            .collect();
        let result: Result<TrackNamespace, _> = fields.try_into();
        assert!(matches!(
            result.unwrap_err(),
            TrackNamespaceError::TooManyFields(33, 32)
        ));
    }

    #[test]
    fn try_from_field_too_large() {
        let large_value = "x".repeat(TupleField::MAX_VALUE_SIZE + 1);
        let fields = vec![TupleField {
            value: large_value.into_bytes(),
        }];
        let result: Result<TrackNamespace, _> = fields.try_into();
        assert!(matches!(
            result.unwrap_err(),
            TrackNamespaceError::FieldTooLarge(4097, 4096)
        ));
    }

    #[test]
    fn try_from_empty_field_is_error() {
        let fields = vec![TupleField { value: vec![] }];
        let result: Result<TrackNamespace, _> = fields.try_into();
        assert!(matches!(result.unwrap_err(), TrackNamespaceError::EmptyField));
    }

    // ── TrackNamespacePrefix ──────────────────────────────────────────────────

    #[test]
    fn prefix_allows_zero_fields() {
        let prefix = TrackNamespacePrefix::new();
        let mut buf = BytesMut::new();
        prefix.encode(&mut buf).unwrap(); // must not error
        assert_eq!(buf.to_vec(), vec![0x00]);
        let decoded = TrackNamespacePrefix::decode(&mut buf).unwrap();
        assert_eq!(decoded.fields.len(), 0);
    }

    #[test]
    fn prefix_roundtrip() {
        let prefix = TrackNamespacePrefix::from_utf8_path("example.com/meeting=123");
        let mut buf = BytesMut::new();
        prefix.encode(&mut buf).unwrap();
        let decoded = TrackNamespacePrefix::decode(&mut buf).unwrap();
        assert_eq!(decoded.fields.len(), prefix.fields.len());
        assert_eq!(decoded.to_utf8_path(), prefix.to_utf8_path());
    }

    #[test]
    fn prefix_rejects_too_many_fields() {
        let mut prefix = TrackNamespacePrefix::new();
        for i in 0..=TrackNamespacePrefix::MAX_FIELDS {
            prefix.fields.push(TupleField::from_utf8(&format!("f{}", i)));
        }
        let mut buf = BytesMut::new();
        assert!(matches!(
            prefix.encode(&mut buf).unwrap_err(),
            EncodeError::FieldBoundsExceeded(_)
        ));
    }

    #[test]
    fn prefix_rejects_empty_field() {
        let mut prefix = TrackNamespacePrefix::new();
        prefix.fields.push(TupleField { value: vec![] });
        let mut buf = BytesMut::new();
        assert!(matches!(
            prefix.encode(&mut buf).unwrap_err(),
            EncodeError::EmptyNamespaceField
        ));
    }

    // ── full_track_name_len ───────────────────────────────────────────────────

    #[test]
    fn full_track_name_len_basic() {
        let ns = TrackNamespace::from_utf8_path("a/b"); // fields: "a" (1 byte), "b" (1 byte)
        let track_name = b"mytrack"; // 7 bytes
        assert_eq!(full_track_name_len(&ns, track_name), 1 + 1 + 7);
    }

    #[test]
    fn full_track_name_len_at_limit() {
        // Build a namespace with one field of 4088 bytes and an empty track name (0 bytes).
        // Total = 4088 ≤ 4096.
        let big_field = vec![b'x'; 4088];
        let ns = TrackNamespace {
            fields: vec![TupleField { value: big_field }],
        };
        assert!(full_track_name_len(&ns, b"") <= MAX_FULL_TRACK_NAME_LEN);
    }

    #[test]
    fn full_track_name_len_over_limit() {
        // 4092 (namespace) + 5 (track name "hello") = 4097 > 4096
        let big_field = vec![b'x'; 4092];
        let ns = TrackNamespace {
            fields: vec![TupleField { value: big_field }],
        };
        assert!(full_track_name_len(&ns, b"hello") > MAX_FULL_TRACK_NAME_LEN);
    }
}
