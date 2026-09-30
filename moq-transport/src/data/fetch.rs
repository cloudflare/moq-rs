// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs, Location, VarInt};
use crate::data::{ExtensionHeaders, ObjectStatus, StreamHeaderType};

const SUBGROUP_MASK: u64 = 0x03;
const OBJECT_ID_PRESENT: u64 = 0x04;
const GROUP_ID_PRESENT: u64 = 0x08;
const PRIORITY_PRESENT: u64 = 0x10;
const EXTENSIONS_PRESENT: u64 = 0x20;
const DATAGRAM: u64 = 0x40;
const END_NOT_EXIST: u64 = 0x8c;
const END_UNKNOWN: u64 = 0x10c;
pub(crate) const MAX_FETCH_RECORD_HEADER_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FetchHeader {
    pub header_type: StreamHeaderType,
    pub request_id: u64,
}

impl FetchHeader {
    pub fn decode<R: bytes::Buf>(
        header_type: StreamHeaderType,
        r: &mut R,
    ) -> Result<Self, DecodeError> {
        Ok(Self {
            header_type,
            request_id: u64::decode(r)?,
        })
    }
}

impl Encode for FetchHeader {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.header_type.encode(w)?;
        self.request_id.encode(w)
    }
}

/// Legacy FETCH object representation retained for source compatibility.
///
/// New code should use [`FetchRecordObject`] through [`FetchRecord`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FetchObject {
    pub group_id: u64,
    pub subgroup_id: u64,
    pub object_id: u64,
    pub publisher_priority: u8,
    pub extension_headers: KeyValuePairs,
    pub payload_length: usize,
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
                .ok_or_else(|| EncodeError::MissingField("Status".to_string()))?
                .encode(w)?;
        }
        Ok(())
    }
}

/// Metadata for one draft-16 Object on a FETCH stream.
///
/// The payload immediately follows the encoded record and is read or written
/// separately so applications can apply backpressure without buffering an
/// entire Object.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FetchRecordObject {
    pub group_id: u64,
    /// `None` preserves the original Datagram forwarding preference.
    pub subgroup_id: Option<u64>,
    pub object_id: u64,
    pub publisher_priority: u8,
    pub extension_headers: ExtensionHeaders,
    pub payload_length: u64,
}

/// One semantic record on a FETCH stream.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FetchRecord {
    Object(FetchRecordObject),
    /// Inclusive final Location in a contiguous non-existent range.
    NotExist {
        end: Location,
    },
    /// Inclusive final Location in a contiguous range with unknown status.
    Unknown {
        end: Location,
    },
}

impl Encode for FetchRecord {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        match self {
            Self::Object(object) => encode_explicit_object(w, object),
            Self::NotExist { end } => encode_end(w, END_NOT_EXIST, *end),
            Self::Unknown { end } => encode_end(w, END_UNKNOWN, *end),
        }
    }
}

fn encode_end<W: bytes::BufMut>(
    w: &mut W,
    serialization_flags: u64,
    end: Location,
) -> Result<(), EncodeError> {
    serialization_flags.encode(w)?;
    end.group_id.encode(w)?;
    end.object_id.encode(w)
}

fn encode_explicit_object<W: bytes::BufMut>(
    w: &mut W,
    object: &FetchRecordObject,
) -> Result<(), EncodeError> {
    validate_object(object)?;
    let mut flags = OBJECT_ID_PRESENT | GROUP_ID_PRESENT | PRIORITY_PRESENT;
    match object.subgroup_id {
        Some(_) => flags |= SUBGROUP_MASK,
        None => flags |= DATAGRAM,
    }
    if !object.extension_headers.is_empty() {
        flags |= EXTENSIONS_PRESENT;
    }

    flags.encode(w)?;
    object.group_id.encode(w)?;
    if let Some(subgroup_id) = object.subgroup_id {
        subgroup_id.encode(w)?;
    }
    object.object_id.encode(w)?;
    object.publisher_priority.encode(w)?;
    if flags & EXTENSIONS_PRESENT != 0 {
        object.extension_headers.encode(w)?;
    }
    object.payload_length.encode(w)
}

fn validate_object(object: &FetchRecordObject) -> Result<(), EncodeError> {
    VarInt::try_from(object.group_id)?;
    VarInt::try_from(object.object_id)?;
    VarInt::try_from(object.payload_length)?;
    if let Some(subgroup_id) = object.subgroup_id {
        VarInt::try_from(subgroup_id)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PreviousObject {
    group_id: u64,
    subgroup_id: Option<u64>,
    object_id: u64,
    publisher_priority: u8,
}

impl From<&FetchRecordObject> for PreviousObject {
    fn from(object: &FetchRecordObject) -> Self {
        Self {
            group_id: object.group_id,
            subgroup_id: object.subgroup_id,
            object_id: object.object_id,
            publisher_priority: object.publisher_priority,
        }
    }
}

/// Stateful decoder for consecutive records on one FETCH stream.
///
/// The caller must consume each Object payload before decoding the next record.
#[derive(Clone, Default)]
pub(crate) struct FetchRecordDecoder {
    previous: Option<PreviousObject>,
}

impl FetchRecordDecoder {
    pub fn decode<R: bytes::Buf>(&mut self, r: &mut R) -> Result<FetchRecord, DecodeError> {
        let flags = u64::decode(r)?;
        match flags {
            END_NOT_EXIST => return self.decode_end(r, false),
            END_UNKNOWN => return self.decode_end(r, true),
            0x80.. => return Err(DecodeError::InvalidValue),
            _ => {}
        }

        let group_id = if flags & GROUP_ID_PRESENT != 0 {
            u64::decode(r)?
        } else {
            self.previous.ok_or(DecodeError::InvalidValue)?.group_id
        };

        let subgroup_id = if flags & DATAGRAM != 0 {
            None
        } else {
            Some(match flags & SUBGROUP_MASK {
                0 => 0,
                1 => self
                    .previous
                    .and_then(|previous| previous.subgroup_id)
                    .ok_or(DecodeError::InvalidValue)?,
                2 => checked_increment(
                    self.previous
                        .and_then(|previous| previous.subgroup_id)
                        .ok_or(DecodeError::InvalidValue)?,
                )?,
                3 => u64::decode(r)?,
                _ => unreachable!(),
            })
        };

        let object_id = if flags & OBJECT_ID_PRESENT != 0 {
            u64::decode(r)?
        } else {
            checked_increment(self.previous.ok_or(DecodeError::InvalidValue)?.object_id)?
        };
        let publisher_priority = if flags & PRIORITY_PRESENT != 0 {
            u8::decode(r)?
        } else {
            self.previous
                .ok_or(DecodeError::InvalidValue)?
                .publisher_priority
        };
        let extension_headers = if flags & EXTENSIONS_PRESENT != 0 {
            ExtensionHeaders::decode(r)?
        } else {
            ExtensionHeaders::default()
        };
        let payload_length = u64::decode(r)?;

        let object = FetchRecordObject {
            group_id,
            subgroup_id,
            object_id,
            publisher_priority,
            extension_headers,
            payload_length,
        };
        self.previous = Some((&object).into());
        Ok(FetchRecord::Object(object))
    }

    fn decode_end<R: bytes::Buf>(
        &self,
        r: &mut R,
        unknown: bool,
    ) -> Result<FetchRecord, DecodeError> {
        let end = Location::new(u64::decode(r)?, u64::decode(r)?);
        Ok(if unknown {
            FetchRecord::Unknown { end }
        } else {
            FetchRecord::NotExist { end }
        })
    }
}

fn checked_increment(value: u64) -> Result<u64, DecodeError> {
    value
        .checked_add(1)
        .filter(|value| *value <= VarInt::MAX.into_inner())
        .ok_or(DecodeError::InvalidValue)
}

/// Canonical stateful encoder for consecutive records on one FETCH stream.
#[derive(Clone, Default)]
pub(crate) struct FetchRecordEncoder {
    previous: Option<PreviousObject>,
}

impl FetchRecordEncoder {
    pub fn encode<W: bytes::BufMut>(
        &mut self,
        record: &FetchRecord,
        output: &mut W,
    ) -> Result<(), EncodeError> {
        let mut encoded = bytes::BytesMut::new();
        match record {
            FetchRecord::Object(object) => self.encode_object(object, &mut encoded)?,
            FetchRecord::NotExist { end } => encode_end(&mut encoded, END_NOT_EXIST, *end)?,
            FetchRecord::Unknown { end } => encode_end(&mut encoded, END_UNKNOWN, *end)?,
        }
        output.put_slice(&encoded);
        if let FetchRecord::Object(object) = record {
            self.previous = Some(object.into());
        }
        Ok(())
    }

    fn encode_object<W: bytes::BufMut>(
        &self,
        object: &FetchRecordObject,
        output: &mut W,
    ) -> Result<(), EncodeError> {
        validate_object(object)?;
        let mut flags = 0;
        let mut subgroup = None;

        match object.subgroup_id {
            None => flags |= DATAGRAM,
            Some(0) => {}
            Some(subgroup_id) => {
                let mode = match self.previous.and_then(|previous| previous.subgroup_id) {
                    Some(previous) if subgroup_id == previous => 1,
                    Some(previous)
                        if previous
                            .checked_add(1)
                            .is_some_and(|next| subgroup_id == next) =>
                    {
                        2
                    }
                    _ => {
                        subgroup = Some(subgroup_id);
                        3
                    }
                };
                flags |= mode;
            }
        }

        if self
            .previous
            .is_none_or(|previous| previous.group_id != object.group_id)
        {
            flags |= GROUP_ID_PRESENT;
        }
        if self.previous.is_none_or(|previous| {
            previous
                .object_id
                .checked_add(1)
                .is_none_or(|next| next != object.object_id)
        }) {
            flags |= OBJECT_ID_PRESENT;
        }
        if self
            .previous
            .is_none_or(|previous| previous.publisher_priority != object.publisher_priority)
        {
            flags |= PRIORITY_PRESENT;
        }
        if !object.extension_headers.is_empty() {
            flags |= EXTENSIONS_PRESENT;
        }

        flags.encode(output)?;
        if flags & GROUP_ID_PRESENT != 0 {
            object.group_id.encode(output)?;
        }
        if let Some(subgroup_id) = subgroup {
            subgroup_id.encode(output)?;
        }
        if flags & OBJECT_ID_PRESENT != 0 {
            object.object_id.encode(output)?;
        }
        if flags & PRIORITY_PRESENT != 0 {
            object.publisher_priority.encode(output)?;
        }
        if flags & EXTENSIONS_PRESENT != 0 {
            object.extension_headers.encode(output)?;
        }
        object.payload_length.encode(output)
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};

    use super::*;

    fn object() -> FetchRecordObject {
        FetchRecordObject {
            group_id: 3,
            subgroup_id: Some(2),
            object_id: 7,
            publisher_priority: 5,
            extension_headers: ExtensionHeaders::default(),
            payload_length: 4,
        }
    }

    #[test]
    fn legacy_fetch_object_public_shape_is_preserved() {
        let expected = FetchObject {
            group_id: 1,
            subgroup_id: 2,
            object_id: 3,
            publisher_priority: 4,
            extension_headers: KeyValuePairs::default(),
            payload_length: 0,
            status: Some(ObjectStatus::EndOfGroup),
        };
        let mut encoded = BytesMut::new();
        expected.encode(&mut encoded).unwrap();
        assert_eq!(FetchObject::decode(&mut encoded).unwrap(), expected);
    }

    fn decode(decoder: &mut FetchRecordDecoder, encoded: &BytesMut) -> FetchRecord {
        let mut cursor = std::io::Cursor::new(encoded.as_ref());
        decoder.decode(&mut cursor).unwrap()
    }

    #[test]
    fn explicit_object_round_trip() {
        let expected = object();
        let mut encoded = BytesMut::new();
        FetchRecord::Object(expected.clone())
            .encode(&mut encoded)
            .unwrap();

        assert_eq!(
            decode(&mut FetchRecordDecoder::default(), &encoded),
            FetchRecord::Object(expected)
        );
    }

    #[test]
    fn range_markers_round_trip() {
        for expected in [
            FetchRecord::NotExist {
                end: Location::new(4, 8),
            },
            FetchRecord::Unknown {
                end: Location::new(9, 2),
            },
        ] {
            let mut encoded = BytesMut::new();
            expected.encode(&mut encoded).unwrap();
            assert_eq!(
                decode(&mut FetchRecordDecoder::default(), &encoded),
                expected
            );
        }
    }

    #[test]
    fn decoder_resolves_inherited_fields() {
        let mut first = BytesMut::new();
        FetchRecord::Object(object()).encode(&mut first).unwrap();
        let mut second = BytesMut::new();
        1_u64.encode(&mut second).unwrap();
        0_u64.encode(&mut second).unwrap();

        let mut decoder = FetchRecordDecoder::default();
        decode(&mut decoder, &first);
        assert_eq!(
            decode(&mut decoder, &second),
            FetchRecord::Object(FetchRecordObject {
                group_id: 3,
                subgroup_id: Some(2),
                object_id: 8,
                publisher_priority: 5,
                extension_headers: ExtensionHeaders::default(),
                payload_length: 0,
            })
        );
    }

    #[test]
    fn canonical_encoder_inherits_fields() {
        let mut encoder = FetchRecordEncoder::default();
        let mut encoded = BytesMut::new();
        encoder
            .encode(&FetchRecord::Object(object()), &mut encoded)
            .unwrap();
        assert_eq!(encoded[0], 0x1f);

        encoded.clear();
        let next = FetchRecordObject {
            object_id: 8,
            payload_length: 0,
            ..object()
        };
        encoder
            .encode(&FetchRecord::Object(next), &mut encoded)
            .unwrap();
        assert_eq!(encoded.as_ref(), &[0x01, 0x00]);
    }

    #[test]
    fn markers_do_not_change_inheritance() {
        let mut encoder = FetchRecordEncoder::default();
        let mut encoded = BytesMut::new();
        encoder
            .encode(&FetchRecord::Object(object()), &mut encoded)
            .unwrap();
        encoder
            .encode(
                &FetchRecord::Unknown {
                    end: Location::new(20, 30),
                },
                &mut encoded,
            )
            .unwrap();
        let next = FetchRecordObject {
            object_id: 8,
            payload_length: 0,
            ..object()
        };
        let mut inherited = BytesMut::new();
        encoder
            .encode(&FetchRecord::Object(next.clone()), &mut inherited)
            .unwrap();

        let mut decoder = FetchRecordDecoder::default();
        let mut cursor = std::io::Cursor::new(encoded.as_ref());
        decoder.decode(&mut cursor).unwrap();
        decoder.decode(&mut cursor).unwrap();
        assert_eq!(decode(&mut decoder, &inherited), FetchRecord::Object(next));
    }

    #[test]
    fn datagram_preference_and_extensions_round_trip() {
        let mut extensions = ExtensionHeaders::new();
        extensions.set_intvalue(2, 9);
        let expected = FetchRecordObject {
            subgroup_id: None,
            extension_headers: extensions,
            payload_length: 0,
            ..object()
        };
        let mut encoded = BytesMut::new();
        FetchRecord::Object(expected.clone())
            .encode(&mut encoded)
            .unwrap();
        assert_eq!(
            decode(&mut FetchRecordDecoder::default(), &encoded),
            FetchRecord::Object(expected)
        );
    }

    #[test]
    fn first_object_cannot_reference_previous_fields() {
        let mut encoded = BytesMut::new();
        encoded.put_u8(0);
        assert!(matches!(
            FetchRecordDecoder::default().decode(&mut encoded),
            Err(DecodeError::InvalidValue)
        ));
    }

    #[test]
    fn undefined_serialization_flags_are_rejected() {
        for flags in [0x80_u64, 0x8d, 0x10d] {
            let mut encoded = BytesMut::new();
            flags.encode(&mut encoded).unwrap();
            assert!(matches!(
                FetchRecordDecoder::default().decode(&mut encoded),
                Err(DecodeError::InvalidValue)
            ));
        }
    }

    #[test]
    fn inherited_increment_stays_within_varint() {
        let mut first = object();
        first.object_id = VarInt::MAX.into_inner();
        first.subgroup_id = Some(VarInt::MAX.into_inner());
        let mut encoded = BytesMut::new();
        FetchRecord::Object(first).encode(&mut encoded).unwrap();

        let mut decoder = FetchRecordDecoder::default();
        decoder.decode(&mut encoded).unwrap();
        for flags in [0_u8, 2] {
            let mut next = BytesMut::new();
            next.put_u8(flags);
            assert!(matches!(
                decoder.decode(&mut next),
                Err(DecodeError::InvalidValue)
            ));
        }
    }

    #[test]
    fn failed_decode_does_not_advance_previous_state() {
        let mut encoder = FetchRecordEncoder::default();
        let mut first = BytesMut::new();
        encoder
            .encode(&FetchRecord::Object(object()), &mut first)
            .unwrap();
        let mut decoder = FetchRecordDecoder::default();
        decoder.decode(&mut first).unwrap();

        let mut truncated = BytesMut::from(&[0x03][..]);
        assert!(matches!(
            decoder.decode(&mut truncated),
            Err(DecodeError::More(_))
        ));

        let mut next = BytesMut::from(&[0x01, 0x00][..]);
        assert_eq!(
            decoder.decode(&mut next).unwrap(),
            FetchRecord::Object(FetchRecordObject {
                group_id: 3,
                subgroup_id: Some(2),
                object_id: 8,
                publisher_priority: 5,
                extension_headers: ExtensionHeaders::default(),
                payload_length: 0,
            })
        );
    }
}
