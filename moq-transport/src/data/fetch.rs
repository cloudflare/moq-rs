// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs, Location};
use crate::data::{ExtensionHeaders, ObjectStatus, StreamHeaderType};
use crate::message::GroupOrder;

/// Stream header for a FETCH data stream (draft-18 §11.4.4).
///
/// A publisher opens a **unidirectional** stream per FETCH response.  The
/// stream begins with this header, which ties it back to the originating FETCH
/// request via [`request_id`].  All subsequent bytes are zero or more
/// FETCH object records (see [`FetchRecord`]), each followed by its payload.
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

/// Legacy fixed-layout FETCH object, retained for source compatibility.
///
/// This is **not** the draft-18 FETCH object encoding. Draft-18 §11.4.4
/// serializes FETCH objects with Serialization Flags and delta-encoded Group
/// and Object IDs; use [`FetchRecord`] with the stateful
/// [`FetchRecordDecoder`] / [`FetchRecordEncoder`] for that.
///
/// # Payload contract — callers must consume payload bytes
///
/// `FetchObject` encodes only the *header* fields.  The object payload
/// (`payload_length` raw bytes) immediately follows the encoded header and is
/// **not** consumed by [`Decode`] or produced by [`Encode`].  For status-only
/// objects (`payload_length == 0`) the single-byte [`ObjectStatus`] *is*
/// consumed by `decode`.
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

// ── Draft-18 FETCH object records (§11.4.4) ─────────────────────────────────

const SUBGROUP_MASK: u64 = 0x03;
const OBJECT_ID_PRESENT: u64 = 0x04;
const GROUP_ID_PRESENT: u64 = 0x08;
const PRIORITY_PRESENT: u64 = 0x10;
const PROPERTIES_PRESENT: u64 = 0x20;
const DATAGRAM: u64 = 0x40;
/// End of Non-Existent Range (§11.4.4, Table 7).
const END_NOT_EXIST: u64 = 0x8c;
/// End of Unknown Range (§11.4.4, Table 7).
const END_UNKNOWN: u64 = 0x10c;

/// Upper bound on one encoded record header (everything before the payload).
/// Properties are length-prefixed, so a peer could otherwise make a receiver
/// buffer an arbitrary amount before the record can be decoded.
pub(crate) const MAX_FETCH_RECORD_HEADER_SIZE: usize = 1024 * 1024;

/// Metadata for one Object on a FETCH stream (draft-18 §11.4.4).
///
/// The payload immediately follows the encoded record and is read or written
/// separately so callers can apply backpressure without buffering a whole
/// Object. Draft-18 FETCH objects carry no Object Status field; absent Objects
/// are conveyed as gaps or End of Range records.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FetchRecordObject {
    pub group_id: u64,
    /// `None` preserves the Datagram forwarding preference (flag 0x40).
    pub subgroup_id: Option<u64>,
    pub object_id: u64,
    pub publisher_priority: u8,
    /// Object Properties (§11.2.1.2).
    pub extension_headers: ExtensionHeaders,
    pub payload_length: u64,
}

impl FetchRecordObject {
    pub fn location(&self) -> Location {
        Location::new(self.group_id, self.object_id)
    }
}

/// One semantic record on a FETCH stream.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FetchRecord {
    Object(FetchRecordObject),
    /// Inclusive final Location of a range of Objects that do not exist.
    NotExist {
        end: Location,
    },
    /// Inclusive final Location of a range of Objects with unknown status.
    Unknown {
        end: Location,
    },
}

impl FetchRecord {
    /// The Location this record ends at (the Object itself, or the inclusive
    /// end of the range).
    pub fn location(&self) -> Location {
        match self {
            Self::Object(object) => object.location(),
            Self::NotExist { end } | Self::Unknown { end } => *end,
        }
    }
}

/// The "prior Object" state that draft-18 §11.4.4.1/§11.4.4.2 compression
/// refers to.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct PriorState {
    /// Prior Group/Object ID: from the last Object *or* End of Range record.
    location: Option<Location>,
    /// Prior Subgroup ID: from the last actual Object only. `Some(None)` means
    /// the last Object was a Datagram, which has no Subgroup ID to inherit.
    subgroup_id: Option<Option<u64>>,
    /// Prior Priority: from the last actual Object only.
    priority: Option<u8>,
}

impl PriorState {
    fn observe(&mut self, record: &FetchRecord) {
        match record {
            FetchRecord::Object(object) => {
                self.location = Some(object.location());
                self.subgroup_id = Some(object.subgroup_id);
                self.priority = Some(object.publisher_priority);
            }
            FetchRecord::NotExist { end } | FetchRecord::Unknown { end } => {
                self.location = Some(*end);
            }
        }
    }

    fn prior_subgroup(&self) -> Result<u64, DecodeError> {
        self.subgroup_id.flatten().ok_or(DecodeError::InvalidValue)
    }
}

/// The effective group order of a FETCH response: draft-18 §10.2.8 defaults an
/// omitted GROUP_ORDER on FETCH to Ascending.
pub(crate) fn effective_fetch_order(order: Option<GroupOrder>) -> GroupOrder {
    match order {
        Some(GroupOrder::Descending) => GroupOrder::Descending,
        Some(GroupOrder::Ascending) | Some(GroupOrder::Publisher) | None => GroupOrder::Ascending,
    }
}

/// Stateful decoder for consecutive records on one FETCH stream.
///
/// Group ID deltas are signed by the response's group order (§11.4.4.1), so the
/// decoder must know it. The caller must consume each Object payload before
/// decoding the next record. A failed decode leaves the state untouched, so a
/// partial buffer can be retried once more bytes arrive.
#[derive(Clone, Debug)]
pub(crate) struct FetchRecordDecoder {
    order: GroupOrder,
    prior: PriorState,
}

impl FetchRecordDecoder {
    pub fn new(order: Option<GroupOrder>) -> Self {
        Self {
            order: effective_fetch_order(order),
            prior: PriorState::default(),
        }
    }

    pub fn decode<R: bytes::Buf>(&mut self, r: &mut R) -> Result<FetchRecord, DecodeError> {
        let flags = u64::decode(r)?;
        let record = match flags {
            END_NOT_EXIST => FetchRecord::NotExist {
                end: Self::decode_range_end(r)?,
            },
            END_UNKNOWN => FetchRecord::Unknown {
                end: Self::decode_range_end(r)?,
            },
            // §11.4.4: any other value >= 128 is a PROTOCOL_VIOLATION.
            0x80.. => return Err(DecodeError::InvalidValue),
            _ => FetchRecord::Object(self.decode_object(flags, r)?),
        };
        self.prior.observe(&record);
        Ok(record)
    }

    /// End of Range carries the Group and Object ID as absolute values.
    ///
    /// Draft-18 §11.4.4.2 says only that "the Group ID and Object ID fields are
    /// present"; it does not apply the Object-record delta rules to them.
    fn decode_range_end<R: bytes::Buf>(r: &mut R) -> Result<Location, DecodeError> {
        Ok(Location::new(u64::decode(r)?, u64::decode(r)?))
    }

    fn decode_object<R: bytes::Buf>(
        &self,
        flags: u64,
        r: &mut R,
    ) -> Result<FetchRecordObject, DecodeError> {
        let prior = self.prior;

        // §11.4.4.1: the first record must carry both IDs; flags that reference
        // a prior Object are a PROTOCOL_VIOLATION when there is none.
        let group_present = flags & GROUP_ID_PRESENT != 0;
        let group_id = match (group_present, prior.location) {
            (true, None) => u64::decode(r)?,
            (true, Some(prior)) => {
                let step = u64::decode(r)?
                    .checked_add(1)
                    .ok_or(DecodeError::InvalidValue)?;
                match self.order {
                    GroupOrder::Descending => prior.group_id.checked_sub(step),
                    _ => prior.group_id.checked_add(step),
                }
                .ok_or(DecodeError::InvalidValue)?
            }
            (false, Some(prior)) => prior.group_id,
            (false, None) => return Err(DecodeError::InvalidValue),
        };

        let subgroup_id = if flags & DATAGRAM != 0 {
            // §11.4.4.1: subscribers MUST ignore the two low bits for Datagrams.
            None
        } else {
            Some(match flags & SUBGROUP_MASK {
                0 => 0,
                1 => prior.prior_subgroup()?,
                2 => prior
                    .prior_subgroup()?
                    .checked_add(1)
                    .ok_or(DecodeError::InvalidValue)?,
                _ => u64::decode(r)?,
            })
        };

        let object_present = flags & OBJECT_ID_PRESENT != 0;
        let object_id = match (object_present, prior.location) {
            (true, None) => u64::decode(r)?,
            // With a Group ID Delta, the Object ID Delta is the absolute ID.
            (true, Some(_)) if group_present => u64::decode(r)?,
            // Without one, it is added to the prior Object ID (§11.4.4.1).
            (true, Some(prior)) => prior
                .object_id
                .checked_add(u64::decode(r)?)
                .ok_or(DecodeError::InvalidValue)?,
            // Absent: prior plus one, regardless of group.
            (false, Some(prior)) => prior
                .object_id
                .checked_add(1)
                .ok_or(DecodeError::InvalidValue)?,
            (false, None) => return Err(DecodeError::InvalidValue),
        };

        let publisher_priority = if flags & PRIORITY_PRESENT != 0 {
            u8::decode(r)?
        } else {
            prior.priority.ok_or(DecodeError::InvalidValue)?
        };
        let extension_headers = if flags & PROPERTIES_PRESENT != 0 {
            ExtensionHeaders::decode(r)?
        } else {
            ExtensionHeaders::default()
        };
        let payload_length = u64::decode(r)?;

        Ok(FetchRecordObject {
            group_id,
            subgroup_id,
            object_id,
            publisher_priority,
            extension_headers,
            payload_length,
        })
    }
}

/// Canonical stateful encoder for consecutive records on one FETCH stream.
///
/// Emits the most compact encoding the draft-18 rules allow. Records that
/// cannot be expressed in the response's group order (a group moving against
/// the order, or an Object ID going backwards within a group) are rejected
/// rather than silently re-encoded.
#[derive(Clone, Debug)]
pub(crate) struct FetchRecordEncoder {
    order: GroupOrder,
    prior: PriorState,
}

impl FetchRecordEncoder {
    pub fn new(order: Option<GroupOrder>) -> Self {
        Self {
            order: effective_fetch_order(order),
            prior: PriorState::default(),
        }
    }

    pub fn encode<W: bytes::BufMut>(
        &mut self,
        record: &FetchRecord,
        output: &mut W,
    ) -> Result<(), EncodeError> {
        let mut encoded = bytes::BytesMut::new();
        match record {
            FetchRecord::Object(object) => self.encode_object(object, &mut encoded)?,
            FetchRecord::NotExist { end } => encode_range_end(&mut encoded, END_NOT_EXIST, *end)?,
            FetchRecord::Unknown { end } => encode_range_end(&mut encoded, END_UNKNOWN, *end)?,
        }
        output.put_slice(&encoded);
        self.prior.observe(record);
        Ok(())
    }

    fn encode_object<W: bytes::BufMut>(
        &self,
        object: &FetchRecordObject,
        output: &mut W,
    ) -> Result<(), EncodeError> {
        let prior = self.prior;
        let mut flags = 0;

        // Group / Object IDs.
        let mut group_field = None;
        let mut object_field = None;
        match prior.location {
            None => {
                flags |= GROUP_ID_PRESENT | OBJECT_ID_PRESENT;
                group_field = Some(object.group_id);
                object_field = Some(object.object_id);
            }
            Some(prev) if prev.group_id == object.group_id => {
                if prev.object_id.checked_add(1) != Some(object.object_id) {
                    let delta = object
                        .object_id
                        .checked_sub(prev.object_id)
                        .ok_or(EncodeError::InvalidValue)?;
                    flags |= OBJECT_ID_PRESENT;
                    object_field = Some(delta);
                }
            }
            Some(prev) => {
                let distance = match self.order {
                    GroupOrder::Descending => prev.group_id.checked_sub(object.group_id),
                    _ => object.group_id.checked_sub(prev.group_id),
                }
                .ok_or(EncodeError::InvalidValue)?;
                flags |= GROUP_ID_PRESENT;
                // distance >= 1 because the groups differ and the direction matched.
                group_field = Some(distance - 1);
                if prev.object_id.checked_add(1) != Some(object.object_id) {
                    flags |= OBJECT_ID_PRESENT;
                    object_field = Some(object.object_id);
                }
            }
        }

        // Subgroup.
        let mut subgroup_field = None;
        match object.subgroup_id {
            None => flags |= DATAGRAM,
            Some(0) => {}
            Some(subgroup) => {
                let prev = prior.subgroup_id.flatten();
                if prev == Some(subgroup) {
                    flags |= 1;
                } else if prev.and_then(|p| p.checked_add(1)) == Some(subgroup) {
                    flags |= 2;
                } else {
                    flags |= 3;
                    subgroup_field = Some(subgroup);
                }
            }
        }

        if prior.priority != Some(object.publisher_priority) {
            flags |= PRIORITY_PRESENT;
        }
        if !object.extension_headers.is_empty() {
            flags |= PROPERTIES_PRESENT;
        }

        flags.encode(output)?;
        if let Some(group) = group_field {
            group.encode(output)?;
        }
        if let Some(subgroup) = subgroup_field {
            subgroup.encode(output)?;
        }
        if let Some(object_id) = object_field {
            object_id.encode(output)?;
        }
        if flags & PRIORITY_PRESENT != 0 {
            object.publisher_priority.encode(output)?;
        }
        if flags & PROPERTIES_PRESENT != 0 {
            object.extension_headers.encode(output)?;
        }
        object.payload_length.encode(output)
    }
}

fn encode_range_end<W: bytes::BufMut>(
    w: &mut W,
    serialization_flags: u64,
    end: Location,
) -> Result<(), EncodeError> {
    serialization_flags.encode(w)?;
    end.group_id.encode(w)?;
    end.object_id.encode(w)
}

#[cfg(test)]
mod record_tests {
    use bytes::{Buf, BufMut, BytesMut};

    use super::*;

    fn object(group_id: u64, subgroup_id: Option<u64>, object_id: u64) -> FetchRecordObject {
        FetchRecordObject {
            group_id,
            subgroup_id,
            object_id,
            publisher_priority: 5,
            extension_headers: ExtensionHeaders::default(),
            payload_length: 0,
        }
    }

    fn roundtrip(order: Option<GroupOrder>, records: &[FetchRecord]) -> BytesMut {
        let mut encoder = FetchRecordEncoder::new(order);
        let mut encoded = BytesMut::new();
        for record in records {
            encoder.encode(record, &mut encoded).unwrap();
        }
        let mut decoder = FetchRecordDecoder::new(order);
        let mut cursor = std::io::Cursor::new(encoded.as_ref());
        for record in records {
            assert_eq!(&decoder.decode(&mut cursor).unwrap(), record);
        }
        assert_eq!(cursor.position() as usize, encoded.len());
        encoded
    }

    #[test]
    fn first_object_is_absolute() {
        let encoded = roundtrip(None, &[FetchRecord::Object(object(7, Some(0), 3))]);
        // flags = GROUP|OBJECT|PRIORITY = 0x1c, group 7, object 3, priority 5, len 0
        assert_eq!(encoded.as_ref(), &[0x1c, 7, 3, 5, 0]);
    }

    #[test]
    fn ascending_group_delta_is_distance_minus_one() {
        let encoded = roundtrip(
            Some(GroupOrder::Ascending),
            &[
                FetchRecord::Object(object(2, Some(0), 0)),
                FetchRecord::Object(object(5, Some(0), 0)),
            ],
        );
        // second: flags GROUP|OBJECT (0x0c), group delta 2, object 0 (absolute), len 0
        assert_eq!(&encoded[5..], &[0x0c, 2, 0, 0]);
    }

    #[test]
    fn descending_group_delta_subtracts() {
        let encoded = roundtrip(
            Some(GroupOrder::Descending),
            &[
                FetchRecord::Object(object(5, Some(0), 0)),
                FetchRecord::Object(object(4, Some(0), 0)),
            ],
        );
        assert_eq!(&encoded[5..], &[0x0c, 0, 0, 0]);
    }

    #[test]
    fn group_moving_against_order_cannot_be_encoded() {
        let mut encoder = FetchRecordEncoder::new(Some(GroupOrder::Ascending));
        let mut out = BytesMut::new();
        encoder
            .encode(&FetchRecord::Object(object(5, Some(0), 0)), &mut out)
            .unwrap();
        assert!(encoder
            .encode(&FetchRecord::Object(object(4, Some(0), 0)), &mut out)
            .is_err());
    }

    #[test]
    fn same_group_object_delta_is_added_to_prior() {
        let encoded = roundtrip(
            None,
            &[
                FetchRecord::Object(object(1, Some(0), 0)),
                FetchRecord::Object(object(1, Some(0), 1)),
                FetchRecord::Object(object(1, Some(0), 4)),
            ],
        );
        // second: nothing present but length (flags 0x00); third: OBJECT delta 3.
        assert_eq!(&encoded[5..], &[0x00, 0, 0x04, 3, 0]);
    }

    #[test]
    fn object_id_increments_across_groups_when_absent() {
        let mut decoder = FetchRecordDecoder::new(None);
        let mut wire = BytesMut::new();
        wire.put_slice(&[0x1c, 1, 9, 5, 0]); // group 1 object 9
        wire.put_slice(&[0x08, 0, 0]); // group delta 0 => group 2, object 10
        decoder.decode(&mut wire).unwrap();
        assert_eq!(
            decoder.decode(&mut wire).unwrap(),
            FetchRecord::Object(object(2, Some(0), 10))
        );
    }

    #[test]
    fn subgroup_modes_round_trip() {
        roundtrip(
            None,
            &[
                FetchRecord::Object(object(0, Some(3), 0)),
                FetchRecord::Object(object(0, Some(3), 1)),
                FetchRecord::Object(object(0, Some(4), 2)),
                FetchRecord::Object(object(0, Some(9), 3)),
                FetchRecord::Object(object(0, Some(0), 4)),
                FetchRecord::Object(object(0, None, 5)),
            ],
        );
    }

    #[test]
    fn range_markers_are_absolute_and_reset_prior_location_only() {
        let records = [
            FetchRecord::Object(object(0, Some(2), 0)),
            FetchRecord::Unknown {
                end: Location::new(3, 7),
            },
            // Prior location is (3,7) from the marker; subgroup/priority from
            // the last real Object.
            FetchRecord::Object(object(3, Some(2), 8)),
            FetchRecord::NotExist {
                end: Location::new(4, 2),
            },
        ];
        let encoded = roundtrip(None, &records);
        // Object after the marker: same group, object = prior+1, subgroup =
        // prior (mode 1), priority inherited: just flags 0x01 and length 0.
        let mut marker = BytesMut::new();
        encode_range_end(&mut marker, END_UNKNOWN, Location::new(3, 7)).unwrap();
        // first object: flags(0x1f) group subgroup object priority len = 6 bytes
        let start = 6 + marker.len();
        assert_eq!(&encoded[start..start + 2], &[0x01, 0]);
    }

    #[test]
    fn marker_before_first_object_allows_relative_ids_but_not_inherited_fields() {
        let mut decoder = FetchRecordDecoder::new(None);
        let mut wire = BytesMut::new();
        encode_range_end(&mut wire, END_NOT_EXIST, Location::new(2, 4)).unwrap();
        decoder.decode(&mut wire).unwrap();
        // Next: object id absent (5), group absent (2), priority present, len 0.
        let mut ok = BytesMut::from(&[0x10u8, 9, 0][..]);
        assert_eq!(
            decoder.clone().decode(&mut ok).unwrap(),
            FetchRecord::Object(FetchRecordObject {
                publisher_priority: 9,
                ..object(2, Some(0), 5)
            })
        );
        // Inheriting priority with no prior Object is a violation.
        let mut bad = BytesMut::from(&[0x00u8, 0][..]);
        assert!(matches!(
            decoder.clone().decode(&mut bad),
            Err(DecodeError::InvalidValue)
        ));
        // So is inheriting a subgroup.
        let mut bad = BytesMut::from(&[0x11u8, 9, 0][..]);
        assert!(matches!(
            decoder.decode(&mut bad),
            Err(DecodeError::InvalidValue)
        ));
    }

    #[test]
    fn first_object_without_ids_is_rejected() {
        for flags in [0x00u8, 0x04, 0x08, 0x14, 0x18] {
            let mut wire = BytesMut::new();
            wire.put_u8(flags);
            wire.put_slice(&[1, 1, 5, 0]);
            assert!(
                matches!(
                    FetchRecordDecoder::new(None).decode(&mut wire),
                    Err(DecodeError::InvalidValue)
                ),
                "flags {flags:#x}"
            );
        }
    }

    #[test]
    fn group_delta_wrap_is_rejected() {
        let mut decoder = FetchRecordDecoder::new(Some(GroupOrder::Descending));
        let mut wire = BytesMut::from(&[0x1cu8, 1, 0, 5, 0][..]);
        decoder.decode(&mut wire).unwrap();
        // group 1 - (1 + 1) < 0
        let mut wire = BytesMut::from(&[0x0cu8, 1, 0, 0][..]);
        assert!(matches!(
            decoder.decode(&mut wire),
            Err(DecodeError::InvalidValue)
        ));
    }

    #[test]
    fn undefined_serialization_flags_are_rejected() {
        for flags in [0x80_u64, 0x8d, 0x10d, 0x20c] {
            let mut encoded = BytesMut::new();
            flags.encode(&mut encoded).unwrap();
            encoded.put_slice(&[0, 0]);
            assert!(matches!(
                FetchRecordDecoder::new(None).decode(&mut encoded),
                Err(DecodeError::InvalidValue)
            ));
        }
    }

    #[test]
    fn datagram_ignores_subgroup_bits() {
        for bits in 0..=SUBGROUP_MASK as u8 {
            let mut wire = BytesMut::new();
            wire.put_u8(0x5c | bits);
            wire.put_slice(&[3, 7, 5, 0]);
            assert_eq!(
                FetchRecordDecoder::new(None).decode(&mut wire).unwrap(),
                FetchRecord::Object(object(3, None, 7))
            );
        }
    }

    #[test]
    fn properties_round_trip() {
        let mut properties = ExtensionHeaders::new();
        properties.set_intvalue(2, 9);
        roundtrip(
            None,
            &[FetchRecord::Object(FetchRecordObject {
                extension_headers: properties,
                payload_length: 12,
                ..object(1, Some(1), 1)
            })],
        );
    }

    #[test]
    fn every_prefix_is_retryable_without_state_change() {
        let records = [
            FetchRecord::Object(object(64, Some(65), 16_384)),
            FetchRecord::Object(object(64, Some(66), 16_390)),
        ];
        let mut encoder = FetchRecordEncoder::new(None);
        let mut first = BytesMut::new();
        encoder.encode(&records[0], &mut first).unwrap();
        let mut second = BytesMut::new();
        encoder.encode(&records[1], &mut second).unwrap();

        let mut decoder = FetchRecordDecoder::new(None);
        decoder.decode(&mut first).unwrap();
        for prefix_len in 0..second.len() {
            let mut prefix = std::io::Cursor::new(&second[..prefix_len]);
            assert!(matches!(
                decoder.decode(&mut prefix),
                Err(DecodeError::More(_))
            ));
        }
        assert_eq!(decoder.decode(&mut second).unwrap(), records[1]);
        assert!(!second.has_remaining());
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
        if let Ok(decoded_b) = FetchObject::decode(&mut buf) {
            assert_ne!(
                decoded_b, obj_b,
                "consuming payload bytes before decode is required for correct stream sync"
            );
        }
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
