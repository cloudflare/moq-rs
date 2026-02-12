use crate::coding::{Decode, DecodeError, Encode, EncodeError};
use crate::data::{ExtensionHeaders, ObjectStatus};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatagramType {
    // Payload types with Priority Present (0x00-0x07)
    ObjectIdPayload = 0x00,
    ObjectIdPayloadExt = 0x01,
    ObjectIdPayloadEndOfGroup = 0x02,
    ObjectIdPayloadExtEndOfGroup = 0x03,
    Payload = 0x04,
    PayloadExt = 0x05,
    PayloadEndOfGroup = 0x06,
    PayloadExtEndOfGroup = 0x07,
    // Payload types with Priority Not Present (0x08-0x0F)
    ObjectIdPayloadNoPriority = 0x08,
    ObjectIdPayloadExtNoPriority = 0x09,
    ObjectIdPayloadEndOfGroupNoPriority = 0x0a,
    ObjectIdPayloadExtEndOfGroupNoPriority = 0x0b,
    PayloadNoPriority = 0x0c,
    PayloadExtNoPriority = 0x0d,
    PayloadEndOfGroupNoPriority = 0x0e,
    PayloadExtEndOfGroupNoPriority = 0x0f,
    // Status types with Priority Present (0x20-0x25)
    ObjectIdStatus = 0x20,
    ObjectIdStatusExt = 0x21,
    Status = 0x24,
    StatusExt = 0x25,
    // Status types with Priority Not Present (0x28-0x2D)
    ObjectIdStatusNoPriority = 0x28,
    ObjectIdStatusExtNoPriority = 0x29,
    StatusNoPriority = 0x2c,
    StatusExtNoPriority = 0x2d,
}

impl DatagramType {
    /// Returns true if this datagram type has the Object ID field present
    pub fn has_object_id(&self) -> bool {
        matches!(
            *self,
            DatagramType::ObjectIdPayload
                | DatagramType::ObjectIdPayloadExt
                | DatagramType::ObjectIdPayloadEndOfGroup
                | DatagramType::ObjectIdPayloadExtEndOfGroup
                | DatagramType::ObjectIdPayloadNoPriority
                | DatagramType::ObjectIdPayloadExtNoPriority
                | DatagramType::ObjectIdPayloadEndOfGroupNoPriority
                | DatagramType::ObjectIdPayloadExtEndOfGroupNoPriority
                | DatagramType::ObjectIdStatus
                | DatagramType::ObjectIdStatusExt
                | DatagramType::ObjectIdStatusNoPriority
                | DatagramType::ObjectIdStatusExtNoPriority
        )
    }

    /// Returns true if this datagram type has the Publisher Priority field present
    pub fn has_priority(&self) -> bool {
        matches!(
            *self,
            DatagramType::ObjectIdPayload
                | DatagramType::ObjectIdPayloadExt
                | DatagramType::ObjectIdPayloadEndOfGroup
                | DatagramType::ObjectIdPayloadExtEndOfGroup
                | DatagramType::Payload
                | DatagramType::PayloadExt
                | DatagramType::PayloadEndOfGroup
                | DatagramType::PayloadExtEndOfGroup
                | DatagramType::ObjectIdStatus
                | DatagramType::ObjectIdStatusExt
                | DatagramType::Status
                | DatagramType::StatusExt
        )
    }

    /// Returns true if this datagram type has extension headers
    pub fn has_extensions(&self) -> bool {
        matches!(
            *self,
            DatagramType::ObjectIdPayloadExt
                | DatagramType::ObjectIdPayloadExtEndOfGroup
                | DatagramType::PayloadExt
                | DatagramType::PayloadExtEndOfGroup
                | DatagramType::ObjectIdPayloadExtNoPriority
                | DatagramType::ObjectIdPayloadExtEndOfGroupNoPriority
                | DatagramType::PayloadExtNoPriority
                | DatagramType::PayloadExtEndOfGroupNoPriority
                | DatagramType::ObjectIdStatusExt
                | DatagramType::StatusExt
                | DatagramType::ObjectIdStatusExtNoPriority
                | DatagramType::StatusExtNoPriority
        )
    }

    /// Returns true if this is a status datagram (no payload)
    pub fn is_status(&self) -> bool {
        matches!(
            *self,
            DatagramType::ObjectIdStatus
                | DatagramType::ObjectIdStatusExt
                | DatagramType::Status
                | DatagramType::StatusExt
                | DatagramType::ObjectIdStatusNoPriority
                | DatagramType::ObjectIdStatusExtNoPriority
                | DatagramType::StatusNoPriority
                | DatagramType::StatusExtNoPriority
        )
    }

    /// Returns true if this is a payload datagram
    pub fn is_payload(&self) -> bool {
        !self.is_status()
    }
}

impl Decode for DatagramType {
    fn decode<B: bytes::Buf>(r: &mut B) -> Result<Self, DecodeError> {
        match u64::decode(r)? {
            // Payload types with Priority Present (0x00-0x07)
            0x00 => Ok(Self::ObjectIdPayload),
            0x01 => Ok(Self::ObjectIdPayloadExt),
            0x02 => Ok(Self::ObjectIdPayloadEndOfGroup),
            0x03 => Ok(Self::ObjectIdPayloadExtEndOfGroup),
            0x04 => Ok(Self::Payload),
            0x05 => Ok(Self::PayloadExt),
            0x06 => Ok(Self::PayloadEndOfGroup),
            0x07 => Ok(Self::PayloadExtEndOfGroup),
            // Payload types with Priority Not Present (0x08-0x0F)
            0x08 => Ok(Self::ObjectIdPayloadNoPriority),
            0x09 => Ok(Self::ObjectIdPayloadExtNoPriority),
            0x0a => Ok(Self::ObjectIdPayloadEndOfGroupNoPriority),
            0x0b => Ok(Self::ObjectIdPayloadExtEndOfGroupNoPriority),
            0x0c => Ok(Self::PayloadNoPriority),
            0x0d => Ok(Self::PayloadExtNoPriority),
            0x0e => Ok(Self::PayloadEndOfGroupNoPriority),
            0x0f => Ok(Self::PayloadExtEndOfGroupNoPriority),
            // Status types with Priority Present (0x20-0x25)
            0x20 => Ok(Self::ObjectIdStatus),
            0x21 => Ok(Self::ObjectIdStatusExt),
            0x24 => Ok(Self::Status),
            0x25 => Ok(Self::StatusExt),
            // Status types with Priority Not Present (0x28-0x2D)
            0x28 => Ok(Self::ObjectIdStatusNoPriority),
            0x29 => Ok(Self::ObjectIdStatusExtNoPriority),
            0x2c => Ok(Self::StatusNoPriority),
            0x2d => Ok(Self::StatusExtNoPriority),
            _ => Err(DecodeError::InvalidDatagramType),
        }
    }
}

impl Encode for DatagramType {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        let val = *self as u64;
        val.encode(w)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Datagram {
    /// The type of this datagram object
    pub datagram_type: DatagramType,

    /// The track alias.
    pub track_alias: u64,

    /// The sequence number within the track.
    pub group_id: u64,

    /// The object ID within the group.
    pub object_id: Option<u64>,

    /// Publisher priority, where **smaller** values are sent first.
    /// Optional when using NoPriority datagram types (0x08-0x0F, 0x28-0x2D).
    pub publisher_priority: Option<u8>,

    /// Optional extension headers for types with extensions
    pub extension_headers: Option<ExtensionHeaders>,

    /// The Object Status.
    pub status: Option<ObjectStatus>,

    /// The payload.
    pub payload: Option<bytes::Bytes>,
}

impl Decode for Datagram {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let datagram_type = DatagramType::decode(r)?;
        let track_alias = u64::decode(r)?;
        let group_id = u64::decode(r)?;

        // Decode Object Id if required
        let object_id = if datagram_type.has_object_id() {
            Some(u64::decode(r)?)
        } else {
            None
        };

        // Decode Publisher Priority if required
        let publisher_priority = if datagram_type.has_priority() {
            Some(u8::decode(r)?)
        } else {
            None
        };

        // Decode Extension Headers if required
        let extension_headers = if datagram_type.has_extensions() {
            Some(ExtensionHeaders::decode(r)?)
        } else {
            None
        };

        // Decode Status if required (for status datagram types)
        let status = if datagram_type.is_status() {
            Some(ObjectStatus::decode(r)?)
        } else {
            None
        };

        // Decode Payload if required (for payload datagram types)
        let payload = if datagram_type.is_payload() {
            Some(r.copy_to_bytes(r.remaining()))
        } else {
            None
        };

        Ok(Self {
            datagram_type,
            track_alias,
            group_id,
            object_id,
            publisher_priority,
            extension_headers,
            status,
            payload,
        })
    }
}

impl Encode for Datagram {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.datagram_type.encode(w)?;
        self.track_alias.encode(w)?;
        self.group_id.encode(w)?;

        // Encode Object Id if required
        if self.datagram_type.has_object_id() {
            if let Some(object_id) = &self.object_id {
                object_id.encode(w)?;
            } else {
                return Err(EncodeError::MissingField("ObjectId".to_string()));
            }
        }

        // Encode Publisher Priority if required
        if self.datagram_type.has_priority() {
            if let Some(publisher_priority) = &self.publisher_priority {
                publisher_priority.encode(w)?;
            } else {
                return Err(EncodeError::MissingField("PublisherPriority".to_string()));
            }
        }

        // Encode Extension Headers if required
        if self.datagram_type.has_extensions() {
            if let Some(extension_headers) = &self.extension_headers {
                extension_headers.encode(w)?;
            } else {
                return Err(EncodeError::MissingField("ExtensionHeaders".to_string()));
            }
        }

        // Encode Status if required (for status datagram types)
        if self.datagram_type.is_status() {
            if let Some(status) = &self.status {
                status.encode(w)?;
            } else {
                return Err(EncodeError::MissingField("Status".to_string()));
            }
        }

        // Encode Payload if required (for payload datagram types)
        if self.datagram_type.is_payload() {
            if let Some(payload) = &self.payload {
                Self::encode_remaining(w, payload.len())?;
                w.put_slice(payload);
            } else {
                return Err(EncodeError::MissingField("Payload".to_string()));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use bytes::BytesMut;

    #[test]
    fn encode_decode_datagram_type() {
        let mut buf = BytesMut::new();

        let dt = DatagramType::ObjectIdPayload;
        dt.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x00]);
        let decoded = DatagramType::decode(&mut buf).unwrap();
        assert_eq!(decoded, dt);

        let dt = DatagramType::ObjectIdPayloadExt;
        dt.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x01]);
        let decoded = DatagramType::decode(&mut buf).unwrap();
        assert_eq!(decoded, dt);

        let dt = DatagramType::ObjectIdPayloadEndOfGroup;
        dt.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x02]);
        let decoded = DatagramType::decode(&mut buf).unwrap();
        assert_eq!(decoded, dt);

        let dt = DatagramType::ObjectIdPayloadExtEndOfGroup;
        dt.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x03]);
        let decoded = DatagramType::decode(&mut buf).unwrap();
        assert_eq!(decoded, dt);

        let dt = DatagramType::Payload;
        dt.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x04]);
        let decoded = DatagramType::decode(&mut buf).unwrap();
        assert_eq!(decoded, dt);

        let dt = DatagramType::PayloadExt;
        dt.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x05]);
        let decoded = DatagramType::decode(&mut buf).unwrap();
        assert_eq!(decoded, dt);

        let dt = DatagramType::PayloadEndOfGroup;
        dt.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x06]);
        let decoded = DatagramType::decode(&mut buf).unwrap();
        assert_eq!(decoded, dt);

        let dt = DatagramType::PayloadExtEndOfGroup;
        dt.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x07]);
        let decoded = DatagramType::decode(&mut buf).unwrap();
        assert_eq!(decoded, dt);

        let dt = DatagramType::ObjectIdStatus;
        dt.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x20]);
        let decoded = DatagramType::decode(&mut buf).unwrap();
        assert_eq!(decoded, dt);

        let dt = DatagramType::ObjectIdStatusExt;
        dt.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x21]);
        let decoded = DatagramType::decode(&mut buf).unwrap();
        assert_eq!(decoded, dt);
    }

    #[test]
    fn encode_decode_datagram() {
        let mut buf = BytesMut::new();

        // One ExtensionHeader for testing
        let mut ext_hdrs = ExtensionHeaders::new();
        ext_hdrs.set_bytesvalue(123, vec![0x00, 0x01, 0x02, 0x03]);

        // DatagramType = ObjectIdPayload
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdPayload,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: Some(127),
            extension_headers: None,
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Type(1)+Alias(1)+GroupId(1)+ObjectId(2)+Priority(1)+Payload(7) = 13
        assert_eq!(13, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = ObjectIdPayloadExt
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdPayloadExt,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: Some(127),
            extension_headers: Some(ext_hdrs.clone()),
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Same as above plus NumExt(1),ExtensionKey(2),ExtensionValueLen(1),ExtensionValue(4) = 13 + 8 = 21
        assert_eq!(21, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = ObjectIdPayloadEndOfGroup
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdPayloadEndOfGroup,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: Some(127),
            extension_headers: None,
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Type(1)+Alias(1)+GroupId(1)+ObjectId(2)+Priority(1)+Payload(7) = 13
        assert_eq!(13, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = ObjectIdPayloadExtEndOfGroup
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdPayloadExtEndOfGroup,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: Some(127),
            extension_headers: Some(ext_hdrs.clone()),
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Same as above plus NumExt(1),ExtensionKey(2),ExtensionValueLen(1),ExtensionValue(4) = 13 + 8 = 21
        assert_eq!(21, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = ObjectIdStatus
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdStatus,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: Some(127),
            extension_headers: None,
            status: Some(ObjectStatus::EndOfTrack),
            payload: None,
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Type(1)+Alias(1)+GroupId(1)+ObjectId(2)+Priority(1)+Status(1) = 7
        assert_eq!(7, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = ObjectIdStatusExt
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdStatusExt,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: Some(127),
            extension_headers: Some(ext_hdrs.clone()),
            status: Some(ObjectStatus::EndOfTrack),
            payload: None,
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Same as above plus NumExt(1),ExtensionKey(2),ExtensionValueLen(1),ExtensionValue(4) = 7 + 8 = 15
        assert_eq!(15, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = Payload
        let msg = Datagram {
            datagram_type: DatagramType::Payload,
            track_alias: 12,
            group_id: 10,
            object_id: None,
            publisher_priority: Some(127),
            extension_headers: None,
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Type(1)+Alias(1)+GroupId(1)+Priority(1)+Payload(7) = 11
        assert_eq!(11, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = PayloadExt
        let msg = Datagram {
            datagram_type: DatagramType::PayloadExt,
            track_alias: 12,
            group_id: 10,
            object_id: None,
            publisher_priority: Some(127),
            extension_headers: Some(ext_hdrs.clone()),
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Same as above plus NumExt(1),ExtensionKey(2),ExtensionValueLen(1),ExtensionValue(4) = 11 + 8 = 19
        assert_eq!(19, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = PayloadEndOfGroup
        let msg = Datagram {
            datagram_type: DatagramType::PayloadEndOfGroup,
            track_alias: 12,
            group_id: 10,
            object_id: None,
            publisher_priority: Some(127),
            extension_headers: None,
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Type(1)+Alias(1)+GroupId(1)+Priority(1)+Payload(7) = 11
        assert_eq!(11, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = PayloadExtEndOfGroup
        let msg = Datagram {
            datagram_type: DatagramType::PayloadExtEndOfGroup,
            track_alias: 12,
            group_id: 10,
            object_id: None,
            publisher_priority: Some(127),
            extension_headers: Some(ext_hdrs.clone()),
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Same as above plus NumExt(1),ExtensionKey(2),ExtensionValueLen(1),ExtensionValue(4) = 11 + 8 = 19
        assert_eq!(19, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = ObjectIdPayloadNoPriority (no priority field)
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdPayloadNoPriority,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: None,
            extension_headers: None,
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Type(1)+Alias(1)+GroupId(1)+ObjectId(2)+Payload(7) = 12 (no priority)
        assert_eq!(12, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = PayloadNoPriority (no priority field, no object id)
        let msg = Datagram {
            datagram_type: DatagramType::PayloadNoPriority,
            track_alias: 12,
            group_id: 10,
            object_id: None,
            publisher_priority: None,
            extension_headers: None,
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Type(1)+Alias(1)+GroupId(1)+Payload(7) = 10 (no priority, no object id)
        assert_eq!(10, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_datagram_missing_fields() {
        let mut buf = BytesMut::new();

        // DatagramType = ObjectIdPayloadExt - missing extensions
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdPayloadExt,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: Some(127),
            extension_headers: None,
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));

        // DatagramType = ObjectIdPayloadExtEndOfGroup - missing extensions
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdPayloadExtEndOfGroup,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: Some(127),
            extension_headers: None,
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));

        // DatagramType = ObjectIdPayloadExtEndOfGroup - missing extensions
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdPayloadExtEndOfGroup,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: Some(127),
            extension_headers: None,
            status: Some(ObjectStatus::EndOfTrack),
            payload: None,
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));

        // DatagramType = Payload - missing payload
        let msg = Datagram {
            datagram_type: DatagramType::Payload,
            track_alias: 12,
            group_id: 10,
            object_id: None,
            publisher_priority: Some(127),
            extension_headers: None,
            status: None,
            payload: None,
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));

        // DatagramType = ObjectIdStatus - missing status
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdStatus,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: Some(127),
            extension_headers: None,
            status: None,
            payload: None,
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));

        // DatagramType = ObjectIdPayload - missing priority (priority is required for this type)
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdPayload,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: None,
            extension_headers: None,
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));
    }
}
