// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError};
use crate::data::{ExtensionHeaders, ObjectStatus};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatagramType {
    ObjectIdPayload = 0x00,
    ObjectIdPayloadExt = 0x01,
    ObjectIdPayloadEndOfGroup = 0x02,
    ObjectIdPayloadExtEndOfGroup = 0x03,
    Payload = 0x04,
    PayloadExt = 0x05,
    PayloadEndOfGroup = 0x06,
    PayloadExtEndOfGroup = 0x07,
    ObjectIdStatus = 0x20,
    ObjectIdStatusExt = 0x21,
}

impl DatagramType {
    fn has_extension_headers(self) -> bool {
        matches!(
            self,
            Self::ObjectIdPayloadExt
                | Self::ObjectIdPayloadExtEndOfGroup
                | Self::PayloadExt
                | Self::PayloadExtEndOfGroup
                | Self::ObjectIdStatusExt
        )
    }

    fn has_status(self) -> bool {
        matches!(self, Self::ObjectIdStatus | Self::ObjectIdStatusExt)
    }
}

impl Decode for DatagramType {
    fn decode<B: bytes::Buf>(r: &mut B) -> Result<Self, DecodeError> {
        match u64::decode(r)? {
            0x00 => Ok(Self::ObjectIdPayload),
            0x01 => Ok(Self::ObjectIdPayloadExt),
            0x02 => Ok(Self::ObjectIdPayloadEndOfGroup),
            0x03 => Ok(Self::ObjectIdPayloadExtEndOfGroup),
            0x04 => Ok(Self::Payload),
            0x05 => Ok(Self::PayloadExt),
            0x06 => Ok(Self::PayloadEndOfGroup),
            0x07 => Ok(Self::PayloadExtEndOfGroup),
            0x20 => Ok(Self::ObjectIdStatus),
            0x21 => Ok(Self::ObjectIdStatusExt),
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
    pub publisher_priority: u8,

    /// Optional extension headers if type is 0x1 (NoEndOfGroupWithExtensions) or 0x3 (EndofGroupWithExtensions)
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
        let object_id = match datagram_type {
            DatagramType::ObjectIdPayload
            | DatagramType::ObjectIdPayloadExt
            | DatagramType::ObjectIdPayloadEndOfGroup
            | DatagramType::ObjectIdPayloadExtEndOfGroup
            | DatagramType::ObjectIdStatus
            | DatagramType::ObjectIdStatusExt => Some(u64::decode(r)?),
            _ => None,
        };

        let publisher_priority = u8::decode(r)?;

        // Decode Extension Headers if required
        let extension_headers = if datagram_type.has_extension_headers() {
            let headers = ExtensionHeaders::decode(r)?;
            if headers.is_empty() {
                return Err(DecodeError::InvalidValue);
            }
            Some(headers)
        } else {
            None
        };

        // Decode Status if required
        let status = if datagram_type.has_status() {
            Some(ObjectStatus::decode(r)?)
        } else {
            None
        };

        if status.is_some_and(|status| status != ObjectStatus::NormalObject)
            && extension_headers.is_some()
        {
            return Err(DecodeError::InvalidValue);
        }

        // Decode Payload if required
        let payload = match datagram_type {
            DatagramType::ObjectIdPayload
            | DatagramType::ObjectIdPayloadExt
            | DatagramType::ObjectIdPayloadEndOfGroup
            | DatagramType::ObjectIdPayloadExtEndOfGroup
            | DatagramType::Payload
            | DatagramType::PayloadExt
            | DatagramType::PayloadEndOfGroup
            | DatagramType::PayloadExtEndOfGroup => Some(r.copy_to_bytes(r.remaining())),
            _ => None,
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
        match self.datagram_type {
            DatagramType::ObjectIdPayload
            | DatagramType::ObjectIdPayloadExt
            | DatagramType::ObjectIdPayloadEndOfGroup
            | DatagramType::ObjectIdPayloadExtEndOfGroup
            | DatagramType::ObjectIdStatus
            | DatagramType::ObjectIdStatusExt => {
                if let Some(object_id) = &self.object_id {
                    object_id.encode(w)?;
                } else {
                    return Err(EncodeError::MissingField("ObjectId".to_string()));
                }
            }
            _ => {}
        };

        self.publisher_priority.encode(w)?;

        // Encode Extension Headers if required
        match self.datagram_type {
            DatagramType::ObjectIdPayloadExt
            | DatagramType::ObjectIdPayloadExtEndOfGroup
            | DatagramType::PayloadExt
            | DatagramType::PayloadExtEndOfGroup
            | DatagramType::ObjectIdStatusExt => {
                if let Some(extension_headers) = &self.extension_headers {
                    if extension_headers.is_empty() {
                        return Err(EncodeError::InvalidValue);
                    }
                    extension_headers.encode(w)?;
                } else {
                    return Err(EncodeError::MissingField("ExtensionHeaders".to_string()));
                }
            }
            _ => {}
        };

        // Decode Status if required
        match self.datagram_type {
            DatagramType::ObjectIdStatus | DatagramType::ObjectIdStatusExt => {
                if let Some(status) = &self.status {
                    if self.extension_headers.is_some() && *status != ObjectStatus::NormalObject {
                        return Err(EncodeError::InvalidValue);
                    }
                    status.encode(w)?;
                } else {
                    return Err(EncodeError::MissingField("Status".to_string()));
                }
            }
            _ => {}
        }

        // Decode Payload if required
        match self.datagram_type {
            DatagramType::ObjectIdPayload
            | DatagramType::ObjectIdPayloadExt
            | DatagramType::ObjectIdPayloadEndOfGroup
            | DatagramType::ObjectIdPayloadExtEndOfGroup
            | DatagramType::Payload
            | DatagramType::PayloadExt
            | DatagramType::PayloadEndOfGroup
            | DatagramType::PayloadExtEndOfGroup => {
                if let Some(payload) = &self.payload {
                    Self::encode_remaining(w, payload.len())?;
                    w.put_slice(payload);
                } else {
                    return Err(EncodeError::MissingField("Payload".to_string()));
                }
            }
            _ => {}
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
            publisher_priority: 127,
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
            publisher_priority: 127,
            extension_headers: Some(ext_hdrs.clone()),
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Same as above plus NumExt(1),ExtensionKey(1),ExtensionValueLen(1),ExtensionValue(4) = 13 + 7 = 20
        assert_eq!(20, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = ObjectIdPayloadEndOfGroup
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdPayloadEndOfGroup,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: 127,
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
            publisher_priority: 127,
            extension_headers: Some(ext_hdrs.clone()),
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Same as above plus NumExt(1),ExtensionKey(1),ExtensionValueLen(1),ExtensionValue(4) = 13 + 7 = 20
        assert_eq!(20, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = ObjectIdStatus
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdStatus,
            track_alias: 12,
            group_id: 10,
            object_id: Some(1234),
            publisher_priority: 127,
            extension_headers: None,
            status: Some(ObjectStatus::NormalObject),
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
            publisher_priority: 127,
            extension_headers: Some(ext_hdrs.clone()),
            status: Some(ObjectStatus::NormalObject),
            payload: None,
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Same as above plus NumExt(1),ExtensionKey(1),ExtensionValueLen(1),ExtensionValue(4) = 7 + 7 = 14
        assert_eq!(14, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = Payload
        let msg = Datagram {
            datagram_type: DatagramType::Payload,
            track_alias: 12,
            group_id: 10,
            object_id: None,
            publisher_priority: 127,
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
            publisher_priority: 127,
            extension_headers: Some(ext_hdrs.clone()),
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Same as above plus NumExt(1),ExtensionKey(1),ExtensionValueLen(1),ExtensionValue(4) = 11 + 7 = 18
        assert_eq!(18, buf.len());
        let decoded = Datagram::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);

        // DatagramType = PayloadEndOfGroup
        let msg = Datagram {
            datagram_type: DatagramType::PayloadEndOfGroup,
            track_alias: 12,
            group_id: 10,
            object_id: None,
            publisher_priority: 127,
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
            publisher_priority: 127,
            extension_headers: Some(ext_hdrs.clone()),
            status: None,
            payload: Some(Bytes::from("payload")),
        };
        msg.encode(&mut buf).unwrap();
        // Length should be: Same as above plus NumExt(1),ExtensionKey(1),ExtensionValueLen(1),ExtensionValue(4) = 11 + 7 = 18
        assert_eq!(18, buf.len());
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
            publisher_priority: 127,
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
            publisher_priority: 127,
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
            publisher_priority: 127,
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
            publisher_priority: 127,
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
            publisher_priority: 127,
            extension_headers: None,
            status: None,
            payload: None,
        };
        let encoded = msg.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::MissingField(_)));

        // TODO SLG - add tests
    }

    #[test]
    fn decode_rejects_extension_bit_with_zero_length() {
        let data = vec![
            0x01, // ObjectIdPayloadExt
            0x01, // track alias
            0x01, // group id
            0x01, // object id
            0x7f, // publisher priority
            0x00, // extension headers length
        ];
        let mut buf: Bytes = data.into();

        assert!(matches!(
            Datagram::decode(&mut buf).unwrap_err(),
            DecodeError::InvalidValue
        ));
    }

    #[test]
    fn encode_rejects_extension_bit_with_empty_headers() {
        let mut buf = BytesMut::new();
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdPayloadExt,
            track_alias: 1,
            group_id: 1,
            object_id: Some(1),
            publisher_priority: 1,
            extension_headers: Some(ExtensionHeaders::default()),
            status: None,
            payload: Some(Bytes::new()),
        };

        assert!(matches!(
            msg.encode(&mut buf).unwrap_err(),
            EncodeError::InvalidValue
        ));
    }

    #[test]
    fn decode_rejects_non_normal_status_with_extension_headers() {
        let data = vec![
            0x21, // ObjectIdStatusExt
            0x01, // track alias
            0x01, // group id
            0x01, // object id
            0x7f, // publisher priority
            0x02, // extension headers byte length
            0x00, // extension delta type
            0x01, // extension value
            0x04, // EndOfTrack
        ];
        let mut buf: Bytes = data.into();

        assert!(matches!(
            Datagram::decode(&mut buf).unwrap_err(),
            DecodeError::InvalidValue
        ));
    }

    #[test]
    fn encode_rejects_non_normal_status_with_extension_headers() {
        let mut ext_hdrs = ExtensionHeaders::new();
        ext_hdrs.set_intvalue(0, 1);
        let mut buf = BytesMut::new();
        let msg = Datagram {
            datagram_type: DatagramType::ObjectIdStatusExt,
            track_alias: 1,
            group_id: 1,
            object_id: Some(1),
            publisher_priority: 1,
            extension_headers: Some(ext_hdrs),
            status: Some(ObjectStatus::EndOfTrack),
            payload: None,
        };

        assert!(matches!(
            msg.encode(&mut buf).unwrap_err(),
            EncodeError::InvalidValue
        ));
    }
}
