use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};
use crate::data::{ObjectStatus, StreamHeaderType};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FetchHeader {
    /// Subgroup Header Type
    pub header_type: StreamHeaderType,

    /// The fetch request Id number
    pub request_id: u64,
}

// Note:  Not using the Decode trait, since we need to know the header_type to properly parse this, and it
//        is read before knowing we need to decode this.
impl FetchHeader {
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
        self.request_id.encode(w)?;

        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FetchObject {
    /// The group sequence number
    pub group_id: u64,

    /// The subgroup sequence number
    pub subgroup_id: u64,

    /// The object sequence number
    pub object_id: u64,

    /// Publisher priority, where **smaller** values are sent first.
    pub publisher_priority: u8,

    pub extension_headers: KeyValuePairs,

    pub payload_length: usize,

    pub status: Option<ObjectStatus>,
    //pub payload: bytes::Bytes,  // TODO SLG - payload is sent outside this right now - decide which way to go
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

        //Self::decode_remaining(r, payload_length);
        //let payload = r.copy_to_bytes(payload_length);

        Ok(Self {
            group_id,
            subgroup_id,
            object_id,
            publisher_priority,
            extension_headers,
            payload_length,
            status,
            //payload,
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
            if let Some(status) = self.status {
                status.encode(w)?;
            } else {
                return Err(EncodeError::MissingField("Status".to_string()));
            }
        }
        //Self::encode_remaining(w, self.payload.len())?;
        //w.put_slice(&self.payload);

        Ok(())
    }
}

// TODO SLG - add unit tests
