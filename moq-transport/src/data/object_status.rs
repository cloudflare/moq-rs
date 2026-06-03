use crate::coding::{Decode, DecodeError, Encode, EncodeError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectStatus {
    NormalObject = 0x0,
    ObjectDoesNotExist = 0x1,
    EndOfGroup = 0x3,
    EndOfTrack = 0x4,
}

impl Decode for ObjectStatus {
    fn decode<B: bytes::Buf>(r: &mut B) -> Result<Self, DecodeError> {
        match u64::decode(r)? {
            0x0 => Ok(Self::NormalObject),
            0x1 => Ok(Self::ObjectDoesNotExist),
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
