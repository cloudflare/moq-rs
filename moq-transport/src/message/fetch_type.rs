use crate::coding::{Decode, DecodeError, Encode, EncodeError};

/// Filter Types
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FetchType {
    Standalone = 0x1,
    RelativeJoining = 0x2,
    AbsoluteJoining = 0x3,
}

impl Encode for FetchType {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        let val = *self as u8;
        val.encode(w)?;
        Ok(())
    }
}

impl Decode for FetchType {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        match u8::decode(r)? {
            0x1 => Ok(Self::Standalone),
            0x2 => Ok(Self::RelativeJoining),
            0x3 => Ok(Self::AbsoluteJoining),
            _ => Err(DecodeError::InvalidFetchType),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();

        let ft = FetchType::Standalone;
        ft.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x01]);
        let decoded = FetchType::decode(&mut buf).unwrap();
        assert_eq!(decoded, ft);

        let ft = FetchType::RelativeJoining;
        ft.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x02]);
        let decoded = FetchType::decode(&mut buf).unwrap();
        assert_eq!(decoded, ft);

        let ft = FetchType::AbsoluteJoining;
        ft.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x03]);
        let decoded = FetchType::decode(&mut buf).unwrap();
        assert_eq!(decoded, ft);
    }

    #[test]
    fn decode_bad_value() {
        let data: Vec<u8> = vec![0x04]; // Invalid fetch type
        let mut buf: Bytes = data.into();
        let result = FetchType::decode(&mut buf);
        assert!(matches!(result, Err(DecodeError::InvalidFetchType)));
    }
}
