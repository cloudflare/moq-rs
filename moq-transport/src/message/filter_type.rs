use crate::coding::{Decode, DecodeError, Encode, EncodeError};

/// Filter Types
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterType {
    NextGroupStart = 0x1,
    LargestObject = 0x2,
    AbsoluteStart = 0x3,
    AbsoluteRange = 0x4,
}

impl Encode for FilterType {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        let val = *self as u64;
        val.encode(w)?;
        Ok(())
    }
}

impl Decode for FilterType {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        match u64::decode(r)? {
            0x1_u64 => Ok(Self::NextGroupStart),
            0x2_u64 => Ok(Self::LargestObject),
            0x3_u64 => Ok(Self::AbsoluteStart),
            0x4_u64 => Ok(Self::AbsoluteRange),
            _ => Err(DecodeError::InvalidFilterType),
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

        let ft = FilterType::NextGroupStart;
        ft.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x01]);
        let decoded = FilterType::decode(&mut buf).unwrap();
        assert_eq!(decoded, ft);

        let ft = FilterType::LargestObject;
        ft.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x02]);
        let decoded = FilterType::decode(&mut buf).unwrap();
        assert_eq!(decoded, ft);

        let ft = FilterType::AbsoluteStart;
        ft.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x03]);
        let decoded = FilterType::decode(&mut buf).unwrap();
        assert_eq!(decoded, ft);

        let ft = FilterType::AbsoluteRange;
        ft.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x04]);
        let decoded = FilterType::decode(&mut buf).unwrap();
        assert_eq!(decoded, ft);
    }

    #[test]
    fn decode_bad_value() {
        let data: Vec<u8> = vec![0x05]; // Invalid filter type
        let mut buf: Bytes = data.into();
        let result = FilterType::decode(&mut buf);
        assert!(matches!(result, Err(DecodeError::InvalidFilterType)));
    }
}
