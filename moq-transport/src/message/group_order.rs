use crate::coding::{Decode, DecodeError, Encode, EncodeError};

/// Group Order
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupOrder {
    Publisher = 0x0,
    Ascending = 0x1,
    Descending = 0x2,
}

impl Encode for GroupOrder {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        let val = *self as u8;
        val.encode(w)?;
        Ok(())
    }
}

impl Decode for GroupOrder {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        match u8::decode(r)? {
            0x0 => Ok(Self::Publisher),
            0x1 => Ok(Self::Ascending),
            0x2 => Ok(Self::Descending),
            _ => Err(DecodeError::InvalidGroupOrder),
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

        let go = GroupOrder::Publisher;
        go.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x00]);
        let decoded = GroupOrder::decode(&mut buf).unwrap();
        assert_eq!(decoded, go);

        let go = GroupOrder::Ascending;
        go.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x01]);
        let decoded = GroupOrder::decode(&mut buf).unwrap();
        assert_eq!(decoded, go);

        let go = GroupOrder::Descending;
        go.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x02]);
        let decoded = GroupOrder::decode(&mut buf).unwrap();
        assert_eq!(decoded, go);
    }

    #[test]
    fn decode_bad_value() {
        let data: Vec<u8> = vec![0x03]; // Invalid filter type
        let mut buf: Bytes = data.into();
        let result = GroupOrder::decode(&mut buf);
        assert!(matches!(result, Err(DecodeError::InvalidGroupOrder)));
    }
}
