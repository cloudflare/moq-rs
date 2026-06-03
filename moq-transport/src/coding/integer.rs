use super::{Decode, DecodeError, Encode, EncodeError};

impl Encode for u8 {
    /// Encode a u8 to the given writer.
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        let x = *self;
        w.put_u8(x);
        Ok(())
    }
}

impl Decode for u8 {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        Self::decode_remaining(r, 1)?;
        Ok(r.get_u8())
    }
}

impl Encode for u16 {
    /// Encode a u16 to the given writer.
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        let x = *self;
        Self::encode_remaining(w, 2)?;
        w.put_u16(x);
        Ok(())
    }
}

impl Decode for u16 {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        Self::decode_remaining(r, 2)?;
        Ok(r.get_u16())
    }
}

impl Encode for bool {
    /// Encode a bool as u8 to the given writer.
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        Self::encode_remaining(w, 1)?;
        let x = *self;
        match x {
            true => w.put_u8(1),
            false => w.put_u8(0),
        }
        Ok(())
    }
}

impl Decode for bool {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        Self::decode_remaining(r, 1)?;
        let forward_byte = u8::decode(r)?;
        match forward_byte {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(DecodeError::InvalidValue),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use bytes::BytesMut;

    #[test]
    fn encode_decode_u8() {
        let mut buf = BytesMut::new();

        let i: u8 = 8;
        i.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x08]);
        let decoded = u8::decode(&mut buf).unwrap();
        assert_eq!(decoded, i);
    }

    #[test]
    fn encode_decode_u16() {
        let mut buf = BytesMut::new();

        let i: u16 = 65534;
        i.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0xff, 0xfe]);
        let decoded = u16::decode(&mut buf).unwrap();
        assert_eq!(decoded, i);
    }

    #[test]
    fn encode_decode_bool() {
        let mut buf = BytesMut::new();

        let b = true;
        b.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x01]);
        let decoded = bool::decode(&mut buf).unwrap();
        assert_eq!(decoded, b);
    }

    #[test]
    fn decode_invalid_bool() {
        let data: Vec<u8> = vec![0x02]; // Invalid value for bool
        let mut buf: Bytes = data.into();
        let decoded = bool::decode(&mut buf);
        assert!(matches!(decoded.unwrap_err(), DecodeError::InvalidValue));
    }
}
