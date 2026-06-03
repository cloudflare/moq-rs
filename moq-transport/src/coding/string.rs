// TODO SLG - eventually remove this file, bounded_string should now be used instead

use super::{Decode, DecodeError, Encode, EncodeError};

impl Encode for String {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.len().encode(w)?;
        Self::encode_remaining(w, self.len())?;
        w.put(self.as_ref());
        Ok(())
    }
}

impl Decode for String {
    /// Decode a string with a varint length prefix.
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let size = usize::decode(r)?;

        Self::decode_remaining(r, size)?;

        let mut buf = vec![0; size];
        r.copy_to_slice(&mut buf);
        let str = String::from_utf8(buf)?;

        Ok(str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();

        let s = "teststring".to_string();
        s.encode(&mut buf).unwrap();
        assert_eq!(
            buf.to_vec(),
            vec![
                0x0a, // Length of "teststring" is 10
                0x74, 0x65, 0x73, 0x74, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67
            ]
        );
        let decoded = String::decode(&mut buf).unwrap();
        assert_eq!(decoded, s);
    }
}
