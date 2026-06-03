use crate::coding::{Decode, DecodeError, Encode, EncodeError};

#[derive(Default, Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct Location {
    pub group_id: u64,
    pub object_id: u64,
}

impl Location {
    pub fn new(group_id: u64, object_id: u64) -> Self {
        Self {
            group_id,
            object_id,
        }
    }
}

impl Decode for Location {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let group_id = u64::decode(r)?;
        let object_id = u64::decode(r)?;
        Ok(Location::new(group_id, object_id))
    }
}

impl Encode for Location {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.group_id.encode(w)?;
        self.object_id.encode(w)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();

        let loc = Location::new(12345, 67890);
        loc.encode(&mut buf).unwrap();

        #[rustfmt::skip]
        assert_eq!(
            buf.to_vec(),
            vec![
                0x70, 0x39, // 12345 encoded as VarInt
                0x80, 0x01, 0x09, 0x32 // 67890 encoded as VarInt
            ]
        );
        let decoded = Location::decode(&mut buf).unwrap();
        assert_eq!(decoded, loc);
    }

    #[test]
    fn verify_ordering() {
        let loc1 = Location::new(1, 2);
        let loc2 = Location::new(1, 5);
        let loc3 = Location::new(2, 1);
        let loc4 = Location::new(2, 2);
        let loc5 = Location::new(2, 6);
        let loc6 = Location::new(2, 6);
        assert!(loc1 < loc2);
        assert!(loc1 <= loc2);
        assert!(loc2 > loc1);
        assert!(loc2 < loc3);
        assert!(loc3 < loc4);
        assert!(loc4 < loc5);
        assert!(loc5 == loc6);
        assert!(loc5 >= loc6);
    }
}
