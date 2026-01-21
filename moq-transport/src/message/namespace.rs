use crate::coding::{Decode, DecodeError, Encode, EncodeError, TrackNamespace};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Namespace {
    pub track_namespace_suffix: TrackNamespace,
}

impl Decode for Namespace {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let track_namespace_suffix = TrackNamespace::decode(r)?;

        Ok(Self {
            track_namespace_suffix,
        })
    }
}

impl Encode for Namespace {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.track_namespace_suffix.encode(w)?;

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

        let msg = Namespace {
            track_namespace_suffix: TrackNamespace::from_utf8_path("test/namespace"),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Namespace::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
