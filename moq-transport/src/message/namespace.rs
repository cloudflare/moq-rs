use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs, TrackNamespace};

/// NAMESPACE message (draft-16)
///
/// Sent by relay to subscriber to announce a namespace matching their SUBSCRIBE_NAMESPACE.
/// This is different from PUBLISH_NAMESPACE which is sent by publisher to relay.
///
/// Wire format: 0x08
#[derive(Clone, Debug)]
pub struct Namespace {
    /// Request ID (from the SUBSCRIBE_NAMESPACE)
    pub id: u64,
    /// The namespace being announced
    pub track_namespace: TrackNamespace,
    /// Optional parameters
    pub params: KeyValuePairs,
}

impl Decode for Namespace {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let track_namespace = TrackNamespace::decode(r)?;
        let params = KeyValuePairs::decode(r)?;

        Ok(Self {
            id,
            track_namespace,
            params,
        })
    }
}

impl Encode for Namespace {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.track_namespace.encode(w)?;
        self.params.encode(w)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_encode_decode() {
        let msg = Namespace {
            id: 42,
            track_namespace: TrackNamespace::from_utf8_path("live/room1"),
            params: KeyValuePairs::new(),
        };

        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        let decoded = Namespace::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(decoded.id, 42);
        assert_eq!(decoded.track_namespace.to_utf8_path(), "live/room1");
    }
}
