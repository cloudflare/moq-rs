use crate::coding::{Decode, DecodeError, Encode, EncodeError, TrackNamespace};

/// NAMESPACE_DONE message (type 0xE)
///
/// The publisher sends the NAMESPACE_DONE control message to indicate
/// its intent to stop serving new subscriptions for tracks within the
/// provided Track Namespace. All NAMESPACE_DONE messages are in
/// response to a SUBSCRIBE_NAMESPACE, so only the namespace tuples after
/// the 'Track Namespace Prefix' are included in the 'Track Namespace Suffix'.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceDone {
    /// Track Namespace Suffix: Specifies the final portion of a track's
    /// namespace. The namespace begins with the 'Track Namespace Prefix'
    /// specified in SUBSCRIBE_NAMESPACE.
    pub track_namespace_suffix: TrackNamespace,
}

impl Decode for NamespaceDone {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let track_namespace_suffix = TrackNamespace::decode(r)?;

        Ok(Self {
            track_namespace_suffix,
        })
    }
}

impl Encode for NamespaceDone {
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

        let msg = NamespaceDone {
            track_namespace_suffix: TrackNamespace::from_utf8_path("test/namespace"),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = NamespaceDone::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
