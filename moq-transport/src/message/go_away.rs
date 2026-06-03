use crate::coding::{Decode, DecodeError, Encode, EncodeError, SessionUri};

/// Sent by the server to indicate that the client should connect to a different server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoAway {
    pub uri: SessionUri,
}

impl Decode for GoAway {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let uri = SessionUri::decode(r)?;
        Ok(Self { uri })
    }
}

impl Encode for GoAway {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.uri.encode(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();

        let msg = GoAway {
            uri: SessionUri("moq://example.com:1234".to_string()),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = GoAway::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
