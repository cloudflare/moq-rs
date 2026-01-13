use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};

/// Sent by the server in response to a client setup.
/// This SERVER_SETUP message is used by moq-transport draft versions 11 and later.
#[derive(Debug)]
pub struct Server {
    /// Setup Parameters, ie: MAX_REQUEST_ID, MAX_AUTH_TOKEN_CACHE_SIZE,
    /// AUTHORIZATION_TOKEN, etc.
    pub params: KeyValuePairs,
}

impl Decode for Server {
    /// Decode the server setup.
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let typ = u64::decode(r)?;
        if typ != 0x21 {
            // SERVER_SETUP message ID for draft versions 11 and later
            return Err(DecodeError::InvalidMessage(typ));
        }

        let _len = u16::decode(r)?;
        // TODO: Check the length of the message.

        let params = KeyValuePairs::decode(r)?;

        Ok(Self { params })
    }
}

impl Encode for Server {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        (0x21_u64).encode(w)?; // SERVER_SETUP message ID for draft versions 11 and later

        // Find out the length of the message
        // by encoding it into a buffer and then encoding the length.
        // This is a bit wasteful, but it's the only way to know the length.
        // TODO SLG - perhaps we can store the position of the Length field in the BufMut and
        //       write the length later, to avoid the copy of the message bytes?
        let mut buf = Vec::new();

        self.params.encode(&mut buf).unwrap();

        // Make sure buf.len() <= u16::MAX
        if buf.len() > u16::MAX as usize {
            return Err(EncodeError::MsgBoundsExceeded);
        }
        (buf.len() as u16).encode(w)?;

        // At least don't encode the message twice.
        // Instead, write the buffer directly to the writer.
        Self::encode_remaining(w, buf.len())?;
        w.put_slice(&buf);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::ParameterType;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();

        let mut params = KeyValuePairs::default();
        params.set_intvalue(ParameterType::MaxRequestId.into(), 1000);

        let server = Server { params };

        server.encode(&mut buf).unwrap();

        #[rustfmt::skip]
        assert_eq!(
            buf.to_vec(),
            vec![
                0x21, // Type
                0x00, 0x0c, // Length
                0xC0, 0x00, 0x00, 0x00, 0xFF, 0x00, 0x00, 0x0E, // Version DRAFT_14 (0xff00000E)
                0x01, // 1 Param
                0x02, 0x43, 0xe8, // Key=2 (MaxRequestId), Value=1000
            ]
        );

        let decoded = Server::decode(&mut buf).unwrap();
        assert_eq!(decoded.params, server.params);
    }
}
