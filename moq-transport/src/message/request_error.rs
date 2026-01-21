use crate::coding::{Decode, DecodeError, Encode, EncodeError, ReasonPhrase};

#[derive(Clone, Debug)]
pub struct RequestError {
    pub id: u64,
    pub error_code: u64,
    pub retry_interval: u64,
    pub reason_phrase: ReasonPhrase,
}

impl Decode for RequestError {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let error_code = u64::decode(r)?;
        let retry_interval = u64::decode(r)?;
        let reason_phrase = ReasonPhrase::decode(r)?;

        Ok(Self {
            id,
            error_code,
            retry_interval,
            reason_phrase,
        })
    }
}

impl Encode for RequestError {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.error_code.encode(w)?;
        self.retry_interval.encode(w)?;
        self.reason_phrase.encode(w)?;

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

        let msg = RequestError {
            id: 12345,
            error_code: 0x10,
            retry_interval: 1000,
            reason_phrase: ReasonPhrase("does not exist".to_string()),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = RequestError::decode(&mut buf).unwrap();
        assert_eq!(decoded.id, msg.id);
        assert_eq!(decoded.error_code, msg.error_code);
        assert_eq!(decoded.retry_interval, msg.retry_interval);
        assert_eq!(decoded.reason_phrase, msg.reason_phrase);
    }
}
