use crate::coding::{Decode, DecodeError, Encode, EncodeError, ReasonPhrase};

/// REQUEST_ERROR message (draft-16 Section 9.8).
///
/// Sent in response to any request (SUBSCRIBE, FETCH, PUBLISH, etc.) to indicate failure.
#[derive(Clone, Debug)]
pub struct RequestError {
    pub id: u64,

    /// An error code identifying the failure reason.
    pub error_code: u64,

    /// Minimum time in milliseconds before the request SHOULD be sent again, plus one.
    /// A value of 0 means the request SHOULD NOT be retried.
    /// A value of 1 means the request can be retried immediately.
    pub retry_interval: u64,

    /// An optional, human-readable reason.
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
            id: 42,
            error_code: 0x1,
            retry_interval: 5000,
            reason_phrase: ReasonPhrase("unauthorized".to_string()),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = RequestError::decode(&mut buf).unwrap();
        assert_eq!(decoded.id, msg.id);
        assert_eq!(decoded.error_code, msg.error_code);
        assert_eq!(decoded.retry_interval, msg.retry_interval);
    }

    #[test]
    fn encode_decode_no_retry() {
        let mut buf = BytesMut::new();

        let msg = RequestError {
            id: 10,
            error_code: 0x0,
            retry_interval: 0,
            reason_phrase: ReasonPhrase("internal error".to_string()),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = RequestError::decode(&mut buf).unwrap();
        assert_eq!(decoded.id, msg.id);
        assert_eq!(decoded.error_code, msg.error_code);
        assert_eq!(decoded.retry_interval, 0);
    }
}
