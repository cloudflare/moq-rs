use crate::coding::{Decode, DecodeError, Encode, EncodeError, ReasonPhrase};

#[derive(Clone, Debug)]
pub struct RequestError {
    pub id: u64,

    // An error code.
    pub error_code: u64,

    // An optional, human-readable reason.
    pub reason_phrase: ReasonPhrase,
}

impl Decode for RequestError {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let error_code = u64::decode(r)?;
        let reason_phrase = ReasonPhrase::decode(r)?;

        Ok(Self {
            id,
            error_code,
            reason_phrase,
        })
    }
}

impl Encode for RequestError {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.error_code.encode(w)?;
        self.reason_phrase.encode(w)?;

        Ok(())
    }
}
