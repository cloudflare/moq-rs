use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError, TrackNamespace},
    data::Parameters,
};

/// Per draft-16, TRACK_STATUS message format is identical to SUBSCRIBE message.
/// Subscriber parameters related to Track delivery (e.g. SUBSCRIBER_PRIORITY) are not included.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrackStatus {
    pub id: u64,
    pub track_namespace: TrackNamespace,
    pub track_name: String,
    pub params: Parameters,
}

impl Decode for TrackStatus {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let track_namespace = TrackNamespace::decode(r)?;
        let track_name = String::decode(r)?;
        let params = Parameters::decode(r)?;

        Ok(Self {
            id,
            track_namespace,
            track_name,
            params,
        })
    }
}

impl Encode for TrackStatus {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.track_namespace.encode(w)?;
        self.track_name.encode(w)?;
        self.params.encode(w)?;

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

        let mut params = Parameters::new();
        params.set_bytesvalue(123, vec![0x00, 0x01, 0x02, 0x03]);

        let msg = TrackStatus {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            params: params.clone(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = TrackStatus::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_empty_params() {
        let mut buf = BytesMut::new();

        let msg = TrackStatus {
            id: 1,
            track_namespace: TrackNamespace::from_utf8_path("test"),
            track_name: "track".to_string(),
            params: Parameters::new(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = TrackStatus::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }
}
