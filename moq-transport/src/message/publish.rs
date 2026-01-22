use crate::coding::{Decode, DecodeError, Encode, EncodeError, Location, TrackNamespace};
use crate::data::{ExtensionHeaders, ParameterType, Parameters};

/// Sent by publisher to initiate a subscription to a track.
///
/// Per draft-16, the PUBLISH message contains:
/// - Request ID
/// - Track Namespace
/// - Track Name
/// - Track Alias
/// - Parameters (including EXPIRES, LARGEST_OBJECT, FORWARD, etc.)
/// - Track Extensions
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Publish {
    /// The publish request ID
    pub id: u64,

    /// Track properties
    pub track_namespace: TrackNamespace,
    pub track_name: String, // TODO SLG - consider making a FullTrackName base struct (total size limit of 4096)
    pub track_alias: u64,

    /// Optional parameters (EXPIRES, LARGEST_OBJECT, FORWARD, etc.)
    pub params: Parameters,

    /// Track extensions
    pub track_extensions: ExtensionHeaders,
}

impl Publish {
    /// Get the FORWARD parameter value (default is 1/true if not present)
    pub fn forward(&self) -> bool {
        self.params
            .get_intvalue(ParameterType::Forward.into())
            .map(|v| v != 0)
            .unwrap_or(true) // Default is 1 (forward) per spec
    }

    /// Set the FORWARD parameter
    pub fn set_forward(&mut self, forward: bool) {
        self.params
            .set_intvalue(ParameterType::Forward.into(), forward as u64);
    }

    /// Get the LARGEST_OBJECT parameter as a Location, if present
    pub fn largest_location(&self) -> Option<Location> {
        self.params
            .get_bytesvalue(ParameterType::LargestObject.into())
            .and_then(|bytes| {
                let mut buf = bytes::Bytes::copy_from_slice(bytes);
                Location::decode(&mut buf).ok()
            })
    }

    /// Set the LARGEST_OBJECT parameter from a Location
    pub fn set_largest_location(&mut self, location: Option<Location>) {
        if let Some(loc) = location {
            let mut buf = Vec::new();
            loc.encode(&mut buf).ok();
            self.params
                .set_bytesvalue(ParameterType::LargestObject.into(), buf);
        }
    }

    /// Check if content exists (presence of LARGEST_OBJECT parameter)
    pub fn content_exists(&self) -> bool {
        self.params.has(ParameterType::LargestObject.into())
    }

    /// Get the EXPIRES parameter value in milliseconds, if present
    pub fn expires(&self) -> Option<u64> {
        self.params.get_intvalue(ParameterType::Expires.into())
    }

    /// Set the EXPIRES parameter value in milliseconds
    pub fn set_expires(&mut self, expires: u64) {
        self.params
            .set_intvalue(ParameterType::Expires.into(), expires);
    }
}

impl Decode for Publish {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let track_namespace = TrackNamespace::decode(r)?;
        let track_name = String::decode(r)?;
        let track_alias = u64::decode(r)?;
        let params = Parameters::decode(r)?;
        let track_extensions = ExtensionHeaders::decode(r)?;

        Ok(Self {
            id,
            track_namespace,
            track_name,
            track_alias,
            params,
            track_extensions,
        })
    }
}

impl Encode for Publish {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.track_namespace.encode(w)?;
        self.track_name.encode(w)?;
        self.track_alias.encode(w)?;
        self.params.encode(w)?;
        self.track_extensions.encode(w)?;

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

        let msg = Publish {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            track_alias: 212,
            params: params.clone(),
            track_extensions: Default::default(),
        };
        msg.encode(&mut buf).unwrap();
        let decoded = Publish::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_with_largest_object() {
        let mut buf = BytesMut::new();

        let mut msg = Publish {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            track_alias: 212,
            params: Parameters::new(),
            track_extensions: Default::default(),
        };
        msg.set_largest_location(Some(Location::new(2, 3)));
        msg.set_forward(true);

        msg.encode(&mut buf).unwrap();
        let decoded = Publish::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
        assert!(decoded.content_exists());
        assert_eq!(decoded.largest_location(), Some(Location::new(2, 3)));
        assert!(decoded.forward());
    }

    #[test]
    fn encode_decode_no_content() {
        let mut buf = BytesMut::new();

        let mut msg = Publish {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".to_string(),
            track_alias: 212,
            params: Parameters::new(),
            track_extensions: Default::default(),
        };
        msg.set_forward(false);

        msg.encode(&mut buf).unwrap();
        let decoded = Publish::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
        assert!(!decoded.content_exists());
        assert!(!decoded.forward());
    }

    #[test]
    fn forward_defaults_to_true() {
        let msg = Publish {
            id: 1,
            track_namespace: TrackNamespace::from_utf8_path("test"),
            track_name: "track".to_string(),
            track_alias: 1,
            params: Parameters::new(),
            track_extensions: Default::default(),
        };
        assert!(msg.forward());
    }
}
