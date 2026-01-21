use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePair};
use crate::data::extension_types::{IMMUTABLE_EXTENSIONS, PRIOR_GROUP_ID_GAP, PRIOR_OBJECT_ID_GAP};
use bytes::Buf;
use std::collections::HashSet;
use std::fmt;

/// A collection of KeyValuePair entries, where the length in bytes of key-value-pairs are encoded/decoded first.
/// This structure is appropriate for Data plane extension headers.
/// Since duplicate parameters are allowed for unknown extension headers, we don't do duplicate checking here.
#[derive(Default, Clone, Eq, PartialEq)]
pub struct ExtensionHeaders(pub Vec<KeyValuePair>);

// TODO: These set/get API's all assume no duplicate keys. We can add API's to support duplicates if needed.
impl ExtensionHeaders {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace a KeyValuePair with the same key.
    pub fn set(&mut self, kvp: KeyValuePair) {
        if let Some(existing) = self.0.iter_mut().find(|k| k.key == kvp.key) {
            *existing = kvp;
        } else {
            self.0.push(kvp);
        }
    }

    pub fn set_intvalue(&mut self, key: u64, value: u64) {
        self.set(KeyValuePair::new_int(key, value));
    }

    pub fn set_bytesvalue(&mut self, key: u64, value: Vec<u8>) {
        self.set(KeyValuePair::new_bytes(key, value));
    }

    pub fn has(&self, key: u64) -> bool {
        self.0.iter().any(|k| k.key == key)
    }

    pub fn get(&self, key: u64) -> Option<&KeyValuePair> {
        self.0.iter().find(|k| k.key == key)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl ExtensionHeaders {
    fn is_known_extension_type(key: u64) -> bool {
        key == IMMUTABLE_EXTENSIONS || key == PRIOR_GROUP_ID_GAP || key == PRIOR_OBJECT_ID_GAP
    }
}

impl Decode for ExtensionHeaders {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let length = usize::decode(r)?;

        Self::decode_remaining(r, length)?;

        if length == 0 {
            return Ok(ExtensionHeaders::new());
        }

        let mut buf = vec![0u8; length];
        r.copy_to_slice(&mut buf);
        let mut kvps_bytes = bytes::Bytes::from(buf);

        let mut kvps = Vec::new();
        let mut seen_keys: HashSet<u64> = HashSet::new();

        while kvps_bytes.has_remaining() {
            let kvp = KeyValuePair::decode(&mut kvps_bytes)?;

            if Self::is_known_extension_type(kvp.key) && seen_keys.contains(&kvp.key) {
                return Err(DecodeError::DuplicateParameter(kvp.key));
            }
            seen_keys.insert(kvp.key);
            kvps.push(kvp);
        }

        Ok(ExtensionHeaders(kvps))
    }
}

impl Encode for ExtensionHeaders {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        // Encode all KeyValuePair entries into a temporary buffer to compute total byte length
        let mut tmp = bytes::BytesMut::new();
        for kvp in &self.0 {
            kvp.encode(&mut tmp)?;
        }

        // Write total byte length (u64) followed by the encoded bytes
        (tmp.len() as u64).encode(w)?;
        w.put_slice(&tmp);

        Ok(())
    }
}

impl fmt::Debug for ExtensionHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{ ")?;
        for (i, kv) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{:?}", kv)?;
        }
        write!(f, " }}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_decode_extension_headers() {
        let mut buf = BytesMut::new();

        let mut ext_hdrs = ExtensionHeaders::new();
        ext_hdrs.set_bytesvalue(1, vec![0x01, 0x02, 0x03, 0x04, 0x05]);
        ext_hdrs.encode(&mut buf).unwrap();
        assert_eq!(
            buf.to_vec(),
            vec![
                0x07, // 7 bytes total length
                0x01, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05, // Key=1, Value=[1,2,3,4,5]
            ]
        );
        let decoded = ExtensionHeaders::decode(&mut buf).unwrap();
        assert_eq!(decoded, ext_hdrs);

        let mut ext_hdrs = ExtensionHeaders::new();
        ext_hdrs.set_intvalue(0, 0); // 2 bytes
        ext_hdrs.set_intvalue(100, 100); // 4 bytes
        ext_hdrs.set_bytesvalue(1, vec![0x01, 0x02, 0x03, 0x04, 0x05]); // 1 byte key, 1 byte length, 5 bytes data = 7 bytes
        ext_hdrs.encode(&mut buf).unwrap();
        let buf_vec = buf.to_vec();
        // Validate the encoded length and the KeyValuePair's length.
        assert_eq!(14, buf_vec.len()); // 14 bytes total (length + 3 kvps)
        assert_eq!(13, buf_vec[0]); // 13 bytes for the 3 KeyValuePairs data
        let decoded = ExtensionHeaders::decode(&mut buf).unwrap();
        assert_eq!(decoded, ext_hdrs);
    }
}
