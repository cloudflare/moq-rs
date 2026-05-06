use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePair};
use bytes::Buf;
use std::fmt;

/// A collection of KeyValuePair entries, where the length in bytes of key-value-pairs are encoded/decoded first.
/// This structure is appropriate for Data plane extension headers.
///
/// Per draft-16 Section 1.4.2, Key-Value-Pairs use delta-encoded Type fields.
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
    /// Encode extension headers without length prefix (just the delta-encoded KVPs).
    /// Used for Track Extensions in PUBLISH where the length is implicit from the message.
    pub fn encode_without_length<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        // Sort by key for delta encoding
        let mut sorted: Vec<&KeyValuePair> = self.0.iter().collect();
        sorted.sort_by_key(|kvp| kvp.key);

        let mut prev_key: u64 = 0;
        for kvp in sorted {
            let delta = kvp.key.checked_sub(prev_key).ok_or_else(|| {
                log::error!(
                    "[ExtHdr] Keys not sortable: prev_key={}, current_key={}",
                    prev_key,
                    kvp.key
                );
                EncodeError::InvalidValue
            })?;
            delta.encode(w)?;
            kvp.encode_value(w)?;
            prev_key = kvp.key;
        }

        Ok(())
    }

    /// Decode extension headers from remaining bytes (no length prefix).
    /// Used for Track Extensions in PUBLISH where the length is implicit from the message.
    pub fn decode_remaining_bytes<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        if !r.has_remaining() {
            return Ok(ExtensionHeaders::new());
        }

        let mut kvps = Vec::new();
        let mut prev_key: u64 = 0;

        while r.has_remaining() {
            // Read delta type and reconstruct absolute key
            let delta = u64::decode(r)?;
            let key = prev_key.checked_add(delta).ok_or_else(|| {
                log::error!(
                    "[ExtHdr] Delta type overflow: prev_key={}, delta={}",
                    prev_key,
                    delta
                );
                DecodeError::BoundsExceeded(crate::coding::BoundsExceeded)
            })?;

            let kvp = KeyValuePair::decode_value(key, r)?;
            kvps.push(kvp);
            prev_key = key;
        }

        Ok(ExtensionHeaders(kvps))
    }
}

impl Decode for ExtensionHeaders {
    /// Decode extension headers with delta-encoded Type fields (draft-16 Section 1.4.2).
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        // Read total byte length of the encoded kvps
        // Note: this is the difference between KeyValuePairs and ExtensionHeaders.
        // KeyValuePairs encodes the count of kvps, whereas ExtensionHeaders encodes the total byte length.
        let length = usize::decode(r)?;

        // Ensure we have that many bytes available in the input
        Self::decode_remaining(r, length)?;

        // If zero length, return empty map
        if length == 0 {
            return Ok(ExtensionHeaders::new());
        }

        // Copy the exact slice that contains the encoded kvps and decode from it
        let mut buf = vec![0u8; length];
        r.copy_to_slice(&mut buf);
        let mut kvps_bytes = bytes::Bytes::from(buf);

        let mut kvps = Vec::new();
        let mut prev_key: u64 = 0;

        while kvps_bytes.has_remaining() {
            // Read delta type and reconstruct absolute key
            let delta = u64::decode(&mut kvps_bytes)?;
            let key = prev_key.checked_add(delta).ok_or_else(|| {
                log::error!(
                    "[ExtHdr] Delta type overflow: prev_key={}, delta={}",
                    prev_key,
                    delta
                );
                DecodeError::BoundsExceeded(crate::coding::BoundsExceeded)
            })?;

            let kvp = KeyValuePair::decode_value(key, &mut kvps_bytes)?;
            kvps.push(kvp);
            prev_key = key;
        }

        Ok(ExtensionHeaders(kvps))
    }
}

impl Encode for ExtensionHeaders {
    /// Encode extension headers with delta-encoded Type fields (draft-16 Section 1.4.2).
    /// Entries are sorted by key in ascending order before encoding.
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        // Sort by key for delta encoding
        let mut sorted: Vec<&KeyValuePair> = self.0.iter().collect();
        sorted.sort_by_key(|kvp| kvp.key);

        // Encode all entries into a temporary buffer to compute total byte length
        let mut tmp = bytes::BytesMut::new();
        let mut prev_key: u64 = 0;
        for kvp in sorted {
            let delta = kvp.key.checked_sub(prev_key).ok_or_else(|| {
                log::error!(
                    "[ExtHdr] Keys not sortable: prev_key={}, current_key={}",
                    prev_key,
                    kvp.key
                );
                EncodeError::InvalidValue
            })?;
            delta.encode(&mut tmp)?;
            kvp.encode_value(&mut tmp)?;
            prev_key = kvp.key;
        }

        // Write total byte length followed by the encoded bytes
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
    fn encode_decode_extension_headers_single() {
        let mut buf = BytesMut::new();

        // Single entry: key=1. Delta from 0 = 1.
        let mut ext_hdrs = ExtensionHeaders::new();
        ext_hdrs.set_bytesvalue(1, vec![0x01, 0x02, 0x03, 0x04, 0x05]);
        ext_hdrs.encode(&mut buf).unwrap();
        assert_eq!(
            buf.to_vec(),
            vec![
                0x07, // 7 bytes total length
                // Delta=1, length=5, data
                0x01, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05,
            ]
        );
        let decoded = ExtensionHeaders::decode(&mut buf).unwrap();
        assert_eq!(decoded, ext_hdrs);
    }

    #[test]
    fn encode_decode_extension_headers_multiple() {
        let mut buf = BytesMut::new();

        // Multiple entries inserted out of order — encoding sorts by key.
        // Keys: 0 (even, int), 1 (odd, bytes), 100 (even, int)
        let mut ext_hdrs = ExtensionHeaders::new();
        ext_hdrs.set_intvalue(0, 0);
        ext_hdrs.set_intvalue(100, 100);
        ext_hdrs.set_bytesvalue(1, vec![0x01, 0x02, 0x03, 0x04, 0x05]);
        ext_hdrs.encode(&mut buf).unwrap();
        let buf_vec = buf.to_vec();

        #[rustfmt::skip]
        let expected = vec![
            0x0d, // 13 bytes total length for the KVP data
            // Entry 1: key=0 (delta=0), even, int value=0
            0x00, 0x00,
            // Entry 2: key=1 (delta=1), odd, bytes len=5
            0x01, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05,
            // Entry 3: key=100 (delta=99), even, int value=100
            0x40, 0x63, 0x40, 0x64,
        ];
        assert_eq!(buf_vec, expected);

        // Decode and verify — decoded entries will be in sorted order
        let decoded = ExtensionHeaders::decode(&mut buf).unwrap();
        let mut expected_ext = ExtensionHeaders::new();
        expected_ext.set_intvalue(0, 0);
        expected_ext.set_bytesvalue(1, vec![0x01, 0x02, 0x03, 0x04, 0x05]);
        expected_ext.set_intvalue(100, 100);
        assert_eq!(decoded, expected_ext);
    }

    #[test]
    fn encode_decode_extension_headers_empty() {
        let mut buf = BytesMut::new();

        let ext_hdrs = ExtensionHeaders::new();
        ext_hdrs.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x00]); // length=0
        let decoded = ExtensionHeaders::decode(&mut buf).unwrap();
        assert_eq!(decoded, ext_hdrs);
    }
}
