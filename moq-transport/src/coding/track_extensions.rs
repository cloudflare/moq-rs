use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePair, Value};
use std::fmt;

/// A collection of KeyValuePair entries for Track Extensions.
/// Per draft-16 Section 9.10, Track Extensions are encoded WITHOUT a count or length prefix.
/// They are simply a sequence of delta-encoded key-value pairs until end of message.
///
/// This differs from:
/// - KeyValuePairs: has a count prefix
/// - ExtensionHeaders: has a byte-length prefix
#[derive(Default, Clone, Eq, PartialEq)]
pub struct TrackExtensions(pub Vec<KeyValuePair>);

impl TrackExtensions {
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

    /// Get an integer value by key, returning None if not found or if the value is not an integer
    pub fn get_intvalue(&self, key: u64) -> Option<u64> {
        self.get(key).and_then(|kvp| match &kvp.value {
            Value::IntValue(v) => Some(*v),
            Value::BytesValue(_) => None,
        })
    }

    /// Get a bytes value by key, returning None if not found or if the value is not bytes
    pub fn get_bytesvalue(&self, key: u64) -> Option<&Vec<u8>> {
        self.get(key).and_then(|kvp| match &kvp.value {
            Value::IntValue(_) => None,
            Value::BytesValue(v) => Some(v),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Decode for TrackExtensions {
    /// Decode Track Extensions - reads delta-encoded key-value pairs until end of buffer.
    /// Per draft-16, Track Extensions have NO count or length prefix.
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let mut kvps = Vec::new();
        let mut prev_key: u64 = 0;

        // Read until buffer is exhausted
        while r.has_remaining() {
            // Read delta type
            let delta = u64::decode(r)?;

            // Reconstruct absolute key: prev_key + delta
            let key = prev_key.checked_add(delta).ok_or_else(|| {
                log::error!(
                    "[TrackExt] Delta type overflow: prev_key={}, delta={}",
                    prev_key,
                    delta
                );
                DecodeError::BoundsExceeded(crate::coding::BoundsExceeded)
            })?;

            let kvp = KeyValuePair::decode_value(key, r)?;
            kvps.push(kvp);
            prev_key = key;
        }

        Ok(TrackExtensions(kvps))
    }
}

impl Encode for TrackExtensions {
    /// Encode Track Extensions - writes delta-encoded key-value pairs WITHOUT any prefix.
    /// Entries are sorted by key in ascending order before encoding.
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        // Sort by key for delta encoding (Types must be in ascending order)
        let mut sorted: Vec<&KeyValuePair> = self.0.iter().collect();
        sorted.sort_by_key(|kvp| kvp.key);

        let mut prev_key: u64 = 0;
        for kvp in sorted {
            // Compute and encode the delta
            let delta = kvp.key.checked_sub(prev_key).ok_or_else(|| {
                log::error!(
                    "[TrackExt] Keys not sortable: prev_key={}, current_key={}",
                    prev_key,
                    kvp.key
                );
                EncodeError::InvalidValue
            })?;
            delta.encode(w)?;

            // Encode the value (without the key)
            kvp.encode_value(w)?;

            prev_key = kvp.key;
        }

        Ok(())
    }
}

impl fmt::Debug for TrackExtensions {
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
    fn encode_decode_empty() {
        let mut buf = BytesMut::new();

        let ext = TrackExtensions::new();
        ext.encode(&mut buf).unwrap();
        // Empty TrackExtensions produces NO bytes (no prefix!)
        let expected: Vec<u8> = vec![];
        assert_eq!(buf.to_vec(), expected);
        let decoded = TrackExtensions::decode(&mut buf).unwrap();
        assert_eq!(decoded, ext);
    }

    #[test]
    fn encode_decode_single() {
        let mut buf = BytesMut::new();

        let mut ext = TrackExtensions::new();
        ext.set_intvalue(2, 42); // key=2 (even), value=42
        ext.encode(&mut buf).unwrap();

        // Expected: delta=2, value=42 (no count or length prefix!)
        assert_eq!(buf.to_vec(), vec![0x02, 0x2a]);

        let decoded = TrackExtensions::decode(&mut buf).unwrap();
        assert_eq!(decoded, ext);
    }

    #[test]
    fn encode_decode_multiple() {
        let mut buf = BytesMut::new();

        let mut ext = TrackExtensions::new();
        ext.set_intvalue(0, 0);
        ext.set_intvalue(2, 100);
        ext.encode(&mut buf).unwrap();

        // Expected:
        // Entry 1: delta=0, value=0
        // Entry 2: delta=2 (from 0), value=100
        // No count prefix!
        #[rustfmt::skip]
        let expected = vec![
            0x00, 0x00,       // delta=0, value=0
            0x02, 0x40, 0x64, // delta=2, value=100 (varint)
        ];
        assert_eq!(buf.to_vec(), expected);

        let decoded = TrackExtensions::decode(&mut buf).unwrap();
        assert_eq!(decoded, ext);
    }
}
