use crate::coding::{Decode, DecodeError, Encode, EncodeError};
use std::fmt;

#[derive(Clone, Eq, PartialEq)]
pub enum Value {
    IntValue(u64),
    BytesValue(Vec<u8>),
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::IntValue(v) => write!(f, "{}", v),
            Value::BytesValue(bytes) => {
                // Show up to 16 bytes in hex for readability
                let preview: Vec<String> = bytes
                    .iter()
                    .take(16)
                    .map(|b| format!("{:02X}", b))
                    .collect();
                write!(f, "[{}]", preview.join(" "))
            }
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KeyValuePair {
    pub key: u64,
    pub value: Value,
}

impl KeyValuePair {
    pub fn new(key: u64, value: Value) -> Self {
        Self { key, value }
    }

    pub fn new_int(key: u64, value: u64) -> Self {
        Self {
            key,
            value: Value::IntValue(value),
        }
    }

    pub fn new_bytes(key: u64, value: Vec<u8>) -> Self {
        Self {
            key,
            value: Value::BytesValue(value),
        }
    }

    /// Validate that the key parity matches the value type.
    /// Even keys => IntValue, Odd keys => BytesValue.
    fn validate_key_parity(&self) -> Result<(), EncodeError> {
        match &self.value {
            Value::IntValue(_) => {
                if !self.key.is_multiple_of(2) {
                    return Err(EncodeError::InvalidValue);
                }
            }
            Value::BytesValue(_) => {
                if self.key.is_multiple_of(2) {
                    return Err(EncodeError::InvalidValue);
                }
            }
        }
        Ok(())
    }

    /// Encode only the value portion of this KVP (not the key/delta).
    /// The caller is responsible for encoding the key or delta type.
    pub(crate) fn encode_value<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.validate_key_parity()?;
        match &self.value {
            Value::IntValue(v) => {
                (*v).encode(w)?;
            }
            Value::BytesValue(v) => {
                v.len().encode(w)?;
                Self::encode_remaining(w, v.len())?;
                w.put_slice(v);
            }
        }
        Ok(())
    }

    /// Decode only the value portion of a KVP given the absolute key.
    /// The caller has already decoded the key/delta and resolved the absolute key.
    pub(crate) fn decode_value<R: bytes::Buf>(key: u64, r: &mut R) -> Result<Self, DecodeError> {
        if key.is_multiple_of(2) {
            // VarInt variant
            let value = u64::decode(r)?;
            log::trace!("[KVP] Decoded even key={}, value={}", key, value);
            Ok(KeyValuePair::new_int(key, value))
        } else {
            // Bytes variant
            let length = usize::decode(r)?;
            log::trace!("[KVP] Decoded odd key={}, length={}", key, length);
            if length > u16::MAX as usize {
                log::error!(
                    "[KVP] Length exceeded! key={}, length={} (max={})",
                    key,
                    length,
                    u16::MAX
                );
                return Err(DecodeError::KeyValuePairLengthExceeded());
            }

            Self::decode_remaining(r, length)?;
            let mut buf = vec![0; length];
            r.copy_to_slice(&mut buf);
            Ok(KeyValuePair::new_bytes(key, buf))
        }
    }
}

/// Legacy Decode for KeyValuePair — reads absolute key from wire.
/// Used only by ExtensionHeaders which reads KVPs from a bounded byte slice.
impl Decode for KeyValuePair {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let key = u64::decode(r)?;
        Self::decode_value(key, r)
    }
}

/// Legacy Encode for KeyValuePair — writes absolute key to wire.
/// Used only by ExtensionHeaders which writes KVPs into a temporary buffer.
impl Encode for KeyValuePair {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.validate_key_parity()?;
        self.key.encode(w)?;
        self.encode_value(w)
    }
}

impl fmt::Debug for KeyValuePair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{{}: {:?}}}", self.key, self.value)
    }
}

/// A collection of KeyValuePair entries, where the number of key-value-pairs are encoded/decoded first.
/// This structure is appropriate for Control message parameters.
///
/// Per draft-16 Section 1.4.2, Key-Value-Pairs use delta-encoded Type fields:
/// each Type is encoded as a delta from the previous Type (or from 0 for the first).
/// Entries are sorted by key (Type) in ascending order for encoding.
#[derive(Default, Clone, Eq, PartialEq)]
pub struct KeyValuePairs(pub Vec<KeyValuePair>);

// TODO: These set/get API's all assume no duplicate keys. We can add API's to support duplicates if needed.
impl KeyValuePairs {
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
}

impl Decode for KeyValuePairs {
    /// Decode Key-Value-Pairs with delta-encoded Type fields (draft-16 Section 1.4.2).
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let mut kvps = Vec::new();

        let count = u64::decode(r)?;
        let mut prev_key: u64 = 0;

        for _ in 0..count {
            // Read delta type
            let delta = u64::decode(r)?;

            // Reconstruct absolute key: prev_key + delta
            let key = prev_key.checked_add(delta).ok_or_else(|| {
                log::error!(
                    "[KVP] Delta type overflow: prev_key={}, delta={}",
                    prev_key,
                    delta
                );
                DecodeError::BoundsExceeded(crate::coding::BoundsExceeded)
            })?;

            let kvp = KeyValuePair::decode_value(key, r)?;
            kvps.push(kvp);
            prev_key = key;
        }

        Ok(KeyValuePairs(kvps))
    }
}

impl Encode for KeyValuePairs {
    /// Encode Key-Value-Pairs with delta-encoded Type fields (draft-16 Section 1.4.2).
    /// Entries are sorted by key in ascending order before encoding.
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.0.len().encode(w)?;

        // Sort by key for delta encoding (Types must be in ascending order)
        let mut sorted: Vec<&KeyValuePair> = self.0.iter().collect();
        sorted.sort_by_key(|kvp| kvp.key);

        let mut prev_key: u64 = 0;
        for kvp in sorted {
            // Compute and encode the delta
            let delta = kvp.key.checked_sub(prev_key).ok_or_else(|| {
                log::error!(
                    "[KVP] Keys not sortable: prev_key={}, current_key={}",
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

impl fmt::Debug for KeyValuePairs {
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
    use bytes::Bytes;
    use bytes::BytesMut;

    #[test]
    fn encode_decode_keyvaluepair() {
        let mut buf = BytesMut::new();

        // Type=1, VarInt value=0 - illegal with odd key/type
        let kvp = KeyValuePair::new(1, Value::IntValue(0));
        let encoded = kvp.encode(&mut buf);
        assert!(matches!(encoded.unwrap_err(), EncodeError::InvalidValue));

        // Type=0, VarInt value=0
        let kvp = KeyValuePair::new(0, Value::IntValue(0));
        kvp.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x00, 0x00]);
        let decoded = KeyValuePair::decode(&mut buf).unwrap();
        assert_eq!(decoded, kvp);

        // Type=100, VarInt value=100
        let kvp = KeyValuePair::new(100, Value::IntValue(100));
        kvp.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x40, 0x64, 0x40, 0x64]); // 2 2-byte VarInts with first 2 bits as 01
        let decoded = KeyValuePair::decode(&mut buf).unwrap();
        assert_eq!(decoded, kvp);

        // Type=0, Bytes value=[1,2,3,4,5] - illegal with even key/type
        let kvp = KeyValuePair::new(0, Value::BytesValue(vec![0x01, 0x02, 0x03, 0x04, 0x05]));
        let decoded = kvp.encode(&mut buf);
        assert!(matches!(decoded.unwrap_err(), EncodeError::InvalidValue));

        // Type=1, Bytes value=[1,2,3,4,5]
        let kvp = KeyValuePair::new(1, Value::BytesValue(vec![0x01, 0x02, 0x03, 0x04, 0x05]));
        kvp.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x01, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05]);
        let decoded = KeyValuePair::decode(&mut buf).unwrap();
        assert_eq!(decoded, kvp);
    }

    #[test]
    fn decode_badtype() {
        // Simulate a VarInt value of 5, but with an odd key/type
        let data: Vec<u8> = vec![0x01, 0x05];
        let mut buf: Bytes = data.into();
        let decoded = KeyValuePair::decode(&mut buf);
        assert!(matches!(decoded.unwrap_err(), DecodeError::More(_))); // Framing will be off now
    }

    #[test]
    fn encode_decode_keyvaluepairs_single() {
        let mut buf = BytesMut::new();

        // Single entry: key=1 (odd, bytes). Delta from 0 = 1.
        let mut kvps = KeyValuePairs::new();
        kvps.set_bytesvalue(1, vec![0x01, 0x02, 0x03, 0x04, 0x05]);
        kvps.encode(&mut buf).unwrap();
        assert_eq!(
            buf.to_vec(),
            vec![
                0x01, // 1 KeyValuePair
                // Delta=1 (from 0), then length=5, then data
                0x01, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05,
            ]
        );
        let decoded = KeyValuePairs::decode(&mut buf).unwrap();
        assert_eq!(decoded, kvps);
    }

    #[test]
    fn encode_decode_keyvaluepairs_multiple() {
        let mut buf = BytesMut::new();

        // Multiple entries inserted out of order — encoding should sort by key.
        // Keys: 0 (even, int), 1 (odd, bytes), 100 (even, int)
        let mut kvps = KeyValuePairs::new();
        kvps.set_intvalue(0, 0);
        kvps.set_intvalue(100, 100);
        kvps.set_bytesvalue(1, vec![0x01, 0x02, 0x03, 0x04, 0x05]);
        kvps.encode(&mut buf).unwrap();

        #[rustfmt::skip]
        let expected = vec![
            0x03, // 3 KeyValuePairs
            // Entry 1: key=0 (delta=0 from 0), even, int value=0
            0x00, 0x00,
            // Entry 2: key=1 (delta=1 from 0), odd, bytes len=5
            0x01, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05,
            // Entry 3: key=100 (delta=99 from 1), even, int value=100
            0x40, 0x63, 0x40, 0x64,
        ];
        assert_eq!(buf.to_vec(), expected);

        // Decode and verify — decoded entries will be in sorted order
        let decoded = KeyValuePairs::decode(&mut buf).unwrap();
        // Build expected sorted kvps for comparison
        let mut expected_kvps = KeyValuePairs::new();
        expected_kvps.set_intvalue(0, 0);
        expected_kvps.set_bytesvalue(1, vec![0x01, 0x02, 0x03, 0x04, 0x05]);
        expected_kvps.set_intvalue(100, 100);
        assert_eq!(decoded, expected_kvps);
    }

    #[test]
    fn encode_decode_keyvaluepairs_roundtrip_sorted() {
        let mut buf = BytesMut::new();

        // Insert in sorted order — should roundtrip exactly
        let mut kvps = KeyValuePairs::new();
        kvps.set_intvalue(2, 42);
        kvps.set_intvalue(4, 100);
        kvps.encode(&mut buf).unwrap();

        #[rustfmt::skip]
        let expected = vec![
            0x02, // 2 KeyValuePairs
            // Entry 1: key=2 (delta=2), int value=42
            0x02, 0x2a,
            // Entry 2: key=4 (delta=2 from 2), int value=100
            0x02, 0x40, 0x64,
        ];
        assert_eq!(buf.to_vec(), expected);

        let decoded = KeyValuePairs::decode(&mut buf).unwrap();
        assert_eq!(decoded, kvps);
    }

    #[test]
    fn encode_decode_keyvaluepairs_empty() {
        let mut buf = BytesMut::new();

        let kvps = KeyValuePairs::new();
        kvps.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x00]); // count=0
        let decoded = KeyValuePairs::decode(&mut buf).unwrap();
        assert_eq!(decoded, kvps);
    }
}
