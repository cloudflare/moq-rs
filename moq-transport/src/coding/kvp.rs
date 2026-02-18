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
}

impl Decode for KeyValuePair {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let key = u64::decode(r)?;

        if key % 2 == 0 {
            // VarInt variant
            let value = u64::decode(r)?;
            tracing::trace!("[KVP] Decoded even key={}, value={}", key, value);
            Ok(KeyValuePair::new_int(key, value))
        } else {
            // Bytes variant
            let length = usize::decode(r)?;
            tracing::trace!("[KVP] Decoded odd key={}, length={}", key, length);
            if length > u16::MAX as usize {
                tracing::error!(
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

impl Encode for KeyValuePair {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        match &self.value {
            Value::IntValue(v) => {
                // key must be even for IntValue
                if !self.key.is_multiple_of(2) {
                    return Err(EncodeError::InvalidValue);
                }
                self.key.encode(w)?;
                (*v).encode(w)?;
                Ok(())
            }
            Value::BytesValue(v) => {
                // key must be odd for BytesValue
                if self.key.is_multiple_of(2) {
                    return Err(EncodeError::InvalidValue);
                }
                self.key.encode(w)?;
                v.len().encode(w)?;
                Self::encode_remaining(w, v.len())?;
                w.put_slice(v);
                Ok(())
            }
        }
    }
}

impl fmt::Debug for KeyValuePair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{{}: {:?}}}", self.key, self.value)
    }
}

/// A collection of KeyValuePair entries, where the number of key-value-pairs are encoded/decoded first.
/// This structure is appropriate for Control message parameters.
/// Since duplicate parameters are allowed for unknown parameters, we don't do duplicate checking here.
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
}

impl Decode for KeyValuePairs {
    fn decode<R: bytes::Buf>(mut r: &mut R) -> Result<Self, DecodeError> {
        let mut kvps = Vec::new();

        let count = u64::decode(r)?;
        for _ in 0..count {
            let kvp = KeyValuePair::decode(&mut r)?;
            kvps.push(kvp);
        }

        Ok(KeyValuePairs(kvps))
    }
}

impl Encode for KeyValuePairs {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.0.len().encode(w)?;

        for kvp in &self.0 {
            kvp.encode(w)?;
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
    fn encode_decode_keyvaluepairs() {
        let mut buf = BytesMut::new();

        let mut kvps = KeyValuePairs::new();
        kvps.set_bytesvalue(1, vec![0x01, 0x02, 0x03, 0x04, 0x05]);
        kvps.encode(&mut buf).unwrap();
        assert_eq!(
            buf.to_vec(),
            vec![
                0x01, // 1 KeyValuePair
                0x01, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05, // Key=1, Value=[1,2,3,4,5]
            ]
        );
        let decoded = KeyValuePairs::decode(&mut buf).unwrap();
        assert_eq!(decoded, kvps);

        let mut kvps = KeyValuePairs::new();
        kvps.set_intvalue(0, 0);
        kvps.set_intvalue(100, 100);
        kvps.set_bytesvalue(1, vec![0x01, 0x02, 0x03, 0x04, 0x05]);
        kvps.encode(&mut buf).unwrap();
        let buf_vec = buf.to_vec();
        //  Validate the encoded length and the KeyValuePair count
        assert_eq!(14, buf_vec.len()); // 14 bytes total
        assert_eq!(3, buf_vec[0]); // 3 KeyValuePairs
        let decoded = KeyValuePairs::decode(&mut buf).unwrap();
        assert_eq!(decoded, kvps);
    }
}
