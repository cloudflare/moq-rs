// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Extension headers for MoQT data-plane objects (§2.5, §10.2.1.2).
//!
//! On the wire, extension headers are encoded as a **byte-length prefix**
//! followed by a sequence of delta-encoded Key-Value-Pairs (same KVP format
//! as control-plane parameters).  The byte-length prefix distinguishes this
//! from [`KeyValuePairs`] which uses a count prefix.
//!
//! Because the pairs share a single running `prev` counter across the whole
//! sequence, the encoder sorts them by ascending key before writing.

use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePair};
use bytes::Buf;
use std::fmt;

/// Smallest possible encoded extension KVP: one-byte delta plus one-byte value.
const MIN_EXTENSION_KVP_WIRE_LEN: usize = 2;

/// A length-prefixed sequence of delta-encoded Key-Value-Pairs used for
/// data-plane object extension headers.
///
/// Keys are stored internally as absolute values; delta encoding is applied
/// only on the wire.
#[derive(Default, Clone, Eq, PartialEq)]
pub struct ExtensionHeaders(pub Vec<KeyValuePair>);

impl ExtensionHeaders {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace the entry with matching key.
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

impl Decode for ExtensionHeaders {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        // Extension headers are byte-length prefixed (unlike KeyValuePairs which
        // are count-prefixed).
        let length = usize::decode(r)?;
        Self::decode_remaining(r, length)?;

        if length == 0 {
            return Ok(ExtensionHeaders::new());
        }

        // Decode KVPs from the exact byte slice with a shared prev.
        let mut kvps_bytes = r.copy_to_bytes(length);

        let mut kvps = Vec::with_capacity(length / MIN_EXTENSION_KVP_WIRE_LEN);
        let mut prev = 0u64;

        while kvps_bytes.has_remaining() {
            let (pair, new_prev) = KeyValuePair::decode_with_prev(&mut kvps_bytes, prev)?;
            prev = new_prev;
            kvps.push(pair);
        }

        Ok(ExtensionHeaders(kvps))
    }
}

impl Encode for ExtensionHeaders {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        if self.0.is_empty() {
            0usize.encode(w)?;
            return Ok(());
        }

        // Encode into a temporary buffer to measure the byte length before writing
        // the length prefix.
        let mut tmp = bytes::BytesMut::new();

        if let [kvp] = self.0.as_slice() {
            kvp.encode_with_prev(&mut tmp, 0)?;
        } else {
            // Sort by ascending key so deltas are always non-negative.
            let mut sorted: Vec<&KeyValuePair> = self.0.iter().collect();
            sorted.sort_by_key(|k| k.key);
            let mut prev = 0u64;
            for kvp in &sorted {
                prev = kvp.encode_with_prev(&mut tmp, prev)?;
            }
        }

        // Write the byte-length prefix followed by the encoded pairs.
        tmp.len().encode(w)?;
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

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    // ── single pair ───────────────────────────────────────────────────────────

    #[test]
    fn single_bytes_pair_roundtrip() {
        // key=1 (odd, bytes), value=[0x01..0x05]
        // Wire: length=7, delta=1 (prev=0→abs=1), length_of_value=5, bytes
        let mut buf = BytesMut::new();
        let mut ext = ExtensionHeaders::new();
        ext.set_bytesvalue(1, vec![0x01, 0x02, 0x03, 0x04, 0x05]);
        ext.encode(&mut buf).unwrap();
        assert_eq!(
            buf.to_vec(),
            vec![
                0x07, // 7 bytes of KVP data
                0x01, // delta=1 → abs_type=1 (odd, bytes)
                0x05, 0x01, 0x02, 0x03, 0x04, 0x05, // length=5, value
            ]
        );
        let decoded = ExtensionHeaders::decode(&mut buf).unwrap();
        assert_eq!(decoded, ext);
    }

    // ── multiple pairs with correct delta encoding ────────────────────────────

    #[test]
    fn multi_pair_delta_encoding() {
        // Three pairs: key=0 (int=0), key=1 (bytes=[1..5]), key=100 (int=100)
        // Sorted order on wire: key=0, key=1, key=100
        // Deltas: 0→0=0 (even, int), 0→1=1 (odd, bytes), 1→100=99 (even, int)
        let mut ext = ExtensionHeaders::new();
        ext.set_intvalue(0, 0);
        ext.set_intvalue(100, 100);
        ext.set_bytesvalue(1, vec![0x01, 0x02, 0x03, 0x04, 0x05]);

        let mut buf = BytesMut::new();
        ext.encode(&mut buf).unwrap();

        // Manually compute expected bytes (QUIC varints ≥64 take 2 bytes):
        // key=0 (even,int): delta=0 (1B), value=0 (1B) = 2B
        // key=1 (odd,bytes): delta=1 (1B), length=5 (1B), 5 bytes = 7B
        // key=100 (even,int): delta=99 (2B, since 99≥64), value=100 (2B, since 100≥64) = 4B
        // total KVP bytes = 13; length prefix = 1B (13 < 64 so 1B varint)
        let buf_vec = buf.to_vec();
        assert_eq!(buf_vec[0], 13); // 13 bytes of KVP data
        assert_eq!(buf_vec.len(), 14); // 1 (length prefix) + 13

        // Decode and verify all three pairs survive
        let decoded = ExtensionHeaders::decode(&mut buf).unwrap();
        assert_eq!(decoded.0.len(), 3);
        assert_eq!(
            decoded.get(0).unwrap().value,
            crate::coding::Value::IntValue(0)
        );
        assert_eq!(
            decoded.get(100).unwrap().value,
            crate::coding::Value::IntValue(100)
        );
        assert_eq!(
            decoded.get(1).unwrap().value,
            crate::coding::Value::BytesValue(vec![0x01, 0x02, 0x03, 0x04, 0x05])
        );
    }

    // ── round-trip with out-of-order insertion ────────────────────────────────

    #[test]
    fn encode_sorts_before_delta() {
        // Insert in reverse order; encode must produce correct ascending deltas.
        let mut ext = ExtensionHeaders::new();
        ext.set_intvalue(100, 99);
        ext.set_intvalue(0, 1);

        let mut buf = BytesMut::new();
        ext.encode(&mut buf).unwrap();
        let decoded = ExtensionHeaders::decode(&mut buf).unwrap();

        assert_eq!(
            decoded.get(0).unwrap().value,
            crate::coding::Value::IntValue(1)
        );
        assert_eq!(
            decoded.get(100).unwrap().value,
            crate::coding::Value::IntValue(99)
        );
    }

    // ── empty ─────────────────────────────────────────────────────────────────

    #[test]
    fn empty_roundtrip() {
        let ext = ExtensionHeaders::new();
        let mut buf = BytesMut::new();
        ext.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x00]); // length=0
        let decoded = ExtensionHeaders::decode(&mut buf).unwrap();
        assert_eq!(decoded, ext);
    }
}
