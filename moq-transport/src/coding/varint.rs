// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

// Variable-length integer encoding for MoQ Transport draft-18+.
//
// Uses the "leading-1-bits" encoding defined in draft-ietf-moq-transport-17 §1.4.1:
// Count the leading 1-bits of the first byte to determine the total length.
// Non-minimal encodings are explicitly allowed by the spec.
//
// | Leading bits | Total bytes | Usable bits | Max value         |
// |--------------|-------------|-------------|-------------------|
// | 0            | 1           | 7           | 127               |
// | 10           | 2           | 14          | 16383             |
// | 110          | 3           | 21          | 2097151           |
// | 1110         | 4           | 28          | 268435455         |
// | 11110        | 5           | 35          | 34359738367       |
// | 111110       | 6           | 42          | 4398046511103     |
// | 1111110      | 7           | 49          | 562949953421311   |
// | 11111110     | 8           | 56          | 72057594037927935 |
// | 11111111     | 9           | 64          | u64::MAX          |

use std::convert::TryFrom;
use std::fmt;

use thiserror::Error;

use super::{Decode, DecodeError, Encode, EncodeError};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
#[error("value out of range")]
pub struct BoundsExceeded;

/// A variable-length integer (vi64) as defined in MoQ Transport draft-18.
///
/// All u64 values are representable. The encoding uses 1-9 bytes with
/// leading-1-bits to signal the length.
#[derive(Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VarInt(u64);

impl VarInt {
    /// The largest representable value.
    pub const MAX: Self = Self(u64::MAX);

    /// The smallest possible value.
    pub const ZERO: Self = Self(0);

    /// Construct a `VarInt` infallibly from a u32.
    pub const fn from_u32(x: u32) -> Self {
        Self(x as u64)
    }

    /// Extract the integer value.
    pub const fn into_inner(self) -> u64 {
        self.0
    }
}

impl From<VarInt> for u64 {
    fn from(x: VarInt) -> Self {
        x.0
    }
}

impl From<VarInt> for usize {
    fn from(x: VarInt) -> Self {
        x.0 as usize
    }
}

impl From<VarInt> for u128 {
    fn from(x: VarInt) -> Self {
        x.0 as u128
    }
}

impl From<u8> for VarInt {
    fn from(x: u8) -> Self {
        Self(x.into())
    }
}

impl From<u16> for VarInt {
    fn from(x: u16) -> Self {
        Self(x.into())
    }
}

impl From<u32> for VarInt {
    fn from(x: u32) -> Self {
        Self(x.into())
    }
}

impl From<u64> for VarInt {
    fn from(x: u64) -> Self {
        Self(x)
    }
}


impl TryFrom<u128> for VarInt {
    type Error = BoundsExceeded;

    /// Succeeds iff `x` fits in a u64.
    fn try_from(x: u128) -> Result<Self, BoundsExceeded> {
        if x <= u64::MAX as u128 {
            Ok(Self(x as u64))
        } else {
            Err(BoundsExceeded)
        }
    }
}

impl TryFrom<usize> for VarInt {
    type Error = BoundsExceeded;

    fn try_from(x: usize) -> Result<Self, BoundsExceeded> {
        Ok(Self(x as u64))
    }
}

impl TryFrom<VarInt> for u32 {
    type Error = BoundsExceeded;

    fn try_from(x: VarInt) -> Result<Self, BoundsExceeded> {
        if x.0 <= u32::MAX.into() {
            Ok(x.0 as u32)
        } else {
            Err(BoundsExceeded)
        }
    }
}

impl TryFrom<VarInt> for u16 {
    type Error = BoundsExceeded;

    fn try_from(x: VarInt) -> Result<Self, BoundsExceeded> {
        if x.0 <= u16::MAX.into() {
            Ok(x.0 as u16)
        } else {
            Err(BoundsExceeded)
        }
    }
}

impl TryFrom<VarInt> for u8 {
    type Error = BoundsExceeded;

    fn try_from(x: VarInt) -> Result<Self, BoundsExceeded> {
        if x.0 <= u8::MAX.into() {
            Ok(x.0 as u8)
        } else {
            Err(BoundsExceeded)
        }
    }
}

impl fmt::Debug for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Determine the minimal number of bytes needed to encode `x`.
#[inline]
fn encoded_size(x: u64) -> usize {
    if x <= 0x7F { 1 }
    else if x <= 0x3FFF { 2 }
    else if x <= 0x1F_FFFF { 3 }
    else if x <= 0x0FFF_FFFF { 4 }
    else if x <= 0x07_FFFF_FFFF { 5 }
    else if x <= 0x03FF_FFFF_FFFF { 6 }
    else if x <= 0x01_FFFF_FFFF_FFFF { 7 }
    else if x <= 0x00FF_FFFF_FFFF_FFFF { 8 }
    else { 9 }
}

impl Decode for VarInt {
    /// Decode a leading-1-bits varint from the given reader.
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        Self::decode_remaining(r, 1)?;
        let first = r.get_u8();

        let ones = (!first).leading_zeros() as usize; // count of leading 1-bits
        let extra = ones.min(8); // additional bytes after the first

        Self::decode_remaining(r, extra)?;

        let value = match extra {
            0 => {
                // 0xxxxxxx -- 7 usable bits
                u64::from(first & 0x7F)
            }
            8 => {
                // 11111111 + 8 more bytes -- full u64
                let mut buf = [0u8; 8];
                r.copy_to_slice(&mut buf);
                u64::from_be_bytes(buf)
            }
            7 => {
                // 11111110 + 7 more bytes -- 56 usable bits, no data bits in first byte
                let mut buf = [0u8; 8];
                r.copy_to_slice(&mut buf[1..]);
                u64::from_be_bytes(buf)
            }
            n => {
                // n leading 1-bits, then a 0-bit, then (7 - n) data bits in first byte,
                // plus n more bytes. Mask off the tag: clear the top (n+1) bits.
                let mask = 0xFFu8 >> (n + 1);
                let mut buf = [0u8; 8];
                buf[8 - 1 - n] = first & mask;
                r.copy_to_slice(&mut buf[8 - n..]);
                u64::from_be_bytes(buf)
            }
        };

        Ok(Self(value))
    }
}

impl Encode for VarInt {
    /// Encode a leading-1-bits varint to the given writer.
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        let x = self.0;
        let size = encoded_size(x);

        Self::encode_remaining(w, size)?;

        match size {
            1 => {
                w.put_u8(x as u8);
            }
            9 => {
                w.put_u8(0xFF);
                w.put_u64(x);
            }
            n => {
                // n bytes total: (n-1) leading 1-bits, then 0-bit, then data.
                let first_data = (x >> ((n - 1) * 8)) as u8;
                let tag: u8 = !((1u16 << (9 - n)) - 1) as u8;
                debug_assert!(first_data < (1u8 << (8 - n)));
                w.put_u8(tag | first_data);
                let remaining = x.to_be_bytes();
                w.put_slice(&remaining[8 - (n - 1)..]);
            }
        }

        Ok(())
    }
}

// Encode/Decode u64 via VarInt for wire convenience.
impl Encode for u64 {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        VarInt::from(*self).encode(w)
    }
}

impl Decode for u64 {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        VarInt::decode(r).map(|v| v.into_inner())
    }
}

impl Encode for usize {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        VarInt::from(*self as u64).encode(w)
    }
}

impl Decode for usize {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let var = VarInt::decode(r)?;
        #[allow(clippy::unnecessary_fallible_conversions)]
        usize::try_from(var).map_err(|_| DecodeError::BoundsExceeded(BoundsExceeded))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn round_trip(value: u64) {
        let v = VarInt::from(value);
        let mut buf = BytesMut::new();
        v.encode(&mut buf).unwrap();
        let decoded = VarInt::decode(&mut buf).unwrap();
        assert_eq!(decoded, v, "round-trip failed for {value}");
        assert!(buf.is_empty(), "leftover bytes for {value}");
    }

    fn encode_check(value: u64, expected: &[u8]) {
        let v = VarInt::from(value);
        let mut buf = BytesMut::new();
        v.encode(&mut buf).unwrap();
        assert_eq!(&buf[..], expected, "encode mismatch for {value}");
    }

    fn decode_check(bytes: &[u8], expected: u64) {
        let mut buf = BytesMut::from(bytes);
        let v = VarInt::decode(&mut buf).unwrap();
        assert_eq!(v.into_inner(), expected, "decode mismatch for {bytes:?}");
        assert!(buf.is_empty(), "leftover bytes for {bytes:?}");
    }

    // --- Test vectors from draft-ietf-moq-transport-18 Section 1.4.1 ---

    #[test]
    fn spec_37() { encode_check(37, &[0x25]); decode_check(&[0x25], 37); }

    #[test]
    fn spec_37_nonminimal() { decode_check(&[0x80, 0x25], 37); }

    #[test]
    fn spec_15293() { encode_check(15293, &[0xBB, 0xBD]); decode_check(&[0xBB, 0xBD], 15293); }

    #[test]
    fn spec_226442877() {
        encode_check(226442877, &[0xED, 0x7F, 0x3E, 0x7D]);
        decode_check(&[0xED, 0x7F, 0x3E, 0x7D], 226442877);
    }

    #[test]
    fn spec_2893212287960() {
        encode_check(2893212287960, &[0xFA, 0xA1, 0xA0, 0xE4, 0x03, 0xD8]);
        decode_check(&[0xFA, 0xA1, 0xA0, 0xE4, 0x03, 0xD8], 2893212287960);
    }

    #[test]
    fn spec_151288809941952() {
        encode_check(151288809941952, &[0xFC, 0x89, 0x98, 0xAB, 0xC6, 0x6B, 0xC0]);
        decode_check(&[0xFC, 0x89, 0x98, 0xAB, 0xC6, 0x6B, 0xC0], 151288809941952);
    }

    #[test]
    fn spec_70423237261249041() {
        encode_check(70423237261249041, &[0xFE, 0xFA, 0x31, 0x8F, 0xA8, 0xE3, 0xCA, 0x11]);
        decode_check(&[0xFE, 0xFA, 0x31, 0x8F, 0xA8, 0xE3, 0xCA, 0x11], 70423237261249041);
    }

    #[test]
    fn spec_u64_max() {
        encode_check(u64::MAX, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
        decode_check(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], u64::MAX);
    }

    // --- Boundary values ---

    #[test]
    fn boundary_1byte() { round_trip(0); round_trip(0x7F); encode_check(0, &[0x00]); encode_check(0x7F, &[0x7F]); }

    #[test]
    fn boundary_2byte() { round_trip(0x80); round_trip(0x3FFF); encode_check(0x80, &[0x80, 0x80]); }

    #[test]
    fn boundary_3to8() {
        round_trip(0x4000); round_trip(0x1F_FFFF);
        round_trip(0x20_0000); round_trip(0x0FFF_FFFF);
        round_trip(0x1000_0000); round_trip(0x07_FFFF_FFFF);
        round_trip(0x08_0000_0000); round_trip(0x03FF_FFFF_FFFF);
        round_trip(0x0400_0000_0000); round_trip(0x01_FFFF_FFFF_FFFF);
        round_trip(0x02_0000_0000_0000); round_trip(0x00FF_FFFF_FFFF_FFFF);
    }

    #[test]
    fn boundary_9byte() { round_trip(0x0100_0000_0000_0000); round_trip(u64::MAX); round_trip(u64::MAX - 1); }

    #[test]
    fn round_trip_small() { for i in 0..=200 { round_trip(i); } }

    #[test]
    fn round_trip_large() { round_trip(1_000_000); round_trip(1_000_000_000); round_trip(u64::MAX / 2); }

    #[test]
    fn nonminimal_zero() { decode_check(&[0x80, 0x00], 0); }

    #[test]
    fn nonminimal_one() { decode_check(&[0xC0, 0x00, 0x01], 1); }

    #[test]
    fn setup_stream_type_0x2f00() { encode_check(0x2F00, &[0xAF, 0x00]); round_trip(0x2F00); }

    #[test]
    fn from_u64_always_succeeds() { assert_eq!(VarInt::try_from(u64::MAX).unwrap(), VarInt::MAX); }

    #[test]
    fn max_is_u64_max() { assert_eq!(VarInt::MAX.into_inner(), u64::MAX); }
}
