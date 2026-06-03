//! Known extension header type constants for the MOQT data plane.
//!
//! These extension headers can be attached to objects in subgroups, datagrams, and fetch streams.
//! See the MOQT specification for detailed semantics of each extension type.

/// Immutable Extensions (0xB)
///
/// A container extension header that wraps other extension headers that MUST NOT
/// be modified by relays or intermediaries. The contents of this extension header
/// should be preserved exactly as received when forwarding objects.
pub const IMMUTABLE_EXTENSIONS: u64 = 0xB;

/// Prior Group ID Gap (0x3C)
///
/// Indicates that one or more groups prior to this one are missing or unavailable.
/// The value is an integer indicating the number of missing prior groups.
/// This is used to signal discontinuities in the group sequence to subscribers.
pub const PRIOR_GROUP_ID_GAP: u64 = 0x3C;

/// Prior Object ID Gap (0x3E)
///
/// Indicates that one or more objects prior to this one within the same group/subgroup
/// are missing or unavailable. The value is an integer indicating the number of missing
/// prior objects. This is used to signal discontinuities in the object sequence.
pub const PRIOR_OBJECT_ID_GAP: u64 = 0x3E;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extension_type_values() {
        // Verify the spec-defined values
        assert_eq!(IMMUTABLE_EXTENSIONS, 0xB);
        assert_eq!(PRIOR_GROUP_ID_GAP, 0x3C);
        assert_eq!(PRIOR_OBJECT_ID_GAP, 0x3E);
    }
}
