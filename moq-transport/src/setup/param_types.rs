/// Setup Parameter Types
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u64)]
pub enum ParameterType {
    Path = 0x1,
    MaxRequestId = 0x2,
    AuthorizationToken = 0x3,
    MaxAuthTokenCacheSize = 0x4,
    Authority = 0x5,
    /// Maximum number of Range pairs allowed per subscription/fetch (PR #1518)
    MaxFilterRanges = 0x6,
    MOQTImplementation = 0x7,
    /// Maximum value for MaxTracksSelected parameter in TRACK_FILTER (PR #1518)
    MaxTracksSelected = 0x8,
}

impl From<ParameterType> for u64 {
    fn from(value: ParameterType) -> Self {
        value as u64
    }
}
