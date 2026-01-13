/// Version-Specific Message Parameter Types
/// Used in SUBSCRIBE, SUBSCRIBE_OK, PUBLISH, FETCH, REQUEST_UPDATE, etc.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u64)]
pub enum ParameterType {
    /// Used in: REQUEST_OK, PUBLISH, PUBLISH_OK, SUBSCRIBE, SUBSCRIBE_OK, REQUEST_UPDATE
    DeliveryTimeout = 0x02,
    /// Used in: CLIENT_SETUP, SERVER_SETUP, PUBLISH, SUBSCRIBE, REQUEST_UPDATE,
    /// SUBSCRIBE_NAMESPACE, PUBLISH_NAMESPACE, TRACK_STATUS, FETCH
    AuthorizationToken = 0x03,
    /// Used in: PUBLISH, SUBSCRIBE_OK, FETCH_OK, REQUEST_OK
    MaxCacheDuration = 0x04,
    /// Used in: SUBSCRIBE_OK, PUBLISH, PUBLISH_OK
    Expires = 0x08,
    /// Used in: SUBSCRIBE_OK, PUBLISH, REQUEST_OK
    LargestObject = 0x09,
    /// Used in: SUBSCRIBE_OK, PUBLISH
    PublisherPriority = 0x0E,
    /// Used in: SUBSCRIBE, REQUEST_UPDATE, PUBLISH, PUBLISH_OK, SUBSCRIBE_NAMESPACE
    Forward = 0x10,
    /// Used in: SUBSCRIBE, FETCH, REQUEST_UPDATE, PUBLISH_OK
    SubscriberPriority = 0x20,
    /// Used in: SUBSCRIBE, PUBLISH_OK, REQUEST_UPDATE
    SubscriptionFilter = 0x21,
    /// Used in: SUBSCRIBE, SUBSCRIBE_OK, REQUEST_OK, PUBLISH, PUBLISH_OK, FETCH
    GroupOrder = 0x22,
    /// Used in: PUBLISH, SUBSCRIBE_OK
    DynamicGroups = 0x30,
    /// Used in: PUBLISH_OK, SUBSCRIBE, REQUEST_UPDATE
    NewGroupRequest = 0x32,
}

impl From<ParameterType> for u64 {
    fn from(value: ParameterType) -> Self {
        value as u64
    }
}
