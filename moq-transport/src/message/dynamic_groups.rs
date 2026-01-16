//! Dynamic Groups support for MOQT.
//!
//! This module provides helper functions for working with Dynamic Groups parameters
//! as defined in the MOQT specification. Dynamic Groups allow subscribers to request
//! publishers to create new groups on demand.

use crate::coding::KeyValuePairs;
use crate::message::ParameterType;

/// Helper trait for Dynamic Groups parameter operations on KeyValuePairs.
pub trait DynamicGroupsExt {
    /// Check if dynamic groups are enabled/supported
    fn has_dynamic_groups(&self) -> bool;

    /// Get the dynamic groups value (if present)
    fn get_dynamic_groups(&self) -> Option<u64>;

    /// Enable dynamic groups support
    fn set_dynamic_groups(&mut self, value: u64);

    /// Check if a new group request is present
    fn has_new_group_request(&self) -> bool;

    /// Get the new group request value (if present)
    fn get_new_group_request(&self) -> Option<u64>;

    /// Request a new group from the publisher
    fn set_new_group_request(&mut self, value: u64);
}

impl DynamicGroupsExt for KeyValuePairs {
    fn has_dynamic_groups(&self) -> bool {
        self.has(ParameterType::DynamicGroups.into())
    }

    fn get_dynamic_groups(&self) -> Option<u64> {
        self.get_intvalue(ParameterType::DynamicGroups.into())
    }

    fn set_dynamic_groups(&mut self, value: u64) {
        self.set_intvalue(ParameterType::DynamicGroups.into(), value);
    }

    fn has_new_group_request(&self) -> bool {
        self.has(ParameterType::NewGroupRequest.into())
    }

    fn get_new_group_request(&self) -> Option<u64> {
        self.get_intvalue(ParameterType::NewGroupRequest.into())
    }

    fn set_new_group_request(&mut self, value: u64) {
        self.set_intvalue(ParameterType::NewGroupRequest.into(), value);
    }
}

/// Dynamic Groups configuration for a track
#[derive(Clone, Debug, Default)]
pub struct DynamicGroupsConfig {
    /// Whether dynamic groups are enabled for this track
    pub enabled: bool,
    /// The current pending new group request (if any)
    pub pending_request: Option<u64>,
}

impl DynamicGroupsConfig {
    /// Create a new configuration with dynamic groups disabled
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new configuration with dynamic groups enabled
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            pending_request: None,
        }
    }

    /// Request a new group with the given request ID
    pub fn request_new_group(&mut self, request_id: u64) {
        self.pending_request = Some(request_id);
    }

    /// Clear the pending request (after it has been processed)
    pub fn clear_pending_request(&mut self) {
        self.pending_request = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dynamic_groups_ext() {
        let mut params = KeyValuePairs::new();

        // Initially no dynamic groups
        assert!(!params.has_dynamic_groups());
        assert_eq!(params.get_dynamic_groups(), None);

        // Enable dynamic groups
        params.set_dynamic_groups(1);
        assert!(params.has_dynamic_groups());
        assert_eq!(params.get_dynamic_groups(), Some(1));

        // New group request
        assert!(!params.has_new_group_request());
        params.set_new_group_request(42);
        assert!(params.has_new_group_request());
        assert_eq!(params.get_new_group_request(), Some(42));
    }

    #[test]
    fn test_dynamic_groups_config() {
        let config = DynamicGroupsConfig::new();
        assert!(!config.enabled);
        assert!(config.pending_request.is_none());

        let config = DynamicGroupsConfig::enabled();
        assert!(config.enabled);

        let mut config = DynamicGroupsConfig::enabled();
        config.request_new_group(123);
        assert_eq!(config.pending_request, Some(123));

        config.clear_pending_request();
        assert!(config.pending_request.is_none());
    }
}
