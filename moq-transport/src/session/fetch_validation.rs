// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Range arithmetic and response validation shared by both ends of a FETCH.

use std::collections::HashMap;

use crate::{
    coding::{KeyValuePairs, Location},
    data::{effective_fetch_order, FetchRecord},
    message::{GroupOrder, TrackExtensions},
    serve::ServeError,
};

use super::SessionError;

/// Bound on distinct subgroups tracked per group while checking that a
/// subgroup's publisher priority does not change mid-response.
const MAX_SUBGROUPS_PER_GROUP: usize = 4096;

/// Convert a FETCH End Location to the inclusive last Location it covers.
///
/// Draft-18 §10.12.1 encodes End Location as "the end Location, plus 1", with
/// an Object of 0 meaning the whole group. FETCH_OK uses the same encoding
/// (§10.13).
pub(crate) fn inclusive_end(end: Location) -> Location {
    if end.object_id == 0 {
        Location::new(end.group_id, u64::MAX)
    } else {
        Location::new(end.group_id, end.object_id - 1)
    }
}

/// Whether a requested `start..end` range is non-empty under the draft-18
/// End Location encoding (§10.12.3: End MUST be the same or larger than Start).
pub(crate) fn fetch_range_is_valid(start: Location, end: Location) -> bool {
    start <= inclusive_end(end)
}

/// The exclusive End Location for a Joining FETCH whose Joining Location is
/// `largest` (§10.12.2.1: `{Joining Location.Group, Joining Location.Object + 1}`).
///
/// An Object ID of 2^64-1 has no `+1`; it is the last possible Object in the
/// group, so the whole-group encoding `{group, 0}` covers it exactly.
pub(crate) fn joining_fetch_end_location(largest: Location) -> Location {
    match largest.object_id.checked_add(1) {
        Some(next) => Location::new(largest.group_id, next),
        None => Location::new(largest.group_id, 0),
    }
}

/// Validate the parameters of a FETCH, sent or received.
///
/// GROUP_ORDER and SUBSCRIBER_PRIORITY have constrained values; a bad value is
/// a session-level PROTOCOL_VIOLATION (§10.2.7, §10.2.8).
pub(crate) fn validate_fetch_params(
    params: &KeyValuePairs,
) -> Result<(), crate::coding::DecodeError> {
    params.subscriber_priority()?;
    params.group_order()?;
    Ok(())
}

/// Checks a FETCH response against the request and against itself.
pub(crate) struct FetchValidator {
    range: Option<(Location, Location)>,
    /// A known start when the end is not: an Absolute Joining FETCH starts
    /// at `{joining_start, 0}` but its end is chosen by the publisher.
    min_start: Option<Location>,
    order: GroupOrder,
    previous: Option<Location>,
    largest: Option<Location>,
    priority_group: Option<u64>,
    priorities: HashMap<u64, u8>,
    response_end: Option<Location>,
}

impl FetchValidator {
    /// `range` is the requested `(start, end)` in wire encoding, if known.
    pub fn new(range: Option<(Location, Location)>, order: Option<GroupOrder>) -> Self {
        Self {
            range,
            min_start: None,
            order: effective_fetch_order(order),
            previous: None,
            largest: None,
            priority_group: None,
            priorities: HashMap::new(),
            response_end: None,
        }
    }

    /// Bound the response below by `start` when only the start is known.
    pub fn with_min_start(mut self, start: Option<Location>) -> Self {
        self.min_start = start;
        self
    }

    /// The requested start, from the full range or a start-only bound.
    fn start(&self) -> Option<Location> {
        self.range.map(|(start, _)| start).or(self.min_start)
    }

    /// Validate the next record on the response stream.
    ///
    /// Errors are [`ServeError::Size`], which callers turn into a
    /// MALFORMED_TRACK stream reset (§2.4.2) rather than a session close.
    pub fn validate_record(&mut self, record: &FetchRecord) -> Result<(), SessionError> {
        let location = record.location();
        self.validate_location(location)?;

        if self.priority_group != Some(location.group_id) {
            self.priority_group = Some(location.group_id);
            self.priorities.clear();
        }
        if let FetchRecord::Object(object) = record {
            if let Some(subgroup) = object.subgroup_id {
                match self.priorities.get(&subgroup) {
                    Some(existing) if *existing != object.publisher_priority => {
                        return Err(Self::malformed(
                            "publisher priority changed within a subgroup",
                        ));
                    }
                    Some(_) => {}
                    None if self.priorities.len() >= MAX_SUBGROUPS_PER_GROUP => {
                        return Err(Self::malformed("too many subgroups in one FETCH group"));
                    }
                    None => {
                        self.priorities.insert(subgroup, object.publisher_priority);
                    }
                }
            }
        }
        self.previous = Some(location);
        self.largest = Some(self.largest.map_or(location, |l| l.max(location)));
        Ok(())
    }

    /// Validate FETCH_OK. Violations are session-fatal (§10.13).
    pub fn validate_ok(
        &mut self,
        end: Location,
        extensions: &TrackExtensions,
    ) -> Result<(), SessionError> {
        // §10.13: "If End Location is smaller than the Start Location in the
        // corresponding FETCH the receiver MUST close the session".
        if self.start().is_some_and(|start| inclusive_end(end) < start) {
            return Err(Self::invalid_ok(
                "FETCH_OK end precedes the requested start",
            ));
        }
        if let Some((_, requested_end)) = self.range {
            if inclusive_end(end) > inclusive_end(requested_end) {
                return Err(Self::invalid_ok("FETCH_OK end exceeds the requested range"));
            }
        }
        if self.largest.is_some_and(|l| l > inclusive_end(end)) {
            return Err(Self::invalid_ok("FETCH_OK does not cover streamed records"));
        }
        extensions.default_publisher_priority()?;
        extensions.default_publisher_group_order()?;
        extensions.dynamic_groups()?;
        self.response_end = Some(end);
        Ok(())
    }

    fn validate_location(&self, location: Location) -> Result<(), SessionError> {
        if let Some((start, end)) = self.range {
            if location < start || location > inclusive_end(end) {
                return Err(Self::malformed("record is outside the requested range"));
            }
        }
        if self.min_start.is_some_and(|start| location < start) {
            return Err(Self::malformed("record precedes the requested start"));
        }
        if let Some(end) = self.response_end {
            if location > inclusive_end(end) {
                return Err(Self::malformed("record is beyond FETCH_OK coverage"));
            }
        }
        if let Some(previous) = self.previous {
            // §10.12.3: groups in the requested order; Object ID order within.
            if location.group_id == previous.group_id {
                if location.object_id <= previous.object_id {
                    return Err(Self::malformed(
                        "locations within a group are not strictly increasing",
                    ));
                }
            } else {
                let direction = if location.group_id > previous.group_id {
                    GroupOrder::Ascending
                } else {
                    GroupOrder::Descending
                };
                if direction != self.order {
                    return Err(Self::malformed("groups are not in the requested order"));
                }
            }
        }
        Ok(())
    }

    fn malformed(reason: &str) -> SessionError {
        tracing::debug!(reason, "malformed FETCH response");
        ServeError::Size.into()
    }

    fn invalid_ok(reason: &str) -> SessionError {
        SessionError::ProtocolViolation(reason.to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::data::{ExtensionHeaders, FetchRecordObject};

    use super::*;

    fn object(group_id: u64, subgroup_id: u64, object_id: u64, priority: u8) -> FetchRecord {
        FetchRecord::Object(FetchRecordObject {
            group_id,
            subgroup_id: Some(subgroup_id),
            object_id,
            publisher_priority: priority,
            extension_headers: ExtensionHeaders::default(),
            payload_length: 0,
        })
    }

    fn range() -> Option<(Location, Location)> {
        Some((Location::new(0, 0), Location::new(3, 0)))
    }

    #[test]
    fn inclusive_end_converts_exclusive_location_sentinels() {
        assert_eq!(inclusive_end(Location::new(4, 8)), Location::new(4, 7));
        assert_eq!(
            inclusive_end(Location::new(4, 0)),
            Location::new(4, u64::MAX)
        );
    }

    #[test]
    fn joining_end_is_exclusive_and_handles_max_object() {
        assert_eq!(
            joining_fetch_end_location(Location::new(5, 9)),
            Location::new(5, 10)
        );
        let max = Location::new(5, u64::MAX);
        let end = joining_fetch_end_location(max);
        assert_eq!(end, Location::new(5, 0));
        // The whole-group encoding still covers exactly the Joining Location.
        assert_eq!(inclusive_end(end), max);
    }

    #[test]
    fn range_validity() {
        assert!(fetch_range_is_valid(
            Location::new(1, 0),
            Location::new(1, 0)
        ));
        assert!(fetch_range_is_valid(
            Location::new(1, 3),
            Location::new(1, 4)
        ));
        assert!(!fetch_range_is_valid(
            Location::new(1, 4),
            Location::new(1, 4)
        ));
        assert!(!fetch_range_is_valid(
            Location::new(2, 0),
            Location::new(1, 5)
        ));
    }

    /// An Absolute Joining FETCH knows its start but not its end.
    #[test]
    fn start_only_bound_checks_fetch_ok_and_records() {
        let validator =
            || FetchValidator::new(None, None).with_min_start(Some(Location::new(4, 0)));
        let ext = TrackExtensions::default();
        assert!(validator().validate_ok(Location::new(3, 0), &ext).is_err());
        assert!(validator().validate_ok(Location::new(4, 1), &ext).is_ok());
        assert!(validator().validate_ok(Location::new(9, 0), &ext).is_ok());
        assert!(validator().validate_record(&object(3, 0, 9, 1)).is_err());
        assert!(validator().validate_record(&object(4, 0, 0, 1)).is_ok());
    }

    #[test]
    fn fetch_ok_must_stay_inside_requested_range() {
        let mut v = FetchValidator::new(
            Some((Location::new(0, 0), Location::new(1, 5))),
            Some(GroupOrder::Ascending),
        );
        assert!(v
            .validate_ok(Location::new(1, 6), &TrackExtensions::default())
            .is_err());
    }

    #[test]
    fn fetch_ok_before_start_is_a_violation() {
        let mut v = FetchValidator::new(Some((Location::new(2, 3), Location::new(3, 0))), None);
        assert!(matches!(
            v.validate_ok(Location::new(2, 3), &TrackExtensions::default()),
            Err(SessionError::ProtocolViolation(_))
        ));
        v.validate_ok(Location::new(2, 4), &TrackExtensions::default())
            .unwrap();
    }

    #[test]
    fn explicit_order_is_enforced() {
        let mut ascending = FetchValidator::new(range(), Some(GroupOrder::Ascending));
        ascending.validate_record(&object(2, 0, 0, 1)).unwrap();
        assert!(ascending.validate_record(&object(1, 0, 0, 1)).is_err());

        let mut descending = FetchValidator::new(range(), Some(GroupOrder::Descending));
        descending.validate_record(&object(2, 0, 0, 1)).unwrap();
        descending.validate_record(&object(1, 0, 0, 1)).unwrap();
        assert!(descending.validate_record(&object(2, 0, 1, 1)).is_err());
    }

    #[test]
    fn omitted_order_defaults_to_ascending() {
        // draft-18 §10.2.8: "If omitted from FETCH, the receiver uses Ascending".
        let mut v = FetchValidator::new(range(), None);
        v.validate_record(&object(2, 0, 0, 1)).unwrap();
        assert!(v.validate_record(&object(1, 0, 0, 1)).is_err());
    }

    #[test]
    fn objects_within_a_group_strictly_increase() {
        let mut v = FetchValidator::new(range(), None);
        v.validate_record(&object(1, 0, 4, 1)).unwrap();
        assert!(v.validate_record(&object(1, 1, 4, 1)).is_err());
    }

    #[test]
    fn ok_first_bounds_later_records() {
        let mut v = FetchValidator::new(range(), None);
        v.validate_ok(Location::new(0, 2), &TrackExtensions::default())
            .unwrap();
        assert!(v.validate_record(&object(0, 0, 2, 1)).is_err());
    }

    #[test]
    fn stream_first_bounds_later_ok() {
        let mut v = FetchValidator::new(range(), None);
        v.validate_record(&object(0, 0, 3, 1)).unwrap();
        assert!(v
            .validate_ok(Location::new(0, 3), &TrackExtensions::default())
            .is_err());
    }

    #[test]
    fn records_outside_range_are_malformed() {
        let mut v = FetchValidator::new(Some((Location::new(1, 2), Location::new(1, 5))), None);
        assert!(v.validate_record(&object(1, 0, 1, 1)).is_err());
        let mut v = FetchValidator::new(Some((Location::new(1, 2), Location::new(1, 5))), None);
        assert!(v.validate_record(&object(1, 0, 5, 1)).is_err());
    }

    #[test]
    fn priority_change_within_subgroup_is_malformed() {
        let mut v = FetchValidator::new(range(), None);
        v.validate_record(&object(0, 1, 0, 1)).unwrap();
        assert!(v.validate_record(&object(0, 1, 1, 2)).is_err());
    }

    #[test]
    fn priorities_are_cleared_at_group_boundaries() {
        let mut v = FetchValidator::new(range(), Some(GroupOrder::Ascending));
        for subgroup in 0..MAX_SUBGROUPS_PER_GROUP as u64 {
            v.validate_record(&object(0, subgroup, subgroup, 1))
                .unwrap();
        }
        assert!(v
            .validate_record(&object(0, MAX_SUBGROUPS_PER_GROUP as u64, 5000, 1))
            .is_err());
        let mut v = FetchValidator::new(range(), Some(GroupOrder::Ascending));
        v.validate_record(&object(0, 0, 0, 1)).unwrap();
        v.validate_record(&object(1, 0, 0, 2)).unwrap();
        assert_eq!(v.priorities.len(), 1);
    }
}
