// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;

use crate::{
    coding::{Location, VarInt},
    data::FetchRecord,
    message::{GroupOrder, TrackExtensions},
    serve::ServeError,
};

use super::SessionError;

const MAX_SUBGROUPS_PER_GROUP: usize = 4096;

pub(crate) struct FetchValidator {
    range: Option<(Location, Location)>,
    requested_order: Option<GroupOrder>,
    effective_order: Option<GroupOrder>,
    inferred_order: Option<GroupOrder>,
    previous: Option<Location>,
    largest: Option<Location>,
    priority_group: Option<u64>,
    priorities: HashMap<u64, u8>,
    response_end: Option<Location>,
}

impl FetchValidator {
    pub fn new(range: Option<(Location, Location)>, order: Option<GroupOrder>) -> Self {
        Self {
            range,
            requested_order: match order {
                Some(GroupOrder::Ascending | GroupOrder::Descending) => order,
                Some(GroupOrder::Publisher) | None => None,
            },
            effective_order: match order {
                Some(GroupOrder::Ascending | GroupOrder::Descending) => order,
                Some(GroupOrder::Publisher) | None => None,
            },
            inferred_order: None,
            previous: None,
            largest: None,
            priority_group: None,
            priorities: HashMap::new(),
            response_end: None,
        }
    }

    pub fn validate_record(&mut self, record: &FetchRecord) -> Result<(), SessionError> {
        let (location, priority) = match record {
            FetchRecord::Object(object) => (
                Location::new(object.group_id, object.object_id),
                object
                    .subgroup_id
                    .map(|subgroup| (subgroup, object.publisher_priority)),
            ),
            FetchRecord::NotExist { end } | FetchRecord::Unknown { end } => (*end, None),
        };
        self.validate_location(location)?;

        if self.priority_group != Some(location.group_id) {
            self.priority_group = Some(location.group_id);
            self.priorities.clear();
        }
        if let Some((subgroup, priority)) = priority {
            if self
                .priorities
                .get(&subgroup)
                .is_some_and(|existing| *existing != priority)
            {
                return Err(Self::malformed(
                    "publisher priority changed within a subgroup",
                ));
            }
            if !self.priorities.contains_key(&subgroup)
                && self.priorities.len() >= MAX_SUBGROUPS_PER_GROUP
            {
                return Err(Self::malformed("too many subgroups in one FETCH group"));
            }
            self.priorities.insert(subgroup, priority);
        }
        self.previous = Some(location);
        self.largest = Some(
            self.largest
                .map_or(location, |largest| largest.max(location)),
        );
        Ok(())
    }

    pub fn validate_ok(
        &mut self,
        end: Location,
        extensions: &TrackExtensions,
    ) -> Result<(), SessionError> {
        VarInt::try_from(end.group_id)?;
        VarInt::try_from(end.object_id)?;
        if let Some((start, requested_end)) = self.range {
            if end != start && inclusive_end(end) < start {
                return Err(Self::invalid_ok(
                    "FETCH_OK end precedes the requested start",
                ));
            }
            if inclusive_end(end) > inclusive_end(requested_end) {
                return Err(Self::invalid_ok("FETCH_OK end exceeds the requested range"));
            }
        }
        if self
            .largest
            .is_some_and(|largest| largest > inclusive_end(end))
        {
            return Err(Self::invalid_ok("FETCH_OK does not cover streamed records"));
        }

        let publisher_order = extensions
            .default_publisher_group_order()?
            .unwrap_or(GroupOrder::Ascending);
        let effective = self.requested_order.unwrap_or(publisher_order);
        if self
            .inferred_order
            .is_some_and(|inferred| inferred != effective)
        {
            return Err(Self::invalid_ok(
                "records do not use the effective group order",
            ));
        }
        self.effective_order = Some(effective);
        self.response_end = Some(end);
        Ok(())
    }

    fn validate_location(&mut self, location: Location) -> Result<(), SessionError> {
        if let Some((start, end)) = self.range {
            if location < start || location > inclusive_end(end) {
                return Err(Self::malformed("record is outside the requested range"));
            }
        }
        if let Some(end) = self.response_end {
            if location > inclusive_end(end) {
                return Err(Self::malformed("record is beyond FETCH_OK coverage"));
            }
        }
        if let Some(previous) = self.previous {
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
                if let Some(order) = self.effective_order.or(self.inferred_order) {
                    if direction != order {
                        return Err(Self::malformed("groups are not in the effective order"));
                    }
                } else {
                    self.inferred_order = Some(direction);
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

pub(crate) fn inclusive_end(end: Location) -> Location {
    if end.object_id == 0 {
        Location::new(end.group_id, VarInt::MAX.into_inner())
    } else {
        Location::new(end.group_id, end.object_id - 1)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        data::{ExtensionHeaders, FetchRecordObject},
        message::TrackExtensions,
    };

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

    #[test]
    fn fetch_ok_must_stay_inside_requested_range() {
        let mut validator = FetchValidator::new(
            Some((Location::new(0, 0), Location::new(1, 5))),
            Some(GroupOrder::Ascending),
        );
        assert!(validator
            .validate_ok(Location::new(1, 6), &TrackExtensions::default())
            .is_err());
    }

    #[test]
    fn explicit_order_is_enforced_before_fetch_ok() {
        let mut ascending = FetchValidator::new(
            Some((Location::new(0, 0), Location::new(3, 0))),
            Some(GroupOrder::Ascending),
        );
        ascending.validate_record(&object(2, 0, 0, 1)).unwrap();
        assert!(ascending.validate_record(&object(1, 0, 0, 1)).is_err());

        let mut descending = FetchValidator::new(
            Some((Location::new(0, 0), Location::new(3, 0))),
            Some(GroupOrder::Descending),
        );
        descending.validate_record(&object(1, 0, 0, 1)).unwrap();
        assert!(descending.validate_record(&object(2, 0, 0, 1)).is_err());
    }

    #[test]
    fn ok_first_bounds_later_records() {
        let mut validator = FetchValidator::new(
            Some((Location::new(0, 0), Location::new(1, 0))),
            Some(GroupOrder::Ascending),
        );
        validator
            .validate_ok(Location::new(0, 2), &TrackExtensions::default())
            .unwrap();
        assert!(validator.validate_record(&object(0, 0, 2, 1)).is_err());
    }

    #[test]
    fn stream_first_bounds_later_ok() {
        let mut validator = FetchValidator::new(
            Some((Location::new(0, 0), Location::new(1, 0))),
            Some(GroupOrder::Ascending),
        );
        validator.validate_record(&object(0, 0, 3, 1)).unwrap();
        assert!(validator
            .validate_ok(Location::new(0, 3), &TrackExtensions::default())
            .is_err());
    }

    #[test]
    fn publisher_default_descending_order_is_honored() {
        let mut validator =
            FetchValidator::new(Some((Location::new(0, 0), Location::new(3, 0))), None);
        validator.validate_record(&object(2, 0, 0, 1)).unwrap();
        validator.validate_record(&object(1, 0, 0, 1)).unwrap();
        let mut extensions = TrackExtensions::default();
        extensions
            .set_default_publisher_group_order(GroupOrder::Descending)
            .unwrap();
        validator
            .validate_ok(Location::new(2, 1), &extensions)
            .unwrap();
    }

    #[test]
    fn priorities_are_cleared_at_group_boundaries() {
        let mut validator = FetchValidator::new(
            Some((Location::new(0, 0), Location::new(3, 0))),
            Some(GroupOrder::Ascending),
        );
        for subgroup in 0..MAX_SUBGROUPS_PER_GROUP as u64 {
            validator
                .validate_record(&object(0, subgroup, subgroup, 1))
                .unwrap();
        }
        assert!(validator
            .validate_record(&object(0, MAX_SUBGROUPS_PER_GROUP as u64, 5000, 1))
            .is_err());
        validator.validate_record(&object(1, 0, 0, 2)).unwrap();
        assert_eq!(validator.priorities.len(), 1);
    }
}
