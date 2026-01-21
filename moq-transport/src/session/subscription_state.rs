use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionState {
    Idle,
    Pending,
    Established,
    Terminated,
}

impl Default for SubscriptionState {
    fn default() -> Self {
        Self::Idle
    }
}

impl fmt::Display for SubscriptionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Idle => write!(f, "Idle"),
            Self::Pending => write!(f, "Pending"),
            Self::Established => write!(f, "Established"),
            Self::Terminated => write!(f, "Terminated"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionTransition {
    Publish,
    Subscribe,
    SubscribeOk,
    PublishOk,
    RequestError,
    Unsubscribe,
    PublishDone,
    RequestUpdate,
}

#[derive(Debug, Clone)]
pub struct InvalidTransition {
    pub from: SubscriptionState,
    pub transition: SubscriptionTransition,
}

impl fmt::Display for InvalidTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid transition {:?} from state {}",
            self.transition, self.from
        )
    }
}

impl std::error::Error for InvalidTransition {}

impl SubscriptionState {
    pub fn transition(self, t: SubscriptionTransition) -> Result<Self, InvalidTransition> {
        match (self, t) {
            (Self::Idle, SubscriptionTransition::Subscribe) => Ok(Self::Pending),
            (Self::Idle, SubscriptionTransition::Publish) => Ok(Self::Pending),
            (Self::Pending, SubscriptionTransition::SubscribeOk) => Ok(Self::Established),
            (Self::Pending, SubscriptionTransition::PublishOk) => Ok(Self::Established),
            (Self::Pending, SubscriptionTransition::RequestError) => Ok(Self::Terminated),
            (Self::Pending, SubscriptionTransition::Unsubscribe) => Ok(Self::Terminated),
            (Self::Pending, SubscriptionTransition::PublishDone) => Ok(Self::Terminated),
            (Self::Established, SubscriptionTransition::Unsubscribe) => Ok(Self::Terminated),
            (Self::Established, SubscriptionTransition::PublishDone) => Ok(Self::Terminated),
            (Self::Established, SubscriptionTransition::RequestUpdate) => Ok(Self::Established),
            _ => Err(InvalidTransition {
                from: self,
                transition: t,
            }),
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self, Self::Pending | Self::Established)
    }

    pub fn is_terminated(&self) -> bool {
        matches!(self, Self::Terminated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_transitions() {
        let state = SubscriptionState::Idle;

        let state = state.transition(SubscriptionTransition::Subscribe).unwrap();
        assert_eq!(state, SubscriptionState::Pending);

        let state = state
            .transition(SubscriptionTransition::SubscribeOk)
            .unwrap();
        assert_eq!(state, SubscriptionState::Established);

        let state = state
            .transition(SubscriptionTransition::Unsubscribe)
            .unwrap();
        assert_eq!(state, SubscriptionState::Terminated);
    }

    #[test]
    fn test_error_path() {
        let state = SubscriptionState::Idle;

        let state = state.transition(SubscriptionTransition::Subscribe).unwrap();
        assert_eq!(state, SubscriptionState::Pending);

        let state = state
            .transition(SubscriptionTransition::RequestError)
            .unwrap();
        assert_eq!(state, SubscriptionState::Terminated);
    }

    #[test]
    fn test_invalid_transition() {
        let state = SubscriptionState::Idle;

        let result = state.transition(SubscriptionTransition::SubscribeOk);
        assert!(result.is_err());
    }

    #[test]
    fn test_publish_done() {
        let state = SubscriptionState::Established;

        let state = state
            .transition(SubscriptionTransition::PublishDone)
            .unwrap();
        assert_eq!(state, SubscriptionState::Terminated);
    }
}
