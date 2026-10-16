// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Test {
    SetupOnly,
    AnnounceOnly,
    PublishNamespaceDone,
    SubscribeNamespaceLifecycle,
    SubscribeError,
    RendezvousTimeout,
    AnnounceSubscribe,
    SubscribeBeforeAnnounceHold,
    SubscribeBeforeAnnounceReject,
}

pub struct Entry {
    pub name: &'static str,
    pub variant: Option<&'static str>,
    pub test: Test,
    pub timeout: Duration,
}

pub const ENTRIES: &[Entry] = &[
    Entry {
        name: "setup-only",
        variant: None,
        test: Test::SetupOnly,
        timeout: Duration::from_secs(2),
    },
    Entry {
        name: "announce-only",
        variant: None,
        test: Test::AnnounceOnly,
        timeout: Duration::from_secs(2),
    },
    Entry {
        name: "publish-namespace-done",
        variant: None,
        test: Test::PublishNamespaceDone,
        timeout: Duration::from_secs(2),
    },
    Entry {
        name: "subscribe-namespace-lifecycle",
        variant: None,
        test: Test::SubscribeNamespaceLifecycle,
        timeout: Duration::from_secs(5),
    },
    Entry {
        name: "subscribe-error",
        variant: None,
        test: Test::SubscribeError,
        timeout: Duration::from_secs(2),
    },
    Entry {
        name: "rendezvous-timeout",
        variant: None,
        test: Test::RendezvousTimeout,
        timeout: Duration::from_secs(2),
    },
    Entry {
        name: "announce-subscribe",
        variant: None,
        test: Test::AnnounceSubscribe,
        timeout: Duration::from_secs(3),
    },
    Entry {
        name: "subscribe-before-announce",
        variant: Some("hold"),
        test: Test::SubscribeBeforeAnnounceHold,
        timeout: Duration::from_millis(3500),
    },
    Entry {
        name: "subscribe-before-announce",
        variant: Some("reject"),
        test: Test::SubscribeBeforeAnnounceReject,
        timeout: Duration::from_millis(3500),
    },
];

impl Test {
    pub fn needs_namespace(self) -> bool {
        matches!(
            self,
            Self::AnnounceOnly
                | Self::PublishNamespaceDone
                | Self::SubscribeNamespaceLifecycle
                | Self::AnnounceSubscribe
                | Self::SubscribeBeforeAnnounceHold
                | Self::SubscribeBeforeAnnounceReject
        )
    }

    pub fn needs_subscribe(self) -> bool {
        matches!(
            self,
            Self::SubscribeError
                | Self::RendezvousTimeout
                | Self::AnnounceSubscribe
                | Self::SubscribeBeforeAnnounceHold
                | Self::SubscribeBeforeAnnounceReject
        )
    }

    pub fn subscribe_first(self) -> bool {
        matches!(
            self,
            Self::SubscribeBeforeAnnounceHold | Self::SubscribeBeforeAnnounceReject
        )
    }

    pub fn namespace_deadline(self) -> bool {
        matches!(self, Self::AnnounceOnly | Self::PublishNamespaceDone)
    }
}

impl Entry {
    pub fn identifier(&self) -> String {
        match self.variant {
            Some(variant) => format!("{}@{variant}", self.name),
            None => self.name.to_owned(),
        }
    }
}

pub fn find(name: &str, variant: Option<&str>) -> Option<&'static Entry> {
    let variant = variant.filter(|value| !value.is_empty());
    ENTRIES
        .iter()
        .find(|entry| entry.name == name && entry.variant == variant)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listed_entries_are_selectable_and_variants_are_not_implicit() {
        let listed: Vec<_> = ENTRIES.iter().map(Entry::identifier).collect();
        assert_eq!(
            listed,
            [
                "setup-only",
                "announce-only",
                "publish-namespace-done",
                "subscribe-namespace-lifecycle",
                "subscribe-error",
                "rendezvous-timeout",
                "announce-subscribe",
                "subscribe-before-announce@hold",
                "subscribe-before-announce@reject"
            ]
        );
        for entry in ENTRIES {
            assert!(find(entry.name, entry.variant).is_some());
            assert!(find(entry.name, Some("unknown")).is_none());
            assert_eq!(
                find(entry.name, Some("")).is_some(),
                entry.variant.is_none()
            );
        }
        assert!(find("subscribe-before-announce", None).is_none());
    }
}
