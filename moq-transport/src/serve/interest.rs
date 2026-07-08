// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Downstream-interest tracking for deduplicated tracks.
//!
//! A relay deduplicates many downstream subscribers onto a single upstream
//! subscription. Nothing in the plain [`Track`](super::Track) machinery counts
//! how many downstream subscribers are currently interested, because the dedup
//! cache pins a reader clone that keeps the track alive indefinitely. Without a
//! separate count the upstream-subscribe task can never tell when the last
//! downstream subscriber has left, so it never drops the upstream subscription
//! (and therefore never emits an UNSUBSCRIBE).
//!
//! [`TrackInterest`] is that missing count. Each reader handed to a downstream
//! subscriber carries a [`TrackInterestGuard`] that increments the count while it
//! lives and decrements on drop. The pinned cache clone deliberately does not
//! hold a guard, so it is not counted as interest.

use crate::watch::State;

/// A shared counter of live downstream reader handles for a single deduplicated track.
///
/// Cheap to clone; all clones observe the same count. See the module docs for how
/// this plugs into the relay's dedup + upstream-unsubscribe logic.
#[derive(Clone, Default)]
pub struct TrackInterest {
    // Number of live downstream reader handles, wrapped in a watchable State so
    // tasks can await the count reaching (or leaving) zero.
    count: State<usize>,
}

impl TrackInterest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new interested reader, returning a guard that decrements the
    /// count when dropped.
    pub fn guard(&self) -> TrackInterestGuard {
        if let Some(mut count) = self.count.lock_mut() {
            *count += 1;
        }
        TrackInterestGuard {
            count: self.count.clone(),
        }
    }

    /// The current number of live interested readers.
    pub fn count(&self) -> usize {
        *self.count.lock()
    }

    /// Wait until there are no interested readers (the count reaches zero).
    /// Returns immediately if already idle.
    pub async fn idle(&self) {
        loop {
            let notify = {
                let count = self.count.lock();
                if *count == 0 {
                    return;
                }
                count.modified()
            };

            match notify {
                Some(notify) => notify.await,
                None => return,
            }
        }
    }

    /// Wait until there is at least one interested reader.
    /// Returns immediately if already busy.
    pub async fn busy(&self) {
        loop {
            let notify = {
                let count = self.count.lock();
                if *count > 0 {
                    return;
                }
                count.modified()
            };

            match notify {
                Some(notify) => notify.await,
                None => return,
            }
        }
    }
}

/// A guard representing a single downstream subscriber's interest in a track.
///
/// Held by the [`TrackReader`](super::TrackReader) handed to a downstream
/// subscriber. Cloning increments the interest count; dropping decrements it.
pub struct TrackInterestGuard {
    count: State<usize>,
}

impl Clone for TrackInterestGuard {
    fn clone(&self) -> Self {
        if let Some(mut count) = self.count.lock_mut() {
            *count += 1;
        }
        Self {
            count: self.count.clone(),
        }
    }
}

impl Drop for TrackInterestGuard {
    fn drop(&mut self) {
        if let Some(mut count) = self.count.lock_mut() {
            *count -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_increments_and_decrements() {
        let interest = TrackInterest::new();
        assert_eq!(interest.count(), 0);

        let a = interest.guard();
        assert_eq!(interest.count(), 1);

        let b = interest.guard();
        assert_eq!(interest.count(), 2);

        // Cloning a handed-out reader (and therefore its guard) also counts.
        let b2 = b.clone();
        assert_eq!(interest.count(), 3);

        drop(b2);
        assert_eq!(interest.count(), 2);

        drop(a);
        drop(b);
        assert_eq!(interest.count(), 0);
    }

    #[tokio::test]
    async fn idle_completes_when_count_reaches_zero() {
        let interest = TrackInterest::new();
        let guard = interest.guard();

        // idle() must not complete while a guard is alive.
        tokio::select! {
            _ = interest.idle() => panic!("idle() completed while interest was held"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }

        drop(guard);

        // idle() must complete promptly once the last guard is dropped.
        tokio::time::timeout(std::time::Duration::from_millis(100), interest.idle())
            .await
            .expect("idle() should complete after the last guard is dropped");
    }

    #[tokio::test]
    async fn busy_completes_when_a_guard_is_created() {
        let interest = TrackInterest::new();

        // busy() must not complete while there is no interest.
        tokio::select! {
            _ = interest.busy() => panic!("busy() completed with no interest"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }

        let _guard = interest.guard();

        tokio::time::timeout(std::time::Duration::from_millis(100), interest.busy())
            .await
            .expect("busy() should complete once a guard exists");
    }
}
