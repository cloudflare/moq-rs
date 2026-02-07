use std::sync::Arc;
use tokio::sync::RwLock;

#[doc(hidden)]
pub static GLOBAL_STATS_COLLECTOR: once_cell::sync::Lazy<Arc<RwLock<Box<dyn Stats>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(RwLock::new(Box::new(NoopStatsCollector::default()))));

/// Set a custom stats collector implementation.
///
/// This should be called early in application startup, before any stats are reported.
pub async fn set_collector(collector: Box<dyn Stats>) {
    let mut guard = GLOBAL_STATS_COLLECTOR.write().await;
    *guard = collector;
}

/// A simple trait for reporting relay stats.
///
/// Implementors decide what to do when these methods are called - whether to
/// update Prometheus counters, send to StatsD, log, or do nothing at all.
pub trait Stats: Send + Sync + 'static {
    /// Called when a new session is established.
    fn session_created(&self);

    /// Called when a session is closed.
    fn session_dropped(&self);

    /// publisher, subscriber should always be same because for each
    /// session we have one publisher and one subscriber
    /// Called when a new publisher is added.
    fn publisher_created(&self);

    /// Called when a publisher is removed.
    fn publisher_dropped(&self);

    /// Called when a new subscriber is added.
    fn subscriber_created(&self);

    /// Called when a subscriber is removed.
    fn subscriber_dropped(&self);

    /// Called when a created namespace is announced.
    fn namespace_created(&self);

    /// Called when a namespace is removed.
    fn namespace_dropped(&self);

    /// Called when a created track is published.
    fn track_published(&self);

    /// Called when a track is removed.
    fn track_unpublished(&self);

    /// Called when a namespace is announced
    fn namespace_announced(&self);

    /// Called when a namespace is removed
    fn namespace_unannounced(&self);
}

#[derive(Clone, Default)]
pub struct NoopStatsCollector {}

impl Stats for NoopStatsCollector {
    fn session_created(&self) {}
    fn session_dropped(&self) {}
    fn publisher_created(&self) {}
    fn publisher_dropped(&self) {}
    fn subscriber_created(&self) {}
    fn subscriber_dropped(&self) {}
    fn namespace_created(&self) {}
    fn namespace_dropped(&self) {}
    fn track_published(&self) {}
    fn track_unpublished(&self) {}
    fn namespace_announced(&self) {}
    fn namespace_unannounced(&self) {}
}

// ============================================================================
// Macros for reporting stats via GLOBAL_STATS_COLLECTOR
// ============================================================================

/// Report that a session was created.
#[macro_export]
macro_rules! session_created {
    () => {
        $crate::stats::GLOBAL_STATS_COLLECTOR
            .read()
            .await
            .session_created()
    };
}

/// Report that a session was dropped.
#[macro_export]
macro_rules! session_dropped {
    () => {
        $crate::stats::GLOBAL_STATS_COLLECTOR
            .read()
            .await
            .session_dropped()
    };
}

/// Report that a publisher was created.
#[macro_export]
macro_rules! publisher_created {
    () => {
        $crate::stats::GLOBAL_STATS_COLLECTOR
            .read()
            .await
            .publisher_created()
    };
}

/// Report that a publisher was dropped.
#[macro_export]
macro_rules! publisher_dropped {
    () => {
        $crate::stats::GLOBAL_STATS_COLLECTOR
            .read()
            .await
            .publisher_dropped()
    };
}

/// Report that a subscriber was created.
#[macro_export]
macro_rules! subscriber_created {
    () => {
        $crate::stats::GLOBAL_STATS_COLLECTOR
            .read()
            .await
            .subscriber_created()
    };
}

/// Report that a subscriber was dropped.
#[macro_export]
macro_rules! subscriber_dropped {
    () => {
        $crate::stats::GLOBAL_STATS_COLLECTOR
            .read()
            .await
            .subscriber_dropped()
    };
}

/// Report that a namespace was created.
#[macro_export]
macro_rules! namespace_created {
    () => {
        $crate::stats::GLOBAL_STATS_COLLECTOR
            .read()
            .await
            .namespace_created()
    };
}

/// Report that a namespace was dropped.
#[macro_export]
macro_rules! namespace_dropped {
    () => {
        $crate::stats::GLOBAL_STATS_COLLECTOR
            .read()
            .await
            .namespace_dropped()
    };
}

/// Report that a track was published.
#[macro_export]
macro_rules! track_published {
    () => {
        $crate::stats::GLOBAL_STATS_COLLECTOR
            .read()
            .await
            .track_published()
    };
}

/// Report that a track was unpublished.
#[macro_export]
macro_rules! track_unpublished {
    () => {
        $crate::stats::GLOBAL_STATS_COLLECTOR
            .read()
            .await
            .track_unpublished()
    };
}

/// Report that a namespace was announced.
#[macro_export]
macro_rules! namespace_announced {
    () => {
        $crate::stats::GLOBAL_STATS_COLLECTOR
            .read()
            .await
            .namespace_announced()
    };
}

/// Report that a namespace was unannounced.
#[macro_export]
macro_rules! namespace_unannounced {
    () => {
        $crate::stats::GLOBAL_STATS_COLLECTOR
            .read()
            .await
            .namespace_unannounced()
    };
}
