//! Metrics instrumentation for moq-relay-ietf
//!
//! When the `metrics` feature is enabled, these emit to the `metrics` crate facade.
//! When disabled, they compile to nothing.
//!
//! # Available Metrics
//!
//! All metrics are prefixed with `moq_relay_` to avoid collisions.
//!
//! ## Counters
//!
//! | Name | Labels | Description |
//! |------|--------|-------------|
//! | `moq_relay_connections_total` | - | Total incoming connections accepted |
//! | `moq_relay_connection_errors_total` | `stage` | Connection failures (stage: session_accept, session_run) |
//! | `moq_relay_publishers_total` | - | Total publishers (ANNOUNCE requests) received |
//! | `moq_relay_announce_ok_total` | - | Successful ANNOUNCE_OK responses sent |
//! | `moq_relay_announce_errors_total` | `phase` | Announce failures (phase: coordinator_register, local_register, send_ok) |
//! | `moq_relay_subscribers_total` | - | Total subscribers (SUBSCRIBE requests) received |
//! | `moq_relay_subscribe_failures_total` | `reason` | Subscription failures (reason: not_found, route_error) |
//! | `moq_relay_upstream_errors_total` | `stage` | Upstream connection failures (stage: connect, session) |
//!
//! ## Gauges
//!
//! | Name | Description |
//! |------|-------------|
//! | `moq_relay_active_connections` | Current number of active client connections |
//! | `moq_relay_active_publishers` | Current number of active publishers |
//! | `moq_relay_active_subscriptions` | Current number of active subscriptions |
//! | `moq_relay_active_tracks` | Current number of tracks being served |
//! | `moq_relay_announced_namespaces` | Current number of registered namespaces |
//! | `moq_relay_upstream_connections` | Current number of upstream/origin connections |
//!
//! ## Histograms
//!
//! | Name | Labels | Description |
//! |------|--------|-------------|
//! | `moq_relay_subscribe_latency_seconds` | `source` | Time to resolve subscription (source: local, remote, not_found) |
//!
//! # Usage
//!
//! Enable the `metrics` feature in your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! moq-relay-ietf = { version = "0.7", features = ["metrics"] }
//! ```
//!
//! Then install a metrics recorder (e.g., `metrics-exporter-prometheus`) before
//! starting the relay. See the `metrics` crate documentation for details.

// ============================================================================
// GaugeGuard - RAII guard for gauge increment/decrement
// ============================================================================

/// RAII guard that increments a gauge on creation and decrements on drop.
/// No-op when metrics feature is disabled.
#[cfg(feature = "metrics")]
#[must_use = "GaugeGuard must be held for the duration you want the gauge incremented"]
pub struct GaugeGuard {
    name: &'static str,
}

#[cfg(feature = "metrics")]
impl GaugeGuard {
    pub fn new(name: &'static str) -> Self {
        metrics::gauge!(name).increment(1.0);
        Self { name }
    }
}

#[cfg(feature = "metrics")]
impl Drop for GaugeGuard {
    fn drop(&mut self) {
        metrics::gauge!(self.name).decrement(1.0);
    }
}

#[cfg(not(feature = "metrics"))]
#[must_use = "GaugeGuard must be held for the duration you want the gauge incremented"]
pub struct GaugeGuard;

#[cfg(not(feature = "metrics"))]
impl GaugeGuard {
    #[inline]
    pub fn new(_name: &'static str) -> Self {
        Self
    }
}

// ============================================================================
// TimingGuard - RAII guard for recording duration histograms
// ============================================================================

/// RAII guard that records elapsed time to a histogram on drop.
/// No-op when metrics feature is disabled.
#[cfg(feature = "metrics")]
#[must_use = "TimingGuard must be held for the duration you want to measure"]
pub struct TimingGuard {
    name: &'static str,
    start: std::time::Instant,
    labels: Option<(&'static str, &'static str)>,
}

#[cfg(feature = "metrics")]
impl TimingGuard {
    #[allow(dead_code)] // Keep API available for future histograms without labels
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            start: std::time::Instant::now(),
            labels: None,
        }
    }

    pub fn with_label(
        name: &'static str,
        label_key: &'static str,
        label_value: &'static str,
    ) -> Self {
        Self {
            name,
            start: std::time::Instant::now(),
            labels: Some((label_key, label_value)),
        }
    }

    /// Update the label value (useful when outcome determines the label)
    pub fn set_label(&mut self, label_key: &'static str, label_value: &'static str) {
        self.labels = Some((label_key, label_value));
    }
}

#[cfg(feature = "metrics")]
impl Drop for TimingGuard {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        if let Some((key, value)) = self.labels {
            metrics::histogram!(self.name, key => value).record(elapsed);
        } else {
            metrics::histogram!(self.name).record(elapsed);
        }
    }
}

#[cfg(not(feature = "metrics"))]
#[must_use = "TimingGuard must be held for the duration you want to measure"]
pub struct TimingGuard;

#[cfg(not(feature = "metrics"))]
impl TimingGuard {
    #[inline]
    #[allow(dead_code)] // Keep API available for future histograms without labels
    pub fn new(_name: &'static str) -> Self {
        Self
    }

    #[inline]
    pub fn with_label(
        _name: &'static str,
        _label_key: &'static str,
        _label_value: &'static str,
    ) -> Self {
        Self
    }

    #[inline]
    pub fn set_label(&mut self, _label_key: &'static str, _label_value: &'static str) {}
}
