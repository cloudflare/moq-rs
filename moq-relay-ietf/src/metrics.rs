//! Metrics instrumentation for moq-relay-ietf
//!
//! Metrics are always compiled in via the [`metrics`] crate facade. When no
//! recorder is installed the overhead is negligible (an atomic load + early
//! return per call site), similar to how the `log` crate works when no logger
//! is configured.
//!
//! To actually collect metrics, install a recorder at startup. The optional
//! `metrics-prometheus` feature adds a Prometheus exporter — see the binary
//! in `src/bin/moq-relay-ietf/main.rs` for an example.
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
//! | `moq_relay_connections_closed_total` | - | Total connections that have closed (graceful or error) |
//! | `moq_relay_connection_errors_total` | `stage` | Connection failures (stage: session_accept, session_run) |
//! | `moq_relay_publishers_total` | - | Total publishers (ANNOUNCE requests) received |
//! | `moq_relay_announce_ok_total` | - | Successful ANNOUNCE_OK responses sent |
//! | `moq_relay_announce_errors_total` | `phase` | Announce failures (phase: coordinator_register, local_register, send_ok) |
//! | `moq_relay_subscribers_total` | - | Total subscribers (SUBSCRIBE requests) received |
//! | `moq_relay_subscribe_not_found_total` | - | Track not found after checking all sources |
//! | `moq_relay_subscribe_route_errors_total` | - | Infrastructure failure when routing to remote |
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
//! | `moq_relay_subscribe_latency_seconds` | `source` | Time to resolve subscription (source: local, remote, not_found, route_error) |

// ============================================================================
// GaugeGuard - RAII guard for gauge increment/decrement
// ============================================================================

/// RAII guard that increments a gauge on creation and decrements on drop.
#[must_use = "GaugeGuard must be held for the duration you want the gauge incremented"]
pub struct GaugeGuard {
    name: &'static str,
}

impl GaugeGuard {
    pub fn new(name: &'static str) -> Self {
        metrics::gauge!(name).increment(1.0);
        Self { name }
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        metrics::gauge!(self.name).decrement(1.0);
    }
}

// ============================================================================
// TimingGuard - RAII guard for recording duration histograms
// ============================================================================

/// RAII guard that records elapsed time to a histogram on drop.
#[must_use = "TimingGuard must be held for the duration you want to measure"]
pub struct TimingGuard {
    name: &'static str,
    start: std::time::Instant,
    labels: Option<(&'static str, &'static str)>,
}

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
