// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Metrics instrumentation for moq-relay-ietf
//!
//! Metrics are always compiled in via the [`metrics`] crate facade. When no
//! recorder is installed, each facade call is only an atomic load and early
//! return. The per-scope `moq_relay_connection_*` metrics are opt-in: they are
//! emitted only after [`crate::Relay::with_connection_metrics`].
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
//! | `moq_relay_connection_errors_total` | `stage` | Connection failures (stage: session_accept, scope_resolve, auth_setup, session_run) |
//! | `moq_relay_publishers_total` | - | Total publishers (PUBLISH_NAMESPACE requests) received |
//! | `moq_relay_published_tracks_total` | - | Total tracks offered via PUBLISH |
//! | `moq_relay_announce_ok_total` | `kind` | Successful REQUEST_OK responses sent for PUBLISH_NAMESPACE (kind: client, proxied) |
//! | `moq_relay_announce_errors_total` | `phase` | PUBLISH_NAMESPACE failures (phase: auth, local_register, remote_register, coordinator_register, coordinator_lookup, send_ok, forward, peer_fanout) |
//! | `moq_relay_publish_errors_total` | `phase` | PUBLISH failures (phase: auth, auth_fanout, session_limit, take_reader, local_register, coordinator_register, send_ok) |
//! | `moq_relay_subscribers_total` | - | Total subscribers (SUBSCRIBE requests) received |
//! | `moq_relay_subscribe_errors_total` | `phase` | SUBSCRIBE rejected by the relay (phase: auth) |
//! | `moq_relay_subscribe_namespace_errors_total` | `phase` | SUBSCRIBE_NAMESPACE rejected by the relay (phase: auth) |
//! | `moq_relay_track_status_errors_total` | `phase` | TRACK_STATUS rejected by the relay (phase: auth) |
//! | `moq_relay_fetch_errors_total` | `phase` | Standalone FETCH rejected by the relay (phase: auth) |
//! | `moq_relay_subscribe_not_found_total` | - | Track not found after checking all sources |
//! | `moq_relay_subscribe_route_errors_total` | - | Infrastructure failure when routing to remote |
//! | `moq_relay_subscribe_upstream_errors_total` | - | Upstream subscription could not be established, so the downstream SUBSCRIBE was rejected |
//! | `moq_relay_upstream_errors_total` | `stage` | Upstream connection failures (stage: connect, session) |
//! | `moq_relay_namespace_transition_timeouts_total` | - | Namespace pull streams reset after graceful transition timeout |
//! | `moq_relay_cache_idle_evictions_total` | `source` | Unwatched cache entries evicted, releasing an upstream subscription (source: local, remote) |
//! | `moq_relay_change_channel_lagged_total` | `channel` | Change notifications skipped by a lagging receiver, forcing a resync (channel: namespace, track) |
//! | `moq_relay_lease_registry_lock_poisoned_total` | `operation` | Upstream namespace lease registry lock found poisoned (operation: acquire, release) |
//! | `moq_relay_auth_denied_total` | `phase`, `operation`, `reason` | Operations refused by the authorization hook. `phase`: setup, request. `operation`: client_setup, publish_namespace, publish, subscribe, subscribe_namespace, track_status. `reason`: token_missing, token_invalid, token_expired, token_replayed, token_malformed, scope_mismatch, issuer_unknown, policy_denied, hook_fault, alias_in_setup, too_many_tokens |
//! | `moq_relay_auth_errors_total` | `stage` | Authorization could not reach a verdict (stage: scope_config, setup, request). Distinct from `auth_denied_total`: these indicate a relay or configuration fault, not a rejected peer, and are the ones worth alerting on |
//! | `moq_relay_connection_ingress_bytes_total` | `scope`, `scope_state`, `interface` | QUIC ingress bytes, added by each connection every 10 s, on relabel and on close |
//! | `moq_relay_connection_egress_bytes_total` | `scope`, `scope_state`, `interface` | QUIC egress bytes, added the same way |
//! | `moq_relay_connection_metrics_errors_total` | `reason` | Connection metric faults (reason: counter_regression) |
//!
//! ## Gauges
//!
//! | Name | Description |
//! |------|-------------|
//! | `moq_relay_active_connections` | Current number of active client connections |
//! | `moq_relay_active_publishers` | Current number of active publishers |
//! | `moq_relay_active_subscriptions` | Current number of active subscriptions |
//! | `moq_relay_active_tracks` | Current number of tracks being served |
//! | `moq_relay_active_published_tracks` | Current number of exact tracks registered from PUBLISH |
//! | `moq_relay_announced_namespaces` | Current number of namespaces registered via PUBLISH_NAMESPACE |
//! | `moq_relay_upstream_connections` | Current number of upstream/origin connections |
//! | `moq_relay_connection_active` | Active connections by `scope`, `scope_state`, and `interface` |
//! | `moq_relay_connection_active_publishers` | Active publishers by `scope`, `scope_state`, and `interface` |
//! | `moq_relay_connection_active_subscriptions` | Active subscriptions by `scope`, `scope_state`, and `interface` |
//!
//! Scoped metrics use exactly `scope`, `scope_state`, and `interface` labels;
//! their names and values are in [`connection`]. `scope` is supplied only by
//! [`crate::Coordinator::metrics_scope_id`], and only once the session is
//! authorized. It is `<unscoped>` when the session has no scope (`scope_state`
//! `missing`) and empty otherwise. `moq_relay_connection_active` counts only
//! sessions past MoQT setup, so it can be lower than
//! `moq_relay_active_connections`.
//!
//! ## Histograms
//!
//! | Name | Labels | Description |
//! |------|--------|-------------|
//! | `moq_relay_subscribe_latency_seconds` | `source` | Time to resolve subscription (source: local, remote, not_found, unauthorized, route_error, upstream_error, downstream_left) |

use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

/// Names and label values of the per-scope connection metrics, for recorders
/// that route them.
pub mod connection {
    pub const INGRESS_BYTES: &str = "moq_relay_connection_ingress_bytes_total";
    pub const EGRESS_BYTES: &str = "moq_relay_connection_egress_bytes_total";
    pub const ACTIVE: &str = "moq_relay_connection_active";
    pub const ACTIVE_PUBLISHERS: &str = "moq_relay_connection_active_publishers";
    pub const ACTIVE_SUBSCRIPTIONS: &str = "moq_relay_connection_active_subscriptions";
    pub const ERRORS: &str = "moq_relay_connection_metrics_errors_total";

    pub const LABEL_SCOPE: &str = "scope";
    pub const LABEL_SCOPE_STATE: &str = "scope_state";
    pub const LABEL_INTERFACE: &str = "interface";
    pub const LABEL_REASON: &str = "reason";

    /// The `scope` value for a session with no scope (`scope_state` `missing`).
    pub const UNSCOPED: &str = crate::auth::UNSCOPED;

    /// The only `reason` on [`ERRORS`]: a QUIC byte counter went backwards.
    pub const REASON_COUNTER_REGRESSION: &str = "counter_regression";

    /// Values of the `scope_state` label.
    pub mod scope_state {
        /// Authorized, with a metrics scope ID in `scope`.
        pub const RESOLVED: &str = "resolved";
        /// Scope resolution and authorization have not finished yet.
        pub const PRE_RESOLUTION: &str = "pre_resolution";
        /// The connection has no routing scope.
        pub const MISSING: &str = "missing";
        /// Authorized, but the coordinator supplied no metrics scope ID.
        pub const UNPROJECTED: &str = "unprojected";
        /// The coordinator rejected the scope as invalid.
        pub const INVALID: &str = "invalid";
        /// The coordinator failed while resolving the scope.
        pub const LOOKUP_FAILED: &str = "lookup_failed";
        /// The session was refused at authorization.
        pub const UNAUTHORIZED: &str = "unauthorized";
    }
}

// ============================================================================
// describe_metrics - Register metric descriptions for Prometheus HELP text
// ============================================================================

/// Register metric descriptions with the metrics recorder.
///
/// Call this once after installing a metrics recorder (e.g., Prometheus exporter).
/// The descriptions appear as `# HELP` comments in Prometheus output.
pub fn describe_metrics() {
    // Counters
    describe_counter!(
        "moq_relay_connections_total",
        "Total incoming connections accepted"
    );
    describe_counter!(
        "moq_relay_connections_closed_total",
        "Total connections that have closed (graceful or error)"
    );
    describe_counter!(
        "moq_relay_connection_errors_total",
        "Connection failures by stage (session_accept, scope_resolve, auth_setup, session_run)"
    );
    describe_counter!(
        "moq_relay_publishers_total",
        "Total publishers (PUBLISH_NAMESPACE requests) received"
    );
    describe_counter!(
        "moq_relay_published_tracks_total",
        "Total publisher-initiated PUBLISH track requests received"
    );
    describe_counter!(
        "moq_relay_publish_errors_total",
        "Publisher-initiated PUBLISH failures by phase (session_limit, auth, auth_fanout, take_reader, local_register, coordinator_register, send_ok)"
    );
    describe_counter!(
        "moq_relay_announce_ok_total",
        "Successful REQUEST_OK responses sent for PUBLISH_NAMESPACE, by kind \
         (client: published to this relay; proxied: forwarded by a peer relay \
         and advertised for discovery only)"
    );
    describe_counter!(
        "moq_relay_announce_errors_total",
        "PUBLISH_NAMESPACE failures by phase (auth, local_register, remote_register, \
         coordinator_register, coordinator_lookup, send_ok, forward, peer_fanout)"
    );
    describe_counter!(
        "moq_relay_subscribers_total",
        "Total subscribers (SUBSCRIBE requests) received"
    );
    describe_counter!(
        "moq_relay_subscribe_not_found_total",
        "Track not found after checking all sources"
    );
    describe_counter!(
        "moq_relay_subscribe_route_errors_total",
        "Infrastructure failure when routing to remote"
    );
    describe_counter!(
        "moq_relay_subscribe_upstream_errors_total",
        "Upstream subscription could not be established, so the downstream SUBSCRIBE was rejected"
    );
    describe_counter!(
        "moq_relay_upstream_errors_total",
        "Upstream connection failures by stage (connect, session)"
    );
    describe_counter!(
        "moq_relay_namespace_transition_timeouts_total",
        "Namespace pull streams reset after graceful transition timeout"
    );
    describe_counter!(
        "moq_relay_cache_idle_evictions_total",
        "Unwatched cache entries evicted, releasing an upstream subscription, by source (local, remote)"
    );
    describe_counter!(
        "moq_relay_change_channel_lagged_total",
        "Change notifications skipped by a lagging receiver, forcing a resync, by channel (namespace, track)"
    );
    describe_counter!(
        "moq_relay_lease_registry_lock_poisoned_total",
        "Upstream namespace lease registry lock found poisoned by operation (acquire, release)"
    );
    describe_counter!(
        "moq_relay_auth_denied_total",
        "Operations denied by the authorization hook, by phase (setup, request), operation, and reason"
    );
    describe_counter!(
        "moq_relay_auth_errors_total",
        "Authorization failures that prevented a verdict being reached, by stage (scope_config, setup, request). \
         Distinct from moq_relay_auth_denied_total: these indicate a relay or configuration fault, not a rejected peer"
    );
    describe_counter!(
        "moq_relay_subscribe_errors_total",
        "SUBSCRIBE requests rejected by the relay, by phase (auth)"
    );
    describe_counter!(
        "moq_relay_subscribe_namespace_errors_total",
        "SUBSCRIBE_NAMESPACE requests rejected by the relay, by phase (auth)"
    );
    describe_counter!(
        "moq_relay_track_status_errors_total",
        "TRACK_STATUS requests rejected by the relay, by phase (auth)"
    );

    // Gauges
    describe_gauge!(
        "moq_relay_active_connections",
        "Current number of active client connections"
    );
    describe_gauge!(
        "moq_relay_active_publishers",
        "Current number of active publishers"
    );
    describe_gauge!(
        "moq_relay_active_subscriptions",
        "Current number of active subscriptions"
    );
    describe_gauge!(
        "moq_relay_active_tracks",
        "Current number of tracks being served"
    );
    describe_gauge!(
        "moq_relay_active_published_tracks",
        "Current number of exact tracks registered from PUBLISH"
    );
    describe_gauge!(
        "moq_relay_announced_namespaces",
        "Current number of registered namespaces"
    );
    describe_gauge!(
        "moq_relay_upstream_connections",
        "Current number of upstream/origin connections"
    );

    // Histograms
    describe_histogram!(
        "moq_relay_subscribe_latency_seconds",
        Unit::Seconds,
        "Time to resolve subscription by source (local, remote, not_found, unauthorized, route_error, upstream_error, downstream_left)"
    );
    describe_counter!(
        "moq_relay_fetch_errors_total",
        "Standalone FETCH requests rejected by the relay, by phase (phase: auth)"
    );

    describe_counter!(
        connection::INGRESS_BYTES,
        Unit::Bytes,
        "QUIC ingress bytes by scope and interface"
    );
    describe_counter!(
        connection::EGRESS_BYTES,
        Unit::Bytes,
        "QUIC egress bytes by scope and interface"
    );
    describe_gauge!(
        connection::ACTIVE,
        "Active connections by scope and interface"
    );
    describe_gauge!(
        connection::ACTIVE_PUBLISHERS,
        "Active publishers by scope and interface"
    );
    describe_gauge!(
        connection::ACTIVE_SUBSCRIPTIONS,
        "Active subscriptions by scope and interface"
    );
    describe_counter!(connection::ERRORS, "Connection metric faults by reason");
}

// ============================================================================
// GaugeGuard - RAII guard for gauge increment/decrement
// ============================================================================

/// RAII guard that increments a gauge on creation and decrements on drop.
#[must_use = "GaugeGuard must be held for the duration you want the gauge incremented"]
pub struct GaugeGuard {
    gauge: metrics::Gauge,
}

impl GaugeGuard {
    pub fn new(name: &'static str) -> Self {
        Self::from_gauge(metrics::gauge!(name))
    }

    pub(crate) fn from_gauge(gauge: metrics::Gauge) -> Self {
        gauge.increment(1.0);
        Self { gauge }
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.gauge.decrement(1.0);
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
