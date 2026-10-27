// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Per-scope connection metrics.
//!
//! Each accepted connection owns a [`ConnectionMeter`]. The meter adds the QUIC
//! bytes the connection carried since its last reading to counters labelled by
//! scope: every [`FLUSH_INTERVAL`] while the session runs, whenever the labels
//! change, and when the connection ends. Connections share no state, so a
//! reader that cuts intervals on its own clock sees each connection's bytes
//! within one flush interval of when they were carried.

use std::{future::Future, time::Duration};

use metrics::{Counter, Label};
use moq_native_ietf::quic::{ConnectionStatistics, ConnectionStatisticsSnapshot};
use tokio::time::{Instant, MissedTickBehavior};

use crate::{
    metrics::{connection as names, GaugeGuard},
    Coordinator, ScopeInfo, SessionInterface,
};

/// How often a running connection adds its bytes to the counters.
pub(crate) const FLUSH_INTERVAL: Duration = Duration::from_secs(10);

/// Scope attribution state, exported as the `scope_state` label.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScopeState {
    /// The session is authorized and the coordinator supplied a metrics scope ID.
    Resolved,
    /// Scope resolution and authorization have not finished yet.
    PreResolution,
    /// The connection has no routing scope.
    Missing,
    /// A routing scope exists but has no metrics scope ID.
    Unprojected,
    /// The supplied scope was invalid.
    Invalid,
    /// The coordinator failed while resolving the scope.
    LookupFailed,
    /// The session was refused at authorization.
    Unauthorized,
}

impl ScopeState {
    const fn as_str(self) -> &'static str {
        use names::scope_state::*;
        match self {
            Self::Resolved => RESOLVED,
            Self::PreResolution => PRE_RESOLUTION,
            Self::Missing => MISSING,
            Self::Unprojected => UNPROJECTED,
            Self::Invalid => INVALID,
            Self::LookupFailed => LOOKUP_FAILED,
            Self::Unauthorized => UNAUTHORIZED,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConnectionMetricLabels {
    scope: String,
    scope_state: ScopeState,
    interface: SessionInterface,
}

impl ConnectionMetricLabels {
    fn new(scope: Option<String>, scope_state: ScopeState, interface: SessionInterface) -> Self {
        // A session with no scope is named the same way the auth module logs it.
        let scope = match scope_state {
            ScopeState::Missing => names::UNSCOPED.to_owned(),
            _ => scope.unwrap_or_default(),
        };
        Self {
            scope,
            scope_state,
            interface,
        }
    }

    fn pre_resolution(interface: SessionInterface) -> Self {
        Self::new(None, ScopeState::PreResolution, interface)
    }

    /// Labels for a connection whose scope could not be resolved.
    pub(crate) fn unresolved(
        reason: ScopeResolutionFailureKind,
        interface: SessionInterface,
    ) -> Self {
        Self::new(None, reason.scope_state(), interface)
    }

    /// Labels for a relay-initiated link, which has no metrics scope ID.
    pub(crate) fn internal_link(routing_scope_present: bool) -> Self {
        let state = if routing_scope_present {
            ScopeState::Unprojected
        } else {
            ScopeState::Missing
        };
        Self::new(None, state, SessionInterface::Internal)
    }

    fn metric_labels(&self) -> Vec<Label> {
        vec![
            Label::new(names::LABEL_SCOPE, self.scope.clone()),
            Label::new(names::LABEL_SCOPE_STATE, self.scope_state.as_str()),
            Label::new(names::LABEL_INTERFACE, self.interface.as_str()),
        ]
    }

    fn gauge_guard(&self, name: &'static str) -> GaugeGuard {
        GaugeGuard::from_gauge(metrics::gauge!(name, self.metric_labels()))
    }

    pub(crate) fn publisher_guard(&self) -> GaugeGuard {
        self.gauge_guard(names::ACTIVE_PUBLISHERS)
    }

    pub(crate) fn subscription_guard(&self) -> GaugeGuard {
        self.gauge_guard(names::ACTIVE_SUBSCRIPTIONS)
    }
}

/// Labels for a session once its admission is decided.
///
/// The tenant's scope label is used only for an authorized session, so a
/// refused session's traffic is never counted against the tenant. Only the
/// coordinator's metrics scope ID becomes the label: the routing scope's
/// format is coordinator-defined and may carry credentials, so only its
/// presence is used.
pub(crate) fn admission_labels(
    coordinator: &dyn Coordinator,
    scope_info: Option<&ScopeInfo>,
    interface: SessionInterface,
    authorized: bool,
) -> ConnectionMetricLabels {
    if !authorized {
        return ConnectionMetricLabels::new(None, ScopeState::Unauthorized, interface);
    }
    let routing_scope = scope_info.filter(|info| !info.scope_id.is_empty());
    let metrics_scope = routing_scope
        .and_then(|info| coordinator.metrics_scope_id(info))
        .filter(|scope| !scope.is_empty());
    let (scope, state) = match (routing_scope, metrics_scope) {
        (None, _) => (None, ScopeState::Missing),
        (Some(_), None) => (None, ScopeState::Unprojected),
        (Some(_), Some(scope)) => (Some(scope), ScopeState::Resolved),
    };
    ConnectionMetricLabels::new(scope, state, interface)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ScopeResolutionFailureKind {
    Missing,
    Invalid,
    LookupFailed,
}

impl ScopeResolutionFailureKind {
    fn scope_state(self) -> ScopeState {
        match self {
            Self::Missing => ScopeState::Missing,
            Self::Invalid => ScopeState::Invalid,
            Self::LookupFailed => ScopeState::LookupFailed,
        }
    }
}

trait ByteStatistics: Send + Sync {
    fn snapshot(&self) -> ConnectionStatisticsSnapshot;
}

impl ByteStatistics for ConnectionStatistics {
    fn snapshot(&self) -> ConnectionStatisticsSnapshot {
        ConnectionStatistics::snapshot(self)
    }
}

/// Counts one connection's QUIC bytes and its place in the active gauge.
pub(crate) struct ConnectionMeter {
    statistics: Box<dyn ByteStatistics>,
    previous: ConnectionStatisticsSnapshot,
    labels: ConnectionMetricLabels,
    ingress: Counter,
    egress: Counter,
    active: GaugeGuard,
}

impl ConnectionMeter {
    pub(crate) fn new(statistics: ConnectionStatistics, interface: SessionInterface) -> Self {
        Self::with_statistics(Box::new(statistics), interface)
    }

    fn with_statistics(statistics: Box<dyn ByteStatistics>, interface: SessionInterface) -> Self {
        let labels = ConnectionMetricLabels::pre_resolution(interface);
        let (ingress, egress) = byte_counters(&labels);
        Self {
            statistics,
            previous: ConnectionStatisticsSnapshot::default(),
            active: labels.gauge_guard(names::ACTIVE),
            labels,
            ingress,
            egress,
        }
    }

    pub(crate) fn labels(&self) -> &ConnectionMetricLabels {
        &self.labels
    }

    /// Add the bytes carried since the last reading to the current counters.
    pub(crate) fn flush(&mut self) {
        let current = self.statistics.snapshot();
        let ingress = current
            .ingress_bytes
            .checked_sub(self.previous.ingress_bytes);
        let egress = current.egress_bytes.checked_sub(self.previous.egress_bytes);
        self.previous = current;
        // Each direction is reported on its own, so one regressed counter
        // does not discard the other's delta.
        if ingress.is_none() || egress.is_none() {
            metrics::counter!(
                names::ERRORS,
                names::LABEL_REASON => names::REASON_COUNTER_REGRESSION
            )
            .increment(1);
        }
        if let Some(bytes) = ingress.filter(|bytes| *bytes > 0) {
            self.ingress.increment(bytes);
        }
        if let Some(bytes) = egress.filter(|bytes| *bytes > 0) {
            self.egress.increment(bytes);
        }
    }

    /// Flush under the current labels, then count under `labels`.
    pub(crate) fn relabel(&mut self, labels: ConnectionMetricLabels) {
        self.flush();
        (self.ingress, self.egress) = byte_counters(&labels);
        self.active = labels.gauge_guard(names::ACTIVE);
        self.labels = labels;
    }
}

impl Drop for ConnectionMeter {
    fn drop(&mut self) {
        self.flush();
    }
}

fn byte_counters(labels: &ConnectionMetricLabels) -> (Counter, Counter) {
    let labels = labels.metric_labels();
    (
        metrics::counter!(names::INGRESS_BYTES, labels.clone()),
        metrics::counter!(names::EGRESS_BYTES, labels),
    )
}

/// Drive `session`, flushing `meter` every [`FLUSH_INTERVAL`] while it runs.
pub(crate) async fn run_metered<F: Future>(
    session: F,
    meter: Option<&mut ConnectionMeter>,
) -> F::Output {
    let Some(meter) = meter else {
        return session.await;
    };
    tokio::pin!(session);
    let mut ticks = tokio::time::interval_at(Instant::now() + FLUSH_INTERVAL, FLUSH_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            output = &mut session => return output,
            _ = ticks.tick() => meter.flush(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use metrics::{
        Counter, CounterFn, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder,
        SharedString, Unit,
    };
    use moq_native_ietf::quic;
    use moq_transport::coding::TrackNamespace;

    use super::*;
    use crate::{
        CoordinatorContext, CoordinatorError, CoordinatorResult, NamespaceOrigin,
        NamespaceRegistration, ScopePermissions,
    };

    #[derive(Clone, Default)]
    struct FakeStatistics {
        snapshot: Arc<Mutex<ConnectionStatisticsSnapshot>>,
        reads: Arc<Mutex<usize>>,
    }

    impl FakeStatistics {
        fn set(&self, ingress_bytes: u64, egress_bytes: u64) {
            let mut snapshot = ConnectionStatisticsSnapshot::default();
            snapshot.ingress_bytes = ingress_bytes;
            snapshot.egress_bytes = egress_bytes;
            *self.snapshot.lock().unwrap() = snapshot;
        }

        fn reads(&self) -> usize {
            *self.reads.lock().unwrap()
        }
    }

    impl ByteStatistics for FakeStatistics {
        fn snapshot(&self) -> ConnectionStatisticsSnapshot {
            *self.reads.lock().unwrap() += 1;
            *self.snapshot.lock().unwrap()
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum Update {
        Counter(u64),
        Gauge(f64),
    }

    #[derive(Clone, Debug)]
    struct Event {
        name: String,
        labels: BTreeMap<String, String>,
        update: Update,
    }

    #[derive(Default)]
    struct TestRecorder {
        events: Arc<Mutex<Vec<Event>>>,
    }

    impl TestRecorder {
        fn named(&self, name: &str) -> Vec<Event> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.name == name)
                .cloned()
                .collect()
        }

        fn handle(&self, key: &Key) -> Arc<Handle> {
            Arc::new(Handle {
                name: key.name().to_owned(),
                labels: key
                    .labels()
                    .map(|label| (label.key().to_owned(), label.value().to_owned()))
                    .collect(),
                events: self.events.clone(),
            })
        }
    }

    struct Handle {
        name: String,
        labels: BTreeMap<String, String>,
        events: Arc<Mutex<Vec<Event>>>,
    }

    impl Handle {
        fn push(&self, update: Update) {
            self.events.lock().unwrap().push(Event {
                name: self.name.clone(),
                labels: self.labels.clone(),
                update,
            });
        }
    }

    impl CounterFn for Handle {
        fn increment(&self, value: u64) {
            self.push(Update::Counter(value));
        }

        fn absolute(&self, _value: u64) {}
    }

    impl GaugeFn for Handle {
        fn increment(&self, value: f64) {
            self.push(Update::Gauge(value));
        }

        fn decrement(&self, value: f64) {
            self.push(Update::Gauge(-value));
        }

        fn set(&self, value: f64) {
            self.push(Update::Gauge(value));
        }
    }

    impl Recorder for TestRecorder {
        fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
            Counter::from_arc(self.handle(key))
        }

        fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
            Gauge::from_arc(self.handle(key))
        }

        fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    /// A coordinator whose metrics scope ID is a fixed value.
    struct ProjectingCoordinator(Option<&'static str>);

    #[async_trait]
    impl Coordinator for ProjectingCoordinator {
        async fn register_namespace(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
            _context: &CoordinatorContext,
        ) -> CoordinatorResult<NamespaceRegistration> {
            Ok(NamespaceRegistration::new(()))
        }

        async fn unregister_namespace(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
        ) -> CoordinatorResult<()> {
            Ok(())
        }

        async fn lookup(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
        ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)> {
            Err(CoordinatorError::NamespaceNotFound)
        }

        fn metrics_scope_id(&self, _scope: &ScopeInfo) -> Option<String> {
            self.0.map(str::to_owned)
        }
    }

    fn scope_with_credential() -> ScopeInfo {
        ScopeInfo {
            scope_id: "/uid/eyJhbGciOiJFUzI1NiJ9.secret".to_owned(),
            permissions: ScopePermissions::ReadWrite,
        }
    }

    fn assert_no_credential(labels: &ConnectionMetricLabels) {
        for label in labels.metric_labels() {
            assert!(!label.value().contains("eyJ"), "{label:?}");
            assert!(!label.value().contains("secret"), "{label:?}");
        }
    }

    #[test]
    fn an_authorized_session_is_labelled_only_by_the_metrics_scope_id() {
        let scope = scope_with_credential();
        let labels = admission_labels(
            &ProjectingCoordinator(Some("uid")),
            Some(&scope),
            SessionInterface::Public,
            true,
        );
        assert_eq!(labels.scope, "uid");
        assert_eq!(labels.scope_state, ScopeState::Resolved);
        assert_no_credential(&labels);

        // Without a projection the routing scope must not become the label.
        let labels = admission_labels(
            &ProjectingCoordinator(None),
            Some(&scope),
            SessionInterface::Public,
            true,
        );
        assert_eq!(labels.scope, "");
        assert_eq!(labels.scope_state, ScopeState::Unprojected);
        assert_no_credential(&labels);
    }

    #[test]
    fn a_refused_session_is_never_counted_against_the_tenant() {
        let labels = admission_labels(
            &ProjectingCoordinator(Some("uid")),
            Some(&scope_with_credential()),
            SessionInterface::Public,
            false,
        );
        assert_eq!(labels.scope, "");
        assert_eq!(labels.scope_state, ScopeState::Unauthorized);
    }

    #[test]
    fn a_session_without_a_routing_scope_is_unscoped() {
        for scope in [
            None,
            Some(ScopeInfo {
                scope_id: String::new(),
                permissions: ScopePermissions::ReadWrite,
            }),
        ] {
            let labels = admission_labels(
                &ProjectingCoordinator(Some("uid")),
                scope.as_ref(),
                SessionInterface::Internal,
                true,
            );
            assert_eq!(labels.scope, names::UNSCOPED);
            assert_eq!(labels.scope_state, ScopeState::Missing);
        }
    }

    #[test]
    fn unresolved_labels_use_bounded_scope_values_and_states() {
        for (reason, scope, state) in [
            (
                ScopeResolutionFailureKind::Missing,
                names::UNSCOPED,
                ScopeState::Missing,
            ),
            (ScopeResolutionFailureKind::Invalid, "", ScopeState::Invalid),
            (
                ScopeResolutionFailureKind::LookupFailed,
                "",
                ScopeState::LookupFailed,
            ),
        ] {
            let labels = ConnectionMetricLabels::unresolved(reason, SessionInterface::Public);
            assert_eq!(labels.scope, scope);
            assert_eq!(labels.scope_state, state);
        }
    }

    #[test]
    fn the_meter_counts_deltas_under_the_labels_current_when_they_were_carried() {
        let recorder = TestRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            let statistics = FakeStatistics::default();
            let mut meter = ConnectionMeter::with_statistics(
                Box::new(statistics.clone()),
                SessionInterface::Public,
            );

            statistics.set(10, 20);
            meter.relabel(admission_labels(
                &ProjectingCoordinator(Some("uid")),
                Some(&scope_with_credential()),
                SessionInterface::Public,
                true,
            ));
            statistics.set(15, 26);
            meter.flush();
            statistics.set(18, 26);
            drop(meter);

            let ingress = recorder.named(names::INGRESS_BYTES);
            let updates_in = |state: &str| {
                ingress
                    .iter()
                    .filter(|event| event.labels[names::LABEL_SCOPE_STATE] == state)
                    .map(|event| event.update.clone())
                    .collect::<Vec<_>>()
            };
            assert_eq!(updates_in("pre_resolution"), [Update::Counter(10)]);
            assert_eq!(
                updates_in("resolved"),
                [Update::Counter(5), Update::Counter(3)]
            );
            assert!(ingress
                .iter()
                .filter(|event| event.labels[names::LABEL_SCOPE_STATE] == "resolved")
                .all(|event| event.labels[names::LABEL_SCOPE] == "uid"));
            // The last reading carried no egress, so it emits nothing.
            assert_eq!(recorder.named(names::EGRESS_BYTES).len(), 2);
        });
    }

    #[test]
    fn the_active_gauge_moves_with_the_labels_and_ends_balanced() {
        let recorder = TestRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            let mut meter = ConnectionMeter::with_statistics(
                Box::new(FakeStatistics::default()),
                SessionInterface::Public,
            );
            meter.relabel(admission_labels(
                &ProjectingCoordinator(Some("uid")),
                None,
                SessionInterface::Public,
                false,
            ));
            drop(meter);

            let active = recorder.named(names::ACTIVE);
            let total = |state: &str| -> f64 {
                active
                    .iter()
                    .filter(|event| event.labels[names::LABEL_SCOPE_STATE] == state)
                    .map(|event| match event.update {
                        Update::Gauge(value) => value,
                        Update::Counter(_) => unreachable!(),
                    })
                    .sum()
            };
            assert_eq!(total("pre_resolution"), 0.0);
            assert_eq!(total("unauthorized"), 0.0);
            assert_eq!(active.len(), 4);
        });
    }

    #[test]
    fn a_regressed_direction_is_reported_without_losing_the_other() {
        let recorder = TestRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            let statistics = FakeStatistics::default();
            let mut meter = ConnectionMeter::with_statistics(
                Box::new(statistics.clone()),
                SessionInterface::Public,
            );
            statistics.set(10, 10);
            meter.flush();
            statistics.set(5, 15);
            meter.flush();

            assert_eq!(
                recorder
                    .named(names::EGRESS_BYTES)
                    .iter()
                    .map(|event| event.update.clone())
                    .collect::<Vec<_>>(),
                [Update::Counter(10), Update::Counter(5)]
            );
            assert_eq!(recorder.named(names::INGRESS_BYTES).len(), 1);
            let errors = recorder.named(names::ERRORS);
            assert_eq!(errors.len(), 1);
            assert_eq!(
                errors[0].labels[names::LABEL_REASON],
                names::REASON_COUNTER_REGRESSION
            );
            drop(meter);
        });
    }

    #[test]
    fn activity_guards_are_balanced_and_scoped() {
        let recorder = TestRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            let labels = ConnectionMetricLabels::internal_link(false);
            drop(labels.publisher_guard());
            drop(labels.subscription_guard());

            for name in [names::ACTIVE_PUBLISHERS, names::ACTIVE_SUBSCRIPTIONS] {
                let events = recorder.named(name);
                assert_eq!(events.len(), 2);
                assert_eq!(events[0].update, Update::Gauge(1.0));
                assert_eq!(events[1].update, Update::Gauge(-1.0));
                assert_eq!(events[0].labels[names::LABEL_INTERFACE], "internal");
                assert_eq!(events[0].labels[names::LABEL_SCOPE], names::UNSCOPED);
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn a_running_session_flushes_on_every_interval() {
        let statistics = FakeStatistics::default();
        let mut meter = ConnectionMeter::with_statistics(
            Box::new(statistics.clone()),
            SessionInterface::Public,
        );

        let session = tokio::time::sleep(FLUSH_INTERVAL * 3 + FLUSH_INTERVAL / 2);
        run_metered(session, Some(&mut meter)).await;
        assert_eq!(statistics.reads(), 3);

        drop(meter);
        assert_eq!(
            statistics.reads(),
            4,
            "dropping the meter flushes once more"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn without_a_meter_the_session_runs_unchanged() {
        assert_eq!(run_metered(async { 7 }, None).await, 7);
    }
}
