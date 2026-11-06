// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Per-connection usage, for embedders that report it.
//!
//! With [`crate::Relay::with_connection_usage`], the relay hands the observer
//! a [`ConnectionUsage`] for each accepted connection once MoQT setup
//! completes and its interface is classified. A connection refused at scope
//! resolution or authorization is reported without a tenant. The embedder reads it on its own schedule; the relay keeps no
//! timer, registry or lock shared between connections. When the connection
//! ends, the relay records the final totals and releases the connection, so a
//! handle kept until its final totals are reported does not keep the
//! connection alive.

use std::sync::{Arc, Mutex, PoisonError};

use moq_native_ietf::quic::{ConnectionStatistics, ConnectionStatisticsSnapshot};
use moq_transport::session::{SessionTraffic, Transport};

use crate::{Coordinator, ScopeInfo, SessionInterface};

/// Receives the usage handle of every accepted connection.
pub trait ConnectionUsageObserver: Send + Sync + 'static {
    /// Called once per connection, after MoQT setup and before scope
    /// resolution, on the connection's task. It must not block.
    fn connection_opened(&self, usage: ConnectionUsage);
}

/// One connection's usage. Cheap to clone.
#[derive(Clone)]
pub struct ConnectionUsage {
    inner: Arc<Inner>,
}

struct Inner {
    connection_id: String,
    interface: SessionInterface,
    transport: Transport,
    state: Mutex<State>,
}

struct State {
    scope: Option<String>,
    /// Where the totals are read from, until the connection ends.
    sources: Option<Sources>,
    /// The final totals, once the connection has ended.
    last: UsageSnapshot,
}

struct Sources {
    bytes: Box<dyn ByteStatistics>,
    traffic: SessionTraffic,
}

trait ByteStatistics: Send {
    fn snapshot(&self) -> ConnectionStatisticsSnapshot;
}

impl ByteStatistics for ConnectionStatistics {
    fn snapshot(&self) -> ConnectionStatisticsSnapshot {
        ConnectionStatistics::snapshot(self)
    }
}

impl Sources {
    fn read(&self, closed: bool) -> UsageSnapshot {
        let bytes = self.bytes.snapshot();
        let traffic = self.traffic.snapshot();
        UsageSnapshot {
            ingress_bytes: bytes.ingress_bytes,
            egress_bytes: bytes.egress_bytes,
            ingress_objects: traffic.ingress_objects,
            egress_objects: traffic.egress_objects,
            ingress_groups: traffic.ingress_groups,
            egress_groups: traffic.egress_groups,
            closed,
        }
    }
}

impl ConnectionUsage {
    pub(crate) fn new(
        connection_id: String,
        interface: SessionInterface,
        transport: Transport,
        statistics: ConnectionStatistics,
        traffic: SessionTraffic,
    ) -> Self {
        Self::with_sources(
            connection_id,
            interface,
            transport,
            Box::new(statistics),
            traffic,
        )
    }

    fn with_sources(
        connection_id: String,
        interface: SessionInterface,
        transport: Transport,
        bytes: Box<dyn ByteStatistics>,
        traffic: SessionTraffic,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                connection_id,
                interface,
                transport,
                state: Mutex::new(State {
                    scope: None,
                    sources: Some(Sources { bytes, traffic }),
                    last: UsageSnapshot::default(),
                }),
            }),
        }
    }

    /// The QUIC connection ID (hex) that also names the connection in logs,
    /// qlog and mlog.
    pub fn connection_id(&self) -> &str {
        &self.inner.connection_id
    }

    pub fn interface(&self) -> SessionInterface {
        self.inner.interface
    }

    /// What MoQT runs over on this connection.
    pub fn transport(&self) -> Transport {
        self.inner.transport
    }

    /// The tenant this connection is reported under: the coordinator's
    /// [`Coordinator::metrics_scope_id`], set only once the session is
    /// authorized. `None` before then, for a refused session, and when the
    /// coordinator supplies no ID.
    pub fn scope(&self) -> Option<String> {
        self.state().scope.clone()
    }

    /// Totals since the connection was accepted. Each value only increases,
    /// and once [`UsageSnapshot::closed`] is set they no longer change.
    pub fn snapshot(&self) -> UsageSnapshot {
        let state = self.state();
        match &state.sources {
            Some(sources) => sources.read(false),
            None => state.last,
        }
    }

    pub(crate) fn authorize(&self, scope: Option<String>) {
        self.state().scope = scope;
    }

    /// Record the final totals and release the connection.
    fn close(&self) {
        let mut state = self.state();
        if let Some(sources) = state.sources.take() {
            state.last = sources.read(true);
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// Closes the usage when the connection task ends, on every exit path.
pub(crate) struct ConnectionUsageGuard(ConnectionUsage);

impl ConnectionUsageGuard {
    pub(crate) fn new(usage: ConnectionUsage) -> Self {
        Self(usage)
    }

    pub(crate) fn usage(&self) -> &ConnectionUsage {
        &self.0
    }
}

impl Drop for ConnectionUsageGuard {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// A connection's totals since it was accepted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct UsageSnapshot {
    /// Bytes received in UDP datagrams.
    pub ingress_bytes: u64,
    /// Bytes sent in UDP datagrams.
    pub egress_bytes: u64,
    /// MoQT objects received.
    pub ingress_objects: u64,
    /// MoQT objects sent.
    pub egress_objects: u64,
    /// MoQT groups received, each counted once.
    pub ingress_groups: u64,
    /// MoQT groups sent, each counted once.
    pub egress_groups: u64,
    /// The connection has ended and these are its final totals.
    pub closed: bool,
}

/// The scope a session is reported under once its admission is decided.
///
/// A refused session is never reported under a tenant. Only the
/// coordinator's metrics scope ID is used: the routing scope's format is
/// coordinator-defined and may carry credentials, so only its presence is
/// consulted.
pub(crate) fn reported_scope(
    coordinator: &dyn Coordinator,
    scope_info: Option<&ScopeInfo>,
    authorized: bool,
) -> Option<String> {
    if !authorized {
        return None;
    }
    scope_info
        .filter(|info| !info.scope_id.is_empty())
        .and_then(|info| coordinator.metrics_scope_id(info))
        .filter(|scope| !scope.is_empty())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use async_trait::async_trait;
    use moq_native_ietf::Client;
    use moq_transport::coding::TrackNamespace;

    use super::*;
    use crate::{
        CoordinatorContext, CoordinatorError, CoordinatorResult, NamespaceOrigin,
        NamespaceRegistration, ScopePermissions,
    };

    /// Byte counters that also report whether they are still held.
    #[derive(Clone, Default)]
    struct FakeBytes(Arc<AtomicU64>);

    impl ByteStatistics for FakeBytes {
        fn snapshot(&self) -> ConnectionStatisticsSnapshot {
            let bytes = self.0.load(Ordering::Relaxed);
            let mut snapshot = ConnectionStatisticsSnapshot::default();
            snapshot.ingress_bytes = bytes;
            snapshot.egress_bytes = bytes * 2;
            snapshot
        }
    }

    fn usage(bytes: &FakeBytes) -> ConnectionUsage {
        ConnectionUsage::with_sources(
            "c0ffee".to_owned(),
            SessionInterface::Public,
            Transport::RawQuic,
            Box::new(bytes.clone()),
            SessionTraffic::default(),
        )
    }

    #[test]
    fn closing_freezes_the_totals_and_releases_the_connection() {
        let bytes = FakeBytes::default();
        let usage = usage(&bytes);
        bytes.0.store(10, Ordering::Relaxed);
        assert_eq!(usage.snapshot().ingress_bytes, 10);
        assert!(!usage.snapshot().closed);

        drop(ConnectionUsageGuard::new(usage.clone()));
        assert_eq!(Arc::strong_count(&bytes.0), 1, "the connection is released");

        bytes.0.store(99, Ordering::Relaxed);
        let last = usage.snapshot();
        assert!(last.closed);
        assert_eq!((last.ingress_bytes, last.egress_bytes), (10, 20));
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
        ) -> CoordinatorResult<(NamespaceOrigin, Option<Client>)> {
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

    #[test]
    fn only_the_metrics_scope_id_is_reported() {
        let scope = scope_with_credential();
        assert_eq!(
            reported_scope(&ProjectingCoordinator(Some("uid")), Some(&scope), true),
            Some("uid".to_owned())
        );
        // Without a projection the routing scope must not be reported.
        assert_eq!(
            reported_scope(&ProjectingCoordinator(None), Some(&scope), true),
            None
        );
        assert_eq!(
            reported_scope(&ProjectingCoordinator(Some("")), Some(&scope), true),
            None
        );
    }

    #[test]
    fn a_refused_session_is_never_reported_under_the_tenant() {
        assert_eq!(
            reported_scope(
                &ProjectingCoordinator(Some("uid")),
                Some(&scope_with_credential()),
                false
            ),
            None
        );
    }

    #[test]
    fn a_session_without_a_routing_scope_is_unscoped() {
        let coordinator = ProjectingCoordinator(Some("uid"));
        assert_eq!(reported_scope(&coordinator, None, true), None);
        let empty = ScopeInfo {
            scope_id: String::new(),
            permissions: ScopePermissions::ReadWrite,
        };
        assert_eq!(reported_scope(&coordinator, Some(&empty), true), None);
    }
}
