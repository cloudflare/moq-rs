// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashSet;
use std::time::Duration;

use anyhow::Context;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    coding::{KeyValuePairs, TrackName, TrackNamespace, TrackNamespacePrefix},
    message::{RequestErrorCode, SubscribeOptions},
    serve::{FullTrackName, ServeError, TrackReader, TracksReader},
    session::{
        FetchRequested, Publisher, SessionError, Subscribed, SubscribedNamespace,
        TrackStatusRequested,
    },
};
use tokio::sync::broadcast;

use crate::auth::{authorize, AuthzOperation, DenyReason, SessionAuth};
use crate::{
    metrics::{GaugeGuard, TimingGuard},
    upstream_namespaces::UpstreamNamespaces,
    Locals, NamespaceChange, RemoteManager, SessionContext, TrackChange, UpstreamReady,
};

/// Producer of tracks to a remote Subscriber
#[derive(Clone)]
pub struct Producer {
    publisher: Publisher,
    locals: Locals,
    remotes: RemoteManager,
    upstream_namespaces: UpstreamNamespaces,
    /// Relay-level context for this MoQT session.
    context: SessionContext,
    /// Authorization state for this session. `None` when the scope has no
    /// authorization policy, in which case every request is permitted.
    auth: Option<SessionAuth>,
}

/// Why the wait for upstream readiness ended without the subscription being
/// established.
///
/// The two cases carry the same [`ServeError`] variants — an upstream rejection
/// can be `Cancel` or `Done` just as a departing subscriber can — so the
/// direction has to come from which branch resolved rather than from the error.
/// Getting it wrong misreports publisher failures as subscriber cancellations in
/// the subscribe metrics.
enum UpstreamWait {
    /// The upstream subscription could not be established.
    UpstreamFailed(ServeError),

    /// The downstream subscriber went away before it was established.
    DownstreamLeft(ServeError),
}

impl Producer {
    /// Create a producer for a session.
    ///
    /// `auth` is the session's authorization state, and is deliberately a
    /// required argument rather than something attached afterwards: a producer
    /// with no authorization serves every request, so forgetting to supply it
    /// must be a compile error rather than a silent grant. `None` states that
    /// the session needs no authorization — its scope has no policy, or the
    /// relay dialled the peer itself.
    ///
    /// **Breaking change** (SemVer `0.7.x` → `0.8.x`): the old `Producer::new`
    /// public convenience constructor has been removed because it could only
    /// produce an unauthenticated producer. External embedders must supply the
    /// `auth` state explicitly. See the `feat(auth)!` commit for the rationale.
    pub(crate) fn new_with_upstream_namespaces(
        publisher: Publisher,
        locals: Locals,
        remotes: RemoteManager,
        upstream_namespaces: UpstreamNamespaces,
        context: SessionContext,
        auth: Option<SessionAuth>,
    ) -> Self {
        Self {
            publisher,
            locals,
            remotes,
            upstream_namespaces,
            context,
            auth,
        }
    }

    /// Send PUBLISH_NAMESPACE for a set of tracks to the remote peer.
    pub async fn publish_namespace(&mut self, tracks: TracksReader) -> Result<(), SessionError> {
        self.publisher.publish_namespace(tracks).await
    }

    /// Run the producer to serve subscribe requests.
    pub async fn run(self) -> Result<(), SessionError> {
        let mut tasks: FuturesUnordered<futures::future::BoxFuture<'static, ()>> =
            FuturesUnordered::new();

        loop {
            let mut publisher_subscribed = self.publisher.clone();
            let mut publisher_track_status = self.publisher.clone();
            let mut publisher_subscribed_namespace = self.publisher.clone();
            let mut publisher_fetch = self.publisher.clone();

            tokio::select! {
                // Handle a new subscribe request
                Some(subscribed) = publisher_subscribed.subscribed() => {
                    metrics::counter!("moq_relay_subscribers_total").increment(1);

                    let this = self.clone();

                    // Spawn a new task to handle the subscribe
                    tasks.push(async move {
                        let info = subscribed.clone();
                        let namespace = info.track_namespace.to_utf8_path();
                        let track_name = info.track_name.clone();
                        tracing::info!(namespace = %namespace, track = %track_name, "serving subscribe: {:?}", info);

                        // Serve the subscribe request
                        if let Err(err) = this.serve_subscribe(subscribed).await {
                            if Self::is_expected_serve_shutdown(&err) {
                                tracing::debug!(namespace = %namespace, track = %track_name, subscribe_info = ?info, error = %err, "stopped serving subscribe");
                            } else {
                                tracing::warn!(namespace = %namespace, track = %track_name, subscribe_info = ?info, error = %err, "failed serving subscribe");
                            }
                        }
                    }.boxed())
                },
                // Handle a new track_status request
                Some(track_status_requested) = publisher_track_status.track_status_requested() => {
                    let this = self.clone();

                    // Spawn a new task to handle the track_status request
                    tasks.push(async move {
                        let info = track_status_requested.request_msg.clone();
                        let namespace = info.track_namespace.to_utf8_path();
                        let track_name = info.track_name.clone();
                        tracing::info!(namespace = %namespace, track = %track_name, "serving track_status: {:?}", info);

                        // Serve the track_status request
                        if let Err(err) = this.serve_track_status(track_status_requested).await {
                            tracing::warn!(namespace = %namespace, track = %track_name, error = %err, "failed serving track_status: {:?}, error: {}", info, err)
                        }
                    }.boxed())
                },
                // Handle a new namespace subscription request.
                Some(subscribed_namespace) = publisher_subscribed_namespace.subscribed_namespace() => {
                    let this = self.clone();

                    tasks.push(async move {
                        let prefix = subscribed_namespace.namespace_prefix.to_utf8_path();
                        tracing::info!(namespace_prefix = %prefix, "serving subscribe namespace");

                        if let Err(err) = this.serve_subscribe_namespace(subscribed_namespace).await {
                            if Self::is_expected_serve_shutdown(&err) {
                                tracing::debug!(namespace_prefix = %prefix, error = %err, "stopped serving subscribe namespace");
                            } else {
                                tracing::warn!(namespace_prefix = %prefix, error = %err, "failed serving subscribe namespace");
                            }
                        }
                    }.boxed())
                },
                Some(fetch) = publisher_fetch.fetch_requested() => {
                    let this = self.clone();
                    tasks.push(async move {
                        if let Err(err) = this.serve_fetch(fetch).await {
                            tracing::debug!(error = %err, "failed serving FETCH");
                        }
                    }.boxed())
                },
                _= tasks.next(), if !tasks.is_empty() => {},
                else => return Ok(()),
            };
        }
    }

    async fn serve_fetch(self, fetch: FetchRequested) -> Result<(), anyhow::Error> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let standalone = tokio::select! {
            biased;
            _ = fetch.closed() => return Ok(()),
            _ = tokio::time::sleep_until(deadline) => {
                fetch.reject(RequestErrorCode::Timeout, "FETCH resolution timed out")?;
                return Ok(());
            }
            result = fetch.resolve() => match result? {
                Some(standalone) => standalone,
                None => return Ok(()),
            },
        };

        // Authorize before any lookup: a standalone FETCH retrieves track
        // content and requires the same Subscribe grant as a SUBSCRIBE for the
        // same track. Deciding afterwards would make response timing an
        // existence oracle for tracks the peer may not access.
        if let Err(reason) = may_fetch_track(
            self.auth.as_ref(),
            &self.context,
            &standalone.track_namespace,
            &standalone.track_name,
            fetch.request.id,
        )
        .await
        {
            metrics::counter!("moq_relay_fetch_errors_total", "phase" => "auth").increment(1);
            let code = match reason {
                DenyReason::TokenMalformed => RequestErrorCode::MalformedAuthToken,
                DenyReason::TokenExpired => RequestErrorCode::ExpiredAuthToken,
                _ => RequestErrorCode::Unauthorized,
            };
            fetch.reject(code, "unauthorized")?;
            return Err(anyhow::anyhow!("unauthorized fetch"));
        }

        let params = match upstream_fetch_params(&fetch.request.params) {
            Ok(params) => params,
            Err(err) => {
                fetch.reject(RequestErrorCode::InternalError, "invalid FETCH parameters")?;
                return Err(err).context("invalid FETCH parameters");
            }
        };
        if fetch.closed().now_or_never().is_some() {
            return Ok(());
        }
        let open = async {
            if let Some(mut source) = self
                .locals
                .fetch_source(self.context.scope(), &standalone.track_namespace)
            {
                Ok(Some(source.fetch(standalone, params)?))
            } else {
                self.remotes
                    .fetch(self.context.scope(), standalone, params)
                    .await
            }
        };
        let upstream = tokio::select! {
            biased;
            _ = fetch.closed() => return Ok(()),
            _ = tokio::time::sleep_until(deadline) => {
                fetch.reject(RequestErrorCode::Timeout, "FETCH routing timed out")?;
                return Ok(());
            }
            result = open => match result {
                Ok(Some(upstream)) => upstream,
                Ok(None) => {
                    fetch.reject(RequestErrorCode::DoesNotExist, "track not found")?;
                    return Ok(());
                }
                Err(err) => {
                    fetch.reject(RequestErrorCode::InternalError, "failed to open upstream FETCH")?;
                    return Err(err).context("failed to open upstream FETCH");
                }
            },
        };
        fetch
            .proxy(
                upstream,
                deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await?;
        Ok(())
    }

    /// Serve a subscribe request.
    async fn serve_subscribe(self, subscribed: Subscribed) -> Result<(), anyhow::Error> {
        // Track subscribe latency from request to track resolution (records on drop)
        let mut timing_guard =
            TimingGuard::with_label("moq_relay_subscribe_latency_seconds", "source", "not_found");
        // Track active subscriptions - decrements when this function returns
        let _sub_guard = GaugeGuard::new("moq_relay_active_subscriptions");

        let namespace = subscribed.track_namespace.clone();
        let track_name = subscribed.track_name.clone();

        // Authorize before any lookup: deciding after would let response
        // timing reveal whether a track the peer may not access exists.
        if let Err(reason) = authorize(
            self.auth.as_ref(),
            &self.context,
            AuthzOperation::Subscribe {
                namespace: &namespace,
                track: &track_name,
            },
            Some(subscribed.id),
        )
        .await
        {
            metrics::counter!("moq_relay_subscribe_errors_total", "phase" => "auth").increment(1);
            timing_guard.set_label("source", "unauthorized");
            let err = ServeError::Closed(reason.request_error_code());
            subscribed.close(err.clone())?;
            return Err(err.into());
        }

        // Local lookup order inside Locals:
        // 1. actual FullTrackName -> TrackReader media cache
        // 2. PUBLISH_NAMESPACE route source, which triggers upstream SUBSCRIBE
        let mut locals = self.locals.clone();
        if let Some(local) = locals
            .get_or_request_track(self.context.scope(), namespace.clone(), &track_name)
            .await
        {
            let ns = namespace.to_utf8_path();
            tracing::info!(namespace = %ns, track = %track_name, source = "local", "serving subscribe from local: {:?}", local.reader.info);
            timing_guard.set_label("source", "local");
            let _track_guard = GaugeGuard::new("moq_relay_active_tracks");
            // Held until serving finishes. Once the last guard for a cached track
            // drops, its upstream subscription becomes eligible for release.
            let _interest_guard = local.interest;

            // Draft-16 §8.4: a relay MUST have an Established upstream
            // subscription before it sends SUBSCRIBE_OK. A pull-through cache
            // entry exists before its upstream subscription does, so wait for it.
            if let Some(upstream) = local.upstream {
                if let Err(outcome) = Self::await_upstream(&subscribed, &upstream).await {
                    // Which side ended the wait is decided by the branch that
                    // resolved, not by the error variant: an upstream failure can
                    // itself be Cancel or Done, so sniffing the variant would
                    // report a publisher-side failure as a subscriber cancellation
                    // and skip the upstream-error counter.
                    let err = match outcome {
                        UpstreamWait::DownstreamLeft(err) => {
                            tracing::debug!(namespace = %ns, track = %track_name, error = %err, "downstream subscriber left before the upstream subscription was established");
                            timing_guard.set_label("source", "downstream_left");
                            err
                        }
                        UpstreamWait::UpstreamFailed(err) => {
                            tracing::warn!(namespace = %ns, track = %track_name, error = %err, "upstream subscription could not be established");
                            metrics::counter!("moq_relay_subscribe_upstream_errors_total")
                                .increment(1);
                            timing_guard.set_label("source", "upstream_error");
                            err
                        }
                    };

                    // Rejects when the subscription is already closed (the
                    // downstream-left case), which is fine: the error below is
                    // still the reason we stopped.
                    let _ = subscribed.close(err.clone());
                    return Err(err.into());
                }
            }

            return Ok(subscribed.serve(local.reader).await?);
        }

        // Check remote tracks after local exact tracks and namespace route sources.
        match self
            .remotes
            .subscribe(self.context.scope(), &namespace, &track_name)
            .await
        {
            Ok(track) => {
                if let Some((track, interest_guard)) = track {
                    let ns = namespace.to_utf8_path();
                    tracing::info!(namespace = %ns, track = %track_name, source = "remote", "serving subscribe from remote: {:?}", track.info);
                    // Update label to indicate remote source, timing recorded on drop
                    timing_guard.set_label("source", "remote");
                    // Track active tracks - decrements when serve completes
                    let _track_guard = GaugeGuard::new("moq_relay_active_tracks");
                    // Held until serving finishes; the cross-relay subscription is
                    // released once the last guard for this track drops.
                    let _interest_guard = interest_guard;
                    return Ok(subscribed.serve(track).await?);
                }
            }
            Err(e) => {
                // Route error = infrastructure failure (couldn't reach coordinator/upstream)
                // This is different from "not found" - we don't know if the track exists
                let ns = namespace.to_utf8_path();
                tracing::error!(namespace = %ns, track = %track_name, error = %e, "failed to route to remote: {}", e);
                timing_guard.set_label("source", "route_error");
                metrics::counter!("moq_relay_subscribe_route_errors_total").increment(1);

                // Return an internal error rather than "not found" since we couldn't check
                // TODO: Consider returning a more specific error to the subscriber
                let err = ServeError::internal_ctx(format!(
                    "route error for namespace '{}': {}",
                    namespace, e
                ));
                subscribed.close(err.clone())?;
                return Err(err.into());
            }
        }

        // Track not found - we checked all sources and the track doesn't exist
        // timing_guard label already set to "not_found", will record on drop
        metrics::counter!("moq_relay_subscribe_not_found_total").increment(1);

        let err = ServeError::not_found_ctx(format!(
            "track '{}/{}' not found in local or remote tracks",
            namespace, track_name
        ));
        subscribed.close(err.clone())?;
        Err(err.into())
    }

    /// Wait for the upstream subscription behind a cached track to be established.
    ///
    /// Also completes when the downstream subscriber goes away first, so a
    /// cancelled SUBSCRIBE is not held here for the full upstream response
    /// timeout. The two cases are reported separately because they cannot be
    /// told apart from the error alone — see [`UpstreamWait`].
    async fn await_upstream(
        subscribed: &Subscribed,
        upstream: &UpstreamReady,
    ) -> Result<(), UpstreamWait> {
        tokio::select! {
            res = upstream.established() => res.map_err(UpstreamWait::UpstreamFailed),
            res = subscribed.closed() => Err(UpstreamWait::DownstreamLeft(
                res.err().unwrap_or(ServeError::Done),
            )),
        }
    }

    /// Serve a SUBSCRIBE_NAMESPACE request using relay-local namespace state.
    async fn serve_subscribe_namespace(
        self,
        mut subscribed_namespace: SubscribedNamespace,
    ) -> Result<(), anyhow::Error> {
        // Authorize before taking the upstream lease: an unauthorized prefix
        // must not cause the relay to open upstream subscriptions.
        if let Err(reason) = authorize(
            self.auth.as_ref(),
            &self.context,
            AuthzOperation::SubscribeNamespace {
                prefix: &subscribed_namespace.namespace_prefix,
            },
            Some(subscribed_namespace.info.request_id),
        )
        .await
        {
            metrics::counter!("moq_relay_subscribe_namespace_errors_total", "phase" => "auth")
                .increment(1);
            subscribed_namespace.reject(reason.request_error_code(), "unauthorized")?;
            return Err(anyhow::anyhow!("unauthorized subscribe_namespace"));
        }

        let wants_namespace = wants_namespace(subscribed_namespace.subscribe_options);
        let wants_publish = wants_publish(subscribed_namespace.subscribe_options);
        let namespace_changes = self.locals.subscribe_namespace_changes();
        let track_changes = self.locals.subscribe_track_changes();
        let mut publish_tasks: FuturesUnordered<futures::future::BoxFuture<'static, ()>> =
            FuturesUnordered::new();

        let _upstream_lease = if wants_namespace {
            match self
                .upstream_namespaces
                .subscribe(&self.context, subscribed_namespace.namespace_prefix.clone())
            {
                Ok(lease) => Some(lease),
                Err(error) => {
                    tracing::error!(
                        prefix = %subscribed_namespace.namespace_prefix,
                        error = %error,
                        "failed to acquire shared upstream namespace lease; serving local state only"
                    );
                    None
                }
            }
        } else {
            None
        };

        subscribed_namespace.ok()?;

        let mut known_namespaces = HashSet::new();

        if wants_namespace {
            self.send_namespace_snapshot(&mut subscribed_namespace, &mut known_namespaces)
                .await?;
        }

        let mut known_tracks = HashSet::new();
        if wants_publish {
            self.send_publish_snapshot(
                &subscribed_namespace,
                &mut known_tracks,
                &mut publish_tasks,
            )
            .await?;
        }

        self.serve_subscribe_namespace_loop(
            subscribed_namespace,
            wants_namespace,
            wants_publish,
            namespace_changes,
            track_changes,
            publish_tasks,
            known_namespaces,
            known_tracks,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn serve_subscribe_namespace_loop(
        self,
        subscribed_namespace: SubscribedNamespace,
        wants_namespace: bool,
        wants_publish: bool,
        mut namespace_changes: tokio::sync::broadcast::Receiver<NamespaceChange>,
        mut track_changes: tokio::sync::broadcast::Receiver<TrackChange>,
        mut publish_tasks: FuturesUnordered<futures::future::BoxFuture<'static, ()>>,
        mut known_namespaces: HashSet<TrackNamespace>,
        mut known_tracks: HashSet<FullTrackName>,
    ) -> Result<(), anyhow::Error> {
        let mut subscribed_namespace = subscribed_namespace;
        loop {
            tokio::select! {
                res = subscribed_namespace.closed() => {
                    res?;
                    return Ok(());
                }
                change = namespace_changes.recv(), if wants_namespace => {
                    match change {
                        Ok(change) => {
                            self.apply_namespace_change(&mut subscribed_namespace, &mut known_namespaces, change).await?;
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            // Recoverable: a full resync reconstructs the state the
                            // skipped events would have produced. Counted so that
                            // sustained churn outgrowing the channel capacity is
                            // visible before it shows up as latency.
                            metrics::counter!("moq_relay_change_channel_lagged_total", "channel" => "namespace")
                                .increment(skipped);
                            self.resync_namespaces(&mut subscribed_namespace, &mut known_namespaces).await?;
                        }
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
                change = track_changes.recv(), if wants_publish => {
                    match change {
                        Ok(change) => {
                            self.apply_track_change(&subscribed_namespace, &mut known_tracks, &mut publish_tasks, change).await?;
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            metrics::counter!("moq_relay_change_channel_lagged_total", "channel" => "track")
                                .increment(skipped);
                            self.resync_publish_tracks(&subscribed_namespace, &mut known_tracks, &mut publish_tasks).await?;
                        }
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
                _ = publish_tasks.next(), if !publish_tasks.is_empty() => {},
            }
        }
    }

    /// Whether the peer may be told that `namespace` exists.
    async fn may_announce(&self, namespace: &TrackNamespace) -> bool {
        may_announce_namespace(self.auth.as_ref(), &self.context, namespace).await
    }

    async fn send_namespace_snapshot(
        &self,
        subscribed_namespace: &mut SubscribedNamespace,
        known: &mut HashSet<TrackNamespace>,
    ) -> Result<(), ServeError> {
        for namespace in self
            .locals
            .list_namespaces_matching(self.context.scope(), &subscribed_namespace.namespace_prefix)
        {
            if !self.may_announce(&namespace).await {
                continue;
            }
            if known.insert(namespace.clone()) {
                subscribed_namespace.namespace(&namespace)?;
            }
        }

        Ok(())
    }

    async fn apply_namespace_change(
        &self,
        subscribed_namespace: &mut SubscribedNamespace,
        known: &mut HashSet<TrackNamespace>,
        change: NamespaceChange,
    ) -> Result<(), ServeError> {
        if change.scope.as_deref() != self.context.scope() {
            return Ok(());
        }

        if !subscribed_namespace
            .namespace_prefix
            .is_prefix_of(&change.namespace)
        {
            return Ok(());
        }

        if change.added {
            if !self.may_announce(&change.namespace).await {
                return Ok(());
            }
            if known.insert(change.namespace.clone()) {
                subscribed_namespace.namespace(&change.namespace)?;
            }
        } else if known.remove(&change.namespace) {
            subscribed_namespace.namespace_done(&change.namespace)?;
        }

        Ok(())
    }

    async fn resync_namespaces(
        &self,
        subscribed_namespace: &mut SubscribedNamespace,
        known: &mut HashSet<TrackNamespace>,
    ) -> Result<(), ServeError> {
        let mut current = HashSet::new();
        for namespace in self
            .locals
            .list_namespaces_matching(self.context.scope(), &subscribed_namespace.namespace_prefix)
        {
            if self.may_announce(&namespace).await {
                current.insert(namespace);
            }
        }

        for namespace in current.difference(known) {
            subscribed_namespace.namespace(namespace)?;
        }

        for namespace in known.difference(&current) {
            subscribed_namespace.namespace_done(namespace)?;
        }

        *known = current;
        Ok(())
    }

    async fn send_publish_snapshot(
        &self,
        subscribed_namespace: &SubscribedNamespace,
        known: &mut HashSet<FullTrackName>,
        publish_tasks: &mut FuturesUnordered<futures::future::BoxFuture<'static, ()>>,
    ) -> Result<(), anyhow::Error> {
        for track in self
            .locals
            .list_tracks_matching(self.context.scope(), &subscribed_namespace.namespace_prefix)
        {
            self.publish_track_for_namespace(subscribed_namespace, known, publish_tasks, track)
                .await?;
        }

        Ok(())
    }

    async fn apply_track_change(
        &self,
        subscribed_namespace: &SubscribedNamespace,
        known: &mut HashSet<FullTrackName>,
        publish_tasks: &mut FuturesUnordered<futures::future::BoxFuture<'static, ()>>,
        change: TrackChange,
    ) -> Result<(), anyhow::Error> {
        match change {
            TrackChange::Added { scope, track } => {
                if scope.as_deref() != self.context.scope()
                    || !subscribed_namespace
                        .namespace_prefix
                        .is_prefix_of(&track.namespace)
                {
                    return Ok(());
                }

                self.publish_track_for_namespace(subscribed_namespace, known, publish_tasks, track)
                    .await
            }
            TrackChange::Removed { scope, full_name } => {
                if scope.as_deref() == self.context.scope() {
                    known.remove(&full_name);
                }
                Ok(())
            }
        }
    }

    async fn resync_publish_tracks(
        &self,
        subscribed_namespace: &SubscribedNamespace,
        known: &mut HashSet<FullTrackName>,
        publish_tasks: &mut FuturesUnordered<futures::future::BoxFuture<'static, ()>>,
    ) -> Result<(), anyhow::Error> {
        // Single pass: build only the `current` set while publishing new tracks,
        // instead of materializing an intermediate Vec of (name, reader) pairs.
        let mut current = HashSet::new();
        for track in self
            .locals
            .list_tracks_matching(self.context.scope(), &subscribed_namespace.namespace_prefix)
        {
            let full_name = full_name_for_track(&track);
            if !known.contains(&full_name) {
                self.publish_track_for_namespace(subscribed_namespace, known, publish_tasks, track)
                    .await?;
            }
            current.insert(full_name);
        }

        known.retain(|full_name| current.contains(full_name));
        Ok(())
    }

    async fn publish_track_for_namespace(
        &self,
        subscribed_namespace: &SubscribedNamespace,
        known: &mut HashSet<FullTrackName>,
        publish_tasks: &mut FuturesUnordered<futures::future::BoxFuture<'static, ()>>,
        track: TrackReader,
    ) -> Result<(), anyhow::Error> {
        let full_name = full_name_for_track(&track);
        if known.contains(&full_name) {
            return Ok(());
        }

        // SUBSCRIBE_NAMESPACE grants discovery of a prefix, not delivery of
        // everything under it. This path pushes PUBLISH and then streams the
        // track's objects, so it needs the same authorization a SUBSCRIBE for
        // that track would: a token scoped to a prefix for discovery, or to a
        // subset of track names, must not receive media outside that grant.
        //
        // A denial skips the track rather than failing the subscription: the
        // peer asked for a prefix, not for this track, so the rest of the
        // prefix is still legitimately theirs.
        if let Err(reason) = may_serve_track_in_fanout(
            self.auth.as_ref(),
            &self.context,
            &full_name,
            subscribed_namespace.info.request_id,
        )
        .await
        {
            tracing::debug!(
                namespace = %full_name.namespace,
                track = %full_name.name,
                reason = %reason,
                "withholding track from SUBSCRIBE_NAMESPACE fan-out"
            );
            metrics::counter!("moq_relay_publish_errors_total", "phase" => "auth_fanout")
                .increment(1);

            // A policy denial is stable for the session, so record it and stop
            // re-deciding on every change event. A hook *fault* is not: it says
            // nothing about this track, so leave it out of `known` and let the
            // next event retry rather than withholding the track for the life
            // of the subscription over one transient failure.
            if reason.is_policy_denial() {
                known.insert(full_name);
            }
            return Ok(());
        }

        let mut params = KeyValuePairs::default();
        if !subscribed_namespace.forward {
            params.set_forward(false);
        }

        let namespace = full_name.namespace.to_utf8_path();
        let track_name = full_name.name.to_string();
        let mut publisher = self.publisher.clone();
        let published = match publisher.publish(track, params).await {
            Ok(published) => published,
            Err(SessionError::Serve(ServeError::Duplicate)) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        known.insert(full_name);
        publish_tasks.push(
            async move {
                if let Err(err) = published.serve().await {
                    tracing::warn!(namespace = %namespace, track = %track_name, error = %err, "failed serving PUBLISH for SUBSCRIBE_NAMESPACE");
                }
            }
            .boxed(),
        );

        Ok(())
    }

    fn is_expected_serve_shutdown(err: &anyhow::Error) -> bool {
        let serve = match err.downcast_ref::<SessionError>() {
            Some(SessionError::Serve(err)) => Some(err),
            _ => err.downcast_ref::<ServeError>(),
        };

        serve.is_some_and(Self::is_expected_serve_shutdown_err)
    }

    /// True for the errors that mean nobody is waiting for the subscription any
    /// more, rather than a failure worth warning about.
    fn is_expected_serve_shutdown_err(err: &ServeError) -> bool {
        matches!(err, ServeError::Cancel | ServeError::Done)
    }

    /// Serve a track_status request.
    async fn serve_track_status(
        self,
        mut track_status_requested: TrackStatusRequested,
    ) -> Result<(), anyhow::Error> {
        let full_name = FullTrackName {
            namespace: track_status_requested.request_msg.track_namespace.clone(),
            name: track_status_requested.request_msg.track_name.clone(),
        };

        // Authorize before the lookup: TRACK_STATUS is an existence oracle, so
        // answering it for an unauthorized track leaks exactly what the token
        // scope is meant to hide.
        if let Err(reason) = authorize(
            self.auth.as_ref(),
            &self.context,
            AuthzOperation::TrackStatus {
                namespace: &full_name.namespace,
                track: &full_name.name,
            },
            Some(track_status_requested.request_msg.id),
        )
        .await
        {
            metrics::counter!("moq_relay_track_status_errors_total", "phase" => "auth")
                .increment(1);
            track_status_requested.respond_error(reason.request_error_code(), "unauthorized")?;
            return Err(anyhow::anyhow!("unauthorized track_status"));
        }

        // Check actual local tracks first.
        if let Some(track) = self.locals.retrieve_track(self.context.scope(), &full_name) {
            let namespace = full_name.namespace.to_utf8_path();
            let track_name = &full_name.name;
            tracing::info!(namespace = %namespace, track = %track_name, source = "local", "serving track_status from local: {:?}", track.info);
            return Ok(track_status_requested.respond_ok(&track)?);
        }

        // TODO - forward track status to remotes?
        // Check remote tracks second, and serve from remote if possible
        /*
        if let Some(remotes) = &self.remotes {
            // Try to route to a remote for this namespace
            if let Some(remote) = remotes.route(&subscribe.track_namespace).await? {
                if let Some(track) =
                    remote.subscribe(subscribe.track_namespace.clone(), subscribe.track_name.clone())?
                {
                    tracing::info!("serving from remote: {:?} {:?}", remote.info, track.info);

                    // NOTE: Depends on drop(track) being called afterwards
                    return Ok(subscribe.serve(track.reader).await?);
                }
            }
        }*/

        track_status_requested.respond_error(
            moq_transport::message::RequestErrorCode::DoesNotExist as u64,
            "track not found",
        )?;

        Err(ServeError::not_found_ctx(format!(
            "track '{}/{}' not found for track_status",
            track_status_requested.request_msg.track_namespace,
            track_status_requested.request_msg.track_name
        ))
        .into())
    }
}

fn wants_namespace(options: SubscribeOptions) -> bool {
    matches!(
        options,
        SubscribeOptions::Namespace | SubscribeOptions::Both
    )
}

fn wants_publish(options: SubscribeOptions) -> bool {
    matches!(options, SubscribeOptions::Publish | SubscribeOptions::Both)
}

fn full_name_for_track(track: &TrackReader) -> FullTrackName {
    FullTrackName {
        namespace: track.namespace.clone(),
        name: track.name.clone(),
    }
}

fn upstream_fetch_params(
    params: &KeyValuePairs,
) -> Result<KeyValuePairs, moq_transport::coding::DecodeError> {
    let mut upstream = KeyValuePairs::default();
    if let Some(order) = params.group_order()? {
        upstream.set_group_order(order);
    }
    Ok(upstream)
}

/// Whether the peer may FETCH `track` in `namespace`.
///
/// Standalone FETCH retrieves track content — the same content a SUBSCRIBE
/// delivers — so it requires the same grant. This is the authorization gate
/// for `serve_fetch`; it is a free function so the decision is unit-testable
/// without constructing a live transport session.
///
/// The tests therefore pin this decision and its operation, but not that
/// `serve_fetch` still consults it — removing the call site produces a
/// dead-code warning rather than a failing test. An end-to-end FETCH session
/// harness would close that gap.
async fn may_fetch_track(
    auth: Option<&SessionAuth>,
    context: &SessionContext,
    namespace: &TrackNamespace,
    track: &TrackName,
    request_id: u64,
) -> Result<(), DenyReason> {
    authorize(
        auth,
        context,
        AuthzOperation::Subscribe { namespace, track },
        Some(request_id),
    )
    .await
}

/// Whether a track may be delivered through the SUBSCRIBE_NAMESPACE fan-out.
///
/// That path pushes PUBLISH and then streams the track's objects, so it needs
/// the authorization a SUBSCRIBE for the same track would: a prefix
/// subscription grants discovery, not delivery of everything beneath it. A
/// token scoped to a subset of track names must not receive the rest merely
/// because it subscribed to the enclosing prefix.
///
/// A free function so the decision is testable without a live session, which
/// `Producer` requires: a `Publisher` cannot be constructed outside
/// `moq-transport`, so the gate could not otherwise be covered at all.
///
/// The tests therefore pin this decision and the operation it asks about, but
/// not that the caller still consults it — removing the call site produces a
/// dead-code warning rather than a failing test. Closing that would need an
/// end-to-end session harness.
async fn may_serve_track_in_fanout(
    auth: Option<&SessionAuth>,
    context: &SessionContext,
    full_name: &FullTrackName,
    request_id: u64,
) -> Result<(), DenyReason> {
    authorize(
        auth,
        context,
        AuthzOperation::Subscribe {
            namespace: &full_name.namespace,
            track: &full_name.name,
        },
        Some(request_id),
    )
    .await
}

/// Whether the peer may be told that `namespace` exists.
///
/// A prefix subscription is not a grant over everything beneath it. The `nil`
/// terminator is what decides: a scope of `[exact("sports"), nil]` authorizes
/// SUBSCRIBE_NAMESPACE for `["sports"]` while denying every operation under
/// `["sports", "football"]`, so announcing that deeper namespace would disclose
/// the existence of something the token cannot touch. A scope without the nil
/// terminator — just `[exact("sports")]` — is a prefix grant and does allow
/// announcing `["sports", "football"]`. The nil terminator is the distinction.
///
/// Announcement authorization uses `AuthzOperation::SubscribeNamespace`, not
/// `AuthzOperation::Subscribe`, because disclosing that a namespace *exists*
/// is a discovery operation — the same kind of disclosure SUBSCRIBE_NAMESPACE
/// makes. Subscribe would be too restrictive: it would require track-level
/// grants just to see namespace listings. SubscribeNamespace is correct: it
/// requires the token to permit discovery of the namespace, which is exactly
/// what announcement implies.
///
/// The `TrackNamespace` is converted to a `TrackNamespacePrefix` with the same
/// fields so that the prefix-matching logic in the authorization layer checks
/// this concrete namespace as the prefix naming exactly it.
async fn may_announce_namespace(
    auth: Option<&SessionAuth>,
    context: &SessionContext,
    namespace: &TrackNamespace,
) -> bool {
    // Convert the concrete namespace to the prefix that names exactly it,
    // so that the SubscribeNamespace authorization check can apply its
    // prefix-matching logic correctly.
    let prefix = TrackNamespacePrefix {
        fields: namespace.fields.clone(),
    };

    authorize(
        auth,
        context,
        AuthzOperation::SubscribeNamespace { prefix: &prefix },
        None,
    )
    .await
    .is_ok()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use moq_native_ietf::quic;
    use moq_transport::{
        coding::{
            Decode, DecodeError, Encode, KeyValuePairs, Location, TrackName, TrackNamespace, VarInt,
        },
        data::{
            Datagram as WireDatagram, ExtensionHeaders, FetchHeader, FetchRecord,
            FetchRecordObject, StreamHeader, StreamHeaderType,
        },
        message::{
            self, parameter_type, FetchType, FilterType, GroupOrder, JoiningFetch, Message,
            RequestErrorCode, SubscriptionFilter,
        },
        serve::{Datagram, ServeError, Track},
        session::{FetchOkInfo, FetchRejection, Session, SessionError},
        setup,
    };

    use crate::test::{test_endpoint, TestEndpoint};
    use crate::{
        Consumer, Coordinator, CoordinatorContext, CoordinatorError, CoordinatorResult, Locals,
        NamespaceOrigin, NamespaceRegistration, RemoteManager, SessionContext,
    };

    use super::*;
    use crate::auth::{AuthDecision, AuthError, AuthHook, AuthRequest, AuthToken, Principal};

    /// Records every operation it is asked about, and answers from a script.
    ///
    /// Lets the fan-out gates be tested for the operation they construct, not
    /// merely for their yes/no answer: authorizing the wrong thing would be as
    /// much a bypass as authorizing nothing.
    struct RecordingHook {
        seen: Mutex<Vec<String>>,
        decision: fn() -> Result<AuthDecision, AuthError>,
    }

    impl RecordingHook {
        fn new(decision: fn() -> Result<AuthDecision, AuthError>) -> Arc<Self> {
            Arc::new(Self {
                seen: Mutex::new(Vec::new()),
                decision,
            })
        }

        fn seen(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl AuthHook for RecordingHook {
        async fn on_setup(
            &self,
            _session: &SessionContext,
            _tokens: &[AuthToken],
        ) -> Result<AuthDecision, AuthError> {
            (self.decision)()
        }

        async fn on_request(&self, request: &AuthRequest<'_>) -> Result<AuthDecision, AuthError> {
            let detail = match &request.operation {
                AuthzOperation::Subscribe { namespace, track } => format!(
                    "subscribe {} {}",
                    namespace.to_utf8_path(),
                    track.to_string_lossy()
                ),
                AuthzOperation::SubscribeNamespace { prefix } => {
                    format!("subscribe_namespace {}", prefix.to_utf8_path())
                }
                other => format!("other {}", other.label()),
            };
            self.seen.lock().unwrap().push(detail);
            (self.decision)()
        }
    }

    fn allow() -> Result<AuthDecision, AuthError> {
        Ok(AuthDecision::allow(Principal::anonymous()))
    }

    fn deny() -> Result<AuthDecision, AuthError> {
        Ok(AuthDecision::deny(DenyReason::ScopeMismatch))
    }

    fn fault() -> Result<AuthDecision, AuthError> {
        Err(AuthError::Backend("backend unavailable".to_string()))
    }

    fn context() -> SessionContext {
        SessionContext::public(Some("tenant".to_string()))
    }

    fn session_auth(hook: Arc<RecordingHook>) -> SessionAuth {
        SessionAuth::new(hook, Principal::anonymous())
    }

    fn full_name() -> FullTrackName {
        FullTrackName {
            namespace: TrackNamespace::from_utf8_path("sports/football"),
            name: "premium-4k".into(),
        }
    }

    /// The fan-out streams media, so it must ask for SUBSCRIBE on the exact
    /// track — not for the enclosing prefix, and not nothing at all.
    #[tokio::test]
    async fn fanout_authorizes_each_track_as_a_subscribe() {
        let hook = RecordingHook::new(allow);
        let auth = session_auth(hook.clone());

        may_serve_track_in_fanout(Some(&auth), &context(), &full_name(), 7)
            .await
            .expect("allowed");

        assert_eq!(hook.seen(), vec!["subscribe /sports/football premium-4k"]);
    }

    #[tokio::test]
    async fn fanout_withholds_a_track_the_token_does_not_cover() {
        let hook = RecordingHook::new(deny);
        let auth = session_auth(hook.clone());

        let result = may_serve_track_in_fanout(Some(&auth), &context(), &full_name(), 7).await;

        assert!(result.is_err(), "a denied track must not be served");
        assert_eq!(hook.seen().len(), 1, "the decision must actually be sought");
    }

    /// A hook fault is not a statement about this track, so it must not be
    /// remembered as one; `publish_track_for_namespace` only caches denials
    /// that are policy decisions.
    #[tokio::test]
    async fn fanout_distinguishes_a_fault_from_a_denial() {
        let denied = may_serve_track_in_fanout(
            Some(&session_auth(RecordingHook::new(deny))),
            &context(),
            &full_name(),
            7,
        )
        .await
        .expect_err("denied");
        assert!(denied.is_policy_denial());

        let faulted = may_serve_track_in_fanout(
            Some(&session_auth(RecordingHook::new(fault))),
            &context(),
            &full_name(),
            7,
        )
        .await
        .expect_err("faulted");
        assert!(
            !faulted.is_policy_denial(),
            "a fault must not be cached as a denial"
        );
    }

    /// A session whose scope has no policy is unaffected.
    #[tokio::test]
    async fn fanout_permits_everything_when_no_policy_applies() {
        assert!(may_serve_track_in_fanout(None, &context(), &full_name(), 7)
            .await
            .is_ok());
        assert!(
            may_announce_namespace(
                None,
                &context(),
                &TrackNamespace::from_utf8_path("sports/football")
            )
            .await
        );
    }

    /// Announcements disclose existence, so each concrete namespace is
    /// authorized as the prefix naming exactly it.
    #[tokio::test]
    async fn announcements_are_authorized_per_namespace() {
        let hook = RecordingHook::new(allow);
        let auth = session_auth(hook.clone());
        let namespace = TrackNamespace::from_utf8_path("sports/football");

        assert!(may_announce_namespace(Some(&auth), &context(), &namespace).await);
        assert_eq!(hook.seen(), vec!["subscribe_namespace /sports/football"]);
    }

    #[tokio::test]
    async fn announcements_are_withheld_when_denied() {
        let hook = RecordingHook::new(deny);
        let auth = session_auth(hook.clone());
        let namespace = TrackNamespace::from_utf8_path("sports/football");

        assert!(
            !may_announce_namespace(Some(&auth), &context(), &namespace).await,
            "a namespace the token cannot touch must not be announced"
        );
        assert_eq!(hook.seen().len(), 1);
    }

    /// FETCH requires the same grant as SUBSCRIBE: fetching track content is
    /// the same operation, just without a streaming subscription.
    #[tokio::test]
    async fn fetch_is_authorized_as_subscribe() {
        let hook = RecordingHook::new(allow);
        let auth = session_auth(hook.clone());
        let namespace = TrackNamespace::from_utf8_path("sports/football");
        let track = TrackName::from("video");

        may_fetch_track(Some(&auth), &context(), &namespace, &track, 9)
            .await
            .expect("allowed");

        // The decision asked about is Subscribe — same grant as a SUBSCRIBE.
        assert_eq!(hook.seen(), vec!["subscribe /sports/football video"]);
    }

    /// A FETCH for a track outside the token's scope must be denied.
    #[tokio::test]
    async fn fetch_is_withheld_for_unauthorized_track() {
        let hook = RecordingHook::new(deny);
        let auth = session_auth(hook.clone());
        let namespace = TrackNamespace::from_utf8_path("sports/football");
        let track = TrackName::from("video");

        let result = may_fetch_track(Some(&auth), &context(), &namespace, &track, 9).await;

        assert!(result.is_err(), "unauthorized FETCH must be denied");
        assert_eq!(
            hook.seen().len(),
            1,
            "the authorization decision must actually be sought"
        );
    }

    /// A scope with no policy allows FETCH unconditionally.
    #[tokio::test]
    async fn fetch_permits_everything_when_no_policy_applies() {
        let namespace = TrackNamespace::from_utf8_path("sports/football");
        let track = TrackName::from("video");

        assert!(may_fetch_track(None, &context(), &namespace, &track, 9)
            .await
            .is_ok());
    }

    type LookupRequest = (Option<String>, TrackNamespace);

    #[derive(Clone)]
    struct MockCoordinator {
        route: Option<(url::Url, std::net::SocketAddr, quic::Client)>,
        lookups: Arc<AtomicUsize>,
        lookup_requests: Arc<Mutex<Vec<LookupRequest>>>,
    }

    impl MockCoordinator {
        fn without_route() -> Self {
            Self {
                route: None,
                lookups: Arc::new(AtomicUsize::new(0)),
                lookup_requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn with_route(url: url::Url, addr: std::net::SocketAddr, client: quic::Client) -> Self {
            Self {
                route: Some((url, addr, client)),
                lookups: Arc::new(AtomicUsize::new(0)),
                lookup_requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl Coordinator for MockCoordinator {
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
            scope: Option<&str>,
            namespace: &TrackNamespace,
        ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            self.lookup_requests
                .lock()
                .unwrap()
                .push((scope.map(str::to_string), namespace.clone()));
            let Some((url, addr, client)) = &self.route else {
                return Err(CoordinatorError::NamespaceNotFound);
            };
            Ok((
                NamespaceOrigin::new(namespace.clone(), url.clone(), Some(*addr)),
                Some(client.clone()),
            ))
        }
    }

    struct WireReader {
        stream: web_transport::RecvStream,
        buffer: Vec<u8>,
    }

    impl WireReader {
        fn new(stream: web_transport::RecvStream) -> Self {
            Self {
                stream,
                buffer: Vec::new(),
            }
        }

        async fn decode<T: Decode>(&mut self) -> T {
            loop {
                let mut cursor = Cursor::new(self.buffer.as_slice());
                match T::decode(&mut cursor) {
                    Ok(value) => {
                        self.buffer.drain(..cursor.position() as usize);
                        return value;
                    }
                    Err(DecodeError::More(_)) => {
                        let chunk = self.stream.read(64 * 1024).await.unwrap().unwrap();
                        self.buffer.extend_from_slice(&chunk);
                    }
                    Err(err) => panic!("failed to decode test wire message: {err}"),
                }
            }
        }

        async fn read_to_end(mut self) -> Vec<u8> {
            while let Some(chunk) = self.stream.read(64 * 1024).await.unwrap() {
                self.buffer.extend_from_slice(&chunk);
            }
            self.buffer
        }

        async fn read_exact(&mut self, len: usize) -> Vec<u8> {
            while self.buffer.len() < len {
                let chunk = self.stream.read(64 * 1024).await.unwrap().unwrap();
                self.buffer.extend_from_slice(&chunk);
            }
            self.buffer.drain(..len).collect()
        }

        async fn reset_code(mut self) -> u8 {
            self.stream.closed().await.unwrap().unwrap()
        }
    }

    async fn write<T: Encode>(stream: &mut web_transport::SendStream, value: &T) {
        let mut encoded = Vec::new();
        value.encode(&mut encoded).unwrap();
        write_bytes(stream, &encoded).await;
    }

    async fn write_bytes(stream: &mut web_transport::SendStream, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            let written = stream.write(bytes).await.unwrap();
            assert_ne!(written, 0);
            bytes = &bytes[written..];
        }
    }

    struct ManualPeer {
        transport: web_transport::Session,
        control_send: web_transport::SendStream,
        control_recv: WireReader,
        server_session: Session,
        server_publisher: moq_transport::session::Publisher,
        server_subscriber: moq_transport::session::Subscriber,
        _client: quic::Client,
        _server: quic::Server,
    }

    struct ApiPeer {
        client_session: Session,
        subscriber: moq_transport::session::Subscriber,
        server_session: Session,
        publisher: moq_transport::session::Publisher,
        _client: quic::Client,
        _server: quic::Server,
    }

    async fn api_peer() -> ApiPeer {
        let TestEndpoint {
            client,
            mut server,
            url,
            addr,
        } = test_endpoint();
        let (client_connection, server_connection) =
            tokio::join!(client.connect(&url, Some(addr)), server.accept());
        let (client_transport, _, client_kind) = client_connection.unwrap();
        let (server_transport, server_info) = server_connection.unwrap();
        let (client_parts, server_parts) = tokio::join!(
            Session::connect(client_transport, None, client_kind),
            Session::accept(server_transport, None, server_info.transport),
        );
        let (client_session, _, subscriber) = client_parts.unwrap();
        let (server_session, publisher, _) = server_parts.unwrap();
        ApiPeer {
            client_session,
            subscriber,
            server_session,
            publisher: publisher.unwrap(),
            _client: client,
            _server: server,
        }
    }

    async fn manual_peer() -> ManualPeer {
        tokio::time::timeout(Duration::from_secs(5), manual_peer_inner())
            .await
            .unwrap()
    }

    async fn manual_peer_inner() -> ManualPeer {
        let TestEndpoint {
            client: quic_client,
            mut server,
            url,
            addr,
        } = test_endpoint();
        let (client_connection, server_connection) =
            tokio::join!(quic_client.connect(&url, Some(addr)), server.accept());
        let (client_transport, _, client_kind) = client_connection.unwrap();
        let (server_transport, server_info) = server_connection.unwrap();
        let manual_setup = async {
            let (mut send, recv) = client_transport.open_bi().await.unwrap();
            let mut params = KeyValuePairs::default();
            params.set_intvalue(setup::ParameterType::MaxRequestId.into(), 100);
            write(&mut send, &setup::Client { params }).await;
            let mut recv = WireReader::new(recv);
            let _: setup::Server = recv.decode().await;
            (client_transport, send, recv)
        };
        let (manual, server_parts) = tokio::try_join!(
            async { Ok::<_, SessionError>(manual_setup.await) },
            Session::accept(server_transport, None, server_info.transport),
        )
        .unwrap();
        assert_eq!(client_kind, moq_transport::session::Transport::RawQuic);
        let (transport, control_send, control_recv) = manual;
        let (server_session, server_publisher, server_subscriber) = server_parts;
        ManualPeer {
            transport,
            control_send,
            control_recv,
            server_session,
            server_publisher: server_publisher.unwrap(),
            server_subscriber: server_subscriber.unwrap(),
            _client: quic_client,
            _server: server,
        }
    }

    fn fetch_request(id: u64, namespace: &TrackNamespace, group_id: u64) -> Message {
        fetch_request_with_params(id, namespace, group_id, KeyValuePairs::default())
    }

    fn fetch_request_with_params(
        id: u64,
        namespace: &TrackNamespace,
        group_id: u64,
        params: KeyValuePairs,
    ) -> Message {
        message::Fetch {
            id,
            fetch_type: FetchType::Standalone,
            standalone_fetch: Some(message::StandaloneFetch {
                track_namespace: namespace.clone(),
                track_name: "video".into(),
                start_location: Location::new(group_id, 0),
                end_location: Location::new(group_id + 1, 0),
            }),
            joining_fetch: None,
            params,
        }
        .into()
    }

    fn joining_fetch_request(
        id: u64,
        joining_request_id: u64,
        fetch_type: FetchType,
        joining_start: u64,
        params: KeyValuePairs,
    ) -> Message {
        message::Fetch {
            id,
            fetch_type,
            standalone_fetch: None,
            joining_fetch: Some(JoiningFetch {
                joining_request_id,
                joining_start,
            }),
            params,
        }
        .into()
    }

    fn largest_object_subscribe(
        id: u64,
        namespace: &TrackNamespace,
        track_name: TrackName,
    ) -> Message {
        let mut params = KeyValuePairs::default();
        params
            .set_subscription_filter(&SubscriptionFilter {
                filter_type: FilterType::LargestObject,
                start_location: None,
                end_group_id: None,
            })
            .unwrap();
        message::Subscribe {
            id,
            track_namespace: namespace.clone(),
            track_name,
            params,
        }
        .into()
    }

    fn fetch_group(request: &message::Fetch) -> u64 {
        request
            .standalone_fetch
            .as_ref()
            .unwrap()
            .start_location
            .group_id
    }

    fn assert_publisher_fetch(request: &message::Fetch) {
        assert_eq!(request.id % 2, 1, "publisher-facing FETCH ID must be odd");
    }

    fn push_varint(encoded: &mut Vec<u8>, value: u64) {
        match value {
            0..=63 => encoded.push(value as u8),
            64..=16_383 => encoded.extend_from_slice(&((value as u16) | 0x4000).to_be_bytes()),
            16_384..=1_073_741_823 => {
                encoded.extend_from_slice(&((value as u32) | 0x8000_0000).to_be_bytes())
            }
            1_073_741_824..=0x3fff_ffff_ffff_ffff => {
                encoded.extend_from_slice(&(value | 0xc000_0000_0000_0000).to_be_bytes())
            }
            _ => panic!("test varint out of range"),
        }
    }

    fn fetch_object(group_id: u64, object_id: u64, payload: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(payload.len() + 16);
        push_varint(&mut encoded, 0x1c); // Group, Object, and Priority fields present.
        push_varint(&mut encoded, group_id);
        push_varint(&mut encoded, object_id);
        encoded.push(17);
        push_varint(&mut encoded, payload.len() as u64);
        encoded.extend_from_slice(payload);
        encoded
    }

    fn deterministic_payload(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    fn fetch_object_with_wire_len(group_id: u64, len: usize) -> Vec<u8> {
        for payload_len in len.saturating_sub(16)..len {
            let body = fetch_object(group_id, 0, &deterministic_payload(payload_len, group_id));
            if body.len() == len {
                return body;
            }
        }
        panic!("could not build FETCH object with wire length {len}");
    }

    fn authorization_token(value: &[u8]) -> Vec<u8> {
        let mut token = Vec::with_capacity(value.len() + 2);
        push_varint(&mut token, 0x03); // USE_VALUE
        push_varint(&mut token, 0x00); // Application-defined token type.
        token.extend_from_slice(value);
        token
    }

    #[derive(Clone, Copy)]
    enum FetchResponseOrder {
        StreamFirst,
        OkFirst,
    }

    fn fetch_ok(request: &message::Fetch) -> Message {
        Message::FetchOk(message::FetchOk {
            id: request.id,
            end_of_track: false,
            end_location: request.standalone_fetch.as_ref().unwrap().end_location,
            params: KeyValuePairs::default(),
            track_extensions: Default::default(),
        })
    }

    async fn send_fetch_stream(transport: &web_transport::Session, request_id: u64, body: &[u8]) {
        let mut stream = transport.open_uni().await.unwrap();
        write(
            &mut stream,
            &FetchHeader {
                header_type: StreamHeaderType::Fetch,
                request_id,
            },
        )
        .await;
        write_bytes(&mut stream, body).await;
        stream.finish().unwrap();
    }

    async fn send_fetch_success(
        transport: &web_transport::Session,
        control: &mut web_transport::SendStream,
        request: &message::Fetch,
        body: &[u8],
        order: FetchResponseOrder,
    ) {
        if matches!(order, FetchResponseOrder::OkFirst) {
            write(control, &fetch_ok(request)).await;
        }
        send_fetch_stream(transport, request.id, body).await;
        if matches!(order, FetchResponseOrder::StreamFirst) {
            write(control, &fetch_ok(request)).await;
        }
    }

    async fn receive_fetch_reader(transport: &web_transport::Session) -> (u64, WireReader) {
        let stream = transport.accept_uni().await.unwrap();
        let mut stream = WireReader::new(stream);
        let header: StreamHeader = stream.decode().await;
        let id = header.fetch_header.unwrap().request_id;
        (id, stream)
    }

    async fn receive_fetch_stream(transport: &web_transport::Session) -> (u64, Vec<u8>) {
        let (id, stream) = receive_fetch_reader(transport).await;
        let body = stream.read_to_end().await;
        (id, body)
    }

    async fn receive_reset_stream(transport: &web_transport::Session, code: u8) {
        let mut stream = transport.accept_uni().await.unwrap();
        assert_eq!(stream.closed().await.unwrap(), Some(code));
    }

    async fn receive_fetch_ok(control: &mut WireReader, id: u64) {
        let Message::FetchOk(ok) = control.decode::<Message>().await else {
            panic!("expected FETCH_OK");
        };
        assert_eq!(ok.id, id);
    }

    async fn publisher_control_barrier(
        send: &mut web_transport::SendStream,
        recv: &mut WireReader,
    ) {
        write(
            send,
            &Message::PublishNamespace(message::PublishNamespace {
                id: 2,
                track_namespace: TrackNamespace::from_utf8_path("test/fetch-ok-barrier"),
                params: KeyValuePairs::default(),
            }),
        )
        .await;
        assert!(matches!(
            recv.decode::<Message>().await,
            Message::RequestOk(message::RequestOk { id: 2, .. })
        ));
    }

    fn public_fetch_object(group_id: u64, object_id: u64, payload_length: u64) -> FetchRecord {
        FetchRecord::Object(FetchRecordObject {
            group_id,
            subgroup_id: Some(0),
            object_id,
            publisher_priority: 7,
            extension_headers: ExtensionHeaders::default(),
            payload_length,
        })
    }

    #[tokio::test]
    async fn public_fetch_api_supports_both_success_orderings_and_rejection() {
        let ApiPeer {
            client_session,
            mut subscriber,
            server_session,
            mut publisher,
            _client,
            _server,
        } = api_peer().await;
        let namespace = TrackNamespace::from_utf8_path("test/public-fetch-api");

        let scenario = async {
            let request = message::StandaloneFetch {
                track_namespace: namespace.clone(),
                track_name: "video".into(),
                start_location: Location::new(0, 0),
                end_location: Location::new(1, 0),
            };

            let mut ok_first = subscriber
                .fetch(request.clone(), KeyValuePairs::default())
                .unwrap();
            let requested = publisher.fetch_requested().await.unwrap();
            let mut writer = requested
                .prepare_response(FetchOkInfo {
                    end_of_track: false,
                    end_location: Location::new(0, 3),
                    params: KeyValuePairs::default(),
                    track_extensions: Default::default(),
                })
                .await
                .unwrap();
            writer
                .write_record(&public_fetch_object(0, 0, 5))
                .await
                .unwrap();
            assert_eq!(
                ok_first.ok().await.unwrap().end_location,
                Location::new(0, 3)
            );
            writer
                .write_payload(Vec::from(&b"first"[..]).into())
                .await
                .unwrap();
            writer
                .write_record(&FetchRecord::NotExist {
                    end: Location::new(0, 2),
                })
                .await
                .unwrap();
            writer.finish().await.unwrap();
            assert!(matches!(
                ok_first.next().await.unwrap(),
                Some(FetchRecord::Object(_))
            ));
            assert_eq!(
                ok_first
                    .read_payload_chunk(64)
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                b"first"
            );
            assert_eq!(
                ok_first.next().await.unwrap(),
                Some(FetchRecord::NotExist {
                    end: Location::new(0, 2),
                })
            );
            assert!(ok_first.next().await.unwrap().is_none());

            let mut stream_first = subscriber
                .fetch(request.clone(), KeyValuePairs::default())
                .unwrap();
            let requested = publisher.fetch_requested().await.unwrap();
            let mut writer = requested.stream().await.unwrap();
            writer
                .write_record(&public_fetch_object(0, 0, 6))
                .await
                .unwrap();
            writer
                .write_payload(Vec::from(&b"second"[..]).into())
                .await
                .unwrap();
            assert!(matches!(
                stream_first.next().await.unwrap(),
                Some(FetchRecord::Object(_))
            ));
            assert_eq!(
                stream_first
                    .read_payload_chunk(64)
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                b"second"
            );
            writer
                .respond(FetchOkInfo {
                    end_of_track: false,
                    end_location: Location::new(0, 1),
                    params: KeyValuePairs::default(),
                    track_extensions: Default::default(),
                })
                .await
                .unwrap();
            writer.finish().await.unwrap();
            assert!(stream_first.next().await.unwrap().is_none());
            assert_eq!(
                stream_first.ok().await.unwrap().end_location,
                Location::new(0, 1)
            );

            let mut empty = subscriber
                .fetch(request.clone(), KeyValuePairs::default())
                .unwrap();
            let requested = publisher.fetch_requested().await.unwrap();
            let writer = requested
                .prepare_response(FetchOkInfo {
                    end_of_track: false,
                    end_location: Location::new(0, 0),
                    params: KeyValuePairs::default(),
                    track_extensions: Default::default(),
                })
                .await
                .unwrap();
            writer.finish().await.unwrap();
            assert!(empty.next().await.unwrap().is_none());
            assert_eq!(empty.ok().await.unwrap().end_location, Location::new(0, 0));

            let rejected = subscriber
                .fetch(request.clone(), KeyValuePairs::default())
                .unwrap();
            publisher
                .fetch_requested()
                .await
                .unwrap()
                .reject_with(
                    FetchRejection::new(RequestErrorCode::DoesNotExist, 42, "not retained")
                        .unwrap(),
                )
                .unwrap();
            assert!(rejected.ok().await.is_err());
            let rejection = rejected.rejection().unwrap();
            assert_eq!(
                rejection.error_code(),
                RequestErrorCode::DoesNotExist as u64
            );
            assert_eq!(rejection.retry_interval(), 42);
            assert_eq!(rejection.reason().0, "not retained");

            let incomplete = subscriber
                .fetch(request.clone(), KeyValuePairs::default())
                .unwrap();
            let requested = publisher.fetch_requested().await.unwrap();
            let mut writer = requested
                .prepare_response(FetchOkInfo {
                    end_of_track: false,
                    end_location: Location::new(0, 1),
                    params: KeyValuePairs::default(),
                    track_extensions: Default::default(),
                })
                .await
                .unwrap();
            writer
                .write_record(&public_fetch_object(0, 0, 2))
                .await
                .unwrap();
            writer
                .write_payload(Vec::from(&b"x"[..]).into())
                .await
                .unwrap();
            assert!(writer.finish().await.is_err());
            drop(incomplete);

            let overflow = subscriber
                .fetch(request.clone(), KeyValuePairs::default())
                .unwrap();
            let requested = publisher.fetch_requested().await.unwrap();
            let mut writer = requested
                .prepare_response(FetchOkInfo {
                    end_of_track: false,
                    end_location: Location::new(0, 1),
                    params: KeyValuePairs::default(),
                    track_extensions: Default::default(),
                })
                .await
                .unwrap();
            writer
                .write_record(&public_fetch_object(0, 0, 1))
                .await
                .unwrap();
            assert!(writer
                .write_payload(Vec::from(&b"xx"[..]).into())
                .await
                .is_err());
            drop(overflow);

            let mut abandoned = subscriber
                .fetch(request.clone(), KeyValuePairs::default())
                .unwrap();
            let requested = publisher.fetch_requested().await.unwrap();
            let writer = requested
                .prepare_response(FetchOkInfo {
                    end_of_track: false,
                    end_location: Location::new(0, 1),
                    params: KeyValuePairs::default(),
                    track_extensions: Default::default(),
                })
                .await
                .unwrap();
            drop(writer);
            assert!(
                tokio::time::timeout(Duration::from_secs(1), abandoned.next())
                    .await
                    .expect("dropped response did not settle the FETCH")
                    .is_err()
            );

            let canceled_before_write = subscriber
                .fetch(request.clone(), KeyValuePairs::default())
                .unwrap();
            let requested = publisher.fetch_requested().await.unwrap();
            let writer = requested
                .prepare_response(FetchOkInfo {
                    end_of_track: false,
                    end_location: Location::new(0, 1),
                    params: KeyValuePairs::default(),
                    track_extensions: Default::default(),
                })
                .await
                .unwrap();
            drop(canceled_before_write);
            assert!(matches!(writer.closed().await, Err(ServeError::Cancel)));

            let canceled = subscriber.fetch(request, KeyValuePairs::default()).unwrap();
            let requested = publisher.fetch_requested().await.unwrap();
            let writer = requested.stream().await.unwrap();
            drop(canceled);
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(1), writer.closed())
                    .await
                    .expect("FETCH_CANCEL did not wake the writer"),
                Err(ServeError::Cancel)
            ));
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = scenario => {},
                result = client_session.run() => panic!("client session ended: {result:?}"),
                result = server_session.run() => panic!("server session ended: {result:?}"),
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn actual_session_run_exit_cancels_existing_and_late_fetches() {
        let ManualPeer {
            transport,
            control_send: _control_send,
            mut control_recv,
            server_session,
            server_publisher: _server_publisher,
            mut server_subscriber,
            _client,
            _server,
        } = manual_peer().await;
        let session = tokio::spawn(server_session.run());
        let namespace = TrackNamespace::from_utf8_path("test/malformed-fetch-session");
        let request = message::StandaloneFetch {
            track_namespace: namespace,
            track_name: "video".into(),
            start_location: Location::new(0, 0),
            end_location: Location::new(1, 0),
        };
        let late_request = request.clone();

        let mut malformed = server_subscriber
            .fetch(request.clone(), KeyValuePairs::default())
            .unwrap();
        let sibling = server_subscriber
            .fetch(request, KeyValuePairs::default())
            .unwrap();
        let Message::Fetch(malformed_request) = control_recv.decode::<Message>().await else {
            panic!("expected first FETCH");
        };
        let Message::Fetch(_sibling_request) = control_recv.decode::<Message>().await else {
            panic!("expected sibling FETCH");
        };
        let sibling_ok = sibling.ok();
        tokio::pin!(sibling_ok);
        assert!(futures::poll!(&mut sibling_ok).is_pending());

        // On the first Object, 0x01 references a prior Subgroup, Group, Object,
        // and Priority that do not exist (draft-16 section 10.4.4.1).
        send_fetch_stream(&transport, malformed_request.id, &[0x01]).await;
        let fetch_error = malformed.next().await.unwrap_err();
        assert!(matches!(
            fetch_error,
            SessionError::Decode(DecodeError::InvalidValue)
        ));

        let session_error = tokio::time::timeout(Duration::from_secs(5), session)
            .await
            .expect("malformed FETCH did not end the session")
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            session_error,
            SessionError::Decode(DecodeError::InvalidValue)
        ));
        assert_eq!(session_error.code(), 0x3);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), &mut sibling_ok)
                .await
                .expect("session shutdown did not cancel the sibling FETCH"),
            Err(ServeError::Cancel)
        ));
        let late = server_subscriber
            .fetch(late_request, KeyValuePairs::default())
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::ZERO, late.ok())
                .await
                .expect("late FETCH did not observe session cancellation"),
            Err(ServeError::Cancel)
        ));

        let peer_error = tokio::time::timeout(Duration::from_secs(5), transport.closed())
            .await
            .expect("peer did not receive the session close");
        let web_transport::Error::Session(web_transport::quinn::SessionError::ConnectionError(
            web_transport::quinn::quinn::ConnectionError::ApplicationClosed(close),
        )) = peer_error
        else {
            panic!("expected peer PROTOCOL_VIOLATION, got {peer_error:?}");
        };
        assert_eq!(close.error_code.into_inner(), 0x3);
    }

    #[tokio::test]
    async fn public_api_issues_relative_and_absolute_joining_fetches() {
        let ApiPeer {
            client_session,
            mut subscriber,
            server_session,
            mut publisher,
            _client,
            _server,
        } = api_peer().await;
        let namespace = TrackNamespace::from_utf8_path("test/public-joining-api");
        let (subscribe_writer, _subscribe_reader) =
            Track::new(namespace.clone(), "video").produce();
        let (source_writer, source_reader) = Track::new(namespace.clone(), "video").produce();
        let mut source = source_writer.datagrams().unwrap();
        source
            .write(Datagram {
                group_id: 7,
                object_id: 11,
                priority: 1,
                payload: Vec::from(&b"live"[..]).into(),
                extension_headers: Default::default(),
            })
            .unwrap();
        let (max_subscribe_writer, _max_subscribe_reader) =
            Track::new(namespace.clone(), "max").produce();
        let (max_source_writer, max_source_reader) = Track::new(namespace.clone(), "max").produce();
        let mut max_source = max_source_writer.datagrams().unwrap();
        max_source
            .write(Datagram {
                group_id: 8,
                object_id: VarInt::MAX.into_inner(),
                priority: 1,
                payload: Vec::from(&b"max"[..]).into(),
                extension_headers: Default::default(),
            })
            .unwrap();

        let scenario = async {
            let mut subscribe_params = KeyValuePairs::default();
            subscribe_params
                .set_subscription_filter(&SubscriptionFilter::largest_object())
                .unwrap();
            {
                let subscribe = subscriber
                    .subscribe_open_with_params(subscribe_writer, subscribe_params.clone());
                tokio::pin!(subscribe);
                let subscribed = tokio::select! {
                    _ = &mut subscribe => panic!("SUBSCRIBE completed before serving"),
                    subscribed = publisher.subscribed() => subscribed.unwrap(),
                };
                let serve = tokio::spawn(async move { subscribed.serve(source_reader).await });
                let subscribe = subscribe.await.unwrap();

                let relative = subscribe
                    .fetch_joining(
                        moq_transport::session::JoiningStart::Relative(3),
                        KeyValuePairs::default(),
                    )
                    .unwrap();
                let relative_request = publisher.fetch_requested().await.unwrap();
                assert_eq!(
                    relative_request.resolve().await.unwrap(),
                    Some(message::StandaloneFetch {
                        track_namespace: namespace.clone(),
                        track_name: "video".into(),
                        start_location: Location::new(4, 0),
                        end_location: Location::new(7, 12),
                    })
                );
                relative_request
                    .reject(RequestErrorCode::DoesNotExist, "relative tested")
                    .unwrap();
                assert!(relative.ok().await.is_err());

                let absolute = subscribe
                    .fetch_joining(
                        moq_transport::session::JoiningStart::Absolute(5),
                        KeyValuePairs::default(),
                    )
                    .unwrap();
                let absolute_request = publisher.fetch_requested().await.unwrap();
                assert_eq!(
                    absolute_request.resolve().await.unwrap(),
                    Some(message::StandaloneFetch {
                        track_namespace: namespace.clone(),
                        track_name: "video".into(),
                        start_location: Location::new(5, 0),
                        end_location: Location::new(7, 12),
                    })
                );
                absolute_request
                    .reject(RequestErrorCode::DoesNotExist, "absolute tested")
                    .unwrap();
                assert!(absolute.ok().await.is_err());

                drop(subscribe);
                drop(source);
                serve.await.unwrap().unwrap();
            }

            let max_subscribe =
                subscriber.subscribe_open_with_params(max_subscribe_writer, subscribe_params);
            tokio::pin!(max_subscribe);
            let max_subscribed = tokio::select! {
                _ = &mut max_subscribe => panic!("max SUBSCRIBE completed before serving"),
                subscribed = publisher.subscribed() => subscribed.unwrap(),
            };
            let max_serve =
                tokio::spawn(async move { max_subscribed.serve(max_source_reader).await });
            let max_subscribe = max_subscribe.await.unwrap();
            let mut max_fetch = max_subscribe
                .fetch_joining(
                    moq_transport::session::JoiningStart::Relative(0),
                    KeyValuePairs::default(),
                )
                .unwrap();
            let max_request = publisher.fetch_requested().await.unwrap();
            assert_eq!(
                max_request.resolve().await.unwrap(),
                Some(message::StandaloneFetch {
                    track_namespace: namespace,
                    track_name: "max".into(),
                    start_location: Location::new(8, 0),
                    end_location: Location::new(8, 0),
                })
            );
            let mut max_writer = max_request
                .prepare_response(FetchOkInfo {
                    end_of_track: false,
                    end_location: Location::new(8, 0),
                    params: KeyValuePairs::default(),
                    track_extensions: Default::default(),
                })
                .await
                .unwrap();
            max_writer
                .write_record(&public_fetch_object(8, VarInt::MAX.into_inner(), 3))
                .await
                .unwrap();
            max_writer
                .write_payload(Vec::from(&b"max"[..]).into())
                .await
                .unwrap();
            max_writer.finish().await.unwrap();
            assert_eq!(
                max_fetch.ok().await.unwrap().end_location,
                Location::new(8, 0)
            );
            let Some(FetchRecord::Object(max_object)) = max_fetch.next().await.unwrap() else {
                panic!("expected max Object");
            };
            assert_eq!(max_object.group_id, 8);
            assert_eq!(max_object.object_id, VarInt::MAX.into_inner());
            assert_eq!(
                max_fetch
                    .read_payload_chunk(64)
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                b"max"
            );
            assert!(max_fetch.next().await.unwrap().is_none());
            drop(max_subscribe);
            drop(max_source);
            max_serve.await.unwrap().unwrap();
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = scenario => {},
                result = client_session.run() => panic!("client session ended: {result:?}"),
                result = server_session.run() => panic!("server session ended: {result:?}"),
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn local_joining_fetch_handoff_is_contiguous_and_non_overlapping() {
        let mut downstream = manual_peer().await;
        let mut upstream = manual_peer().await;
        let coordinator: Arc<dyn Coordinator> = Arc::new(MockCoordinator::without_route());
        let mut locals = Locals::new();
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let (upstream_namespaces, runner) =
            UpstreamNamespaces::new(locals.clone(), remotes.clone(), coordinator.clone());
        tokio::spawn(runner.run());
        let producer = Producer::new_with_upstream_namespaces(
            downstream.server_publisher,
            locals.clone(),
            remotes,
            upstream_namespaces,
            SessionContext::public(None),
            None, // auth: no token enforcement for this FETCH test
        );
        let namespace = TrackNamespace::from_utf8_path("test/joining/exact");
        let track_name = TrackName::from(vec![0, 0xff, b'v']);
        let (writer, reader) = Track::new(namespace.clone(), track_name.clone()).produce();
        let mut datagrams = writer.datagrams().unwrap();
        datagrams
            .write(Datagram {
                group_id: 7,
                object_id: 11,
                priority: 9,
                payload: Vec::from(&b"saved"[..]).into(),
                extension_headers: Default::default(),
            })
            .unwrap();
        let track_registration = locals.register_track(None, reader).await.unwrap();
        let (namespace_registration, namespace_requests) = locals
            .register_namespace_with_fetch(
                None,
                namespace.clone(),
                upstream.server_subscriber.clone(),
            )
            .await
            .unwrap();

        let scenario = async {
            write(
                &mut downstream.control_send,
                &largest_object_subscribe(0, &namespace, track_name.clone()),
            )
            .await;
            let Message::SubscribeOk(ok) = downstream.control_recv.decode::<Message>().await else {
                panic!("expected SUBSCRIBE_OK");
            };
            assert_eq!(ok.id, 0);
            assert_eq!(
                ok.params.largest_object().unwrap(),
                Some(Location::new(7, 11))
            );
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(100),
                    downstream.transport.recv_datagram()
                )
                .await
                .is_err(),
                "SUBSCRIBE delivered the saved Largest Object"
            );

            let mut params = KeyValuePairs::default();
            params.set_group_order(GroupOrder::Descending);
            write(
                &mut downstream.control_send,
                &joining_fetch_request(2, 0, FetchType::RelativeJoining, 3, params),
            )
            .await;
            let Message::Fetch(upstream_fetch) = upstream.control_recv.decode::<Message>().await
            else {
                panic!("expected normalized upstream FETCH");
            };
            assert_eq!(upstream_fetch.fetch_type, FetchType::Standalone);
            assert!(upstream_fetch.joining_fetch.is_none());
            assert_eq!(
                upstream_fetch.standalone_fetch,
                Some(message::StandaloneFetch {
                    track_namespace: namespace.clone(),
                    track_name: track_name.clone(),
                    start_location: Location::new(4, 0),
                    end_location: Location::new(7, 12),
                })
            );
            assert_eq!(
                upstream_fetch.params.group_order().unwrap(),
                Some(GroupOrder::Descending)
            );

            let body = fetch_object(7, 11, b"saved");
            send_fetch_success(
                &upstream.transport,
                &mut upstream.control_send,
                &upstream_fetch,
                &body,
                FetchResponseOrder::StreamFirst,
            )
            .await;
            assert_eq!(receive_fetch_stream(&downstream.transport).await, (2, body));
            receive_fetch_ok(&mut downstream.control_recv, 2).await;
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(100),
                    downstream.transport.accept_uni()
                )
                .await
                .is_err(),
                "FETCH delivered more than one stream"
            );

            datagrams
                .write(Datagram {
                    group_id: 7,
                    object_id: 12,
                    priority: 9,
                    payload: Vec::from(&b"next"[..]).into(),
                    extension_headers: Default::default(),
                })
                .unwrap();
            let mut encoded =
                tokio::time::timeout(Duration::from_secs(1), downstream.transport.recv_datagram())
                    .await
                    .expect("SUBSCRIBE did not deliver the next Object")
                    .unwrap();
            let next = WireDatagram::decode(&mut encoded).unwrap();
            assert_eq!(next.group_id, 7);
            assert_eq!(next.object_id, Some(12));
            assert_eq!(next.payload.as_deref(), Some(&b"next"[..]));
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(100),
                    downstream.transport.recv_datagram()
                )
                .await
                .is_err(),
                "SUBSCRIBE delivered more than one object"
            );

            drop(namespace_requests);
            drop(namespace_registration);
            drop(track_registration);
            drop(datagrams);
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = scenario => {},
                result = producer.run() => panic!("producer ended: {result:?}"),
                result = downstream.server_session.run() => panic!("downstream session ended: {result:?}"),
                result = upstream.server_session.run() => panic!("upstream session ended: {result:?}"),
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cancelled_pending_joining_fetch_creates_no_upstream_request() {
        let mut downstream = manual_peer().await;
        let mut upstream = manual_peer().await;
        let coordinator: Arc<dyn Coordinator> = Arc::new(MockCoordinator::without_route());
        let mut locals = Locals::new();
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let (upstream_namespaces, runner) =
            UpstreamNamespaces::new(locals.clone(), remotes.clone(), coordinator.clone());
        tokio::spawn(runner.run());
        let producer = Producer::new_with_upstream_namespaces(
            downstream.server_publisher,
            locals.clone(),
            remotes,
            upstream_namespaces,
            SessionContext::public(None),
            None, // auth: no token enforcement for this FETCH test
        );
        let namespace = TrackNamespace::from_utf8_path("test/joining/pending");
        let (namespace_registration, mut namespace_requests) = locals
            .register_namespace_with_fetch(
                None,
                namespace.clone(),
                upstream.server_subscriber.clone(),
            )
            .await
            .unwrap();

        let scenario = async {
            write(
                &mut downstream.control_send,
                &largest_object_subscribe(0, &namespace, "video".into()),
            )
            .await;
            write(
                &mut downstream.control_send,
                &joining_fetch_request(
                    2,
                    0,
                    FetchType::RelativeJoining,
                    1,
                    KeyValuePairs::default(),
                ),
            )
            .await;
            write(
                &mut downstream.control_send,
                &Message::FetchCancel(message::FetchCancel { id: 2 }),
            )
            .await;

            let request = namespace_requests.recv().await.unwrap();
            let mut pending_datagrams = request.writer.datagrams().unwrap();
            pending_datagrams
                .write(Datagram {
                    group_id: 3,
                    object_id: 1,
                    priority: 1,
                    payload: Vec::from(&b"live"[..]).into(),
                    extension_headers: Default::default(),
                })
                .unwrap();
            request.upstream.established();
            let Message::SubscribeOk(ok) = downstream.control_recv.decode::<Message>().await else {
                panic!("expected SUBSCRIBE_OK after pending establishment");
            };
            assert_eq!(ok.id, 0);

            write(
                &mut downstream.control_send,
                &fetch_request(4, &namespace, 9),
            )
            .await;

            let Message::Fetch(upstream_fetch) = upstream.control_recv.decode::<Message>().await
            else {
                panic!("expected sentinel standalone FETCH");
            };
            assert_eq!(upstream_fetch.fetch_type, FetchType::Standalone);
            assert_eq!(fetch_group(&upstream_fetch), 9);
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(50),
                    upstream.control_recv.decode::<Message>()
                )
                .await
                .is_err(),
                "canceled Joining FETCH opened a delayed upstream request"
            );
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(50),
                    downstream.control_recv.decode::<Message>()
                )
                .await
                .is_err(),
                "canceled Joining FETCH produced an extra response"
            );

            drop(request.lease);
            drop(request.upstream);
            drop(pending_datagrams);
            drop(namespace_registration);
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = scenario => {},
                result = producer.run() => panic!("producer ended: {result:?}"),
                result = downstream.server_session.run() => panic!("downstream session ended: {result:?}"),
                result = upstream.server_session.run() => panic!("upstream session ended: {result:?}"),
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn immediate_unsubscribe_before_subscribe_serve_is_benign() {
        let mut peer = manual_peer().await;
        let mut publisher = peer.server_publisher.clone();
        let namespace = TrackNamespace::from_utf8_path("test/joining/unsubscribe-race");
        let (writer, reader) = Track::new(namespace.clone(), "video").produce();
        let mut datagrams = writer.datagrams().unwrap();
        datagrams
            .write(Datagram {
                group_id: 2,
                object_id: 3,
                priority: 1,
                payload: Vec::from(&b"live"[..]).into(),
                extension_headers: Default::default(),
            })
            .unwrap();

        let scenario = async {
            write(
                &mut peer.control_send,
                &largest_object_subscribe(0, &namespace, "video".into()),
            )
            .await;
            write(
                &mut peer.control_send,
                &Message::Unsubscribe(message::Unsubscribe { id: 0 }),
            )
            .await;
            write(
                &mut peer.control_send,
                &joining_fetch_request(
                    2,
                    0,
                    FetchType::RelativeJoining,
                    0,
                    KeyValuePairs::default(),
                ),
            )
            .await;

            let Message::RequestError(error) = peer.control_recv.decode::<Message>().await else {
                panic!("expected removed subscription rejection");
            };
            assert_eq!(error.id, 2);
            assert_eq!(
                error.error_code,
                RequestErrorCode::InvalidJoiningRequestId as u64
            );

            let subscribed = publisher.subscribed().await.unwrap();
            assert!(matches!(
                subscribed.serve(reader).await,
                Err(SessionError::Serve(ServeError::Cancel))
            ));
            write(
                &mut peer.control_send,
                &joining_fetch_request(
                    4,
                    0,
                    FetchType::RelativeJoining,
                    0,
                    KeyValuePairs::default(),
                ),
            )
            .await;
            let Message::RequestError(error) = peer.control_recv.decode::<Message>().await else {
                panic!("canceled SUBSCRIBE produced an extra response");
            };
            assert_eq!(error.id, 4);
            assert_eq!(
                error.error_code,
                RequestErrorCode::InvalidJoiningRequestId as u64
            );

            drop(datagrams);
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = scenario => {},
                result = peer.server_session.run() => panic!("server session ended: {result:?}"),
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn natural_subscribe_completion_retires_joining_association() {
        let mut peer = manual_peer().await;
        let mut publisher = peer.server_publisher.clone();
        let mut fetch_publisher = publisher.clone();
        let namespace = TrackNamespace::from_utf8_path("test/joining/natural-completion");
        let (writer, reader) = Track::new(namespace.clone(), "video").produce();
        let mut datagrams = writer.datagrams().unwrap();
        datagrams
            .write(Datagram {
                group_id: 2,
                object_id: 3,
                priority: 1,
                payload: Vec::from(&b"live"[..]).into(),
                extension_headers: Default::default(),
            })
            .unwrap();

        let scenario = async {
            write(
                &mut peer.control_send,
                &largest_object_subscribe(0, &namespace, "video".into()),
            )
            .await;
            let subscribed = publisher.subscribed().await.unwrap();
            let serve = subscribed.serve(reader);
            tokio::pin!(serve);
            let Message::SubscribeOk(ok) = (tokio::select! {
                result = &mut serve => panic!("subscription ended before SUBSCRIBE_OK: {result:?}"),
                message = peer.control_recv.decode::<Message>() => message,
            }) else {
                panic!("expected SUBSCRIBE_OK");
            };
            assert_eq!(ok.id, 0);

            write(
                &mut peer.control_send,
                &joining_fetch_request(
                    2,
                    0,
                    FetchType::RelativeJoining,
                    0,
                    KeyValuePairs::default(),
                ),
            )
            .await;
            let accepted = fetch_publisher.fetch_requested().await.unwrap();

            drop(datagrams);
            serve.await.unwrap();
            let Message::PublishDone(done) = peer.control_recv.decode::<Message>().await else {
                panic!("expected PUBLISH_DONE");
            };
            assert_eq!(done.id, 0);

            write(
                &mut peer.control_send,
                &joining_fetch_request(
                    4,
                    0,
                    FetchType::RelativeJoining,
                    0,
                    KeyValuePairs::default(),
                ),
            )
            .await;
            let Message::RequestError(error) = peer.control_recv.decode::<Message>().await else {
                panic!("expected completed association rejection");
            };
            assert_eq!(error.id, 4);
            assert_eq!(
                error.error_code,
                RequestErrorCode::InvalidJoiningRequestId as u64
            );

            assert_eq!(
                accepted.resolve().await.unwrap(),
                Some(message::StandaloneFetch {
                    track_namespace: namespace,
                    track_name: "video".into(),
                    start_location: Location::new(2, 0),
                    end_location: Location::new(2, 4),
                })
            );
            accepted
                .reject(RequestErrorCode::DoesNotExist, "test complete")
                .unwrap();
            let Message::RequestError(error) = peer.control_recv.decode::<Message>().await else {
                panic!("expected accepted FETCH rejection");
            };
            assert_eq!(error.id, 2);
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = scenario => {},
                result = peer.server_session.run() => panic!("server session ended: {result:?}"),
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn failed_established_subscribe_retires_joining_association() {
        let mut peer = manual_peer().await;
        let mut publisher = peer.server_publisher.clone();
        let mut fetch_publisher = publisher.clone();
        let namespace = TrackNamespace::from_utf8_path("test/joining/failed-established");
        let (writer, reader) = Track::new(namespace.clone(), "video").produce();
        let mut datagrams = writer.datagrams().unwrap();
        datagrams
            .write(Datagram {
                group_id: 2,
                object_id: 3,
                priority: 1,
                payload: Vec::from(&b"live"[..]).into(),
                extension_headers: Default::default(),
            })
            .unwrap();

        let scenario = async {
            write(
                &mut peer.control_send,
                &largest_object_subscribe(0, &namespace, "video".into()),
            )
            .await;
            let subscribed = publisher.subscribed().await.unwrap();
            let serve = subscribed.serve(reader);
            tokio::pin!(serve);
            let Message::SubscribeOk(ok) = (tokio::select! {
                result = &mut serve => panic!("subscription ended before SUBSCRIBE_OK: {result:?}"),
                message = peer.control_recv.decode::<Message>() => message,
            }) else {
                panic!("expected SUBSCRIBE_OK");
            };
            assert_eq!(ok.id, 0);

            write(
                &mut peer.control_send,
                &joining_fetch_request(
                    2,
                    0,
                    FetchType::RelativeJoining,
                    0,
                    KeyValuePairs::default(),
                ),
            )
            .await;
            let accepted = fetch_publisher.fetch_requested().await.unwrap();

            datagrams
                .close(ServeError::Internal("source failed".to_string()))
                .unwrap();
            assert!(matches!(
                serve.await,
                Err(SessionError::Serve(ServeError::Internal(reason))) if reason == "source failed"
            ));
            let Message::PublishDone(done) = peer.control_recv.decode::<Message>().await else {
                panic!("expected PUBLISH_DONE");
            };
            assert_eq!(done.id, 0);
            assert_eq!(
                done.status_code,
                message::PublishDoneCode::InternalError as u64
            );

            write(
                &mut peer.control_send,
                &joining_fetch_request(
                    4,
                    0,
                    FetchType::RelativeJoining,
                    0,
                    KeyValuePairs::default(),
                ),
            )
            .await;
            let Message::RequestError(error) = peer.control_recv.decode::<Message>().await else {
                panic!("expected failed association rejection");
            };
            assert_eq!(error.id, 4);
            assert_eq!(
                error.error_code,
                RequestErrorCode::InvalidJoiningRequestId as u64
            );

            assert_eq!(
                accepted.resolve().await.unwrap(),
                Some(message::StandaloneFetch {
                    track_namespace: namespace,
                    track_name: "video".into(),
                    start_location: Location::new(2, 0),
                    end_location: Location::new(2, 4),
                })
            );
            accepted
                .reject(RequestErrorCode::DoesNotExist, "snapshot preserved")
                .unwrap();
            let Message::RequestError(error) = peer.control_recv.decode::<Message>().await else {
                panic!("expected accepted FETCH rejection");
            };
            assert_eq!(error.id, 2);
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = scenario => {},
                result = peer.server_session.run() => panic!("server session ended: {result:?}"),
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn publish_association_is_joinable_only_after_publish_ok() {
        let mut peer = manual_peer().await;
        let namespace = TrackNamespace::from_utf8_path("test/joining/publish");
        let track_name = TrackName::from(vec![0, 0xfe, b'p']);
        let (writer, reader) = Track::new(namespace.clone(), track_name.clone()).produce();
        let mut datagrams = writer.datagrams().unwrap();
        datagrams
            .write(Datagram {
                group_id: 5,
                object_id: 8,
                priority: 1,
                payload: Vec::from(&b"live"[..]).into(),
                extension_headers: Default::default(),
            })
            .unwrap();
        let mut publisher = peer.server_publisher.clone();
        let published = publisher
            .publish(reader, KeyValuePairs::default())
            .await
            .unwrap();
        let publish_id = published.info.id;
        let mut fetch_publisher = publisher.clone();

        let scenario = async {
            let Message::Publish(publish) = peer.control_recv.decode::<Message>().await else {
                panic!("expected PUBLISH");
            };
            assert_eq!(publish.id, publish_id);
            assert_eq!(
                publish.params.largest_object().unwrap(),
                Some(Location::new(5, 8))
            );

            for (id, joining_request_id) in [(0, publish_id), (2, publish_id + 100)] {
                write(
                    &mut peer.control_send,
                    &joining_fetch_request(
                        id,
                        joining_request_id,
                        FetchType::RelativeJoining,
                        1,
                        KeyValuePairs::default(),
                    ),
                )
                .await;
                let Message::RequestError(error) = peer.control_recv.decode::<Message>().await
                else {
                    panic!("expected INVALID_JOINING_REQUEST_ID");
                };
                assert_eq!(error.id, id);
                assert_eq!(
                    error.error_code,
                    RequestErrorCode::InvalidJoiningRequestId as u64
                );
            }

            let mut ok_params = KeyValuePairs::default();
            ok_params
                .set_subscription_filter(&SubscriptionFilter::largest_object())
                .unwrap();
            write(
                &mut peer.control_send,
                &Message::PublishOk(message::PublishOk {
                    id: publish_id,
                    params: ok_params,
                }),
            )
            .await;
            write(
                &mut peer.control_send,
                &joining_fetch_request(
                    4,
                    publish_id,
                    FetchType::AbsoluteJoining,
                    3,
                    KeyValuePairs::default(),
                ),
            )
            .await;
            let fetch = fetch_publisher.fetch_requested().await.unwrap();
            assert_eq!(
                fetch.resolve().await.unwrap(),
                Some(message::StandaloneFetch {
                    track_namespace: namespace.clone(),
                    track_name: track_name.clone(),
                    start_location: Location::new(3, 0),
                    end_location: Location::new(5, 9),
                })
            );
            fetch
                .reject(RequestErrorCode::DoesNotExist, "test complete")
                .unwrap();
            let Message::RequestError(error) = peer.control_recv.decode::<Message>().await else {
                panic!("expected FETCH rejection");
            };
            assert_eq!(error.id, 4);

            write(
                &mut peer.control_send,
                &Message::Unsubscribe(message::Unsubscribe { id: publish_id }),
            )
            .await;
            write(
                &mut peer.control_send,
                &joining_fetch_request(
                    6,
                    publish_id,
                    FetchType::RelativeJoining,
                    0,
                    KeyValuePairs::default(),
                ),
            )
            .await;
            let Message::RequestError(error) = peer.control_recv.decode::<Message>().await else {
                panic!("expected terminated association rejection");
            };
            assert_eq!(error.id, 6);
            assert_eq!(
                error.error_code,
                RequestErrorCode::InvalidJoiningRequestId as u64
            );

            drop(published);
            drop(datagrams);
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = scenario => {},
                result = peer.server_session.run() => panic!("server session ended: {result:?}"),
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn joining_fetch_with_non_largest_filter_closes_session() {
        let filters = [
            None,
            Some(SubscriptionFilter {
                filter_type: FilterType::NextGroupStart,
                start_location: None,
                end_group_id: None,
            }),
            Some(SubscriptionFilter {
                filter_type: FilterType::AbsoluteStart,
                start_location: Some(Location::new(0, 0)),
                end_group_id: None,
            }),
            Some(SubscriptionFilter {
                filter_type: FilterType::AbsoluteRange,
                start_location: Some(Location::new(0, 0)),
                end_group_id: Some(1),
            }),
        ];

        for filter in filters {
            let mut peer = manual_peer().await;
            let namespace = TrackNamespace::from_utf8_path("test/joining/filter");
            let mut params = KeyValuePairs::default();
            if let Some(filter) = filter {
                params.set_subscription_filter(&filter).unwrap();
            }
            write(
                &mut peer.control_send,
                &Message::Subscribe(message::Subscribe {
                    id: 0,
                    track_namespace: namespace,
                    track_name: "video".into(),
                    params,
                }),
            )
            .await;
            write(
                &mut peer.control_send,
                &joining_fetch_request(
                    2,
                    0,
                    FetchType::RelativeJoining,
                    0,
                    KeyValuePairs::default(),
                ),
            )
            .await;

            let result = tokio::time::timeout(Duration::from_secs(5), peer.server_session.run())
                .await
                .unwrap();
            assert!(matches!(result, Err(SessionError::ProtocolViolation(_))));
        }
    }

    #[tokio::test]
    async fn two_relay_joining_fetch_is_normalized_and_cache_free() {
        let mut downstream = manual_peer().await;
        let mut publisher = manual_peer().await;
        let TestEndpoint {
            client: route_client,
            server: mut origin_server,
            url: origin_url,
            addr: origin_addr,
        } = test_endpoint();
        let edge_coordinator: Arc<dyn Coordinator> = Arc::new(MockCoordinator::with_route(
            origin_url,
            origin_addr,
            route_client,
        ));
        let origin_coordinator: Arc<dyn Coordinator> = Arc::new(MockCoordinator::without_route());
        let mut edge_locals = Locals::new();
        let origin_locals = Locals::new();
        let edge_remotes = RemoteManager::new(edge_coordinator.clone(), Vec::new());
        let origin_remotes = RemoteManager::new(origin_coordinator.clone(), Vec::new());
        let (edge_upstream_namespaces, edge_runner) = UpstreamNamespaces::new(
            edge_locals.clone(),
            edge_remotes.clone(),
            edge_coordinator.clone(),
        );
        tokio::spawn(edge_runner.run());
        let edge = Producer::new_with_upstream_namespaces(
            downstream.server_publisher,
            edge_locals.clone(),
            edge_remotes,
            edge_upstream_namespaces,
            SessionContext::public(Some("scope-a".to_string())),
            None, // auth: no token enforcement for this FETCH test
        );
        let origin_consumer = Consumer::new(
            publisher.server_subscriber,
            origin_locals.clone(),
            origin_coordinator.clone(),
            origin_remotes.clone(),
            None,
            SessionContext::public(Some("scope-a".to_string())),
            None, // auth: no token enforcement for this FETCH test
        );
        let origin_locals_for_connection = origin_locals.clone();
        let origin_remotes_for_connection = origin_remotes.clone();
        let origin_coordinator_for_connection = origin_coordinator.clone();
        let origin_connection = async move {
            let (transport, info) = origin_server.accept().await.unwrap();
            let (session, relay_publisher, _) = Session::accept(transport, None, info.transport)
                .await
                .unwrap();
            let (origin_upstream_namespaces, origin_runner) = UpstreamNamespaces::new(
                origin_locals_for_connection.clone(),
                origin_remotes_for_connection.clone(),
                origin_coordinator_for_connection.clone(),
            );
            tokio::spawn(origin_runner.run());
            let origin = Producer::new_with_upstream_namespaces(
                relay_publisher.unwrap(),
                origin_locals_for_connection,
                origin_remotes_for_connection,
                origin_upstream_namespaces,
                SessionContext::internal(Some("scope-a".to_string()), None),
                None, // auth: no token enforcement for this FETCH test
            );
            tokio::select! {
                result = session.run() => panic!("origin relay session ended: {result:?}"),
                result = origin.run() => panic!("origin producer ended: {result:?}"),
            }
        };
        let namespace = TrackNamespace::from_utf8_path("test/joining/two-relay");
        let track_name = TrackName::from(vec![0, 0xfd, b'r']);
        let (writer, reader) = Track::new(namespace.clone(), track_name.clone()).produce();
        let mut datagrams = writer.datagrams().unwrap();
        datagrams
            .write(Datagram {
                group_id: 12,
                object_id: 4,
                priority: 1,
                payload: Vec::from(&b"edge-live"[..]).into(),
                extension_headers: Default::default(),
            })
            .unwrap();
        let edge_track_registration = edge_locals
            .register_track(Some("scope-a"), reader)
            .await
            .unwrap();

        let scenario = async {
            write(
                &mut publisher.control_send,
                &Message::PublishNamespace(message::PublishNamespace {
                    id: 0,
                    track_namespace: namespace.clone(),
                    params: KeyValuePairs::default(),
                }),
            )
            .await;
            assert!(matches!(
                publisher.control_recv.decode::<Message>().await,
                Message::RequestOk(message::RequestOk { id: 0, .. })
            ));

            write(
                &mut downstream.control_send,
                &largest_object_subscribe(0, &namespace, track_name.clone()),
            )
            .await;
            let Message::SubscribeOk(ok) = downstream.control_recv.decode::<Message>().await else {
                panic!("expected SUBSCRIBE_OK");
            };
            assert_eq!(
                ok.params.largest_object().unwrap(),
                Some(Location::new(12, 4))
            );

            let mut upstream_ids = HashSet::new();
            for id in [2, 4] {
                let start = 10;
                write(
                    &mut downstream.control_send,
                    &joining_fetch_request(
                        id,
                        0,
                        FetchType::AbsoluteJoining,
                        start,
                        KeyValuePairs::default(),
                    ),
                )
                .await;
                let Message::Fetch(origin_fetch) = publisher.control_recv.decode::<Message>().await
                else {
                    panic!("expected origin Standalone FETCH");
                };
                assert_publisher_fetch(&origin_fetch);
                assert!(upstream_ids.insert(origin_fetch.id));
                assert_eq!(origin_fetch.fetch_type, FetchType::Standalone);
                assert!(origin_fetch.joining_fetch.is_none());
                assert_eq!(
                    origin_fetch.standalone_fetch,
                    Some(message::StandaloneFetch {
                        track_namespace: namespace.clone(),
                        track_name: track_name.clone(),
                        start_location: Location::new(start, 0),
                        end_location: Location::new(12, 5),
                    })
                );

                let body = fetch_object(start, 0, &[id as u8]);
                send_fetch_success(
                    &publisher.transport,
                    &mut publisher.control_send,
                    &origin_fetch,
                    &body,
                    FetchResponseOrder::StreamFirst,
                )
                .await;
                assert_eq!(
                    receive_fetch_stream(&downstream.transport).await,
                    (id, body)
                );
                receive_fetch_ok(&mut downstream.control_recv, id).await;
            }

            drop(edge_track_registration);
            drop(datagrams);
        };

        tokio::time::timeout(Duration::from_secs(8), async {
            tokio::select! {
                _ = scenario => {},
                result = edge.run() => panic!("edge producer ended: {result:?}"),
                result = origin_consumer.run() => panic!("origin consumer ended: {result:?}"),
                _ = origin_connection => {},
                result = downstream.server_session.run() => panic!("downstream session ended: {result:?}"),
                result = publisher.server_session.run() => panic!("publisher session ended: {result:?}"),
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn local_namespace_fetch_passthrough_is_fresh_and_bidirectional() {
        let mut downstream = manual_peer().await;
        let mut upstream = manual_peer().await;
        let test_coordinator = MockCoordinator::without_route();
        let lookups = test_coordinator.lookups.clone();
        let coordinator: Arc<dyn Coordinator> = Arc::new(test_coordinator);
        let locals = Locals::new();
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let (upstream_namespaces, runner) =
            UpstreamNamespaces::new(locals.clone(), remotes.clone(), coordinator.clone());
        tokio::spawn(runner.run());
        let producer = Producer::new_with_upstream_namespaces(
            downstream.server_publisher,
            locals.clone(),
            remotes.clone(),
            upstream_namespaces,
            SessionContext::public(None),
            None, // auth: no token enforcement for this FETCH test
        );
        let consumer = Consumer::new(
            upstream.server_subscriber,
            locals.clone(),
            coordinator,
            remotes,
            None,
            SessionContext::public(None),
            None, // auth: no token enforcement for this FETCH test
        );
        let namespace = TrackNamespace::from_utf8_path("test/fetch");

        let scenario = async {
            write(
                &mut upstream.control_send,
                &Message::PublishNamespace(message::PublishNamespace {
                    id: 0,
                    track_namespace: namespace.clone(),
                    params: KeyValuePairs::default(),
                }),
            )
            .await;
            assert!(matches!(
                upstream.control_recv.decode::<Message>().await,
                Message::RequestOk(message::RequestOk { id: 0, .. })
            ));

            let bodies = vec![
                (fetch_object(0, 0, &[]), None),
                (fetch_object(1, 0, &[7]), None),
                (fetch_object_with_wire_len(2, 65_535), Some(65_535)),
                (fetch_object_with_wire_len(3, 65_536), Some(65_536)),
                (fetch_object_with_wire_len(4, 65_537), Some(65_537)),
                (
                    fetch_object_with_wire_len(5, 3 * 65_536 + 17),
                    Some(3 * 65_536 + 17),
                ),
            ];
            let mut upstream_ids = HashSet::new();
            for (case, (body, expected_len)) in bodies.into_iter().enumerate() {
                let id = (case * 2) as u64;
                let group_id = case as u64;
                if let Some(expected_len) = expected_len {
                    assert_eq!(body.len(), expected_len);
                }
                write(
                    &mut downstream.control_send,
                    &fetch_request(id, &namespace, group_id),
                )
                .await;
                let Message::Fetch(request) = upstream.control_recv.decode::<Message>().await
                else {
                    panic!("expected upstream FETCH");
                };
                assert_publisher_fetch(&request);
                assert!(upstream_ids.insert(request.id));
                assert_eq!(fetch_group(&request), group_id);
                match case {
                    0 => {
                        send_fetch_stream(&upstream.transport, request.id, &body).await;
                        let (downstream_id, mut reader) =
                            receive_fetch_reader(&downstream.transport).await;
                        assert_eq!(downstream_id, id);
                        assert_eq!(reader.read_exact(body.len()).await, body);
                        write(&mut upstream.control_send, &fetch_ok(&request)).await;
                        assert!(reader.read_to_end().await.is_empty());
                        receive_fetch_ok(&mut downstream.control_recv, id).await;
                    }
                    1 => {
                        write(&mut upstream.control_send, &fetch_ok(&request)).await;
                        publisher_control_barrier(
                            &mut upstream.control_send,
                            &mut upstream.control_recv,
                        )
                        .await;
                        send_fetch_stream(&upstream.transport, request.id, &body).await;
                        assert_eq!(
                            receive_fetch_stream(&downstream.transport).await,
                            (id, body)
                        );
                        receive_fetch_ok(&mut downstream.control_recv, id).await;
                    }
                    _ => {
                        send_fetch_success(
                            &upstream.transport,
                            &mut upstream.control_send,
                            &request,
                            &body,
                            FetchResponseOrder::StreamFirst,
                        )
                        .await;
                        assert_eq!(
                            receive_fetch_stream(&downstream.transport).await,
                            (id, body)
                        );
                        receive_fetch_ok(&mut downstream.control_recv, id).await;
                    }
                }
            }

            write(
                &mut downstream.control_send,
                &fetch_request(12, &namespace, 6),
            )
            .await;
            let Message::Fetch(empty) = upstream.control_recv.decode::<Message>().await else {
                panic!("expected empty upstream FETCH");
            };
            assert_publisher_fetch(&empty);
            assert!(upstream_ids.insert(empty.id));
            send_fetch_success(
                &upstream.transport,
                &mut upstream.control_send,
                &empty,
                &[],
                FetchResponseOrder::OkFirst,
            )
            .await;
            assert_eq!(
                receive_fetch_stream(&downstream.transport).await,
                (12, Vec::new())
            );
            receive_fetch_ok(&mut downstream.control_recv, 12).await;

            for (id, group_id, reason) in [(14, 7, "first miss"), (16, 8, "second miss")] {
                write(
                    &mut downstream.control_send,
                    &fetch_request(id, &namespace, group_id),
                )
                .await;
                let Message::Fetch(request) = upstream.control_recv.decode::<Message>().await
                else {
                    panic!("expected upstream FETCH");
                };
                assert_publisher_fetch(&request);
                assert!(upstream_ids.insert(request.id));
                write(
                    &mut upstream.control_send,
                    &Message::RequestError(message::RequestError::new(
                        request.id,
                        RequestErrorCode::DoesNotExist,
                        0,
                        reason,
                    )),
                )
                .await;
                let Message::RequestError(error) =
                    downstream.control_recv.decode::<Message>().await
                else {
                    panic!("expected downstream REQUEST_ERROR");
                };
                assert_eq!(error.id, id);
                assert_eq!(error.error_code, RequestErrorCode::DoesNotExist as u64);
                assert_eq!(error.reason.0, reason);
                receive_reset_stream(&downstream.transport, 0).await;
            }

            write(
                &mut downstream.control_send,
                &fetch_request(18, &namespace, 9),
            )
            .await;
            let Message::Fetch(recovered) = upstream.control_recv.decode::<Message>().await else {
                panic!("expected successful upstream FETCH after REQUEST_ERROR");
            };
            assert_publisher_fetch(&recovered);
            assert!(upstream_ids.insert(recovered.id));
            let body = fetch_object(9, 0, &deterministic_payload(31, 9));
            send_fetch_success(
                &upstream.transport,
                &mut upstream.control_send,
                &recovered,
                &body,
                FetchResponseOrder::StreamFirst,
            )
            .await;
            assert_eq!(
                receive_fetch_stream(&downstream.transport).await,
                (18, body)
            );
            receive_fetch_ok(&mut downstream.control_recv, 18).await;
            assert_eq!(lookups.load(Ordering::Relaxed), 0);
        };

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = scenario => {},
                result = producer.run() => panic!("producer ended: {result:?}"),
                result = consumer.run() => panic!("consumer ended: {result:?}"),
                result = downstream.server_session.run() => panic!("downstream server ended: {result:?}"),
                result = upstream.server_session.run() => panic!("upstream server ended: {result:?}"),
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn remote_fetch_lookups_keep_scopes_isolated() {
        let coordinator = Arc::new(MockCoordinator::without_route());
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let first = moq_transport::message::StandaloneFetch {
            track_namespace: TrackNamespace::from_utf8_path("scope-a/fetch"),
            track_name: "video".into(),
            start_location: Location::new(0, 0),
            end_location: Location::new(1, 0),
        };
        let second = moq_transport::message::StandaloneFetch {
            track_namespace: TrackNamespace::from_utf8_path("scope-b/fetch"),
            ..first.clone()
        };

        let (first_result, second_result) = tokio::join!(
            remotes.fetch(Some("scope-a"), first, KeyValuePairs::default()),
            remotes.fetch(Some("scope-b"), second, KeyValuePairs::default()),
        );

        assert!(first_result.unwrap().is_none());
        assert!(second_result.unwrap().is_none());
        let requests: HashSet<_> = coordinator
            .lookup_requests
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect();
        assert_eq!(
            requests,
            HashSet::from([
                (
                    Some("scope-a".to_string()),
                    TrackNamespace::from_utf8_path("scope-a/fetch"),
                ),
                (
                    Some("scope-b".to_string()),
                    TrackNamespace::from_utf8_path("scope-b/fetch"),
                ),
            ])
        );
    }

    #[tokio::test]
    async fn two_relay_fetch_is_fresh_and_cache_free() {
        let mut downstream = manual_peer().await;
        let mut publisher = manual_peer().await;
        let TestEndpoint {
            client: route_client,
            server: mut origin_server,
            url: origin_url,
            addr: origin_addr,
        } = test_endpoint();
        let edge_coordinator =
            MockCoordinator::with_route(origin_url.clone(), origin_addr, route_client);
        let edge_lookups = edge_coordinator.lookups.clone();
        let edge_lookup_requests = edge_coordinator.lookup_requests.clone();
        let edge_coordinator: Arc<dyn Coordinator> = Arc::new(edge_coordinator);
        let origin_coordinator = MockCoordinator::without_route();
        let origin_lookups = origin_coordinator.lookups.clone();
        let origin_coordinator: Arc<dyn Coordinator> = Arc::new(origin_coordinator);
        let edge_locals = Locals::new();
        let origin_locals = Locals::new();
        let edge_remotes = RemoteManager::new(edge_coordinator.clone(), Vec::new());
        let origin_remotes = RemoteManager::new(origin_coordinator.clone(), Vec::new());
        let (edge_upstream_namespaces, edge_runner) = UpstreamNamespaces::new(
            edge_locals.clone(),
            edge_remotes.clone(),
            edge_coordinator.clone(),
        );
        tokio::spawn(edge_runner.run());
        let edge = Producer::new_with_upstream_namespaces(
            downstream.server_publisher,
            edge_locals,
            edge_remotes,
            edge_upstream_namespaces,
            SessionContext::public(Some("scope-a".to_string())),
            None, // auth: no token enforcement for this FETCH test
        );
        let origin_consumer = Consumer::new(
            publisher.server_subscriber,
            origin_locals.clone(),
            origin_coordinator.clone(),
            origin_remotes.clone(),
            None,
            SessionContext::public(Some("scope-a".to_string())),
            None, // auth: no token enforcement for this FETCH test
        );
        let origin_locals_for_connection = origin_locals.clone();
        let origin_remotes_for_connection = origin_remotes.clone();
        let origin_coordinator_for_connection = origin_coordinator.clone();
        let origin_connection = async move {
            let (transport, info) = origin_server.accept().await.unwrap();
            let (session, relay_publisher, _) = Session::accept(transport, None, info.transport)
                .await
                .unwrap();
            let (origin_upstream_namespaces, origin_runner) = UpstreamNamespaces::new(
                origin_locals_for_connection.clone(),
                origin_remotes_for_connection.clone(),
                origin_coordinator_for_connection.clone(),
            );
            tokio::spawn(origin_runner.run());
            let origin = Producer::new_with_upstream_namespaces(
                relay_publisher.unwrap(),
                origin_locals_for_connection,
                origin_remotes_for_connection,
                origin_upstream_namespaces,
                SessionContext::internal(Some("scope-a".to_string()), None),
                None, // auth: no token enforcement for this FETCH test
            );
            tokio::select! {
                result = session.run() => panic!("origin relay session ended: {result:?}"),
                result = origin.run() => panic!("origin producer ended: {result:?}"),
            }
        };
        let namespace = TrackNamespace::from_utf8_path("test/fetch");
        let missing = TrackNamespace::from_utf8_path("test/missing");

        let scenario = async {
            write(
                &mut publisher.control_send,
                &Message::PublishNamespace(message::PublishNamespace {
                    id: 0,
                    track_namespace: namespace.clone(),
                    params: KeyValuePairs::default(),
                }),
            )
            .await;
            assert!(matches!(
                publisher.control_recv.decode::<Message>().await,
                Message::RequestOk(message::RequestOk { id: 0, .. })
            ));

            let mut first_params = KeyValuePairs::default();
            first_params.set_bytesvalue(
                parameter_type::AUTHORIZATION_TOKEN,
                authorization_token(b"first"),
            );
            first_params.set_subscriber_priority(7);
            first_params.set_group_order(GroupOrder::Descending);
            let mut second_params = KeyValuePairs::default();
            second_params.set_bytesvalue(
                parameter_type::AUTHORIZATION_TOKEN,
                authorization_token(b"second"),
            );
            second_params.set_subscriber_priority(9);
            second_params.set_group_order(GroupOrder::Ascending);
            write(
                &mut downstream.control_send,
                &fetch_request_with_params(0, &namespace, 0, first_params.clone()),
            )
            .await;
            write(
                &mut downstream.control_send,
                &fetch_request_with_params(2, &namespace, 1, second_params),
            )
            .await;
            let Message::Fetch(first) = publisher.control_recv.decode::<Message>().await else {
                panic!("expected first publisher FETCH");
            };
            let Message::Fetch(second) = publisher.control_recv.decode::<Message>().await else {
                panic!("expected second publisher FETCH");
            };
            assert_ne!(first.id, second.id);
            let mut requests = HashMap::new();
            for request in [first, second] {
                assert_publisher_fetch(&request);
                let group_id = fetch_group(&request);
                let expected_order = match group_id {
                    0 => GroupOrder::Descending,
                    1 => GroupOrder::Ascending,
                    _ => panic!("unexpected concurrent FETCH range"),
                };
                assert_eq!(request.params.group_order().unwrap(), Some(expected_order));
                assert_eq!(request.params.subscriber_priority().unwrap(), None);
                assert!(request
                    .params
                    .get(parameter_type::AUTHORIZATION_TOKEN)
                    .is_none());
                assert!(requests.insert(group_id, request).is_none());
            }
            let first = requests.remove(&0).expect("missing group 0 FETCH");
            let second = requests.remove(&1).expect("missing group 1 FETCH");
            assert!(requests.is_empty());
            let mut upstream_ids = HashSet::from([first.id, second.id]);
            let first_body = fetch_object(0, 0, &deterministic_payload(37, 0));
            let second_body = fetch_object(1, 0, &deterministic_payload(91, 1));
            send_fetch_success(
                &publisher.transport,
                &mut publisher.control_send,
                &second,
                &second_body,
                FetchResponseOrder::OkFirst,
            )
            .await;
            send_fetch_success(
                &publisher.transport,
                &mut publisher.control_send,
                &first,
                &first_body,
                FetchResponseOrder::StreamFirst,
            )
            .await;
            let responses = [
                receive_fetch_stream(&downstream.transport).await,
                receive_fetch_stream(&downstream.transport).await,
            ];
            let mut response_locations = HashMap::new();
            for _ in 0..2 {
                let Message::FetchOk(ok) = downstream.control_recv.decode::<Message>().await else {
                    panic!("expected FETCH_OK");
                };
                assert!(response_locations.insert(ok.id, ok.end_location).is_none());
            }
            assert_eq!(response_locations.get(&0), Some(&Location::new(1, 0)));
            assert_eq!(response_locations.get(&2), Some(&Location::new(2, 0)));
            let bodies: HashMap<_, _> = responses.into_iter().collect();
            assert_eq!(bodies.get(&0), Some(&first_body));
            assert_eq!(bodies.get(&2), Some(&second_body));

            write(
                &mut downstream.control_send,
                &fetch_request_with_params(4, &namespace, 0, first_params),
            )
            .await;
            let Message::Fetch(third) = publisher.control_recv.decode::<Message>().await else {
                panic!("expected sequential publisher FETCH");
            };
            assert_publisher_fetch(&third);
            assert!(upstream_ids.insert(third.id));
            assert_eq!(fetch_group(&third), 0);
            assert_eq!(
                third.standalone_fetch.as_ref(),
                first.standalone_fetch.as_ref()
            );
            assert_eq!(third.params, first.params);
            let repeat_body = fetch_object(0, 0, &deterministic_payload(53, 99));
            send_fetch_success(
                &publisher.transport,
                &mut publisher.control_send,
                &third,
                &repeat_body,
                FetchResponseOrder::OkFirst,
            )
            .await;
            let response = receive_fetch_stream(&downstream.transport).await;
            receive_fetch_ok(&mut downstream.control_recv, 4).await;
            assert_eq!(response, (4, repeat_body));

            write(
                &mut downstream.control_send,
                &fetch_request(6, &namespace, 2),
            )
            .await;
            let Message::Fetch(failed) = publisher.control_recv.decode::<Message>().await else {
                panic!("expected failed publisher FETCH");
            };
            assert_publisher_fetch(&failed);
            assert!(upstream_ids.insert(failed.id));
            write(
                &mut publisher.control_send,
                &Message::RequestError(message::RequestError::new(
                    failed.id,
                    RequestErrorCode::DoesNotExist,
                    42,
                    "origin miss",
                )),
            )
            .await;
            let Message::RequestError(error) = downstream.control_recv.decode::<Message>().await
            else {
                panic!("expected downstream REQUEST_ERROR");
            };
            assert_eq!(error.id, 6);
            assert_eq!(error.error_code, RequestErrorCode::DoesNotExist as u64);
            assert_eq!(error.retry_interval, 42);
            assert_eq!(error.reason.0, "origin miss");
            receive_reset_stream(&downstream.transport, 0).await;

            write(
                &mut downstream.control_send,
                &fetch_request(8, &namespace, 3),
            )
            .await;
            let Message::Fetch(recovered) = publisher.control_recv.decode::<Message>().await else {
                panic!("expected successful FETCH after REQUEST_ERROR");
            };
            assert_publisher_fetch(&recovered);
            assert!(upstream_ids.insert(recovered.id));
            let recovered_body = fetch_object(3, 0, &deterministic_payload(113, 3));
            send_fetch_success(
                &publisher.transport,
                &mut publisher.control_send,
                &recovered,
                &recovered_body,
                FetchResponseOrder::StreamFirst,
            )
            .await;
            assert_eq!(
                receive_fetch_stream(&downstream.transport).await,
                (8, recovered_body)
            );
            receive_fetch_ok(&mut downstream.control_recv, 8).await;

            write(
                &mut downstream.control_send,
                &fetch_request(10, &namespace, 4),
            )
            .await;
            let Message::Fetch(cancelled) = publisher.control_recv.decode::<Message>().await else {
                panic!("expected cancellable publisher FETCH");
            };
            assert_publisher_fetch(&cancelled);
            assert!(upstream_ids.insert(cancelled.id));
            let cancel_payload = deterministic_payload(3 * 65_536 + 17, 4);
            let cancel_body = fetch_object(4, 0, &cancel_payload);
            let framing_len = cancel_body.len() - cancel_payload.len();
            let partial_len = framing_len + 65_536 + 1024;
            assert!(partial_len > 65_536);
            let mut upstream_stream = publisher.transport.open_uni().await.unwrap();
            write(
                &mut upstream_stream,
                &FetchHeader {
                    header_type: StreamHeaderType::Fetch,
                    request_id: cancelled.id,
                },
            )
            .await;
            write_bytes(&mut upstream_stream, &cancel_body[..partial_len]).await;

            let (cancelled_id, mut downstream_stream) =
                receive_fetch_reader(&downstream.transport).await;
            assert_eq!(cancelled_id, 10);
            assert_eq!(
                downstream_stream.read_exact(partial_len).await,
                cancel_body[..partial_len]
            );
            write(
                &mut downstream.control_send,
                &Message::FetchCancel(message::FetchCancel { id: 10 }),
            )
            .await;
            let upstream_cancel = async {
                assert!(matches!(
                    publisher.control_recv.decode::<Message>().await,
                    Message::FetchCancel(message::FetchCancel { id }) if id == cancelled.id
                ));
            };
            let ((), downstream_reset) =
                tokio::join!(upstream_cancel, downstream_stream.reset_code());
            assert_eq!(downstream_reset, 1);
            upstream_stream.reset(0);

            write(
                &mut downstream.control_send,
                &fetch_request(12, &namespace, 5),
            )
            .await;
            let Message::Fetch(after_cancel) = publisher.control_recv.decode::<Message>().await
            else {
                panic!("expected successful FETCH after cancellation");
            };
            assert_publisher_fetch(&after_cancel);
            assert!(upstream_ids.insert(after_cancel.id));
            let after_cancel_body = fetch_object(5, 0, &deterministic_payload(67, 5));
            send_fetch_success(
                &publisher.transport,
                &mut publisher.control_send,
                &after_cancel,
                &after_cancel_body,
                FetchResponseOrder::OkFirst,
            )
            .await;
            assert_eq!(
                receive_fetch_stream(&downstream.transport).await,
                (12, after_cancel_body)
            );
            receive_fetch_ok(&mut downstream.control_recv, 12).await;

            write(
                &mut downstream.control_send,
                &fetch_request(14, &missing, 6),
            )
            .await;
            let Message::RequestError(error) = downstream.control_recv.decode::<Message>().await
            else {
                panic!("expected missing-track REQUEST_ERROR");
            };
            assert_eq!(error.id, 14);
            assert_eq!(error.error_code, RequestErrorCode::DoesNotExist as u64);
            assert_eq!(origin_lookups.load(Ordering::Relaxed), 1);
            assert_eq!(edge_lookups.load(Ordering::Relaxed), 8);
            assert!(edge_lookup_requests
                .lock()
                .unwrap()
                .iter()
                .all(|(scope, _)| scope.as_deref() == Some("scope-a")));
        };

        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = scenario => {},
                _ = origin_connection => {},
                result = edge.run() => panic!("edge producer ended: {result:?}"),
                result = origin_consumer.run() => panic!("origin consumer ended: {result:?}"),
                result = downstream.server_session.run() => panic!("downstream relay session ended: {result:?}"),
                result = publisher.server_session.run() => panic!("publisher relay session ended: {result:?}"),
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn expected_serve_shutdown_accepts_wrapped_session_errors() {
        assert!(Producer::is_expected_serve_shutdown(&anyhow::Error::new(
            SessionError::Serve(ServeError::Cancel)
        )));
        assert!(Producer::is_expected_serve_shutdown(&anyhow::Error::new(
            SessionError::Serve(ServeError::Done)
        )));
        assert!(!Producer::is_expected_serve_shutdown(&anyhow::Error::new(
            SessionError::Serve(ServeError::NotFound)
        )));
    }

    #[test]
    fn expected_serve_shutdown_accepts_direct_serve_errors() {
        assert!(Producer::is_expected_serve_shutdown(&anyhow::Error::new(
            ServeError::Cancel
        )));
        assert!(Producer::is_expected_serve_shutdown(&anyhow::Error::new(
            ServeError::Done
        )));
        assert!(!Producer::is_expected_serve_shutdown(&anyhow::Error::new(
            ServeError::NotFound
        )));
    }
}
