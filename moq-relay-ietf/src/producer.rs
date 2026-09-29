// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    coding::{KeyValuePairs, TrackNamespace},
    message::{RequestErrorCode, SubscribeOptions},
    serve::{FullTrackName, ServeError, TrackReader, TracksReader},
    session::{
        FetchRequested, Publisher, SessionError, Subscribed, SubscribedNamespace,
        TrackStatusRequested,
    },
};
use tokio::sync::broadcast;

use crate::{
    metrics::{GaugeGuard, TimingGuard},
    upstream_namespaces::UpstreamNamespaces,
    Coordinator, Locals, NamespaceChange, RemoteManager, SessionContext, TrackChange,
    UpstreamReady,
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
    pub fn new(
        publisher: Publisher,
        locals: Locals,
        remotes: RemoteManager,
        coordinator: Arc<dyn Coordinator>,
        context: SessionContext,
    ) -> Self {
        let (upstream_namespaces, runner) =
            UpstreamNamespaces::new(locals.clone(), remotes.clone(), coordinator);
        tokio::spawn(runner.run());
        Self::new_with_upstream_namespaces(publisher, locals, remotes, upstream_namespaces, context)
    }

    pub(crate) fn new_with_upstream_namespaces(
        publisher: Publisher,
        locals: Locals,
        remotes: RemoteManager,
        upstream_namespaces: UpstreamNamespaces,
        context: SessionContext,
    ) -> Self {
        Self {
            publisher,
            locals,
            remotes,
            upstream_namespaces,
            context,
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
        let standalone = fetch
            .request
            .standalone_fetch
            .clone()
            .ok_or_else(|| anyhow::anyhow!("standalone FETCH missing range"))?;
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
            self.send_namespace_snapshot(&mut subscribed_namespace, &mut known_namespaces)?;
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
                            self.apply_namespace_change(&mut subscribed_namespace, &mut known_namespaces, change)?;
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            // Recoverable: a full resync reconstructs the state the
                            // skipped events would have produced. Counted so that
                            // sustained churn outgrowing the channel capacity is
                            // visible before it shows up as latency.
                            metrics::counter!("moq_relay_change_channel_lagged_total", "channel" => "namespace")
                                .increment(skipped);
                            self.resync_namespaces(&mut subscribed_namespace, &mut known_namespaces)?;
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

    fn send_namespace_snapshot(
        &self,
        subscribed_namespace: &mut SubscribedNamespace,
        known: &mut HashSet<TrackNamespace>,
    ) -> Result<(), ServeError> {
        for namespace in self
            .locals
            .list_namespaces_matching(self.context.scope(), &subscribed_namespace.namespace_prefix)
        {
            if known.insert(namespace.clone()) {
                subscribed_namespace.namespace(&namespace)?;
            }
        }

        Ok(())
    }

    fn apply_namespace_change(
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
            if known.insert(change.namespace.clone()) {
                subscribed_namespace.namespace(&change.namespace)?;
            }
        } else if known.remove(&change.namespace) {
            subscribed_namespace.namespace_done(&change.namespace)?;
        }

        Ok(())
    }

    fn resync_namespaces(
        &self,
        subscribed_namespace: &mut SubscribedNamespace,
        known: &mut HashSet<TrackNamespace>,
    ) -> Result<(), ServeError> {
        let current: HashSet<_> = self
            .locals
            .list_namespaces_matching(self.context.scope(), &subscribed_namespace.namespace_prefix)
            .into_iter()
            .collect();

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
        coding::{Decode, DecodeError, Encode, KeyValuePairs, Location, TrackNamespace},
        data::{FetchHeader, StreamHeader, StreamHeaderType},
        message::{self, parameter_type, FetchType, GroupOrder, Message, RequestErrorCode},
        serve::ServeError,
        session::{Session, SessionError},
        setup,
    };

    use crate::test::{test_endpoint, TestEndpoint};
    use crate::{
        Consumer, Coordinator, CoordinatorContext, CoordinatorError, CoordinatorResult, Locals,
        NamespaceOrigin, NamespaceRegistration, RemoteManager, SessionContext,
    };

    use super::Producer;

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

    #[tokio::test]
    async fn local_namespace_fetch_passthrough_is_fresh_and_bidirectional() {
        let mut downstream = manual_peer().await;
        let mut upstream = manual_peer().await;
        let test_coordinator = MockCoordinator::without_route();
        let lookups = test_coordinator.lookups.clone();
        let coordinator: Arc<dyn Coordinator> = Arc::new(test_coordinator);
        let locals = Locals::new();
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let producer = Producer::new(
            downstream.server_publisher,
            locals.clone(),
            remotes.clone(),
            coordinator.clone(),
            SessionContext::public(None),
        );
        let consumer = Consumer::new(
            upstream.server_subscriber,
            locals.clone(),
            coordinator,
            remotes,
            None,
            SessionContext::public(None),
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
        let edge = Producer::new(
            downstream.server_publisher,
            edge_locals,
            edge_remotes,
            edge_coordinator,
            SessionContext::public(Some("scope-a".to_string())),
        );
        let origin_consumer = Consumer::new(
            publisher.server_subscriber,
            origin_locals.clone(),
            origin_coordinator.clone(),
            origin_remotes.clone(),
            None,
            SessionContext::public(Some("scope-a".to_string())),
        );
        let origin_locals_for_connection = origin_locals.clone();
        let origin_remotes_for_connection = origin_remotes.clone();
        let origin_coordinator_for_connection = origin_coordinator.clone();
        let origin_connection = async move {
            let (transport, info) = origin_server.accept().await.unwrap();
            let (session, relay_publisher, _) = Session::accept(transport, None, info.transport)
                .await
                .unwrap();
            let origin = Producer::new(
                relay_publisher.unwrap(),
                origin_locals_for_connection,
                origin_remotes_for_connection,
                origin_coordinator_for_connection,
                SessionContext::internal(Some("scope-a".to_string()), None),
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
