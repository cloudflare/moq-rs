// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use anyhow::Context;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::{
    coding::{KeyValuePairs, TrackName, TrackNamespace, TrackNamespacePrefix},
    message::RequestErrorCode,
    serve::{FullTrackName, ServeError, TrackReader, TracksReader},
    session::{
        FetchRequested, Publisher, ServeWithDeadlineError, SessionError, Subscribed,
        SubscribedNamespace, SubscribedTracks, TrackStatusRequested,
    },
};
use tokio::sync::{broadcast, OwnedSemaphorePermit, Semaphore};

use crate::auth::{authorize, AuthzOperation, DenyReason, SessionAuth};
use crate::{
    local::MAX_CONCURRENT_RENDEZVOUS_HOLDS,
    metrics::{GaugeGuard, TimingGuard},
    remote::RemoteSubscribeError,
    upstream_namespaces::UpstreamNamespaces,
    Locals, NamespaceChange, PublishedTrackChange, RemoteManager, SessionContext, TrackChange,
    TrackSnapshot, UpstreamReady,
};

/// Ceiling on how long a SUBSCRIBE is held waiting for a publisher to appear.
///
/// draft-18 §10.2.6 lets a relay use a shorter window than the subscriber asked
/// for, without telling it. Each held subscribe pins a bidi stream slot and its
/// relay-side state for the whole window. [`Locals`] separately caps concurrent
/// holds across all sessions on the relay.
///
/// Hardcoded for now. The eventual source is per-scope configuration, which is
/// the same knob the pre-spec "lingering subscribe" config carried before it was
/// removed; 30s was its default there too.
const MAX_RENDEZVOUS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Upper bound on serving one FETCH, from receipt to the end of the relayed
/// response. A relayed FETCH holds an upstream request and a downstream
/// stream for its whole life, so it is bounded like a rendezvous hold.
const FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Keep one session from consuming the relay's entire rendezvous budget. Half
/// the relay-wide capacity remains available to other sessions.
const MAX_CONCURRENT_RENDEZVOUS_HOLDS_PER_SESSION: usize = MAX_CONCURRENT_RENDEZVOUS_HOLDS / 2;
const MAX_SUBSCRIBE_TRACKS_SLOTS_PER_SESSION: usize = 1024;
const MAX_SUBSCRIBE_TRACKS_CHILDREN_PER_SESSION: usize = 128;

type ProducerTask = futures::future::BoxFuture<'static, ()>;

struct SubscribeTracksSlot {
    desired: Option<TrackReader>,
    active: Option<TrackReader>,
    /// draft-18 section 6.1 forbids a later PUBLISH for this track after
    /// PUBLISH_BLOCKED. The parent SUBSCRIBE_TRACKS owns this tombstone until
    /// it is cancelled, which drops its slot map and releases the permit.
    publish_blocked_tombstone: bool,
    _session_slot_permit: OwnedSemaphorePermit,
}

#[derive(Debug, Eq, PartialEq)]
enum SubscribeTracksSlotAction {
    None,
    Publish,
    Remove,
}

impl SubscribeTracksSlot {
    fn set_desired(&mut self, track: TrackReader) -> bool {
        if self.publish_blocked_tombstone
            || self
                .active
                .as_ref()
                .is_some_and(|active| same_track_generation(active, &track))
        {
            return false;
        }
        self.desired = Some(track);
        true
    }

    fn complete(&mut self, generation: &TrackReader) -> SubscribeTracksSlotAction {
        if self
            .active
            .as_ref()
            .is_some_and(|active| same_track_generation(active, generation))
        {
            self.active = None;
        }
        if self
            .desired
            .as_ref()
            .is_some_and(|desired| same_track_generation(desired, generation))
        {
            self.desired = None;
        }
        if self.active.is_some() || self.publish_blocked_tombstone {
            SubscribeTracksSlotAction::None
        } else if self.desired.is_some() {
            SubscribeTracksSlotAction::Publish
        } else {
            SubscribeTracksSlotAction::Remove
        }
    }

    fn remove_desired(&mut self, generation: &TrackReader) -> bool {
        if self
            .desired
            .as_ref()
            .is_some_and(|desired| same_track_generation(desired, generation))
        {
            self.desired = None;
        }
        self.active.is_none() && self.desired.is_none() && !self.publish_blocked_tombstone
    }
}

struct PublishCompletion {
    full_name: FullTrackName,
    generation: TrackReader,
}

/// How often a held SUBSCRIBE asks the coordinator again whether a publisher has
/// turned up.
///
/// The broadcasts that wake a hold only carry publishers on this relay, but
/// several relay instances serve one scope, so the publisher a subscriber is
/// waiting for routinely lands on a different one. Its namespace goes into
/// shared coordinator state and nothing tells this relay to look again, so
/// without a second trigger the hold expires while the routing information sits
/// there. Asking on an interval limits timer-driven rechecks to fifteen at the
/// 30s ceiling. Publisher events and lagged broadcast receivers can trigger
/// additional lookups.
///
/// Coordinator lookups can require network calls, so a tight interval would let
/// a handful of holds consume routing capacity needed by other subscriptions.
///
/// The two costs of choosing it this way: a cross-relay publisher goes unnoticed
/// for up to one interval after it arrives, and a window shorter than the
/// interval expires without asking again, so those stay same-relay only.
const RENDEZVOUS_RECHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

/// How long to hold a SUBSCRIBE for, given what the subscriber asked for.
///
/// `None` means do not hold: answer immediately with DOES_NOT_EXIST, which is
/// what draft-18 requires for both an absent parameter and an explicit 0.
fn rendezvous_hold(
    requested_ms: Option<u64>,
    max: std::time::Duration,
) -> Option<std::time::Duration> {
    match requested_ms {
        None | Some(0) => None,
        Some(ms) => Some(std::time::Duration::from_millis(ms).min(max)),
    }
}

/// Whether a newly announced namespace could serve a track in `target`.
///
/// `Locals` routes a track to the longest registered namespace that prefixes it,
/// so an announcement is relevant when it prefixes the track's namespace, not
/// only when it matches exactly.
fn namespace_covers(announced: &TrackNamespace, target: &TrackNamespace) -> bool {
    announced.fields.len() <= target.fields.len()
        && announced
            .fields
            .iter()
            .zip(target.fields.iter())
            .all(|(a, b)| a == b)
}

/// How a rendezvous hold ended.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Rendezvous {
    /// Something that could publish this track arrived. The caller repeats the
    /// lookup rather than assuming the track is now servable.
    PublisherAppeared,
    /// No local event, but the re-check interval came round, so the caller looks
    /// again in case a publisher appeared on another relay.
    RecheckDue,
    /// The window closed before routing produced a usable publisher. The caller
    /// distinguishes a publisher timeout from a routing outage.
    TimedOut,
    /// The subscriber withdrew. Nothing to answer.
    DownstreamLeft,
}

/// Result of running routing work during a rendezvous hold.
enum RendezvousWork<T> {
    Completed(T),
    TimedOut,
    DownstreamLeft,
}

/// Producer of tracks to a remote Subscriber
#[derive(Clone)]
pub struct Producer {
    publisher: Publisher,
    locals: Locals,
    remotes: RemoteManager,
    upstream_namespaces: UpstreamNamespaces,
    rendezvous_hold_permits: Arc<Semaphore>,
    /// Relay-level context for this MoQT session.
    context: SessionContext,
    /// Token authorization state. Internal relay peers and sessions in scopes
    /// without a policy carry `None`, permitting every request.
    auth: Option<SessionAuth>,
    /// Shared only by SUBSCRIBE_TRACKS requests on this transport session. Its
    /// bound must match the child-task channel capacity in [`Self::run`].
    subscribe_tracks_session_child_permits: Arc<Semaphore>,
    /// Shared only by SUBSCRIBE_TRACKS requests on this transport session.
    subscribe_tracks_session_slot_permits: Arc<Semaphore>,
}

/// Why the wait for upstream readiness ended without the subscription being
/// established.
///
/// An upstream rejection and a departing subscriber can both produce `Cancel`
/// or `Done`, so the direction has to come from the branch that resolved.
/// Otherwise publisher failures appear as subscriber cancellations in the
/// subscribe metrics.
enum UpstreamWait {
    /// The upstream subscription could not be established.
    UpstreamFailed(ServeError),

    /// The downstream subscriber went away before it was established.
    DownstreamLeft(ServeError),

    /// The rendezvous window closed before the upstream subscription was
    /// established.
    TimedOut,
}

impl Producer {
    /// Create a producer for a session.
    ///
    /// `auth` is the session's authorization state, and is deliberately a
    /// required argument rather than something attached afterwards: a producer
    /// with no authorization serves every request, so forgetting to supply it
    /// must be a compile error rather than a silent grant. `None` states that
    /// the session needs no token authorization because it is an internal
    /// relay peer or its scope has no policy.
    ///
    /// **Breaking change:** the old `Producer::new` public convenience
    /// constructor has been removed because it could only produce an
    /// unauthenticated producer, matching the draft-16 relay.
    pub(crate) fn new_with_upstream_namespaces(
        publisher: Publisher,
        locals: Locals,
        remotes: RemoteManager,
        upstream_namespaces: UpstreamNamespaces,
        context: SessionContext,
        auth: Option<SessionAuth>,
    ) -> Self {
        // The relay serves FETCH, so opt in before the session runs.
        publisher.accept_fetch();
        Self {
            publisher,
            locals,
            remotes,
            upstream_namespaces,
            rendezvous_hold_permits: Arc::new(Semaphore::new(
                MAX_CONCURRENT_RENDEZVOUS_HOLDS_PER_SESSION,
            )),
            context,
            auth,
            subscribe_tracks_session_child_permits: Arc::new(Semaphore::new(
                MAX_SUBSCRIBE_TRACKS_CHILDREN_PER_SESSION,
            )),
            subscribe_tracks_session_slot_permits: Arc::new(Semaphore::new(
                MAX_SUBSCRIBE_TRACKS_SLOTS_PER_SESSION,
            )),
        }
    }

    fn try_acquire_rendezvous_hold(&self) -> Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let session = self
            .rendezvous_hold_permits
            .clone()
            .try_acquire_owned()
            .ok()?;
        let relay = self.locals.try_acquire_rendezvous_hold()?;
        Some((session, relay))
    }

    fn try_reserve_unresolved_capacity(
        &self,
        permit: &mut Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)>,
        gauge: &mut Option<GaugeGuard>,
    ) -> bool {
        if permit.is_some() {
            return true;
        }

        let Some(acquired) = self.try_acquire_rendezvous_hold() else {
            return false;
        };
        *permit = Some(acquired);
        *gauge = Some(GaugeGuard::new("moq_relay_active_rendezvous_holds"));
        true
    }

    fn reject_unresolved_overload(
        &self,
        subscribed: Subscribed,
        timing_guard: &mut TimingGuard,
    ) -> Result<(), anyhow::Error> {
        metrics::counter!("moq_relay_rendezvous_rejections_total").increment(1);
        timing_guard.set_label("source", "rendezvous_overloaded");
        let err = ServeError::Closed(RequestErrorCode::ExcessiveLoad as u64);
        let retry_interval = self.locals.rendezvous_overload_retry_interval();
        let _ = subscribed.close_with_retry(err.clone(), retry_interval);
        Err(err.into())
    }

    /// Send PUBLISH_NAMESPACE for a set of tracks to the remote peer.
    pub async fn publish_namespace(&mut self, tracks: TracksReader) -> Result<(), SessionError> {
        self.publisher.publish_namespace(tracks).await
    }

    /// Run the producer to serve subscribe requests.
    pub async fn run(self) -> Result<(), SessionError> {
        let mut tasks: FuturesUnordered<ProducerTask> = FuturesUnordered::new();
        // A SUBSCRIBE_TRACKS child acquires a permit from the semaphore with
        // this same bound before sending, so the parent cannot stall here.
        let (child_tasks, mut child_task_rx) =
            tokio::sync::mpsc::channel(MAX_SUBSCRIBE_TRACKS_CHILDREN_PER_SESSION);

        loop {
            let mut publisher_subscribed = self.publisher.clone();
            let mut publisher_track_status = self.publisher.clone();
            let mut publisher_subscribed_namespace = self.publisher.clone();
            let mut publisher_subscribed_tracks = self.publisher.clone();
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
                Some(subscribed_tracks) = publisher_subscribed_tracks.subscribed_tracks() => {
                    let this = self.clone();
                    let child_tasks = child_tasks.clone();
                    tasks.push(async move {
                        let prefix = subscribed_tracks.namespace_prefix.to_utf8_path();
                        tracing::info!(namespace_prefix = %prefix, "serving subscribe tracks");
                        if let Err(err) = this.serve_subscribe_tracks(subscribed_tracks, child_tasks).await {
                            if Self::is_expected_serve_shutdown(&err) {
                                tracing::debug!(namespace_prefix = %prefix, error = %err, "stopped serving subscribe tracks");
                            } else {
                                tracing::warn!(namespace_prefix = %prefix, error = %err, "failed serving subscribe tracks");
                            }
                        }
                    }.boxed())
                },
                Some(fetch) = publisher_fetch.fetch_requested() => {
                    metrics::counter!("moq_relay_fetches_total").increment(1);
                    let this = self.clone();
                    tasks.push(async move {
                        let id = fetch.id();
                        if let Err(err) = this.serve_fetch(fetch).await {
                            tracing::debug!(request_id = id, error = %err, "failed serving FETCH");
                        }
                    }.boxed())
                },
                Some(task) = child_task_rx.recv() => tasks.push(task),
                _= tasks.next(), if !tasks.is_empty() => {},
                else => return Ok(()),
            };
        }
    }

    /// Serve a FETCH by forwarding it to the track's origin.
    ///
    /// The relay keeps no history beyond its live subscription caches, so a
    /// FETCH is passed through: to the session that advertised the namespace on
    /// this relay, otherwise to the relay the coordinator names. The upstream
    /// response, or its REQUEST_ERROR, is relayed back.
    ///
    /// A Joining FETCH is first resolved against the Joining Location this
    /// relay sent in its own SUBSCRIBE_OK, then forwarded as the equivalent
    /// standalone FETCH, so the response stays contiguous with what this
    /// session's subscription delivers.
    async fn serve_fetch(self, fetch: FetchRequested) -> Result<(), anyhow::Error> {
        let deadline = tokio::time::Instant::now() + FETCH_TIMEOUT;
        let standalone = tokio::select! {
            biased;
            _ = fetch.closed() => return Ok(()),
            _ = tokio::time::sleep_until(deadline) => {
                fetch.reject(RequestErrorCode::Timeout, "FETCH resolution timed out")?;
                return Ok(());
            }
            result = fetch.resolve() => match result.context("failed to resolve FETCH")? {
                Some(standalone) => standalone,
                // Already answered with REQUEST_ERROR.
                None => return Ok(()),
            },
        };

        // Authorize before any lookup: deciding afterwards would make response
        // timing an existence oracle for tracks the peer may not access.
        if let Err(reason) = may_fetch_track(
            self.auth.as_ref(),
            &self.context,
            &standalone.track_namespace,
            &standalone.track_name,
            fetch.id(),
        )
        .await
        {
            metrics::counter!("moq_relay_fetch_errors_total", "phase" => "auth").increment(1);
            let code = match reason {
                DenyReason::TokenMalformed => RequestErrorCode::MalformedAuthToken,
                DenyReason::TokenExpired => RequestErrorCode::ExpiredAuthToken,
                _ => RequestErrorCode::Unauthorized,
            };
            tracing::debug!(
                request_id = fetch.id(),
                namespace = %standalone.track_namespace.to_utf8_path(),
                track = %standalone.track_name,
                reason = ?reason,
                "FETCH denied"
            );
            // A denial is an answered request, like DOES_NOT_EXIST below.
            fetch.reject(code, "unauthorized")?;
            return Ok(());
        }

        // Only GROUP_ORDER is forwarded: the response's delta encoding depends
        // on it (draft-18 section 11.4.4.1); other parameters are hop-by-hop.
        let mut params = KeyValuePairs::default();
        if let Some(order) = fetch.group_order() {
            params.set_group_order(order);
        }

        let open = async {
            if let Some(mut source) = self
                .locals
                .fetch_source(self.context.scope(), &standalone.track_namespace)
            {
                Ok(Some(source.fetch(standalone, params).await?))
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
                    metrics::counter!("moq_relay_fetch_errors_total", "phase" => "not_found").increment(1);
                    fetch.reject(RequestErrorCode::DoesNotExist, "track not found")?;
                    return Ok(());
                }
                Err(err) => {
                    metrics::counter!("moq_relay_fetch_errors_total", "phase" => "upstream").increment(1);
                    fetch.reject(RequestErrorCode::InternalError, "failed to open upstream FETCH")?;
                    return Err(err.context("failed to open upstream FETCH"));
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

        // Authorize before any lookup or rendezvous hold: deciding after would
        // let response timing reveal whether a track the peer may not access
        // exists, and a held SUBSCRIBE pins relay capacity.
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

        let requested_timeout = match subscribed.params.rendezvous_timeout() {
            Ok(timeout) => timeout,
            Err(err) => {
                let serve_err = ServeError::internal_ctx(format!(
                    "validated rendezvous timeout became invalid: {err}"
                ));
                let _ = subscribed.close(serve_err.clone());
                return Err(serve_err.into());
            }
        };
        let hold = rendezvous_hold(requested_timeout, MAX_RENDEZVOUS_TIMEOUT);
        // Subscribing before the first lookup closes the race where a publisher
        // arrives between the lookup missing and the wait starting.
        let mut rendezvous = hold.map(|hold| {
            (
                tokio::time::Instant::now() + hold,
                self.locals.subscribe_track_changes(),
                self.locals.subscribe_namespace_changes(),
            )
        });
        let deadline = rendezvous.as_ref().map(|(deadline, _, _)| *deadline);
        let mut coordinator_lookup_succeeded = false;
        let mut rendezvous_permit = None;
        let mut rendezvous_guard = None;
        let full_name = FullTrackName {
            namespace: namespace.clone(),
            name: track_name.clone(),
        };

        // Re-entered after a publisher appears. The lookup is repeated rather
        // than trusting the wake, because `Consumer::serve_track` emits
        // TrackChange::Added before the coordinator registration behind it has
        // succeeded, so an event does not guarantee a servable track.
        loop {
            let mut locals = self.locals.clone();
            let mut local = locals.retrieve_track_with_interest(self.context.scope(), &full_name);

            // A namespace route creates an upstream request and cache generation,
            // so admission must precede that work. Exact established tracks above
            // stay on the unmetered fast path.
            if local.is_none() && locals.has_namespace_route(self.context.scope(), &namespace) {
                if !self
                    .try_reserve_unresolved_capacity(&mut rendezvous_permit, &mut rendezvous_guard)
                {
                    return self.reject_unresolved_overload(subscribed, &mut timing_guard);
                }

                local = if let Some(deadline) = deadline {
                    match Self::await_rendezvous_work(
                        &subscribed,
                        deadline,
                        locals.get_or_request_track(
                            self.context.scope(),
                            namespace.clone(),
                            &track_name,
                        ),
                    )
                    .await
                    {
                        RendezvousWork::Completed(result) => result,
                        RendezvousWork::TimedOut => {
                            return Self::finish_unresolved_rendezvous(
                                subscribed,
                                &mut timing_guard,
                                coordinator_lookup_succeeded,
                                &namespace,
                                &track_name,
                            );
                        }
                        RendezvousWork::DownstreamLeft => {
                            tracing::debug!(
                                namespace = %namespace.to_utf8_path(),
                                track = %track_name,
                                "subscriber left during a rendezvous local lookup"
                            );
                            timing_guard.set_label("source", "downstream_left");
                            return Ok(());
                        }
                    }
                } else {
                    locals
                        .get_or_request_track(self.context.scope(), namespace.clone(), &track_name)
                        .await
                };

                if local.is_none() {
                    drop(rendezvous_guard.take());
                    drop(rendezvous_permit.take());
                }
            }

            if let Some(local) = local {
                let largest_location = local.largest_location;
                let ns = namespace.to_utf8_path();
                tracing::info!(namespace = %ns, track = %track_name, source = "local", "serving subscribe from local: {:?}", local.reader.info);
                timing_guard.set_label("source", "local");
                let _track_guard = GaugeGuard::new("moq_relay_active_tracks");
                // Held until serving finishes. Once the last guard for a cached track
                // drops, its upstream subscription becomes eligible for release.
                let _interest_guard = local.interest;

                // Draft-18 §9.4: "The relay MUST have an Established upstream
                // subscription before sending SUBSCRIBE_OK in response to a
                // downstream SUBSCRIBE." A pull-through cache entry exists before
                // its upstream subscription does, so wait for it.
                if let Some(upstream) = local.upstream {
                    if upstream.is_pending()
                        && !self.try_reserve_unresolved_capacity(
                            &mut rendezvous_permit,
                            &mut rendezvous_guard,
                        )
                    {
                        // Recheck after failed admission because the upstream may
                        // have completed while permits were inspected.
                        if upstream.is_pending() {
                            return self.reject_unresolved_overload(subscribed, &mut timing_guard);
                        }
                    }

                    if let Err(outcome) =
                        Self::await_upstream(&subscribed, &upstream, deadline).await
                    {
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
                            UpstreamWait::TimedOut => {
                                return Self::finish_rendezvous_timeout(
                                    subscribed,
                                    &mut timing_guard,
                                );
                            }
                        };

                        // Rejects when the subscription is already closed (the
                        // downstream-left case), which is fine: the error below is
                        // still the reason we stopped.
                        let _ = subscribed.close(err.clone());
                        return Err(err.into());
                    }
                }

                drop(rendezvous_guard.take());
                drop(rendezvous_permit.take());
                return Self::serve_resolved_track(
                    subscribed,
                    local.reader,
                    largest_location,
                    deadline,
                    &mut timing_guard,
                )
                .await;
            }

            // Check remote tracks after local exact tracks and namespace route sources.
            if !self.try_reserve_unresolved_capacity(&mut rendezvous_permit, &mut rendezvous_guard)
            {
                return self.reject_unresolved_overload(subscribed, &mut timing_guard);
            }

            let remote_lookup = self.remotes.subscribe_with_lookup_status(
                self.context.scope(),
                &namespace,
                &track_name,
                &mut coordinator_lookup_succeeded,
            );
            let remote = if let Some(deadline) = deadline {
                match Self::await_rendezvous_work(&subscribed, deadline, remote_lookup).await {
                    RendezvousWork::Completed(result) => result,
                    RendezvousWork::TimedOut => {
                        return Self::finish_unresolved_rendezvous(
                            subscribed,
                            &mut timing_guard,
                            coordinator_lookup_succeeded,
                            &namespace,
                            &track_name,
                        );
                    }
                    RendezvousWork::DownstreamLeft => {
                        tracing::debug!(
                            namespace = %namespace.to_utf8_path(),
                            track = %track_name,
                            "subscriber left during a rendezvous route lookup"
                        );
                        timing_guard.set_label("source", "downstream_left");
                        return Ok(());
                    }
                }
            } else {
                tokio::select! {
                    result = remote_lookup => result,
                    _ = subscribed.closed() => {
                        tracing::debug!(
                            namespace = %namespace.to_utf8_path(),
                            track = %track_name,
                            "subscriber left during a route lookup"
                        );
                        timing_guard.set_label("source", "downstream_left");
                        return Ok(());
                    }
                }
            };

            match remote {
                Ok(Some((track, interest_guard))) => {
                    let largest_location = track.largest_location();
                    let ns = namespace.to_utf8_path();
                    tracing::info!(namespace = %ns, track = %track_name, source = "remote", "serving subscribe from remote: {:?}", track.info);
                    // Update label to indicate remote source, timing recorded on drop
                    timing_guard.set_label("source", "remote");
                    // Track active tracks - decrements when serve completes
                    let _track_guard = GaugeGuard::new("moq_relay_active_tracks");
                    // Held until serving finishes; the cross-relay subscription is
                    // released once the last guard for this track drops.
                    let _interest_guard = interest_guard;
                    drop(rendezvous_guard.take());
                    drop(rendezvous_permit.take());
                    return Self::serve_resolved_track(
                        subscribed,
                        track,
                        largest_location,
                        deadline,
                        &mut timing_guard,
                    )
                    .await;
                }
                Ok(None) => coordinator_lookup_succeeded = true,
                Err(RemoteSubscribeError::Peer(err)) => {
                    let ns = namespace.to_utf8_path();
                    tracing::debug!(namespace = %ns, track = %track_name, error = %err, "upstream relay rejected subscribe");
                    metrics::counter!("moq_relay_subscribe_upstream_errors_total").increment(1);
                    timing_guard.set_label("source", "upstream_error");
                    let _ = subscribed.close(err.clone());
                    return Err(err.into());
                }
                Err(RemoteSubscribeError::Route(e)) => {
                    // Route error = infrastructure failure (couldn't reach coordinator/upstream)
                    // This is different from "not found" - we don't know if the track exists
                    let ns = namespace.to_utf8_path();
                    tracing::error!(namespace = %ns, track = %track_name, error = %e, "failed to route to remote: {}", e);
                    metrics::counter!("moq_relay_subscribe_route_errors_total").increment(1);

                    // A hold turns this from an answer into a retry. The subscriber
                    // asked to be kept waiting, a lookup that failed says nothing
                    // about whether the track exists, and the next re-check may well
                    // succeed, so failing the subscribe here would make one slow
                    // coordinator call the end of every rendezvous crossing it.
                    if deadline.is_none() {
                        timing_guard.set_label("source", "route_error");

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
            }

            // Nothing is publishing this track. Without a rendezvous window that is
            // the end of it; with one, hold the subscription open and re-check when
            // a publisher shows up.
            let Some((deadline, track_changes, namespace_changes)) = rendezvous.as_mut() else {
                // timing_guard label already set to "not_found", will record on drop
                metrics::counter!("moq_relay_subscribe_not_found_total").increment(1);

                let err = ServeError::not_found_ctx(format!(
                    "track '{}/{}' not found in local or remote tracks",
                    namespace, track_name
                ));
                subscribed.close(err.clone())?;
                return Err(err.into());
            };

            // Capacity limits held requests, not ordinary resolution work. An
            // available publisher must be served even when every hold slot is in
            // use, so only acquire a slot after the initial lookup misses.
            if rendezvous_permit.is_none()
                && !self
                    .try_reserve_unresolved_capacity(&mut rendezvous_permit, &mut rendezvous_guard)
            {
                return self.reject_unresolved_overload(subscribed, &mut timing_guard);
            }

            match Self::await_rendezvous(
                &subscribed,
                self.context.scope(),
                &namespace,
                &track_name,
                *deadline,
                track_changes,
                namespace_changes,
            )
            .await
            {
                // A publisher may exist now, either because one arrived here or
                // because enough time passed to be worth asking elsewhere. Either
                // way the answer comes from repeating the lookup.
                Rendezvous::PublisherAppeared | Rendezvous::RecheckDue => continue,
                Rendezvous::TimedOut => {
                    return Self::finish_unresolved_rendezvous(
                        subscribed,
                        &mut timing_guard,
                        coordinator_lookup_succeeded,
                        &namespace,
                        &track_name,
                    );
                }
                Rendezvous::DownstreamLeft => {
                    timing_guard.set_label("source", "downstream_left");
                    return Ok(());
                }
            }
        }
    }

    /// Wait for the upstream subscription behind a cached track to be established.
    ///
    /// Also completes when the rendezvous deadline passes or the downstream
    /// subscriber goes away. Upstream and downstream failures are reported
    /// separately because the error alone does not identify their direction.
    async fn await_upstream(
        subscribed: &Subscribed,
        upstream: &UpstreamReady,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), UpstreamWait> {
        let Some(deadline) = deadline else {
            return tokio::select! {
                res = upstream.established() => res.map_err(UpstreamWait::UpstreamFailed),
                res = subscribed.closed() => Err(UpstreamWait::DownstreamLeft(
                    res.err().unwrap_or(ServeError::Done),
                )),
            };
        };

        tokio::select! {
            biased;
            res = subscribed.closed() => Err(UpstreamWait::DownstreamLeft(
                res.err().unwrap_or(ServeError::Done),
            )),
            () = tokio::time::sleep_until(deadline) => Err(UpstreamWait::TimedOut),
            res = upstream.established() => {
                if tokio::time::Instant::now() >= deadline {
                    Err(UpstreamWait::TimedOut)
                } else {
                    res.map_err(UpstreamWait::UpstreamFailed)
                }
            },
        }
    }

    /// Run routing work under the rendezvous deadline and downstream cancellation
    /// signal.
    async fn await_rendezvous_work<T>(
        subscribed: &Subscribed,
        deadline: tokio::time::Instant,
        lookup: impl Future<Output = T>,
    ) -> RendezvousWork<T> {
        tokio::select! {
            biased;
            _ = subscribed.closed() => RendezvousWork::DownstreamLeft,
            () = tokio::time::sleep_until(deadline) => RendezvousWork::TimedOut,
            result = lookup => {
                if tokio::time::Instant::now() >= deadline {
                    RendezvousWork::TimedOut
                } else {
                    RendezvousWork::Completed(result)
                }
            },
        }
    }

    fn finish_rendezvous_timeout(
        subscribed: Subscribed,
        timing_guard: &mut TimingGuard,
    ) -> Result<(), anyhow::Error> {
        Self::record_rendezvous_timeout(timing_guard);

        // TIMEOUT records that the relay completed routing work but no usable
        // publisher became ready within the requested window.
        let err = ServeError::Timeout;
        subscribed.close(err.clone())?;
        Err(err.into())
    }

    fn record_rendezvous_timeout(timing_guard: &mut TimingGuard) {
        metrics::counter!("moq_relay_rendezvous_timeouts_total").increment(1);
        timing_guard.set_label("source", "rendezvous_timeout");
    }

    async fn serve_resolved_track(
        subscribed: Subscribed,
        track: TrackReader,
        largest_location: Option<moq_transport::coding::Location>,
        deadline: Option<tokio::time::Instant>,
        timing_guard: &mut TimingGuard,
    ) -> Result<(), anyhow::Error> {
        let Some(deadline) = deadline else {
            return Ok(subscribed
                .serve_with_largest_location(track, largest_location)
                .await?);
        };

        match subscribed
            .serve_with_deadline_and_largest_location(track, deadline, largest_location)
            .await
        {
            Ok(()) => Ok(()),
            Err(ServeWithDeadlineError::DeadlineExpired) => {
                Self::record_rendezvous_timeout(timing_guard);
                Err(ServeError::Timeout.into())
            }
            Err(ServeWithDeadlineError::Session(err)) => Err(err.into()),
        }
    }

    fn finish_unresolved_rendezvous(
        subscribed: Subscribed,
        timing_guard: &mut TimingGuard,
        coordinator_lookup_succeeded: bool,
        namespace: &TrackNamespace,
        track_name: &TrackName,
    ) -> Result<(), anyhow::Error> {
        if coordinator_lookup_succeeded {
            return Self::finish_rendezvous_timeout(subscribed, timing_guard);
        }

        // No successful lookup means the relay never established that the track
        // was absent. A publisher timeout would hide the routing outage.
        metrics::counter!("moq_relay_subscribe_route_errors_total").increment(1);
        timing_guard.set_label("source", "route_error");
        let err = ServeError::internal_ctx(format!(
            "no coordinator lookup completed during rendezvous hold for '{}/{}'",
            namespace, track_name
        ));
        subscribed.close(err.clone())?;
        Err(err.into())
    }

    /// Hold a SUBSCRIBE open until a publisher for the track shows up, the
    /// deadline passes, or the subscriber gives up (draft-18 §10.2.6).
    ///
    /// Both a new track and a newly announced namespace count as a publisher
    /// arriving: a track can be served either from an exact PUBLISH or by a
    /// PUBLISH_NAMESPACE route source that triggers an upstream SUBSCRIBE, and
    /// the caller re-runs the full lookup either way.
    ///
    /// Those events are in-process, so they miss a publisher on another relay
    /// serving the same scope. The recheck interval covers that by ending the wait
    /// periodically for the caller to ask the coordinator again.
    ///
    /// Waking the subscriber promptly matters beyond tidiness. A held
    /// subscription keeps its entry in the publisher's per-session name map, and
    /// a second SUBSCRIBE for the same track on that session is rejected with
    /// DUPLICATE_SUBSCRIPTION, so a subscriber that gave up and retried would be
    /// turned away by its own abandoned hold.
    async fn await_rendezvous(
        subscribed: &Subscribed,
        scope: Option<&str>,
        namespace: &TrackNamespace,
        track_name: &TrackName,
        deadline: tokio::time::Instant,
        track_changes: &mut broadcast::Receiver<TrackChange>,
        namespace_changes: &mut broadcast::Receiver<NamespaceChange>,
    ) -> Rendezvous {
        let sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(sleep);
        let recheck = tokio::time::sleep(RENDEZVOUS_RECHECK_INTERVAL);
        tokio::pin!(recheck);

        loop {
            tokio::select! {
                biased;

                res = subscribed.closed() => {
                    tracing::debug!(
                        namespace = %namespace.to_utf8_path(),
                        track = %track_name,
                        reason = ?res.err(),
                        "subscriber left while waiting for a publisher"
                    );
                    return Rendezvous::DownstreamLeft;
                }

                () = &mut sleep => return Rendezvous::TimedOut,

                () = &mut recheck => {
                    tracing::debug!(
                        namespace = %namespace.to_utf8_path(),
                        track = %track_name,
                        "asking the coordinator again for a publisher during a rendezvous hold"
                    );
                    return Rendezvous::RecheckDue;
                }

                change = track_changes.recv() => {
                    match change {
                        Ok(TrackChange::Added { scope: change_scope, track, .. })
                            if change_scope.as_deref() == scope
                                && track.info.namespace == *namespace
                                && track.info.name == *track_name =>
                        {
                            return Rendezvous::PublisherAppeared
                        }
                        // Some other track, or a removal: keep waiting.
                        Ok(_) => continue,
                        // Lagged means events were missed, one of which could have
                        // been ours, so re-check rather than keep waiting blind.
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            return Rendezvous::PublisherAppeared
                        }
                        Err(broadcast::error::RecvError::Closed) => return Rendezvous::TimedOut,
                    }
                }

                change = namespace_changes.recv() => {
                    match change {
                        Ok(NamespaceChange { scope: change_scope, namespace: ns, added: true })
                            if change_scope.as_deref() == scope
                                && namespace_covers(&ns, namespace) =>
                        {
                            return Rendezvous::PublisherAppeared
                        }
                        Ok(_) => continue,
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            return Rendezvous::PublisherAppeared
                        }
                        Err(broadcast::error::RecvError::Closed) => return Rendezvous::TimedOut,
                    }
                }
            }
        }
    }

    /// Serve a SUBSCRIBE_NAMESPACE request using relay-local namespace state.
    ///
    /// Draft-18 §10.18: SUBSCRIBE_NAMESPACE requests namespace announcements
    /// (NAMESPACE / NAMESPACE_DONE) only. Asking for PUBLISH messages is the
    /// separate SUBSCRIBE_TRACKS mechanism (§10.19).
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

        let namespace_changes = self.locals.subscribe_namespace_changes();

        let _upstream_lease = match self
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
        };

        subscribed_namespace.ok()?;

        let mut known_namespaces = HashSet::new();
        self.send_namespace_snapshot(&mut subscribed_namespace, &mut known_namespaces)
            .await?;

        self.serve_subscribe_namespace_loop(
            subscribed_namespace,
            namespace_changes,
            known_namespaces,
        )
        .await
    }

    async fn serve_subscribe_namespace_loop(
        self,
        subscribed_namespace: SubscribedNamespace,
        mut namespace_changes: tokio::sync::broadcast::Receiver<NamespaceChange>,
        mut known_namespaces: HashSet<TrackNamespace>,
    ) -> Result<(), anyhow::Error> {
        let mut subscribed_namespace = subscribed_namespace;
        loop {
            tokio::select! {
                res = subscribed_namespace.closed() => {
                    res?;
                    return Ok(());
                }
                change = namespace_changes.recv() => {
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

    async fn serve_subscribe_tracks(
        self,
        mut request: SubscribedTracks,
        child_tasks: tokio::sync::mpsc::Sender<ProducerTask>,
    ) -> Result<(), anyhow::Error> {
        // Authorize the prefix before snapshotting or subscribing to changes.
        //
        // Interim: the CAT `moqt` action registry (draft-ietf-moq-c4m) has no
        // action for SUBSCRIBE_TRACKS (draft-18 §10.19) yet, so the request is
        // gated as SubscribeNamespace(3) on its prefix. That is a discovery
        // grant only: each generated PUBLISH is separately authorized as a
        // SUBSCRIBE for its exact track (see `may_serve_track_in_fanout`).
        if let Err(reason) = authorize(
            self.auth.as_ref(),
            &self.context,
            AuthzOperation::SubscribeNamespace {
                prefix: &request.namespace_prefix,
            },
            Some(request.request_id),
        )
        .await
        {
            metrics::counter!("moq_relay_subscribe_tracks_errors_total", "phase" => "auth")
                .increment(1);
            request.reject(reason.request_error_code(), "unauthorized")?;
            return Err(anyhow::anyhow!("unauthorized subscribe_tracks"));
        }

        let mut changes = self.locals.subscribe_published_track_changes();
        let snapshot = match self.locals.list_tracks_matching_for_session(
            self.context.scope(),
            &request.namespace_prefix,
            self.context.identity(),
            MAX_SUBSCRIBE_TRACKS_SLOTS_PER_SESSION,
        ) {
            Ok(TrackSnapshot::Tracks(snapshot)) => snapshot,
            Ok(TrackSnapshot::TooLarge) => {
                request.reject(
                    RequestErrorCode::NamespaceTooLarge as u64,
                    "too many matching tracks",
                )?;
                return Ok(());
            }
            Err(error) => {
                let prefix = request.namespace_prefix.to_utf8_path();
                request.reject(RequestErrorCode::InternalError as u64, "internal error")?;
                return Err(anyhow::Error::new(error)
                    .context(format!("failed to snapshot tracks for prefix {prefix}")));
            }
        };

        // This map belongs to one parent SUBSCRIBE_TRACKS. Every cancellation
        // path returns from this function, dropping the map and its session
        // slot permits without affecting other sessions.
        let mut slots = HashMap::with_capacity(snapshot.len());
        for track in snapshot {
            let permit = match self
                .subscribe_tracks_session_slot_permits
                .clone()
                .try_acquire_owned()
            {
                Ok(permit) => permit,
                Err(_) => {
                    request.reject(
                        RequestErrorCode::ExcessiveLoad as u64,
                        "subscribe tracks session limit exceeded",
                    )?;
                    return Ok(());
                }
            };
            slots.insert(
                full_name_for_track(&track),
                SubscribeTracksSlot {
                    desired: Some(track),
                    active: None,
                    publish_blocked_tombstone: false,
                    _session_slot_permit: permit,
                },
            );
        }

        request.ok()?;
        let (completed, mut completions) =
            tokio::sync::mpsc::channel(MAX_SUBSCRIBE_TRACKS_CHILDREN_PER_SESSION);
        let initial: Vec<_> = slots.keys().cloned().collect();
        for full_name in initial {
            self.start_subscribe_tracks_publish(
                &mut request,
                &mut slots,
                &full_name,
                &child_tasks,
                &completed,
            )
            .await?;
        }

        loop {
            tokio::select! {
                biased;
                result = request.closed() => {
                    result?;
                    return Ok(());
                }
                Some(completion) = completions.recv() => {
                    let action = slots
                        .get_mut(&completion.full_name)
                        .map(|slot| slot.complete(&completion.generation));
                    match action {
                        Some(SubscribeTracksSlotAction::Remove) => {
                            slots.remove(&completion.full_name);
                        }
                        Some(SubscribeTracksSlotAction::Publish) => {
                            self.start_subscribe_tracks_publish(
                                &mut request,
                                &mut slots,
                                &completion.full_name,
                                &child_tasks,
                                &completed,
                            ).await?;
                        }
                        Some(SubscribeTracksSlotAction::None) | None => {}
                    }
                }
                change = changes.recv() => {
                    match change {
                        Ok(PublishedTrackChange::Added { scope, track, origin }) => {
                            if scope.as_deref() != self.context.scope()
                                || !request.namespace_prefix.is_prefix_of(&track.namespace)
                                || origin.as_ref().is_some_and(|origin| origin.same_as(self.context.identity()))
                            {
                                continue;
                            }
                            let full_name = full_name_for_track(&track);
                            if let Some(slot) = slots.get_mut(&full_name) {
                                if !slot.set_desired(track) {
                                    continue;
                                }
                            } else {
                                if slots.len() == MAX_SUBSCRIBE_TRACKS_SLOTS_PER_SESSION {
                                    request.terminate(RequestErrorCode::ExcessiveLoad as u32)?;
                                    return Ok(());
                                }
                                let permit = match self.subscribe_tracks_session_slot_permits.clone().try_acquire_owned() {
                                    Ok(permit) => permit,
                                    Err(_) => {
                                        request.terminate(RequestErrorCode::ExcessiveLoad as u32)?;
                                        return Ok(());
                                    }
                                };
                                slots.insert(full_name.clone(), SubscribeTracksSlot {
                                    desired: Some(track),
                                    active: None,
                                    publish_blocked_tombstone: false,
                                    _session_slot_permit: permit,
                                });
                            }
                            self.start_subscribe_tracks_publish(
                                &mut request,
                                &mut slots,
                                &full_name,
                                &child_tasks,
                                &completed,
                            ).await?;
                        }
                        Ok(PublishedTrackChange::Removed { scope, full_name, track }) => {
                            if scope.as_deref() != self.context.scope() {
                                continue;
                            }
                            let remove = slots
                                .get_mut(&full_name)
                                .is_some_and(|slot| slot.remove_desired(&track));
                            if remove {
                                slots.remove(&full_name);
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            metrics::counter!("moq_relay_change_channel_lagged_total", "channel" => "subscribe_tracks")
                                .increment(skipped);
                            self.resync_subscribe_tracks(
                                &mut request,
                                &mut slots,
                                &child_tasks,
                                &completed,
                            ).await?;
                        }
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
            }
        }
    }

    async fn resync_subscribe_tracks(
        &self,
        request: &mut SubscribedTracks,
        slots: &mut HashMap<FullTrackName, SubscribeTracksSlot>,
        child_tasks: &tokio::sync::mpsc::Sender<ProducerTask>,
        completed: &tokio::sync::mpsc::Sender<PublishCompletion>,
    ) -> Result<(), anyhow::Error> {
        let snapshot = match self.locals.list_tracks_matching_for_session(
            self.context.scope(),
            &request.namespace_prefix,
            self.context.identity(),
            MAX_SUBSCRIBE_TRACKS_SLOTS_PER_SESSION,
        ) {
            Ok(TrackSnapshot::Tracks(snapshot)) => snapshot,
            Ok(TrackSnapshot::TooLarge) => {
                request.terminate(RequestErrorCode::ExcessiveLoad as u32)?;
                return Ok(());
            }
            Err(error) => {
                request.terminate(RequestErrorCode::InternalError as u32)?;
                return Err(anyhow::Error::new(error).context(format!(
                    "failed to resync tracks for prefix {}",
                    request.namespace_prefix.to_utf8_path()
                )));
            }
        };

        let current: HashMap<_, _> = snapshot
            .into_iter()
            .map(|track| (full_name_for_track(&track), track))
            .collect();
        for (full_name, slot) in slots.iter_mut() {
            if slot.publish_blocked_tombstone {
                continue;
            }
            slot.desired = current.get(full_name).cloned();
        }
        for (full_name, track) in current {
            if slots.contains_key(&full_name) {
                continue;
            }
            let permit = match self
                .subscribe_tracks_session_slot_permits
                .clone()
                .try_acquire_owned()
            {
                Ok(permit) => permit,
                Err(_) => {
                    request.terminate(RequestErrorCode::ExcessiveLoad as u32)?;
                    return Ok(());
                }
            };
            slots.insert(
                full_name,
                SubscribeTracksSlot {
                    desired: Some(track),
                    active: None,
                    publish_blocked_tombstone: false,
                    _session_slot_permit: permit,
                },
            );
        }
        let pending: Vec<_> = slots
            .iter()
            .filter(|(_, slot)| {
                slot.desired.is_some() && slot.active.is_none() && !slot.publish_blocked_tombstone
            })
            .map(|(full_name, _)| full_name.clone())
            .collect();
        for full_name in pending {
            self.start_subscribe_tracks_publish(request, slots, &full_name, child_tasks, completed)
                .await?;
        }
        slots.retain(|_, slot| {
            slot.desired.is_some() || slot.active.is_some() || slot.publish_blocked_tombstone
        });
        Ok(())
    }

    async fn start_subscribe_tracks_publish(
        &self,
        request: &mut SubscribedTracks,
        slots: &mut HashMap<FullTrackName, SubscribeTracksSlot>,
        full_name: &FullTrackName,
        child_tasks: &tokio::sync::mpsc::Sender<ProducerTask>,
        completed: &tokio::sync::mpsc::Sender<PublishCompletion>,
    ) -> Result<(), anyhow::Error> {
        let Some(slot) = slots.get(full_name) else {
            return Ok(());
        };
        if slot.active.is_some() || slot.publish_blocked_tombstone {
            return Ok(());
        }
        let Some(track) = slot.desired.clone() else {
            return Ok(());
        };

        // SUBSCRIBE_TRACKS grants discovery of a prefix, not delivery of
        // everything under it. This path pushes PUBLISH and then streams the
        // track's objects, so it needs the same authorization a SUBSCRIBE for
        // that track would: a token scoped to a subset of track names must not
        // receive media outside that grant.
        //
        // A denial withholds the track rather than failing the request: the
        // peer asked for a prefix, not for this track, so the rest of the
        // prefix is still legitimately theirs. The slot is dropped so it holds
        // no session slot permit; a later change event for the same track asks
        // again, which is a pure claim check and also lets a transient hook
        // fault recover.
        if let Err(reason) = may_serve_track_in_fanout(
            self.auth.as_ref(),
            &self.context,
            full_name,
            request.request_id,
        )
        .await
        {
            tracing::debug!(
                namespace = %full_name.namespace,
                track = %full_name.name,
                reason = %reason,
                "withholding track from SUBSCRIBE_TRACKS"
            );
            metrics::counter!("moq_relay_publish_errors_total", "phase" => "auth_fanout")
                .increment(1);
            slots.remove(full_name);
            return Ok(());
        }

        let permit = match self
            .subscribe_tracks_session_child_permits
            .clone()
            .try_acquire_owned()
        {
            Ok(permit) => permit,
            Err(_) => {
                request.publish_blocked(&full_name.namespace, &full_name.name)?;
                if let Some(slot) = slots.get_mut(full_name) {
                    // This terminal marker is retained only until this parent
                    // SUBSCRIBE_TRACKS ends; draft-18 section 6.1 forbids a
                    // later generated PUBLISH for the same track.
                    slot.publish_blocked_tombstone = true;
                    slot.desired = None;
                }
                return Ok(());
            }
        };

        let mut params = KeyValuePairs::default();
        if !request.forward {
            params.set_forward(false);
        }
        let mut publisher = self.publisher.clone();
        let publish = publisher.publish(track.clone(), params);
        let published = match tokio::select! {
            biased;
            closed = request.closed() => {
                let error = match closed {
                    Ok(()) => ServeError::Done,
                    Err(error) => error,
                };
                return Err(anyhow::Error::new(error).context(format!(
                    "parent SUBSCRIBE_TRACKS ended while starting generated PUBLISH for {}--{}",
                    full_name.namespace.to_utf8_path(),
                    full_name.name
                )));
            }
            published = publish => published,
        } {
            Ok(published) => published,
            Err(SessionError::Serve(ServeError::Duplicate)) => {
                slots.remove(full_name);
                return Ok(());
            }
            Err(error) if !error.is_session_fatal() => {
                tracing::warn!(
                    namespace = %full_name.namespace.to_utf8_path(),
                    track = %full_name.name,
                    error = %error,
                    "failed to start generated PUBLISH; keeping SUBSCRIBE_TRACKS active"
                );
                slots.remove(full_name);
                return Ok(());
            }
            Err(error) => {
                return Err(anyhow::Error::new(error).context(format!(
                    "failed to start generated PUBLISH for {}--{}",
                    full_name.namespace.to_utf8_path(),
                    full_name.name
                )));
            }
        };
        if let Some(slot) = slots.get_mut(full_name) {
            slot.active = Some(track.clone());
        }

        let completion = PublishCompletion {
            full_name: full_name.clone(),
            generation: track,
        };
        let completed = completed.clone();
        let namespace = full_name.namespace.to_utf8_path();
        let track_name = full_name.name.to_string();
        child_tasks
            .send(
                async move {
                    let _permit = permit;
                    match published.serve().await {
                        Ok(()) => tracing::debug!(namespace = %namespace, track = %track_name, "finished serving PUBLISH for SUBSCRIBE_TRACKS"),
                        Err(error) => tracing::warn!(namespace = %namespace, track = %track_name, error = %error, "failed serving PUBLISH for SUBSCRIBE_TRACKS"),
                    }
                    let _ = completed.send(completion).await;
                }
                .boxed(),
            )
            .await
            .map_err(|_| ServeError::Cancel)?;
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

fn full_name_for_track(track: &TrackReader) -> FullTrackName {
    FullTrackName {
        namespace: track.namespace.clone(),
        name: track.name.clone(),
    }
}

fn same_track_generation(left: &TrackReader, right: &TrackReader) -> bool {
    Arc::ptr_eq(&left.info, &right.info)
}

/// Whether the peer may FETCH `track` in `namespace`.
///
/// Requires `Fetch` (`MoqtAction::Fetch` in the CAT `moqt` claim registry).
/// Any other outcome (wrong scope, no scope, hook fault, expired token) is a
/// definitive denial. FETCH retrieves track content, so it is gated at least
/// as tightly as SUBSCRIBE.
///
/// A free function so the decision is testable without a live session. The
/// tests pin which operation is checked as well as the outcome.
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
        AuthzOperation::Fetch { namespace, track },
        Some(request_id),
    )
    .await
}

/// Whether a track may be delivered through a SUBSCRIBE_TRACKS generated
/// PUBLISH.
///
/// That path pushes PUBLISH and then streams the track's objects, so it needs
/// the authorization a SUBSCRIBE for the same track would: a prefix
/// subscription grants discovery, not delivery of everything beneath it. A
/// token scoped to a subset of track names must not receive the rest merely
/// because it subscribed to the enclosing prefix.
///
/// A free function so the decision is testable without a live session. The
/// tests pin this decision and the operation it asks about, but not that the
/// caller still consults it; removing the call site produces a dead-code
/// warning rather than a failing test.
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
/// terminator, just `[exact("sports")]`, is a prefix grant and does allow
/// announcing `["sports", "football"]`.
///
/// Announcement authorization uses `AuthzOperation::SubscribeNamespace`, not
/// `Subscribe`, because disclosing that a namespace *exists* is the same kind
/// of discovery SUBSCRIBE_NAMESPACE makes; requiring track-level grants just to
/// see namespace listings would be too restrictive. The concrete namespace is
/// checked as the prefix naming exactly it.
async fn may_announce_namespace(
    auth: Option<&SessionAuth>,
    context: &SessionContext,
    namespace: &TrackNamespace,
) -> bool {
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
    use std::{
        collections::HashMap,
        io::Cursor,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;
    use moq_transport::{
        coding::{
            Decode, DecodeError, Encode, KeyValuePairs, TrackName, TrackNamespace,
            TrackNamespacePrefix,
        },
        message::{wire_id, Message, PublishBlocked, RequestErrorCode, SubscribeTracks},
        serve::{ServeError, Track},
        session::{Session as TransportSession, SessionError, Subscriber, Transport},
    };
    use tokio::sync::Semaphore;

    use super::{
        full_name_for_track, may_announce_namespace, may_fetch_track, may_serve_track_in_fanout,
        namespace_covers, rendezvous_hold, Producer, SubscribeTracksSlot,
        SubscribeTracksSlotAction, MAX_CONCURRENT_RENDEZVOUS_HOLDS_PER_SESSION,
        MAX_SUBSCRIBE_TRACKS_CHILDREN_PER_SESSION, MAX_SUBSCRIBE_TRACKS_SLOTS_PER_SESSION,
    };
    use crate::auth::{
        AuthDecision, AuthError, AuthHook, AuthRequest, AuthToken, AuthzOperation, DenyReason,
        Principal, SessionAuth,
    };
    use crate::{
        Coordinator, CoordinatorContext, CoordinatorError, CoordinatorResult, Locals,
        NamespaceOrigin, NamespaceRegistration, RemoteManager, SessionContext, TrackRequest,
    };
    use moq_transport::serve::FullTrackName;

    #[derive(Default)]
    struct CountingCoordinator {
        track_lookups: Mutex<Vec<String>>,
    }

    struct RemoteCoordinator {
        url: url::Url,
    }

    impl CountingCoordinator {
        fn looked_up(&self, track: &str) -> bool {
            self.track_lookups
                .lock()
                .expect("track lookup lock poisoned")
                .iter()
                .any(|lookup| lookup == track)
        }

        fn distinct_lookup_count(&self, prefix: &str) -> usize {
            self.track_lookups
                .lock()
                .expect("track lookup lock poisoned")
                .iter()
                .filter(|lookup| lookup.starts_with(prefix))
                .collect::<std::collections::HashSet<_>>()
                .len()
        }
    }

    #[async_trait]
    impl Coordinator for CountingCoordinator {
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
        ) -> CoordinatorResult<(NamespaceOrigin, Option<moq_native_ietf::quic::Client>)> {
            Err(CoordinatorError::NamespaceNotFound)
        }

        async fn lookup_track(
            &self,
            _scope: Option<&str>,
            _namespace: &TrackNamespace,
            track: &str,
        ) -> CoordinatorResult<(NamespaceOrigin, Option<moq_native_ietf::quic::Client>)> {
            self.track_lookups
                .lock()
                .map_err(|_| anyhow::anyhow!("track lookup lock poisoned"))?
                .push(track.to_string());
            Err(CoordinatorError::NamespaceNotFound)
        }
    }

    #[async_trait]
    impl Coordinator for RemoteCoordinator {
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
            namespace: &TrackNamespace,
        ) -> CoordinatorResult<(NamespaceOrigin, Option<moq_native_ietf::quic::Client>)> {
            Ok((
                NamespaceOrigin::new(namespace.clone(), self.url.clone(), None),
                None,
            ))
        }
    }

    async fn loopback_webtransport_pair() -> (web_transport::Session, web_transport::Session) {
        use web_transport::quinn::{ClientBuilder, ServerBuilder};

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate self-signed certificate");
        let cert_der = cert.cert.der().clone();
        let key = cert
            .key_pair
            .serialize_der()
            .try_into()
            .expect("serialize private key");
        let mut server = ServerBuilder::new()
            .with_addr("127.0.0.1:0".parse().expect("server addr"))
            .with_certificate(vec![cert_der], key)
            .expect("build loopback server");
        let addr = server.local_addr().expect("server local addr");

        let accepted = tokio::spawn(async move {
            server
                .accept()
                .await
                .expect("accept loopback request")
                .ok()
                .await
                .expect("accept loopback session")
        });
        let client = ClientBuilder::new()
            .dangerous()
            .with_no_certificate_verification()
            .expect("build loopback client");
        let url = url::Url::parse(&format!("https://127.0.0.1:{}/", addr.port()))
            .expect("parse loopback URL");
        let connected = tokio::time::timeout(Duration::from_secs(10), client.connect(url))
            .await
            .expect("loopback connect timed out")
            .expect("connect loopback session");

        (
            tokio::time::timeout(Duration::from_secs(10), accepted)
                .await
                .expect("loopback accept timed out")
                .expect("join loopback accept")
                .into(),
            connected.into(),
        )
    }

    async fn spawn_relay_session(
        locals: Locals,
        remotes: RemoteManager,
        coordinator: Arc<dyn Coordinator>,
    ) -> Subscriber {
        let (_, subscriber, _) = spawn_relay_session_with_limits(
            locals,
            remotes,
            coordinator,
            MAX_SUBSCRIBE_TRACKS_CHILDREN_PER_SESSION,
            MAX_SUBSCRIBE_TRACKS_SLOTS_PER_SESSION,
        )
        .await;
        subscriber
    }

    async fn spawn_relay_session_with_limits(
        locals: Locals,
        remotes: RemoteManager,
        coordinator: Arc<dyn Coordinator>,
        child_limit: usize,
        slot_limit: usize,
    ) -> (web_transport::Session, Subscriber, Arc<Semaphore>) {
        spawn_relay_session_with_auth(locals, remotes, coordinator, child_limit, slot_limit, None)
            .await
    }

    async fn spawn_relay_session_with_auth(
        locals: Locals,
        remotes: RemoteManager,
        coordinator: Arc<dyn Coordinator>,
        child_limit: usize,
        slot_limit: usize,
        auth: Option<SessionAuth>,
    ) -> (web_transport::Session, Subscriber, Arc<Semaphore>) {
        let (server_transport, client_transport) = loopback_webtransport_pair().await;
        let request_transport = client_transport.clone();
        let (server, client) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                TransportSession::accept(server_transport, None, Transport::WebTransport),
                TransportSession::connect(client_transport, None, Transport::WebTransport),
            )
        })
        .await
        .expect("MoQT setup timed out");
        let (server, publisher, _server_subscriber) = server.expect("accept MoQT session");
        let (client, _client_publisher, subscriber) = client.expect("connect MoQT session");
        let (upstream_namespaces, runner) = crate::upstream_namespaces::UpstreamNamespaces::new(
            locals.clone(),
            remotes.clone(),
            coordinator,
        );
        tokio::spawn(runner.run());
        let mut producer = Producer::new_with_upstream_namespaces(
            publisher.expect("server publisher"),
            locals,
            remotes,
            upstream_namespaces,
            SessionContext::public(None),
            auth,
        );
        producer.subscribe_tracks_session_child_permits = Arc::new(Semaphore::new(child_limit));
        producer.subscribe_tracks_session_slot_permits = Arc::new(Semaphore::new(slot_limit));
        let slot_permits = producer.subscribe_tracks_session_slot_permits.clone();

        tokio::spawn(async move { server.run().await });
        tokio::spawn(async move { client.run().await });
        tokio::spawn(async move { producer.run().await });

        (request_transport, subscriber, slot_permits)
    }

    async fn read_bidi_response_frame(
        recv: &mut web_transport::RecvStream,
        buffer: &mut Vec<u8>,
    ) -> (u64, Vec<u8>) {
        loop {
            let mut cursor = Cursor::new(buffer.as_slice());
            let message_type = match u64::decode(&mut cursor) {
                Ok(message_type) => Some(message_type),
                Err(DecodeError::More(_)) => None,
                Err(error) => panic!("invalid response message type: {error}"),
            };
            let message_length = if message_type.is_some() {
                match u16::decode(&mut cursor) {
                    Ok(message_length) => Some(message_length as usize),
                    Err(DecodeError::More(_)) => None,
                    Err(error) => panic!("invalid response message length: {error}"),
                }
            } else {
                None
            };
            if let (Some(message_type), Some(message_length)) = (message_type, message_length) {
                let header_length = cursor.position() as usize;
                let frame_length = header_length + message_length;
                if buffer.len() >= frame_length {
                    let payload = buffer[header_length..frame_length].to_vec();
                    buffer.drain(..frame_length);
                    return (message_type, payload);
                }
            }

            if recv
                .read_buf(buffer)
                .await
                .expect("read response frame")
                .is_none()
            {
                panic!("response stream ended before a complete frame");
            }
        }
    }

    async fn subscribe_result(
        subscriber: &mut Subscriber,
        track_name: &str,
        rendezvous_timeout: Option<u64>,
    ) -> Result<moq_transport::session::Subscribe, ServeError> {
        let namespace = TrackNamespace::from_utf8_path("admission");
        let (writer, _reader) = Track::new(namespace, track_name).produce();
        let mut params = KeyValuePairs::default();
        if let Some(timeout) = rendezvous_timeout {
            params.set_rendezvous_timeout(timeout);
        }
        subscriber.subscribe_open_with_params(writer, params).await
    }

    async fn rejected_subscribe(
        subscriber: &mut Subscriber,
        track_name: &str,
        rendezvous_timeout: Option<u64>,
    ) -> ServeError {
        match subscribe_result(subscriber, track_name, rendezvous_timeout).await {
            Ok(_) => panic!("expected {track_name} subscription to be rejected"),
            Err(err) => err,
        }
    }

    fn test_track(name: &str) -> moq_transport::serve::TrackReader {
        let (_writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("subscribe-tracks"), name).produce();
        reader
    }

    #[test]
    fn cancelling_parent_releases_blocked_session_slot() {
        let permits = Arc::new(Semaphore::new(1));
        let generation = test_track("blocked");
        let full_name = full_name_for_track(&generation);
        let permit = permits
            .clone()
            .try_acquire_owned()
            .expect("acquire slot permit");
        let mut slots = HashMap::from([(
            full_name.clone(),
            SubscribeTracksSlot {
                desired: None,
                active: None,
                publish_blocked_tombstone: true,
                _session_slot_permit: permit,
            },
        )]);

        let replacement = test_track("blocked");
        assert!(!slots
            .get_mut(&full_name)
            .expect("blocked slot")
            .set_desired(replacement));
        assert_eq!(permits.available_permits(), 0);

        // Cancelling the parent returns from serve_subscribe_tracks and drops
        // this request-owned slot map.
        drop(slots);
        assert_eq!(permits.available_permits(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn subscribe_tracks_resyncs_after_lag_and_releases_blocked_slots_on_cancellation() {
        let locals = Locals::new();
        let namespace = TrackNamespace::from_utf8_path("subscribe-tracks");
        let (_writer, reader) = Track::new(namespace.clone(), "blocked").produce();
        let mut registering_locals = locals.clone();
        let _registration = registering_locals
            .register_track(None, reader)
            .await
            .expect("register matching track");
        let coordinator = Arc::new(CountingCoordinator::default());
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let (request_transport, _subscriber, slot_permits) =
            spawn_relay_session_with_limits(locals, remotes, coordinator, 0, 2).await;
        let (mut request_send, mut response_recv) = request_transport
            .open_bi()
            .await
            .expect("open SUBSCRIBE_TRACKS stream");
        let request = Message::SubscribeTracks(SubscribeTracks {
            id: 0,
            track_namespace_prefix: TrackNamespacePrefix::from_utf8_path("subscribe-tracks"),
            params: KeyValuePairs::default(),
        });
        let mut frame = Vec::new();
        request.encode(&mut frame).expect("encode SUBSCRIBE_TRACKS");
        let mut frame = Cursor::new(frame);
        while frame.position() < frame.get_ref().len() as u64 {
            request_send
                .write_buf(&mut frame)
                .await
                .expect("write SUBSCRIBE_TRACKS");
        }

        let mut response_buffer = Vec::new();
        let (message_type, _) = tokio::time::timeout(
            Duration::from_secs(2),
            read_bidi_response_frame(&mut response_recv, &mut response_buffer),
        )
        .await
        .expect("REQUEST_OK timed out");
        assert_eq!(message_type, wire_id::RequestOk);

        let (message_type, payload) = tokio::time::timeout(
            Duration::from_secs(2),
            read_bidi_response_frame(&mut response_recv, &mut response_buffer),
        )
        .await
        .expect("PUBLISH_BLOCKED timed out");
        assert_eq!(message_type, wire_id::PublishBlocked);
        let blocked = PublishBlocked::decode(&mut Cursor::new(payload))
            .expect("decode PUBLISH_BLOCKED payload");
        assert_eq!(blocked.track_name, TrackName::from("blocked"));
        assert_eq!(slot_permits.available_permits(), 1);

        // Keep this task scheduled while overflowing the 1024-entry broadcast
        // channel, with the matching change outside the retained tail.
        const CHANGE_BURST: usize = 1025;
        let mut registrations = Vec::with_capacity(CHANGE_BURST * 2 + 1);
        for index in 0..CHANGE_BURST {
            let (_writer, reader) = Track::new(
                TrackNamespace::from_utf8_path(&format!("unrelated/before/{index}")),
                "noise",
            )
            .produce();
            registrations.push(
                registering_locals
                    .register_track(None, reader)
                    .await
                    .expect("register leading unrelated track"),
            );
        }
        let (_writer, reader) = Track::new(namespace, "resynced").produce();
        registrations.push(
            registering_locals
                .register_track(None, reader)
                .await
                .expect("register matching track for resync"),
        );
        for index in 0..CHANGE_BURST {
            let (_writer, reader) = Track::new(
                TrackNamespace::from_utf8_path(&format!("unrelated/after/{index}")),
                "noise",
            )
            .produce();
            registrations.push(
                registering_locals
                    .register_track(None, reader)
                    .await
                    .expect("register trailing unrelated track"),
            );
        }

        let (message_type, payload) = tokio::time::timeout(
            Duration::from_secs(2),
            read_bidi_response_frame(&mut response_recv, &mut response_buffer),
        )
        .await
        .expect("resynced PUBLISH_BLOCKED timed out");
        assert_eq!(message_type, wire_id::PublishBlocked);
        let blocked = PublishBlocked::decode(&mut Cursor::new(payload))
            .expect("decode resynced PUBLISH_BLOCKED payload");
        assert_eq!(blocked.track_name, TrackName::from("resynced"));
        assert_eq!(slot_permits.available_permits(), 0);

        request_send
            .finish()
            .expect("finish SUBSCRIBE_TRACKS request");
        tokio::time::timeout(Duration::from_secs(2), async {
            while slot_permits.available_permits() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("request cancellation did not release slot permit");
        assert_eq!(slot_permits.available_permits(), 2);
    }

    #[test]
    fn subscribe_tracks_generation_replacement_waits_for_completion() {
        let permits = Arc::new(Semaphore::new(1));
        let first = test_track("replacement");
        let replacement = test_track("replacement");
        let mut slot = SubscribeTracksSlot {
            desired: Some(first.clone()),
            active: Some(first.clone()),
            publish_blocked_tombstone: false,
            _session_slot_permit: permits.try_acquire_owned().expect("acquire slot permit"),
        };

        assert!(slot.set_desired(replacement.clone()));
        assert_eq!(slot.complete(&first), SubscribeTracksSlotAction::Publish);
        assert!(slot.active.is_none());
        assert!(slot
            .desired
            .as_ref()
            .is_some_and(|desired| super::same_track_generation(desired, &replacement)));

        slot.active = Some(replacement.clone());
        assert_eq!(
            slot.complete(&replacement),
            SubscribeTracksSlotAction::Remove
        );
    }

    #[test]
    fn stale_subscribe_tracks_removal_preserves_replacement() {
        let permits = Arc::new(Semaphore::new(1));
        let first = test_track("stale-removal");
        let replacement = test_track("stale-removal");
        let mut slot = SubscribeTracksSlot {
            desired: Some(replacement.clone()),
            active: Some(first.clone()),
            publish_blocked_tombstone: false,
            _session_slot_permit: permits.try_acquire_owned().expect("acquire slot permit"),
        };

        assert!(!slot.remove_desired(&first));
        assert!(slot
            .desired
            .as_ref()
            .is_some_and(|desired| super::same_track_generation(desired, &replacement)));
    }

    /// draft-18 §10.2.6: absent and 0 both mean answer immediately, so neither
    /// produces a hold.
    #[test]
    fn no_hold_without_a_rendezvous_request() {
        let max = Duration::from_secs(30);

        assert_eq!(rendezvous_hold(None, max), None);
        assert_eq!(rendezvous_hold(Some(0), max), None);
    }

    #[test]
    fn a_request_within_the_ceiling_is_honoured() {
        assert_eq!(
            rendezvous_hold(Some(5_000), Duration::from_secs(30)),
            Some(Duration::from_secs(5))
        );
    }

    /// The spec lets a relay shorten the window without telling the subscriber,
    /// which is what stops one client pinning state for as long as it likes.
    #[test]
    fn a_request_above_the_ceiling_is_clamped() {
        assert_eq!(
            rendezvous_hold(Some(600_000), Duration::from_secs(30)),
            Some(Duration::from_secs(30))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn relay_admission_serves_available_tracks_before_rejecting_new_holds() {
        let locals = Locals::with_rendezvous_capacity(1);
        let coordinator = Arc::new(CountingCoordinator::default());
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let mut first_subscriber =
            spawn_relay_session(locals.clone(), remotes.clone(), coordinator.clone()).await;
        let mut second_subscriber =
            spawn_relay_session(locals.clone(), remotes, coordinator.clone()).await;

        let first = tokio::spawn(async move {
            subscribe_result(&mut first_subscriber, "held", Some(10_000)).await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !coordinator.looked_up("held") || locals.available_rendezvous_holds() != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first request did not enter rendezvous routing");

        let namespace = TrackNamespace::from_utf8_path("admission");
        let (_available_writer, available_reader) =
            Track::new(namespace.clone(), "available").produce();
        let mut registering_locals = locals.clone();
        let _available_registration = registering_locals
            .register_track(None, available_reader)
            .await
            .expect("register available track");
        let available = subscribe_result(&mut second_subscriber, "available", Some(10_000))
            .await
            .expect("capacity must not reject an available track");
        drop(available);

        let absent = rejected_subscribe(&mut second_subscriber, "absent", None).await;
        assert_eq!(
            absent,
            ServeError::Closed(RequestErrorCode::ExcessiveLoad as u64)
        );
        assert!(!coordinator.looked_up("absent"));

        let zero = rejected_subscribe(&mut second_subscriber, "zero", Some(0)).await;
        assert_eq!(
            zero,
            ServeError::Closed(RequestErrorCode::ExcessiveLoad as u64)
        );
        assert!(!coordinator.looked_up("zero"));

        let overloaded =
            rejected_subscribe(&mut second_subscriber, "overloaded", Some(10_000)).await;
        assert_eq!(
            overloaded,
            ServeError::Closed(RequestErrorCode::ExcessiveLoad as u64)
        );
        assert!(!coordinator.looked_up("overloaded"));

        let (_held_writer, held_reader) = Track::new(namespace.clone(), "held").produce();
        let _held_registration = registering_locals
            .register_track(None, held_reader)
            .await
            .expect("register held track");
        let first_subscription = tokio::time::timeout(Duration::from_secs(2), first)
            .await
            .expect("held request did not resolve")
            .expect("join held request")
            .expect("held request was rejected");

        let after_release =
            rejected_subscribe(&mut second_subscriber, "after-release", Some(25)).await;
        assert_eq!(
            after_release,
            ServeError::Timeout,
            "resolved hold should release capacity for another rendezvous"
        );
        drop(first_subscription);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unresolved_pull_through_obeys_capacity_and_releases_on_timeout() {
        let locals = Locals::with_rendezvous_capacity(1);
        let namespace = TrackNamespace::from_utf8_path("admission");
        let (registration, mut requests) = locals
            .clone()
            .register_namespace(None, namespace)
            .await
            .expect("register namespace source");
        let coordinator = Arc::new(CountingCoordinator::default());
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let mut first_subscriber =
            spawn_relay_session(locals.clone(), remotes.clone(), coordinator.clone()).await;
        let mut second_subscriber = spawn_relay_session(locals.clone(), remotes, coordinator).await;

        let requests_task = tokio::spawn(async move {
            while let Some(TrackRequest {
                writer,
                lease,
                upstream,
            }) = requests.recv().await
            {
                tokio::spawn(async move {
                    lease.abandoned().await;
                    drop(writer);
                    drop(upstream);
                });
            }
        });

        let first = tokio::spawn(async move {
            rejected_subscribe(&mut first_subscriber, "first-pending", Some(100)).await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while locals.available_rendezvous_holds() != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first pull-through did not acquire rendezvous capacity");

        let overloaded =
            rejected_subscribe(&mut second_subscriber, "overloaded-pending", Some(1_000)).await;
        assert_eq!(
            overloaded,
            ServeError::Closed(RequestErrorCode::ExcessiveLoad as u64)
        );

        assert_eq!(
            first.await.expect("join first pull-through"),
            ServeError::Timeout
        );
        let after_timeout =
            rejected_subscribe(&mut second_subscriber, "after-timeout", Some(25)).await;
        assert_eq!(after_timeout, ServeError::Timeout);

        drop(registration);
        requests_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remote_peer_rejection_is_forwarded_with_and_without_rendezvous() {
        let (upstream_server_transport, upstream_client_transport) =
            loopback_webtransport_pair().await;
        let (upstream_server, upstream_client) = tokio::join!(
            TransportSession::accept(upstream_server_transport, None, Transport::WebTransport),
            TransportSession::connect(upstream_client_transport, None, Transport::WebTransport),
        );
        let (upstream_server, mut upstream_publisher, _) =
            upstream_server.expect("accept upstream MoQT session");
        let (upstream_client, remote_publisher, remote_subscriber) =
            upstream_client.expect("connect upstream MoQT session");
        let remote_url = url::Url::parse("https://relay.example.com/live").unwrap();
        let coordinator = Arc::new(RemoteCoordinator {
            url: remote_url.clone(),
        });
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        remotes
            .insert_test_remote(remote_url, remote_publisher, remote_subscriber)
            .await;

        let upstream_server_task = tokio::spawn(upstream_server.run());
        let upstream_client_task = tokio::spawn(upstream_client.run());
        let reject = tokio::spawn(async move {
            let publisher = upstream_publisher
                .as_mut()
                .expect("upstream server publisher");
            for _ in 0..2 {
                let subscribed = publisher
                    .subscribed()
                    .await
                    .expect("receive upstream subscribe");
                subscribed
                    .close(ServeError::Closed(RequestErrorCode::Unauthorized as u64))
                    .expect("reject upstream subscribe");
            }
        });

        let locals = Locals::new();
        let mut subscriber =
            spawn_relay_session(locals, remotes, coordinator as Arc<dyn Coordinator>).await;

        for rendezvous_timeout in [None, Some(1_000)] {
            assert_eq!(
                rejected_subscribe(&mut subscriber, "unauthorized", rendezvous_timeout).await,
                ServeError::Closed(RequestErrorCode::Unauthorized as u64)
            );
        }

        reject.await.expect("join upstream rejection task");
        upstream_server_task.abort();
        upstream_client_task.abort();
    }

    async fn assert_cancelled_remote_subscribe_orders_retry(rendezvous: bool) {
        let (upstream_server_transport, upstream_client_transport) =
            loopback_webtransport_pair().await;
        let (upstream_server, upstream_client) = tokio::join!(
            TransportSession::accept(upstream_server_transport, None, Transport::WebTransport),
            TransportSession::connect(upstream_client_transport, None, Transport::WebTransport),
        );
        let (upstream_server, mut upstream_publisher, _) =
            upstream_server.expect("accept upstream MoQT session");
        let (upstream_client, remote_publisher, remote_subscriber) =
            upstream_client.expect("connect upstream MoQT session");
        let remote_url = url::Url::parse("https://relay.example.com/live").unwrap();
        let coordinator = Arc::new(RemoteCoordinator {
            url: remote_url.clone(),
        });
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        remotes
            .insert_test_remote(remote_url, remote_publisher, remote_subscriber)
            .await;

        let upstream_server_task = tokio::spawn(upstream_server.run());
        let upstream_client_task = tokio::spawn(upstream_client.run());
        let locals = Locals::new();
        let subscriber =
            spawn_relay_session(locals, remotes, coordinator as Arc<dyn Coordinator>).await;
        let namespace = TrackNamespace::from_utf8_path("admission");

        let (first_writer, _first_reader) = Track::new(namespace.clone(), "retry").produce();
        let mut first_subscriber = subscriber.clone();
        let first = tokio::spawn(async move {
            if rendezvous {
                let mut params = KeyValuePairs::default();
                params.set_rendezvous_timeout(30_000);
                first_subscriber
                    .subscribe_open_with_params(first_writer, params)
                    .await
            } else {
                first_subscriber.subscribe_open(first_writer).await
            }
        });
        let first_upstream = tokio::time::timeout(
            Duration::from_secs(2),
            upstream_publisher
                .as_mut()
                .expect("upstream server publisher")
                .subscribed(),
        )
        .await
        .expect("first upstream subscribe timed out")
        .expect("first upstream subscribe");

        first.abort();
        match first.await {
            Err(err) => assert!(err.is_cancelled()),
            Ok(_) => panic!("first subscribe was not cancelled"),
        }

        let mut retry_subscriber = subscriber.clone();
        let retry_namespace = namespace.clone();
        let retry = tokio::spawn(async move {
            let (writer, _reader) = Track::new(retry_namespace, "retry").produce();
            if rendezvous {
                let mut params = KeyValuePairs::default();
                params.set_rendezvous_timeout(30_000);
                retry_subscriber
                    .subscribe_open_with_params(writer, params)
                    .await
            } else {
                retry_subscriber.subscribe_open(writer).await
            }
        });

        let publisher = upstream_publisher
            .as_mut()
            .expect("upstream server publisher");
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                biased;
                _ = first_upstream.closed() => {}
                _ = publisher.subscribed() => {
                    panic!("same-track retry overtook the first upstream teardown")
                }
            }
        })
        .await
        .expect("first upstream teardown did not complete");
        drop(first_upstream);

        let second_upstream = tokio::time::timeout(Duration::from_secs(2), publisher.subscribed())
            .await
            .expect("second upstream subscribe timed out")
            .expect("second upstream subscribe");
        second_upstream
            .close(ServeError::Closed(RequestErrorCode::Uninterested as u64))
            .expect("reject second upstream subscribe");
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), retry)
                .await
                .expect("downstream retry timed out")
                .expect("join downstream retry"),
            Err(ServeError::Closed(code)) if code == RequestErrorCode::Uninterested as u64
        ));

        upstream_server_task.abort();
        upstream_client_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancelled_remote_subscribe_tears_down_before_same_track_retry() {
        assert_cancelled_remote_subscribe_orders_retry(true).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancelled_remote_subscribe_without_rendezvous_releases_the_slot() {
        assert_cancelled_remote_subscribe_orders_retry(false).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn one_session_cannot_monopolize_relay_capacity() {
        let locals = Locals::new();
        let coordinator = Arc::new(CountingCoordinator::default());
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let mut saturated_session =
            spawn_relay_session(locals.clone(), remotes.clone(), coordinator.clone()).await;
        let mut other_session = spawn_relay_session(locals, remotes, coordinator.clone()).await;

        let mut holds = Vec::new();
        for index in 0..MAX_CONCURRENT_RENDEZVOUS_HOLDS_PER_SESSION {
            let mut subscriber = saturated_session.clone();
            let track = format!("session-hold-{index}");
            holds.push(tokio::spawn(async move {
                subscribe_result(&mut subscriber, &track, Some(10_000)).await
            }));
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while coordinator.distinct_lookup_count("session-hold-")
                < MAX_CONCURRENT_RENDEZVOUS_HOLDS_PER_SESSION
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("session did not fill its rendezvous capacity");

        let overloaded = rejected_subscribe(
            &mut saturated_session,
            "same-session-overloaded",
            Some(10_000),
        )
        .await;
        assert_eq!(
            overloaded,
            ServeError::Closed(RequestErrorCode::ExcessiveLoad as u64)
        );
        assert!(!coordinator.looked_up("same-session-overloaded"));

        let other_hold = tokio::spawn(async move {
            subscribe_result(&mut other_session, "other-session-hold", Some(10_000)).await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !coordinator.looked_up("other-session-hold") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("saturated session blocked another session");

        other_hold.abort();
        for hold in holds {
            hold.abort();
        }
    }

    /// Locals routes a track to the longest registered namespace that prefixes
    /// it, so a shorter announcement still makes the track servable and has to
    /// wake the waiter.
    #[test]
    fn a_prefix_announcement_covers_the_track_namespace() {
        let target = TrackNamespace::from_utf8_path("meeting/room/42");

        assert!(namespace_covers(
            &TrackNamespace::from_utf8_path("meeting"),
            &target
        ));
        assert!(namespace_covers(
            &TrackNamespace::from_utf8_path("meeting/room/42"),
            &target
        ));
    }

    #[test]
    fn an_unrelated_or_longer_announcement_does_not_cover_it() {
        let target = TrackNamespace::from_utf8_path("meeting/room/42");

        assert!(!namespace_covers(
            &TrackNamespace::from_utf8_path("other/room"),
            &target
        ));
        // Longer than the target cannot be a prefix of it.
        assert!(!namespace_covers(
            &TrackNamespace::from_utf8_path("meeting/room/42/sub"),
            &target
        ));
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

    /// SUBSCRIBE_NAMESPACE (draft-18 §10.18) is namespace-discovery only.
    ///
    /// Exercises `serve_subscribe_namespace` through the relay's session loop.
    /// A subscriber that issues SUBSCRIBE_NAMESPACE must receive NAMESPACE
    /// events for matching registered namespaces and must NEVER receive a
    /// PUBLISH message for tracks within those namespaces — even when new
    /// tracks are registered under the prefix while the subscription is live.
    ///
    /// PUBLISH fan-out belongs to SUBSCRIBE_TRACKS (§10.19); this test guards
    /// the boundary enforced by removing the `wants_publish` path.
    #[tokio::test(flavor = "multi_thread")]
    async fn subscribe_namespace_delivers_namespace_events_and_never_publish() {
        use moq_transport::session::NamespaceEvent;

        let mut locals = Locals::new();
        let namespace = TrackNamespace::from_utf8_path("live/room/42");
        let prefix = TrackNamespacePrefix::from_utf8_path("live/room");

        // Pre-register the namespace so the relay snapshot has something to announce.
        let (_ns_registration, _ns_requests) = locals
            .register_namespace(None, namespace.clone())
            .await
            .expect("namespace should register");

        let coordinator = Arc::new(CountingCoordinator::default());
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let mut subscriber =
            spawn_relay_session(locals.clone(), remotes, coordinator.clone()).await;

        // Open a SUBSCRIBE_NAMESPACE request for the prefix.
        let handle = subscriber
            .subscribe_namespace(prefix, KeyValuePairs::default())
            .await
            .expect("SUBSCRIBE_NAMESPACE should succeed");

        // The relay snapshot must announce the pre-registered namespace.
        let event = tokio::time::timeout(Duration::from_secs(2), handle.next())
            .await
            .expect("NamespaceEvent::Added must arrive within 2 s")
            .expect("SubscribeNamespace channel must be open")
            .expect("NamespaceEvent must not be an error");
        assert!(
            matches!(event, NamespaceEvent::Added(_)),
            "expected NamespaceEvent::Added, got {event:?}"
        );

        // Register a track under the subscribed prefix. Per §10.18 the relay
        // must NOT send a PUBLISH message to the SUBSCRIBE_NAMESPACE subscriber.
        let (_, track_reader) = Track::new(namespace.clone(), "audio").produce();
        let _track_registration = locals
            .register_track(None, track_reader)
            .await
            .expect("track should register");

        // No PUBLISH message must arrive within 100 ms.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), subscriber.publish_received())
                .await
                .is_err(),
            "SUBSCRIBE_NAMESPACE must not deliver a PUBLISH message (§10.18); \
             use SUBSCRIBE_TRACKS (§10.19) for track fan-out"
        );
    }

    // -----------------------------------------------------------------------
    // Authorization gates
    // -----------------------------------------------------------------------

    /// Records every operation it is asked about, and answers from a script.
    ///
    /// Lets the gates be tested for the operation they construct, not merely
    /// for their yes/no answer: authorizing the wrong thing would be as much a
    /// bypass as authorizing nothing.
    type Decide = Box<dyn Fn(&str) -> Result<AuthDecision, AuthError> + Send + Sync>;

    struct RecordingHook {
        seen: Mutex<Vec<String>>,
        decide: Decide,
    }

    impl RecordingHook {
        fn new(
            decide: impl Fn(&str) -> Result<AuthDecision, AuthError> + Send + Sync + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                seen: Mutex::new(Vec::new()),
                decide: Box::new(decide),
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
            allow()
        }

        async fn on_request(&self, request: &AuthRequest<'_>) -> Result<AuthDecision, AuthError> {
            let detail = match &request.operation {
                AuthzOperation::Subscribe { namespace, track } => format!(
                    "subscribe {} {}",
                    namespace.to_utf8_path(),
                    track.to_string_lossy()
                ),
                AuthzOperation::TrackStatus { namespace, track } => format!(
                    "track_status {} {}",
                    namespace.to_utf8_path(),
                    track.to_string_lossy()
                ),
                AuthzOperation::SubscribeNamespace { prefix } => {
                    format!("subscribe_namespace {}", prefix.to_utf8_path())
                }
                AuthzOperation::Fetch { namespace, track } => format!(
                    "fetch {} {}",
                    namespace.to_utf8_path(),
                    track.to_string_lossy()
                ),
                other => format!("other {}", other.label()),
            };
            let decision = (self.decide)(&detail);
            self.seen.lock().unwrap().push(detail);
            decision
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

    fn session_auth(hook: Arc<RecordingHook>) -> SessionAuth {
        SessionAuth::new(hook, Principal::anonymous())
    }

    fn auth_context() -> SessionContext {
        SessionContext::public(Some("tenant".to_string()))
    }

    fn fanout_name() -> FullTrackName {
        FullTrackName {
            namespace: TrackNamespace::from_utf8_path("sports/football"),
            name: "premium-4k".into(),
        }
    }

    /// The fan-out streams media, so it must ask for SUBSCRIBE on the exact
    /// track, not for the enclosing prefix, and not nothing at all.
    #[tokio::test]
    async fn fanout_authorizes_each_track_as_a_subscribe() {
        let hook = RecordingHook::new(|_| allow());
        let auth = session_auth(hook.clone());

        may_serve_track_in_fanout(Some(&auth), &auth_context(), &fanout_name(), 7)
            .await
            .expect("allowed");

        assert_eq!(hook.seen(), vec!["subscribe /sports/football premium-4k"]);
    }

    #[tokio::test]
    async fn fanout_withholds_a_track_the_token_does_not_cover() {
        let hook = RecordingHook::new(|_| deny());
        let auth = session_auth(hook.clone());

        let result =
            may_serve_track_in_fanout(Some(&auth), &auth_context(), &fanout_name(), 7).await;

        assert!(result.is_err(), "a denied track must not be served");
        assert_eq!(hook.seen().len(), 1, "the decision must actually be sought");
    }

    /// A hook fault is a denial too: the relay must not serve a track it
    /// failed to authorize.
    #[tokio::test]
    async fn fanout_fails_closed_on_a_hook_fault() {
        let faulted = may_serve_track_in_fanout(
            Some(&session_auth(RecordingHook::new(|_| fault()))),
            &auth_context(),
            &fanout_name(),
            7,
        )
        .await
        .expect_err("faulted");
        assert!(!faulted.is_policy_denial());
    }

    /// A session whose scope has no policy is unaffected.
    #[tokio::test]
    async fn gates_permit_everything_when_no_policy_applies() {
        assert!(
            may_serve_track_in_fanout(None, &auth_context(), &fanout_name(), 7)
                .await
                .is_ok()
        );
        assert!(
            may_announce_namespace(
                None,
                &auth_context(),
                &TrackNamespace::from_utf8_path("sports/football")
            )
            .await
        );
    }

    /// Announcements disclose existence, so each concrete namespace is
    /// authorized as the prefix naming exactly it.
    #[tokio::test]
    async fn announcements_are_authorized_per_namespace() {
        let hook = RecordingHook::new(|_| allow());
        let auth = session_auth(hook.clone());
        let namespace = TrackNamespace::from_utf8_path("sports/football");

        assert!(may_announce_namespace(Some(&auth), &auth_context(), &namespace).await);
        assert_eq!(hook.seen(), vec!["subscribe_namespace /sports/football"]);
    }

    #[tokio::test]
    async fn announcements_are_withheld_when_denied() {
        let hook = RecordingHook::new(|_| deny());
        let auth = session_auth(hook.clone());
        let namespace = TrackNamespace::from_utf8_path("sports/football");

        assert!(
            !may_announce_namespace(Some(&auth), &auth_context(), &namespace).await,
            "a namespace the token cannot touch must not be announced"
        );
        assert_eq!(hook.seen().len(), 1);
    }

    async fn spawn_authorized_session(
        locals: Locals,
        coordinator: Arc<CountingCoordinator>,
        hook: Arc<RecordingHook>,
    ) -> (web_transport::Session, Subscriber) {
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let (transport, subscriber, _) = spawn_relay_session_with_auth(
            locals,
            remotes,
            coordinator,
            MAX_SUBSCRIBE_TRACKS_CHILDREN_PER_SESSION,
            MAX_SUBSCRIBE_TRACKS_SLOTS_PER_SESSION,
            Some(session_auth(hook)),
        )
        .await;
        (transport, subscriber)
    }

    // ── FETCH ────────────────────────────────────────────────────────────

    fn fetch_request(namespace: &str, track: &str) -> moq_transport::message::StandaloneFetch {
        moq_transport::message::StandaloneFetch {
            track_namespace: TrackNamespace::from_utf8_path(namespace),
            track_name: track.into(),
            start_location: moq_transport::coding::Location::new(0, 0),
            end_location: moq_transport::coding::Location::new(2, 0),
        }
    }

    fn fetch_object(group_id: u64, object_id: u64, len: u64) -> moq_transport::data::FetchRecord {
        moq_transport::data::FetchRecord::Object(moq_transport::data::FetchRecordObject {
            group_id,
            subgroup_id: Some(0),
            object_id,
            publisher_priority: 1,
            extension_headers: Default::default(),
            payload_length: len,
        })
    }

    /// An origin session whose namespace is registered on `locals` as a FETCH
    /// source, as `Consumer` does on PUBLISH_NAMESPACE. Returns the origin's
    /// publisher and the registration keeping the route alive.
    async fn spawn_fetch_origin(
        locals: &Locals,
        namespace: &str,
    ) -> (
        moq_transport::session::Publisher,
        crate::local::LocalNamespaceRegistration,
        tokio::sync::mpsc::Receiver<TrackRequest>,
    ) {
        let (origin_transport, relay_transport) = loopback_webtransport_pair().await;
        let (origin, relay) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                TransportSession::accept(origin_transport, None, Transport::WebTransport),
                TransportSession::connect(relay_transport, None, Transport::WebTransport),
            )
        })
        .await
        .expect("MoQT setup timed out");
        let (origin, origin_publisher, _) = origin.expect("accept origin session");
        if let Some(publisher) = &origin_publisher {
            publisher.accept_fetch();
        }
        let (relay, _, relay_subscriber) = relay.expect("connect relay session");
        tokio::spawn(async move { origin.run().await });
        tokio::spawn(async move { relay.run().await });
        let (registration, requests) = locals
            .clone()
            .register_namespace_with_fetch(
                None,
                TrackNamespace::from_utf8_path(namespace),
                relay_subscriber,
            )
            .await
            .expect("register FETCH source");
        (
            origin_publisher.expect("origin publisher"),
            registration,
            requests,
        )
    }

    /// End to end: a FETCH to the relay is forwarded to the session that
    /// advertised the namespace, and its response is relayed back.
    #[tokio::test(flavor = "multi_thread")]
    async fn fetch_is_forwarded_to_the_namespace_source() {
        let locals = Locals::new();
        let coordinator = Arc::new(CountingCoordinator::default());
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let (mut origin, _registration, _requests) =
            spawn_fetch_origin(&locals, "fetch/origin").await;
        let mut subscriber = spawn_relay_session(locals, remotes, coordinator.clone()).await;

        let mut fetch = subscriber
            .fetch(
                fetch_request("fetch/origin", "video"),
                KeyValuePairs::default(),
            )
            .await
            .expect("send FETCH");

        let requested = tokio::time::timeout(Duration::from_secs(5), origin.fetch_requested())
            .await
            .expect("FETCH was not forwarded")
            .expect("origin publisher closed");
        let forwarded = requested.resolve().await.unwrap().unwrap();
        assert_eq!(forwarded, fetch_request("fetch/origin", "video"));
        let mut writer = requested
            .respond(moq_transport::session::FetchOkInfo {
                end_of_track: false,
                end_location: moq_transport::coding::Location::new(1, 2),
                params: KeyValuePairs::default(),
                track_extensions: Default::default(),
            })
            .await
            .unwrap();
        writer.write_record(&fetch_object(0, 0, 3)).await.unwrap();
        writer.write_payload(b"abc").await.unwrap();
        writer.write_record(&fetch_object(1, 1, 0)).await.unwrap();
        writer.finish().await.unwrap();

        let ok = tokio::time::timeout(Duration::from_secs(5), fetch.ok())
            .await
            .expect("FETCH_OK timed out")
            .expect("FETCH rejected");
        assert_eq!(ok.end_location, moq_transport::coding::Location::new(1, 2));

        let mut records = Vec::new();
        while let Some(record) = tokio::time::timeout(Duration::from_secs(5), fetch.next())
            .await
            .unwrap()
            .unwrap()
        {
            let mut payload = Vec::new();
            while let Some(chunk) = fetch.read_payload_chunk(16).await.unwrap() {
                payload.extend_from_slice(&chunk);
            }
            records.push((record, payload));
        }
        assert_eq!(
            records,
            vec![
                (fetch_object(0, 0, 3), b"abc".to_vec()),
                (fetch_object(1, 1, 0), Vec::new()),
            ]
        );
        assert!(
            coordinator.track_lookups.lock().unwrap().is_empty(),
            "a local source must be used without consulting the coordinator"
        );
    }

    /// An upstream REQUEST_ERROR reaches the fetcher unchanged.
    #[tokio::test(flavor = "multi_thread")]
    async fn fetch_rejection_from_the_source_is_forwarded() {
        let locals = Locals::new();
        let coordinator = Arc::new(CountingCoordinator::default());
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let (mut origin, _registration, _requests) =
            spawn_fetch_origin(&locals, "fetch/origin").await;
        let mut subscriber = spawn_relay_session(locals, remotes, coordinator).await;

        let fetch = subscriber
            .fetch(
                fetch_request("fetch/origin", "video"),
                KeyValuePairs::default(),
            )
            .await
            .unwrap();
        let requested = tokio::time::timeout(Duration::from_secs(5), origin.fetch_requested())
            .await
            .unwrap()
            .unwrap();
        requested
            .reject(RequestErrorCode::InvalidRange, "no objects in range")
            .unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), fetch.ok())
                .await
                .unwrap(),
            Err(ServeError::Closed(RequestErrorCode::InvalidRange as u64))
        );
        assert_eq!(fetch.rejection().unwrap().reason.0, "no objects in range");
    }

    /// A FETCH with no local source and no coordinator route is DOES_NOT_EXIST.
    #[tokio::test(flavor = "multi_thread")]
    async fn fetch_for_an_unknown_track_does_not_exist() {
        let coordinator = Arc::new(CountingCoordinator::default());
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let mut subscriber = spawn_relay_session(Locals::new(), remotes, coordinator.clone()).await;

        let fetch = subscriber
            .fetch(
                fetch_request("fetch/nowhere", "video"),
                KeyValuePairs::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), fetch.ok())
                .await
                .unwrap(),
            Err(ServeError::Closed(RequestErrorCode::DoesNotExist as u64))
        );
        assert_eq!(*coordinator.track_lookups.lock().unwrap(), vec!["video"]);
    }

    /// End to end: a Joining FETCH is resolved against the Joining Location
    /// in the relay's own SUBSCRIBE_OK and forwarded to the origin as the
    /// equivalent standalone range.
    #[tokio::test(flavor = "multi_thread")]
    async fn joining_fetch_is_resolved_locally_and_forwarded() {
        let locals = Locals::new();
        let coordinator = Arc::new(CountingCoordinator::default());
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let (mut origin, _registration, _requests) =
            spawn_fetch_origin(&locals, "fetch/origin").await;

        // A locally published track whose largest Object is {6, 2}.
        let namespace = TrackNamespace::from_utf8_path("fetch/origin");
        let (track_writer, track_reader) = Track::new(namespace.clone(), "video").produce();
        let mut subgroups = track_writer.subgroups().unwrap();
        let mut subgroup = subgroups
            .create(moq_transport::serve::Subgroup {
                group_id: 6,
                subgroup_id: 0,
                priority: 1,
            })
            .unwrap();
        for _ in 0..3 {
            subgroup.write(bytes::Bytes::from_static(b"o")).unwrap();
        }
        let _track_registration = locals
            .clone()
            .register_track(None, track_reader)
            .await
            .unwrap();

        let mut subscriber = spawn_relay_session(locals, remotes, coordinator).await;
        let (sub_writer, _sub_reader) = Track::new(namespace, "video").produce();
        let subscribe = subscriber.subscribe_open(sub_writer).await.unwrap();

        let mut fetch = subscriber
            .joining_fetch(
                moq_transport::message::FetchType::RelativeJoining,
                subscribe.info.id,
                2,
                KeyValuePairs::default(),
            )
            .await
            .unwrap();
        let requested = tokio::time::timeout(Duration::from_secs(5), origin.fetch_requested())
            .await
            .expect("Joining FETCH was not forwarded")
            .unwrap();
        assert_eq!(
            requested.request.fetch_type,
            moq_transport::message::FetchType::Standalone,
            "the origin has no subscription to join, so the relay sends the resolved range"
        );
        let forwarded = requested.resolve().await.unwrap().unwrap();
        assert_eq!(forwarded.track_name, TrackName::from("video"));
        assert_eq!(
            forwarded.start_location,
            moq_transport::coding::Location::new(4, 0)
        );
        assert_eq!(
            forwarded.end_location,
            moq_transport::coding::Location::new(6, 3)
        );

        let mut writer = requested
            .respond(moq_transport::session::FetchOkInfo {
                end_of_track: false,
                end_location: moq_transport::coding::Location::new(6, 3),
                params: KeyValuePairs::default(),
                track_extensions: Default::default(),
            })
            .await
            .unwrap();
        writer.write_record(&fetch_object(4, 0, 0)).await.unwrap();
        writer.write_record(&fetch_object(6, 2, 0)).await.unwrap();
        writer.finish().await.unwrap();

        let ok = tokio::time::timeout(Duration::from_secs(5), fetch.ok())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ok.end_location, moq_transport::coding::Location::new(6, 3));
        let mut locations = Vec::new();
        while let Some(record) = tokio::time::timeout(Duration::from_secs(5), fetch.next())
            .await
            .unwrap()
            .unwrap()
        {
            locations.push(record.location());
        }
        assert_eq!(
            locations,
            vec![
                moq_transport::coding::Location::new(4, 0),
                moq_transport::coding::Location::new(6, 2)
            ]
        );
        drop(subscribe);
    }

    /// A Joining FETCH naming no subscription on this session is refused
    /// without routing.
    #[tokio::test(flavor = "multi_thread")]
    async fn joining_fetch_for_an_unknown_subscription_is_refused() {
        let coordinator = Arc::new(CountingCoordinator::default());
        let remotes = RemoteManager::new(coordinator.clone(), Vec::new());
        let (transport, _subscriber, _) = spawn_relay_session_with_limits(
            Locals::new(),
            remotes,
            coordinator.clone(),
            MAX_SUBSCRIBE_TRACKS_CHILDREN_PER_SESSION,
            MAX_SUBSCRIBE_TRACKS_SLOTS_PER_SESSION,
        )
        .await;
        let (mut send, mut recv) = transport.open_bi().await.expect("open request stream");
        let mut buffer = Vec::new();
        Message::Fetch(moq_transport::message::Fetch {
            id: 0,
            fetch_type: moq_transport::message::FetchType::RelativeJoining,
            standalone_fetch: None,
            // Odd: an ID only the relay could allocate, so it can never be a
            // SUBSCRIBE from this peer and is refused without waiting.
            joining_fetch: Some(moq_transport::message::JoiningFetch {
                joining_request_id: 1,
                joining_start: 1,
            }),
            params: KeyValuePairs::default(),
        })
        .encode(&mut buffer)
        .expect("encode FETCH");
        let mut buffer = Cursor::new(buffer);
        while buffer.position() < buffer.get_ref().len() as u64 {
            send.write_buf(&mut buffer).await.expect("send FETCH");
        }
        let mut response = Vec::new();
        let (message_type, _) = tokio::time::timeout(
            Duration::from_secs(5),
            read_bidi_response_frame(&mut recv, &mut response),
        )
        .await
        .expect("no response to joining FETCH");
        assert_eq!(message_type, wire_id::RequestError);
        assert!(coordinator.track_lookups.lock().unwrap().is_empty());
    }

    /// The FETCH gate asks about `Fetch` for the exact track, not `Subscribe`.
    #[tokio::test]
    async fn fetch_gate_asks_about_fetch() {
        let hook = RecordingHook::new(|_| allow());
        let auth = session_auth(hook.clone());
        let namespace = TrackNamespace::from_utf8_path("sports/football");
        let track = TrackName::from("video");

        assert!(
            may_fetch_track(Some(&auth), &auth_context(), &namespace, &track, 3)
                .await
                .is_ok()
        );
        assert_eq!(hook.seen(), vec!["fetch /sports/football video"]);
        assert!(
            may_fetch_track(None, &auth_context(), &namespace, &track, 3)
                .await
                .is_ok()
        );
    }

    /// End to end: a denied FETCH is UNAUTHORIZED, decided before any local
    /// or coordinator lookup, so the response does not reveal whether the
    /// track exists.
    #[tokio::test(flavor = "multi_thread")]
    async fn denied_fetch_is_unauthorized_before_any_lookup() {
        let hook = RecordingHook::new(|_| deny());
        let coordinator = Arc::new(CountingCoordinator::default());
        let locals = Locals::new();
        let (mut origin, _registration, _requests) =
            spawn_fetch_origin(&locals, "fetch/origin").await;
        let (_transport, mut subscriber) =
            spawn_authorized_session(locals, coordinator.clone(), hook.clone()).await;

        for namespace in ["fetch/origin", "fetch/nowhere"] {
            let fetch = subscriber
                .fetch(fetch_request(namespace, "secret"), KeyValuePairs::default())
                .await
                .unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), fetch.ok())
                    .await
                    .unwrap(),
                Err(ServeError::Closed(RequestErrorCode::Unauthorized as u64))
            );
        }
        assert_eq!(
            hook.seen(),
            vec!["fetch /fetch/origin secret", "fetch /fetch/nowhere secret"]
        );
        assert!(coordinator.track_lookups.lock().unwrap().is_empty());
        assert!(
            tokio::time::timeout(Duration::from_millis(200), origin.fetch_requested())
                .await
                .is_err(),
            "a denied FETCH must not reach the source"
        );
    }

    /// End to end: a denied SUBSCRIBE is answered with UNAUTHORIZED (draft-18
    /// §10.6) before any lookup, so the relay neither consults the
    /// coordinator nor holds the request for a rendezvous.
    #[tokio::test(flavor = "multi_thread")]
    async fn denied_subscribe_is_unauthorized_before_any_lookup() {
        let hook = RecordingHook::new(|_| deny());
        let coordinator = Arc::new(CountingCoordinator::default());
        let (_transport, mut subscriber) =
            spawn_authorized_session(Locals::new(), coordinator.clone(), hook.clone()).await;

        let err = tokio::time::timeout(
            Duration::from_secs(2),
            rejected_subscribe(&mut subscriber, "secret", Some(10_000)),
        )
        .await
        .expect("a denied SUBSCRIBE must not be held for rendezvous");

        assert_eq!(
            err,
            ServeError::Closed(RequestErrorCode::Unauthorized as u64)
        );
        assert_eq!(hook.seen(), vec!["subscribe /admission secret"]);
        assert!(
            !coordinator.looked_up("secret"),
            "authorization must precede the coordinator lookup"
        );
    }

    /// A hook fault refuses with UNAUTHORIZED as well; the relay must not
    /// serve what it could not authorize.
    #[tokio::test(flavor = "multi_thread")]
    async fn subscribe_fails_closed_on_a_hook_fault() {
        let hook = RecordingHook::new(|_| fault());
        let coordinator = Arc::new(CountingCoordinator::default());
        let (_transport, mut subscriber) =
            spawn_authorized_session(Locals::new(), coordinator.clone(), hook).await;

        let err = rejected_subscribe(&mut subscriber, "any", None).await;
        assert_eq!(
            err,
            ServeError::Closed(RequestErrorCode::Unauthorized as u64)
        );
        assert!(!coordinator.looked_up("any"));
    }

    /// An allowed SUBSCRIBE proceeds to the normal path.
    #[tokio::test(flavor = "multi_thread")]
    async fn allowed_subscribe_reaches_the_lookup() {
        let hook = RecordingHook::new(|_| allow());
        let coordinator = Arc::new(CountingCoordinator::default());
        let (_transport, mut subscriber) =
            spawn_authorized_session(Locals::new(), coordinator.clone(), hook.clone()).await;

        let err = rejected_subscribe(&mut subscriber, "absent", None).await;
        assert_ne!(
            err,
            ServeError::Closed(RequestErrorCode::Unauthorized as u64)
        );
        assert!(coordinator.looked_up("absent"));
        assert_eq!(hook.seen(), vec!["subscribe /admission absent"]);
    }

    /// A denied SUBSCRIBE_NAMESPACE is refused with UNAUTHORIZED and receives
    /// no NAMESPACE for a registered namespace under the prefix.
    #[tokio::test(flavor = "multi_thread")]
    async fn denied_subscribe_namespace_is_unauthorized() {
        let mut locals = Locals::new();
        let (_registration, _requests) = locals
            .register_namespace(None, TrackNamespace::from_utf8_path("live/room/42"))
            .await
            .expect("register namespace");
        let hook = RecordingHook::new(|_| deny());
        let coordinator = Arc::new(CountingCoordinator::default());
        let (_transport, mut subscriber) =
            spawn_authorized_session(locals, coordinator, hook.clone()).await;

        let handle = subscriber
            .subscribe_namespace(
                TrackNamespacePrefix::from_utf8_path("live/room"),
                KeyValuePairs::default(),
            )
            .await
            .expect("send SUBSCRIBE_NAMESPACE");
        let err = tokio::time::timeout(Duration::from_secs(2), handle.ok())
            .await
            .expect("response timed out")
            .expect_err("denied");

        assert_eq!(
            err,
            ServeError::Closed(RequestErrorCode::Unauthorized as u64)
        );
        assert_eq!(hook.seen(), vec!["subscribe_namespace /live/room"]);
    }

    /// An allowed SUBSCRIBE_NAMESPACE announces only the namespaces the token
    /// may discover: a sibling the token does not cover is withheld.
    #[tokio::test(flavor = "multi_thread")]
    async fn subscribe_namespace_withholds_namespaces_outside_the_grant() {
        use moq_transport::session::NamespaceEvent;

        let mut locals = Locals::new();
        let (_open, _open_requests) = locals
            .register_namespace(None, TrackNamespace::from_utf8_path("live/open"))
            .await
            .expect("register open namespace");
        let (_secret, _secret_requests) = locals
            .register_namespace(None, TrackNamespace::from_utf8_path("live/secret"))
            .await
            .expect("register secret namespace");
        let hook = RecordingHook::new(|op| {
            if op.contains("secret") {
                deny()
            } else {
                allow()
            }
        });
        let coordinator = Arc::new(CountingCoordinator::default());
        let (_transport, mut subscriber) =
            spawn_authorized_session(locals.clone(), coordinator, hook.clone()).await;

        let handle = subscriber
            .subscribe_namespace(
                TrackNamespacePrefix::from_utf8_path("live"),
                KeyValuePairs::default(),
            )
            .await
            .expect("send SUBSCRIBE_NAMESPACE");

        let event = tokio::time::timeout(Duration::from_secs(2), handle.next())
            .await
            .expect("NAMESPACE timed out")
            .expect("open")
            .expect("event");
        match event {
            NamespaceEvent::Added(ns) => {
                assert_eq!(ns, TrackNamespace::from_utf8_path("live/open"))
            }
            other => panic!("expected Added, got {other:?}"),
        }

        // A namespace added later under the denied name is withheld too.
        let (_later, _later_requests) = locals
            .register_namespace(None, TrackNamespace::from_utf8_path("live/secret/2"))
            .await
            .expect("register later namespace");
        assert!(
            tokio::time::timeout(Duration::from_millis(150), handle.next())
                .await
                .is_err(),
            "no NAMESPACE may disclose a namespace outside the grant"
        );
        assert!(hook
            .seen()
            .iter()
            .any(|op| op == "subscribe_namespace /live/secret/2"));
    }

    /// TRACK_STATUS is an existence oracle, so a denied request is answered
    /// with UNAUTHORIZED even for a track that exists locally.
    #[tokio::test(flavor = "multi_thread")]
    async fn denied_track_status_is_unauthorized_even_for_a_live_track() {
        let namespace = TrackNamespace::from_utf8_path("status");
        let (_writer, reader) = Track::new(namespace.clone(), "live").produce();
        let mut locals = Locals::new();
        let _registration = locals
            .register_track(None, reader)
            .await
            .expect("register track");
        let hook = RecordingHook::new(|_| deny());
        let coordinator = Arc::new(CountingCoordinator::default());
        let (transport, _subscriber) =
            spawn_authorized_session(locals, coordinator, hook.clone()).await;

        let (message_type, payload) = send_request(
            &transport,
            Message::TrackStatus(moq_transport::message::TrackStatus {
                id: 0,
                track_namespace: namespace,
                track_name: TrackName::from("live"),
                params: KeyValuePairs::default(),
            }),
        )
        .await;
        assert_eq!(message_type, wire_id::RequestError);
        assert_eq!(
            response_error_code(payload),
            RequestErrorCode::Unauthorized as u64
        );
        assert_eq!(hook.seen(), vec!["track_status /status live"]);
    }

    /// SUBSCRIBE_TRACKS is gated as SubscribeNamespace(3) on its prefix, and a
    /// denial is answered with UNAUTHORIZED before any snapshot is taken.
    #[tokio::test(flavor = "multi_thread")]
    async fn denied_subscribe_tracks_is_unauthorized() {
        let hook = RecordingHook::new(|_| deny());
        let coordinator = Arc::new(CountingCoordinator::default());
        let (transport, _subscriber) =
            spawn_authorized_session(Locals::new(), coordinator, hook.clone()).await;

        let (message_type, payload) = send_subscribe_tracks(&transport, "subscribe-tracks").await;
        assert_eq!(message_type, wire_id::RequestError);
        assert_eq!(
            response_error_code(payload),
            RequestErrorCode::Unauthorized as u64
        );
        assert_eq!(hook.seen(), vec!["subscribe_namespace /subscribe-tracks"]);
    }

    /// An allowed SUBSCRIBE_TRACKS generates PUBLISH only for the tracks the
    /// token may SUBSCRIBE to; the rest of the prefix is withheld.
    #[tokio::test(flavor = "multi_thread")]
    async fn subscribe_tracks_publishes_only_authorized_tracks() {
        let namespace = TrackNamespace::from_utf8_path("subscribe-tracks");
        let mut locals = Locals::new();
        let (_open_writer, open) = Track::new(namespace.clone(), "open").produce();
        let (_secret_writer, secret) = Track::new(namespace.clone(), "secret").produce();
        let _open = locals.register_track(None, open).await.expect("register");
        let _secret = locals.register_track(None, secret).await.expect("register");
        let hook = RecordingHook::new(|op| {
            if op.ends_with(" secret") {
                deny()
            } else {
                allow()
            }
        });
        let coordinator = Arc::new(CountingCoordinator::default());
        let (transport, mut subscriber) =
            spawn_authorized_session(locals, coordinator, hook.clone()).await;

        let (message_type, _) = send_subscribe_tracks(&transport, "subscribe-tracks").await;
        assert_eq!(message_type, wire_id::RequestOk);

        // Collect every generated PUBLISH. The first must arrive promptly;
        // after that, wait long enough for a withheld track to have shown up.
        // The handles are kept alive so the relay does not see a rejection.
        let mut published = Vec::new();
        let first = tokio::time::timeout(Duration::from_secs(2), subscriber.publish_received())
            .await
            .expect("PUBLISH timed out")
            .expect("PUBLISH");
        published.push(first);
        while let Ok(Some(next)) =
            tokio::time::timeout(Duration::from_millis(500), subscriber.publish_received()).await
        {
            published.push(next);
        }
        let names: Vec<_> = published.iter().map(|p| p.name().clone()).collect();
        assert_eq!(
            names,
            vec![TrackName::from("open")],
            "a track the token cannot SUBSCRIBE to must not be published"
        );
        let seen = hook.seen();
        assert!(seen.contains(&"subscribe /subscribe-tracks secret".to_string()));
        assert!(seen.contains(&"subscribe /subscribe-tracks open".to_string()));
    }

    /// Send SUBSCRIBE_TRACKS on a fresh request stream and return the first
    /// response frame.
    async fn send_subscribe_tracks(
        transport: &web_transport::Session,
        prefix: &str,
    ) -> (u64, Vec<u8>) {
        send_request(
            transport,
            Message::SubscribeTracks(SubscribeTracks {
                id: 0,
                track_namespace_prefix: TrackNamespacePrefix::from_utf8_path(prefix),
                params: KeyValuePairs::default(),
            }),
        )
        .await
    }

    /// The Error Code of a REQUEST_ERROR read from a request stream. On a
    /// draft-18 bidi response the Request ID is implied by the stream, so the
    /// payload starts with the code.
    fn response_error_code(payload: Vec<u8>) -> u64 {
        u64::decode(&mut Cursor::new(payload)).expect("decode REQUEST_ERROR code")
    }

    /// Send `request` on a fresh bidi request stream and return the first
    /// response frame.
    async fn send_request(transport: &web_transport::Session, request: Message) -> (u64, Vec<u8>) {
        let (mut send, mut recv) = transport.open_bi().await.expect("open stream");
        let mut frame = Vec::new();
        request.encode(&mut frame).expect("encode request");
        let mut frame = Cursor::new(frame);
        while frame.position() < frame.get_ref().len() as u64 {
            send.write_buf(&mut frame).await.expect("write");
        }
        let mut buffer = Vec::new();
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            read_bidi_response_frame(&mut recv, &mut buffer),
        )
        .await
        .expect("response timed out");
        // Keep the request stream open for the duration of the test.
        std::mem::forget(send);
        response
    }
}
