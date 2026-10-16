// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::time::Duration;

use anyhow::{Context, Result};
use moq_native_ietf::quic;
use moq_transport::{
    coding::{TrackNamespace, TrackNamespacePrefix},
    serve::ServeError,
    session::{
        PublishedNamespace, PublishedNamespaceCloseCause, SessionError, Subscribed,
        SubscribedNamespace, Transport,
    },
};
use tokio::{sync::mpsc, task::JoinSet, time::Instant};

use crate::{
    registry::{Entry, Test},
    routing::{self, PublisherRoute, Routing},
};

pub const TEST_NAMESPACE: &str = "moq-test/interop";
pub const TEST_TRACK: &str = "test-track";

#[derive(Debug)]
pub struct SessionRecord {
    pub version: String,
    pub transport: Transport,
    pub connection_id: String,
    pub role: Option<&'static str>,
    first_request: bool,
    setup_complete: bool,
    closed: bool,
    gracefully_closed: bool,
    fin_received: Option<Duration>,
}

#[derive(Default)]
struct Observations {
    sessions: Vec<SessionRecord>,
    namespace: Option<TrackNamespace>,
    namespace_session: Option<usize>,
    subscribe_session: Option<usize>,
    namespace_prefix: Option<TrackNamespacePrefix>,
    namespace_subscriber: Option<usize>,
    withdrawn: bool,
    namespace_notified: bool,
    namespace_done_notified: bool,
    subscribe_answer: Option<SubscribeAnswer>,
    failure: Option<String>,
    session_error: Option<String>,
    skip: Option<String>,
    comments: Vec<String>,
    control_finished: Vec<usize>,
    namespace_received: Option<Duration>,
    subscribe_received: Option<Duration>,
}

pub struct RunResult {
    pub duration: Duration,
    pub sessions: Vec<SessionRecord>,
    pub message: Option<String>,
    pub skip: Option<String>,
    pub comments: Vec<String>,
}

#[derive(Clone, Copy)]
struct Clock {
    start: Instant,
    deadline: Instant,
}

pub enum SubscribeAnswer {
    Accepted,
    Rejected,
    Cancelled,
    /// The subscriber cancelled before any answer, so it never received one.
    SubscriberCancelled,
    HoldExpired(u64),
}

pub enum Event {
    SetupComplete(usize),
    Namespace(usize, PublishedNamespace, Duration, usize),
    NamespaceFailed(usize, String, Duration),
    Subscribe(
        usize,
        Box<Subscribed>,
        Duration,
        Option<Box<PublisherRoute>>,
    ),
    SubscribeNamespace(usize, SubscribedNamespace),
    Unexpected(usize, &'static str),
    Ended(usize, Result<(), SessionError>),
    DriverFailed(usize, String),
    ControlFinished(usize, Duration),
    Withdrawn(usize, TrackNamespace, usize),
    SubscribeAnswer(Result<SubscribeAnswer, String>),
    RoutingFinished,
}

impl Observations {
    fn check_control_finished(&mut self, test: Test) {
        for id in std::mem::take(&mut self.control_finished) {
            let role = self.sessions[id].role.unwrap_or("unassigned");
            let fin_time = describe_time(self.sessions[id].fin_received);
            let missing = match role {
                "client" if !self.sessions[id].setup_complete => Some("completing SETUP"),
                "publisher" if self.namespace_session != Some(id) => {
                    Some("PUBLISH_NAMESPACE was accepted")
                }
                "publisher"
                    if matches!(
                        test,
                        Test::PublishNamespaceDone | Test::SubscribeNamespaceLifecycle
                    ) && !self.withdrawn =>
                {
                    Some("withdrawing the namespace")
                }
                "publisher"
                    if matches!(
                        test,
                        Test::AnnounceSubscribe | Test::SubscribeBeforeAnnounceHold
                    ) && self.subscribe_received.is_some()
                        && matches!(
                            self.subscribe_answer,
                            None | Some(SubscribeAnswer::Cancelled)
                        )
                        && self.skip.is_none() =>
                {
                    // SubscriberCancelled is absent here: the relay then drops
                    // the forwarded SUBSCRIBE, so the publisher owes no answer.
                    Some("the forwarded SUBSCRIBE was answered")
                }
                "subscriber"
                    if test == Test::SubscribeNamespaceLifecycle && !self.namespace_notified =>
                {
                    Some("NAMESPACE was sent")
                }
                "subscriber"
                    if test == Test::SubscribeNamespaceLifecycle
                        && !self.namespace_done_notified =>
                {
                    Some("NAMESPACE_DONE was sent")
                }
                "subscriber"
                    if test.needs_subscribe()
                        && matches!(
                            self.subscribe_answer,
                            None | Some(SubscribeAnswer::SubscriberCancelled)
                        ) =>
                {
                    Some("the SUBSCRIBE response was sent")
                }
                _ => None,
            };
            if let Some(missing) = missing {
                let failure = format!("{role} session {} closed the control stream at the transport layer before {missing}; FIN {fin_time} (draft 18 Section 3.3)", id + 1);
                self.failure = Some(match self.failure.take() {
                    Some(previous) => format!("{failure}; {previous}"),
                    None => failure,
                });
            } else {
                self.comments.push(format!("control-stream FIN on session {} ({role}) after its required steps; FIN {fin_time} (draft 18 Section 3.3)", id + 1));
            }
        }
    }

    fn record_session_error(&mut self, id: usize, error: String) {
        if self.session_error.is_none() {
            self.session_error = Some(format!(
                "session {} ended with error or reset: {error}",
                id + 1
            ));
        }
    }

    fn fail(&mut self, message: String) {
        if self.failure.is_none() {
            self.failure = Some(message);
        }
    }

    fn assign_role(&mut self, id: usize, role: Option<&'static str>) {
        if self.sessions[id].first_request || self.sessions[id].role.is_some() {
            return;
        }
        self.sessions[id].first_request = true;
        if let Some(role) = role {
            if !self
                .sessions
                .iter()
                .any(|session| session.role == Some(role))
            {
                self.sessions[id].role = Some(role);
            }
        }
    }

    fn unexpected(&mut self, id: usize, name: &'static str) {
        self.assign_role(id, None);
        self.comments
            .push(format!("unexpected request: {name} on session {}", id + 1));
    }

    fn role_closed(&self, role: &str) -> bool {
        self.sessions
            .iter()
            .any(|session| session.role == Some(role) && session.setup_complete && session.closed)
    }

    fn missing_observation(&self, test: Test) -> Option<String> {
        if self.sessions.is_empty() {
            return Some("no client session".to_owned());
        }
        if test == Test::SetupOnly {
            return if !self.sessions[0].setup_complete {
                Some("client did not complete SETUP before input ended".to_owned())
            } else if !self.sessions[0].closed {
                Some("client did not close the session gracefully before input ended; no graceful connection close reached the relay".to_owned())
            } else {
                None
            };
        }
        if test.needs_namespace() && self.namespace_received.is_none() {
            return Some("no PUBLISH_NAMESPACE before input ended".to_owned());
        }
        if test.needs_subscribe() && self.subscribe_received.is_none() {
            return Some("no SUBSCRIBE before input ended".to_owned());
        }
        if test == Test::SubscribeNamespaceLifecycle {
            if self.namespace_prefix.is_none() {
                return Some("no SUBSCRIBE_NAMESPACE before input ended".to_owned());
            }
            if !self.namespace_notified {
                return Some("no matching namespace for NAMESPACE before input ended".to_owned());
            }
            if !self.withdrawn {
                return Some(self.missing_withdrawal());
            }
            if !self.namespace_done_notified {
                return Some("no NAMESPACE_DONE before input ended".to_owned());
            }
        }
        if test == Test::PublishNamespaceDone && !self.withdrawn {
            return Some(self.missing_withdrawal());
        }
        if test.needs_subscribe() && self.subscribe_answer.is_none() {
            return Some(
                if matches!(
                    test,
                    Test::AnnounceSubscribe | Test::SubscribeBeforeAnnounceHold
                ) && self.skip.is_none()
                {
                    "publisher did not answer the forwarded SUBSCRIBE before input ended".to_owned()
                } else {
                    "SUBSCRIBE was not answered before input ended".to_owned()
                },
            );
        }
        let close_role = match test {
            Test::AnnounceOnly | Test::PublishNamespaceDone => Some("publisher"),
            Test::SubscribeError | Test::RendezvousTimeout => Some("subscriber"),
            _ => None,
        };
        if close_role.is_some_and(|role| !self.role_closed(role)) {
            return Some(format!(
                "{} did not close the session gracefully before input ended; no graceful connection close reached the relay",
                close_role.unwrap_or("client")
            ));
        }
        None
    }

    fn missing_withdrawal(&self) -> String {
        let mut message =
            "publisher did not cancel the PUBLISH_NAMESPACE request before input ended".to_owned();
        if self
            .namespace_session
            .is_some_and(|id| self.sessions[id].closed)
        {
            message.push_str("; no withdrawal reached the relay before the session closed");
        }
        message
    }

    fn verdict(&self, test: Test) -> Option<String> {
        let missing = self.missing_observation(test);
        let message = match (&self.failure, missing) {
            (Some(failure), Some(missing)) => Some(format!("{failure}; {missing}")),
            (Some(failure), None) => Some(failure.clone()),
            (None, missing) => missing,
        };
        message.map(|mut message| {
            if test == Test::PublishNamespaceDone
                && !self.role_closed("publisher")
                && !self.sessions.is_empty()
            {
                message.push_str("; no graceful connection close reached the relay");
            }
            if test.subscribe_first() || test == Test::AnnounceSubscribe {
                format!(
                    "{message}; PUBLISH_NAMESPACE {}; SUBSCRIBE {}",
                    describe_time(self.namespace_received),
                    describe_time(self.subscribe_received)
                )
            } else {
                message
            }
        })
    }

    fn check_order(&mut self, test: Test) {
        if let (Some(namespace), Some(subscribe)) =
            (self.namespace_received, self.subscribe_received)
        {
            if test.subscribe_first() && namespace < subscribe {
                self.fail("PUBLISH_NAMESPACE arrived before SUBSCRIBE; the procedure sends SUBSCRIBE first".to_owned());
            } else if test == Test::AnnounceSubscribe && subscribe < namespace {
                self.fail("SUBSCRIBE arrived before PUBLISH_NAMESPACE; the procedure requires a publisher route".to_owned());
            }
        }
    }

    fn check_lifecycle_match(&mut self) {
        if let (Some(namespace), Some(prefix)) = (&self.namespace, &self.namespace_prefix) {
            if namespace.strip_prefix(prefix).is_none() {
                self.fail(format!("PUBLISH_NAMESPACE named {namespace}, which does not match SUBSCRIBE_NAMESPACE prefix {prefix}"));
            }
        }
        if self.namespace_session.is_some() && self.namespace_session == self.namespace_subscriber {
            self.fail("PUBLISH_NAMESPACE arrived on the namespace subscriber session".to_owned());
        }
    }

    async fn handle(
        &mut self,
        event: Event,
        entry: &Entry,
        clock: Clock,
        routing: &Routing,
        tasks: &mut JoinSet<Event>,
        tx: &mpsc::Sender<Event>,
    ) {
        let test = entry.test;
        match event {
            Event::SetupComplete(id) => self.sessions[id].setup_complete = true,
            Event::Namespace(id, published, received, notified) => {
                if self.namespace_received.is_some() {
                    self.unexpected(id, "PUBLISH_NAMESPACE");
                } else {
                    self.namespace_received = Some(received);
                    self.assign_role(id, Some("publisher"));
                    if test != Test::SubscribeNamespaceLifecycle
                        && published.namespace != TrackNamespace::from_utf8_path(TEST_NAMESPACE)
                    {
                        self.fail(format!(
                            "PUBLISH_NAMESPACE named {}, expected {TEST_NAMESPACE}",
                            published.namespace
                        ));
                    }
                    self.namespace = Some(published.namespace.clone());
                    self.namespace_session = Some(id);
                    if self.subscribe_session == Some(id) {
                        self.fail("PUBLISH_NAMESPACE arrived on the subscriber session".to_owned());
                    }
                    self.namespace_notified |= notified > 0;
                    self.check_order(test);
                    self.check_lifecycle_match();
                }
                let routing = routing.clone();
                tasks.spawn(async move {
                    match published.closed_cause().await {
                        PublishedNamespaceCloseCause::PeerCancelled { code } => {
                            tracing::info!(
                                session = id + 1,
                                received_us = clock.start.elapsed().as_micros(),
                                ?code,
                                "observed PUBLISH_NAMESPACE request cancellation"
                            );
                            let notified = routing.withdraw(id, &published.namespace).await;
                            Event::Withdrawn(id, published.namespace.clone(), notified)
                        }
                        PublishedNamespaceCloseCause::Teardown => Event::RoutingFinished,
                    }
                });
            }
            Event::NamespaceFailed(id, message, received) => {
                if self.namespace_received.is_some() {
                    self.unexpected(id, "PUBLISH_NAMESPACE");
                } else {
                    self.namespace_received = Some(received);
                    self.assign_role(id, Some("publisher"));
                    self.fail(message);
                }
            }
            Event::Subscribe(id, subscribed, received, route) => {
                if self.subscribe_received.is_some() {
                    let _ = subscribed.close(ServeError::NotImplemented(
                        "additional subscription not served by this test".to_owned(),
                    ));
                    self.unexpected(id, "SUBSCRIBE");
                    return;
                }
                self.subscribe_received = Some(received);
                self.subscribe_session.get_or_insert(id);
                self.assign_role(id, Some("subscriber"));
                self.check_order(test);
                let namespace = match test {
                    Test::SubscribeError => "nonexistent/namespace",
                    Test::RendezvousTimeout => "nonexistent/rendezvous",
                    _ => TEST_NAMESPACE,
                };
                if subscribed.track_namespace != TrackNamespace::from_utf8_path(namespace)
                    || subscribed.track_name.as_ref() != TEST_TRACK.as_bytes()
                {
                    self.fail(format!(
                        "SUBSCRIBE named {}/{}, expected {namespace}/{TEST_TRACK}",
                        subscribed.track_namespace, subscribed.track_name
                    ));
                    let _ = subscribed.close(ServeError::NotFound);
                    self.subscribe_answer = Some(SubscribeAnswer::Rejected);
                    return;
                }
                if self.namespace_session == Some(id) {
                    self.fail("SUBSCRIBE arrived on the publisher session".to_owned());
                }
                let requested = match subscribed.params.rendezvous_timeout() {
                    Ok(value) => value.unwrap_or(0),
                    Err(err) => {
                        self.fail(format!("invalid RENDEZVOUS_TIMEOUT: {err}"));
                        let _ = subscribed.close(ServeError::NotImplemented(
                            "invalid RENDEZVOUS_TIMEOUT".to_owned(),
                        ));
                        self.subscribe_answer = Some(SubscribeAnswer::Rejected);
                        return;
                    }
                };
                self.comments.push(format!(
                    "RENDEZVOUS_TIMEOUT requested: {requested} ms on session {}",
                    id + 1
                ));
                if test == Test::RendezvousTimeout && requested != 500 {
                    self.fail(format!(
                        "RENDEZVOUS_TIMEOUT requested {requested} ms, expected 500 ms"
                    ));
                }
                if test == Test::SubscribeBeforeAnnounceHold && requested == 0 && route.is_none() {
                    self.skip = Some("hold needs a nonzero RENDEZVOUS_TIMEOUT".to_owned());
                    let _ = subscribed.close(ServeError::NotFound);
                    self.subscribe_answer = Some(SubscribeAnswer::Rejected);
                    return;
                }
                if test == Test::AnnounceSubscribe && route.is_none() && requested == 0 {
                    self.fail("SUBSCRIBE arrived without an active PUBLISH_NAMESPACE route; sent DOES_NOT_EXIST (draft-18 Section 10.2.6)".to_owned());
                }
                let routing = routing.clone();
                let tx = tx.clone();
                let hold_until =
                    routing::hold_deadline(clock.start + received, requested, clock.deadline);
                tasks.spawn(async move {
                    routing
                        .subscribe(
                            id,
                            *subscribed,
                            route.map(|route| *route),
                            test,
                            hold_until,
                            tx,
                        )
                        .await
                });
            }
            Event::SubscribeNamespace(id, request) => {
                self.assign_role(id, Some("subscriber"));
                self.namespace_prefix
                    .get_or_insert_with(|| request.namespace_prefix.clone());
                self.namespace_subscriber.get_or_insert(id);
                self.check_lifecycle_match();
                match routing.subscribe_namespace(id, request).await {
                    Ok(notified) => self.namespace_notified |= notified > 0,
                    Err(err) => self.fail(format!("could not answer SUBSCRIBE_NAMESPACE: {err}")),
                }
            }
            Event::Unexpected(id, name) => self.unexpected(id, name),
            Event::ControlFinished(id, received) => {
                self.sessions[id].fin_received = Some(received);
                self.control_finished.push(id);
            }
            Event::DriverFailed(id, error) => {
                self.record_session_error(id, error);
            }
            Event::Ended(id, result) => {
                self.sessions[id].closed = true;
                if let Err(err) = result {
                    if err.is_graceful_close() {
                        self.sessions[id].gracefully_closed = true;
                    } else {
                        self.record_session_error(id, err.to_string());
                    }
                }
                let notified = routing.remove_session(id).await;
                if self.namespace_session == Some(id) {
                    self.namespace_done_notified |= notified > 0;
                }
            }
            Event::Withdrawn(id, namespace, notified) => {
                if self.namespace_session == Some(id) && self.namespace.as_ref() == Some(&namespace)
                {
                    self.withdrawn = true;
                    self.namespace_done_notified |= notified > 0;
                }
            }
            Event::SubscribeAnswer(Ok(answer)) => {
                match &answer {
                    SubscribeAnswer::HoldExpired(requested) => {
                        self.skip = Some(format!(
                            "RENDEZVOUS_TIMEOUT {requested} ms expired before PUBLISH_NAMESPACE"
                        ))
                    }
                    SubscribeAnswer::Rejected if test == Test::AnnounceSubscribe => self.fail(
                        "SUBSCRIBE received REQUEST_ERROR instead of SUBSCRIBE_OK".to_owned(),
                    ),
                    SubscribeAnswer::Cancelled => self.fail(
                        "publisher rejected or failed the forwarded SUBSCRIBE: cancelled"
                            .to_owned(),
                    ),
                    SubscribeAnswer::SubscriberCancelled => self.fail(
                        "subscriber cancelled SUBSCRIBE before receiving SUBSCRIBE_OK or REQUEST_ERROR"
                            .to_owned(),
                    ),
                    _ => {}
                }
                self.subscribe_answer = Some(answer);
            }
            Event::SubscribeAnswer(Err(message)) => {
                self.fail(message);
                self.subscribe_answer = Some(SubscribeAnswer::Rejected);
            }
            Event::RoutingFinished => {}
        }
    }
}

fn describe_time(time: Option<Duration>) -> String {
    match time {
        Some(time) => format!("received at {} us after first session", time.as_micros()),
        None => "not received".to_owned(),
    }
}

pub async fn run(
    server: &mut quic::Server,
    entry: &Entry,
    first_session_wait: Duration,
    post_verdict_wait: Duration,
    terminate: &mut tokio::signal::unix::Signal,
) -> Result<RunResult> {
    let (tx, mut rx) = mpsc::channel(64);
    let mut tasks = JoinSet::new();
    let mut observations = Observations::default();
    let routing = Routing::default();
    let mut transports = Vec::new();
    let mut first_session = None;
    let mut deadline = Instant::now() + first_session_wait;
    let mut namespace_deadline_started = false;
    let mut verdict = None;
    let mut stop_at = deadline;

    loop {
        if !observations.control_finished.is_empty() {
            drain_events(
                &mut observations,
                entry,
                Clock {
                    start: first_session.unwrap_or_else(Instant::now),
                    deadline,
                },
                &routing,
                &mut tasks,
                &tx,
                &mut rx,
            )
            .await?;
            observations.check_control_finished(entry.test);
        }
        if verdict.is_none()
            && !observations.sessions.is_empty()
            && observations.missing_observation(entry.test).is_none()
        {
            let duration = first_session.map_or(Duration::ZERO, |start: Instant| start.elapsed());
            verdict = Some((
                duration,
                observations.verdict(entry.test),
                observations.skip.clone(),
            ));
            stop_at = Instant::now() + post_verdict_wait;
            tracing::info!(
                duration_ms = duration.as_millis(),
                "completion condition reached; waiting for client sessions to close"
            );
        }
        if verdict.is_some() && observations.sessions.iter().all(|session| session.closed) {
            break;
        }
        tokio::select! {
            accepted = server.accept() => {
                let (session, info) = accepted.context("QUIC accept loop ended")?;
                let accepted_at = Instant::now();
                if first_session.is_none() {
                    first_session = Some(accepted_at);
                    deadline = accepted_at + entry.timeout;
                }
                let version = match info.transport {
                    Transport::WebTransport => session.protocol().context("accepted WebTransport has no negotiated MoQT protocol")?.to_owned(),
                    Transport::RawQuic => moq_transport::setup::SUPPORTED_ALPNS[0].to_owned(),
                };
                let id = observations.sessions.len();
                tracing::info!(session = id + 1, cid = %info.id, transport = ?info.transport, "accepted client session");
                observations.sessions.push(SessionRecord {
                    version, transport: info.transport, connection_id: info.id,
                    role: (entry.test == Test::SetupOnly && id == 0).then_some("client"),
                    first_request: false, setup_complete: false, closed: false,
                    gracefully_closed: false, fin_received: None,
                });
                transports.push(session.clone());
                tasks.spawn(crate::sessions::session_task(id, session, info.transport, entry.test, first_session.context("first session clock missing")?, routing.clone(), tx.clone()));
            }
            Some(event) = rx.recv() => {
                observations.handle(event, entry, Clock { start: first_session.unwrap_or_else(Instant::now), deadline }, &routing, &mut tasks, &tx).await;
            }
            Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                observations.handle(joined.context("test relay task failed")?, entry, Clock { start: first_session.unwrap_or_else(Instant::now), deadline }, &routing, &mut tasks, &tx).await;
            }
            _ = tokio::time::sleep_until(if verdict.is_some() { stop_at } else { deadline }) => {
                if verdict.is_some() {
                    observations.comments.push("post-verdict safety cap reached; relay closed remaining sessions".to_owned());
                    break;
                }
                drain_events(&mut observations, entry, Clock { start: first_session.unwrap_or_else(Instant::now), deadline }, &routing, &mut tasks, &tx, &mut rx).await?;
                observations.check_control_finished(entry.test);
                let duration = first_session.map_or(Duration::ZERO, |start| start.elapsed());
                verdict = Some((duration, observations.verdict(entry.test), observations.skip.clone()));
                stop_at = Instant::now() + post_verdict_wait;
                tracing::info!(duration_ms = duration.as_millis(), "test deadline reached; waiting for client sessions to close");
            }
            _ = terminate.recv() => {
                tracing::info!("SIGTERM received; judging collected evidence");
                break;
            }
        }
        if verdict.is_none() && entry.test.namespace_deadline() && !namespace_deadline_started {
            if let (Some(start), Some(received)) = (first_session, observations.namespace_received)
            {
                deadline = start + received + entry.timeout;
                namespace_deadline_started = true;
            }
        }
    }
    drain_events(
        &mut observations,
        entry,
        Clock {
            start: first_session.unwrap_or_else(Instant::now),
            deadline,
        },
        &routing,
        &mut tasks,
        &tx,
        &mut rx,
    )
    .await?;
    observations.check_control_finished(entry.test);
    let (duration, mut message, skip) = verdict.unwrap_or_else(|| {
        (
            first_session.map_or(Duration::ZERO, |start| start.elapsed()),
            observations.verdict(entry.test),
            observations.skip.clone(),
        )
    });
    if let Some(failure) = observations.failure {
        message = Some(match message {
            Some(message) if !message.contains(&failure) => format!("{failure}; {message}"),
            Some(message) => message,
            None => failure,
        });
    }
    if let Some(error) = observations.session_error {
        message = Some(match message {
            Some(message) => format!("{message}; {error}"),
            None => error,
        });
    }
    for (id, session) in observations.sessions.iter().enumerate() {
        if let Some(received) = session.fin_received.filter(|_| !session.gracefully_closed) {
            let failure = format!("{} session {} closed the control stream; FIN {}; no graceful connection close followed before TAP (draft 18 Section 3.3)", session.role.unwrap_or("unassigned"), id + 1, describe_time(Some(received)));
            message = Some(match message {
                Some(message) => format!("{message}; {failure}"),
                None => failure,
            });
        }
    }
    // Local shutdown (SIGTERM or the safety cap) is not a client error.
    for session in transports {
        session.close(0, "test relay finished");
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    Ok(RunResult {
        duration,
        sessions: observations.sessions,
        message,
        skip,
        comments: observations.comments,
    })
}

// Events already observed by tasks count when a timer or SIGTERM races delivery.
async fn drain_events(
    observations: &mut Observations,
    entry: &Entry,
    clock: Clock,
    routing: &Routing,
    tasks: &mut JoinSet<Event>,
    tx: &mpsc::Sender<Event>,
    rx: &mut mpsc::Receiver<Event>,
) -> Result<()> {
    loop {
        if let Ok(event) = rx.try_recv() {
            observations
                .handle(event, entry, clock, routing, tasks, tx)
                .await;
        } else if let Some(joined) = tasks.try_join_next() {
            observations
                .handle(
                    joined.context("test relay task failed")?,
                    entry,
                    clock,
                    routing,
                    tasks,
                    tx,
                )
                .await;
        } else {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> SessionRecord {
        SessionRecord {
            version: "moqt-18".to_owned(),
            transport: Transport::RawQuic,
            connection_id: "abc123".to_owned(),
            role: None,
            first_request: false,
            setup_complete: true,
            closed: false,
            gracefully_closed: false,
            fin_received: None,
        }
    }

    #[test]
    fn first_request_assigns_unique_roles_without_relabeling_sessions() {
        let mut observations = Observations {
            sessions: vec![record(), record(), record(), record()],
            ..Default::default()
        };
        observations.assign_role(1, Some("publisher"));
        observations.assign_role(0, Some("subscriber"));
        observations.assign_role(2, Some("publisher"));
        observations.assign_role(2, Some("subscriber"));
        observations.assign_role(3, None);
        observations.assign_role(3, Some("publisher"));
        assert_eq!(
            observations
                .sessions
                .iter()
                .map(|s| s.role)
                .collect::<Vec<_>>(),
            [Some("subscriber"), Some("publisher"), None, None]
        );
    }
}
