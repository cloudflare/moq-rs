// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Outbound SUBSCRIBE_NAMESPACE handling.

use std::{
    collections::HashSet,
    ops,
    sync::{Arc, Mutex},
};

use futures::channel::oneshot;

use crate::{
    coding::{KeyValuePairs, TrackNamespace, TrackNamespacePrefix},
    message::{self, Message, SubscribeOptions},
    mlog,
    serve::ServeError,
    watch::State,
};

use super::{Reader, SessionError, Subscriber, Writer};

#[derive(Debug, Clone)]
pub struct SubscribeNamespaceInfo {
    pub request_id: u64,
    pub namespace_prefix: TrackNamespacePrefix,
    pub subscribe_options: SubscribeOptions,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum NamespaceEvent {
    Added(TrackNamespace),
    Removed(TrackNamespace),
}

struct SubscribeNamespaceState {
    ok: bool,
    events: std::collections::VecDeque<NamespaceEvent>,
    closed: Result<(), ServeError>,
}

impl Default for SubscribeNamespaceState {
    fn default() -> Self {
        Self {
            ok: false,
            events: Default::default(),
            closed: Ok(()),
        }
    }
}

/// Outbound SUBSCRIBE_NAMESPACE request.
///
/// This handle only exposes `NAMESPACE` / `NAMESPACE_DONE` updates from the
/// request's dedicated response stream. If the request used
/// `SubscribeOptions::Publish` or `SubscribeOptions::Both`, matching `PUBLISH`
/// messages arrive on the session control stream (§9.25) and are surfaced via
/// [`Subscriber::publish_received`].
#[must_use = "cancels SUBSCRIBE_NAMESPACE on drop"]
pub struct SubscribeNamespace {
    subscriber: Subscriber,
    state: State<SubscribeNamespaceState>,
    // Keep the request half alive. Dropping it sends FIN, cancelling the request (§6.1).
    _writer: Writer,

    pub info: SubscribeNamespaceInfo,
}

impl SubscribeNamespace {
    pub(super) fn new(
        subscriber: Subscriber,
        info: SubscribeNamespaceInfo,
        writer: Writer,
    ) -> (Self, SubscribeNamespaceRecv) {
        let (send_state, recv_state) = State::default().split();
        let recv = SubscribeNamespaceRecv {
            state: recv_state,
            request_id: info.request_id,
            namespace_prefix: info.namespace_prefix.clone(),
            responded: false,
            known_suffixes: HashSet::default(),
            subscriber: subscriber.clone(),
        };
        let send = Self {
            subscriber,
            state: send_state,
            _writer: writer,
            info,
        };

        (send, recv)
    }

    pub async fn ok(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                state.closed.clone()?;
                if state.ok {
                    return Ok(());
                }

                match state.modified() {
                    Some(notify) => notify,
                    None => return Err(ServeError::Done),
                }
            }
            .await;
        }
    }

    pub async fn next(&self) -> Result<Option<NamespaceEvent>, ServeError> {
        loop {
            {
                let state = self.state.lock();
                if !state.events.is_empty() {
                    return Ok(state
                        .into_mut()
                        .and_then(|mut state| state.events.pop_front()));
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(None),
                }
            }
            .await;
        }
    }

    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                state.closed.clone()?;

                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(()),
                }
            }
            .await;
        }
    }
}

impl Drop for SubscribeNamespace {
    fn drop(&mut self) {
        self.subscriber
            .remove_subscribe_namespace(self.info.request_id);
    }
}

impl ops::Deref for SubscribeNamespace {
    type Target = SubscribeNamespaceInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

pub(super) struct SubscribeNamespaceRecv {
    state: State<SubscribeNamespaceState>,
    request_id: u64,
    namespace_prefix: TrackNamespacePrefix,
    responded: bool,
    known_suffixes: HashSet<TrackNamespacePrefix>,
    subscriber: Subscriber,
}

impl SubscribeNamespaceRecv {
    pub async fn run(
        mut self,
        mut reader: Reader,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(), SessionError> {
        loop {
            if self.responded && reader.done().await? {
                return Ok(());
            }

            let msg = reader.decode::<Message>().await?;
            self.emit_mlog(&mlog, &msg);
            if !self.recv_message(msg)? {
                return Ok(());
            }
        }
    }

    fn emit_mlog(&self, mlog: &Option<Arc<Mutex<mlog::MlogWriter>>>, msg: &Message) {
        if let Some(mlog) = mlog {
            if let Ok(mut mlog) = mlog.lock() {
                let time = mlog.elapsed_ms();
                let event = match msg {
                    Message::RequestOk(msg) => Some(mlog::events::request_ok_parsed(
                        time,
                        0,
                        "subscribe_namespace",
                        msg,
                    )),
                    Message::RequestError(msg) => Some(mlog::events::request_error_parsed(
                        time,
                        0,
                        "subscribe_namespace",
                        msg,
                    )),
                    Message::Namespace(msg) => Some(mlog::events::namespace_parsed(time, 0, msg)),
                    Message::NamespaceDone(msg) => {
                        Some(mlog::events::namespace_done_parsed(time, 0, msg))
                    }
                    _ => None,
                };
                if let Some(event) = event {
                    let _ = mlog.add_event(event);
                }
            }
        }
    }

    fn recv_message(&mut self, msg: Message) -> Result<bool, SessionError> {
        match msg {
            Message::RequestOk(msg) if !self.responded => {
                self.check_response_id(msg.id)?;
                self.responded = true;
                self.recv_ok();
                Ok(true)
            }
            Message::RequestError(msg) if !self.responded => {
                self.check_response_id(msg.id)?;
                self.responded = true;
                self.recv_error(ServeError::Closed(msg.error_code));
                Ok(false)
            }
            Message::Namespace(msg) if self.responded => self.recv_namespace(msg),
            Message::NamespaceDone(msg) if self.responded => self.recv_namespace_done(msg),
            Message::RequestOk(_) | Message::RequestError(_) => {
                Err(SessionError::ProtocolViolation(
                    "SUBSCRIBE_NAMESPACE response stream received multiple request responses"
                        .to_string(),
                ))
            }
            other => Err(SessionError::ProtocolViolation(format!(
                "unexpected {} on SUBSCRIBE_NAMESPACE response stream",
                other.name()
            ))),
        }
    }

    fn check_response_id(&self, id: u64) -> Result<(), SessionError> {
        if id == self.request_id {
            return Ok(());
        }

        Err(SessionError::ProtocolViolation(
            "SUBSCRIBE_NAMESPACE response request ID mismatch".to_string(),
        ))
    }

    fn recv_ok(&mut self) {
        if let Some(mut state) = self.state.lock_mut() {
            state.ok = true;
        }
    }

    fn recv_error(&mut self, err: ServeError) {
        if let Some(mut state) = self.state.lock_mut() {
            state.closed = Err(err);
        }
        self.subscriber.remove_subscribe_namespace(self.request_id);
    }

    fn recv_namespace(&mut self, msg: message::Namespace) -> Result<bool, SessionError> {
        let namespace = self
            .namespace_prefix
            .join_suffix(&msg.track_namespace_suffix)
            .map_err(|err| {
                SessionError::ProtocolViolation(format!(
                    "invalid NAMESPACE suffix for SUBSCRIBE_NAMESPACE: {}",
                    err
                ))
            })?;

        self.known_suffixes.insert(msg.track_namespace_suffix);
        let Some(mut state) = self.state.lock_mut() else {
            return Ok(false);
        };
        state.events.push_back(NamespaceEvent::Added(namespace));
        Ok(true)
    }

    fn recv_namespace_done(&mut self, msg: message::NamespaceDone) -> Result<bool, SessionError> {
        if !self.known_suffixes.remove(&msg.track_namespace_suffix) {
            return Err(SessionError::ProtocolViolation(
                "NAMESPACE_DONE received before corresponding NAMESPACE".to_string(),
            ));
        }

        let namespace = self
            .namespace_prefix
            .join_suffix(&msg.track_namespace_suffix)
            .map_err(|err| {
                SessionError::ProtocolViolation(format!(
                    "invalid NAMESPACE_DONE suffix for SUBSCRIBE_NAMESPACE: {}",
                    err
                ))
            })?;

        let Some(mut state) = self.state.lock_mut() else {
            return Ok(false);
        };
        state.events.push_back(NamespaceEvent::Removed(namespace));
        Ok(true)
    }
}

impl Drop for SubscribeNamespaceRecv {
    fn drop(&mut self) {
        self.subscriber.remove_subscribe_namespace(self.request_id);
    }
}

pub(super) struct OpenSubscribeNamespace {
    pub subscriber: Subscriber,
    pub info: SubscribeNamespaceInfo,
    pub message: message::SubscribeNamespace,
    pub reply: oneshot::Sender<Result<SubscribeNamespace, SessionError>>,
}

impl OpenSubscribeNamespace {
    pub fn new(
        subscriber: Subscriber,
        request_id: u64,
        namespace_prefix: TrackNamespacePrefix,
        subscribe_options: SubscribeOptions,
        params: KeyValuePairs,
        reply: oneshot::Sender<Result<SubscribeNamespace, SessionError>>,
    ) -> Self {
        let info = SubscribeNamespaceInfo {
            request_id,
            namespace_prefix: namespace_prefix.clone(),
            subscribe_options,
        };
        let message = message::SubscribeNamespace {
            id: request_id,
            track_namespace_prefix: namespace_prefix,
            subscribe_options,
            params,
        };

        Self {
            subscriber,
            info,
            message,
            reply,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{message::RequestOk, session::PendingRequests, session::RequestId, watch::Queue};

    use super::*;

    fn subscriber() -> Subscriber {
        let request_id = RequestId::new(0, 100, 100, 0);
        Subscriber::new(
            Queue::default(),
            Queue::default(),
            None,
            request_id,
            PendingRequests::default(),
        )
    }

    fn recv(prefix: &str) -> SubscribeNamespaceRecv {
        let info = SubscribeNamespaceInfo {
            request_id: 0,
            namespace_prefix: TrackNamespacePrefix::from_utf8_path(prefix),
            subscribe_options: SubscribeOptions::Namespace,
        };
        SubscribeNamespaceRecv {
            state: State::<SubscribeNamespaceState>::default(),
            request_id: info.request_id,
            namespace_prefix: info.namespace_prefix,
            responded: false,
            known_suffixes: HashSet::default(),
            subscriber: subscriber(),
        }
    }

    #[test]
    fn request_ok_marks_subscription_ok() {
        let mut recv = recv("example.com");

        assert!(recv
            .recv_message(Message::RequestOk(RequestOk {
                id: 0,
                params: Default::default(),
            }))
            .unwrap());

        assert!(recv.state.lock().ok);
    }

    #[test]
    fn namespace_event_reconstructs_full_namespace() {
        let mut recv = recv("example.com/meeting=123");
        recv.recv_message(Message::RequestOk(RequestOk {
            id: 0,
            params: Default::default(),
        }))
        .unwrap();

        recv.recv_message(Message::Namespace(message::Namespace {
            track_namespace_suffix: TrackNamespacePrefix::from_utf8_path("participant=100"),
        }))
        .unwrap();

        let event = recv.state.lock_mut().unwrap().events.pop_front().unwrap();
        assert_eq!(
            event,
            NamespaceEvent::Added(TrackNamespace::from_utf8_path(
                "example.com/meeting=123/participant=100"
            ))
        );
    }

    #[test]
    fn namespace_done_before_namespace_is_protocol_violation() {
        let mut recv = recv("example.com");
        recv.recv_message(Message::RequestOk(RequestOk {
            id: 0,
            params: Default::default(),
        }))
        .unwrap();

        let err = recv
            .recv_message(Message::NamespaceDone(message::NamespaceDone {
                track_namespace_suffix: TrackNamespacePrefix::from_utf8_path("meeting=123"),
            }))
            .unwrap_err();

        assert!(matches!(err, SessionError::ProtocolViolation(_)));
    }

    #[test]
    fn second_request_response_is_protocol_violation() {
        let mut recv = recv("example.com");
        recv.recv_message(Message::RequestOk(RequestOk {
            id: 0,
            params: Default::default(),
        }))
        .unwrap();

        let err = recv
            .recv_message(Message::RequestOk(RequestOk {
                id: 0,
                params: Default::default(),
            }))
            .unwrap_err();

        assert!(matches!(err, SessionError::ProtocolViolation(_)));
    }
}
