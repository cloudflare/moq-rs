// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::{
    message::{self, FetchOk},
    serve::ServeError,
    watch::State,
};

use super::{Reader, Subscriber};

struct FetchState {
    reader: Option<Reader>,
    ok: Option<FetchOk>,
    request_error: Option<message::RequestError>,
    closed: Result<(), ServeError>,
    stream_received: bool,
    cancel_sent: bool,
}

impl Default for FetchState {
    fn default() -> Self {
        Self {
            reader: None,
            ok: None,
            request_error: None,
            closed: Ok(()),
            stream_received: false,
            cancel_sent: false,
        }
    }
}

/// An outbound standalone FETCH.
#[must_use = "dropping a FETCH sends FETCH_CANCEL"]
pub struct Fetch {
    subscriber: Subscriber,
    state: State<FetchState>,
    reader: Option<Reader>,
    stream_done: bool,
    id: u64,
    pub request: message::Fetch,
}

pub(crate) struct FetchRecv {
    state: State<FetchState>,
    pub start: crate::coding::Location,
}

impl Fetch {
    pub(super) fn new(subscriber: Subscriber, request: message::Fetch) -> (Self, FetchRecv) {
        let id = request.id;
        let start = request
            .standalone_fetch
            .as_ref()
            .map(|fetch| fetch.start_location)
            .unwrap_or_default();
        let (send, recv) = State::default().split();
        (
            Self {
                subscriber,
                state: send,
                reader: None,
                stream_done: false,
                id,
                request,
            },
            FetchRecv { state: recv, start },
        )
    }

    pub async fn ok(&self) -> Result<FetchOk, ServeError> {
        loop {
            let notify = {
                let state = self.state.lock();
                state.closed.clone()?;
                if let Some(ok) = &state.ok {
                    return Ok(ok.clone());
                }
                state.modified()
            };
            match notify {
                Some(notify) => notify.await,
                None => return Err(ServeError::Done),
            }
        }
    }

    pub fn request_error(&self) -> Option<message::RequestError> {
        self.state.lock().request_error.clone()
    }

    pub(super) async fn read_stream_chunk(
        &mut self,
        max: usize,
    ) -> Result<Option<bytes::Bytes>, super::SessionError> {
        self.ensure_reader().await?;
        let state = self.state.clone();
        let reader = self.reader.as_mut().ok_or(ServeError::Done)?;
        let chunk = tokio::select! {
            result = reader.read_chunk(max) => result?,
            err = wait_closed(state) => return Err(err.into()),
        };
        if chunk.is_none() {
            self.stream_done = true;
        }
        Ok(chunk)
    }

    async fn ensure_reader(&mut self) -> Result<(), ServeError> {
        while self.reader.is_none() {
            let notify = {
                let state = self.state.lock();
                state.closed.clone()?;
                if state.reader.is_some() {
                    self.reader = state.into_mut().and_then(|mut state| state.reader.take());
                    continue;
                }
                state.modified().ok_or(ServeError::Done)?
            };
            notify.await;
        }
        Ok(())
    }
}

impl Drop for Fetch {
    fn drop(&mut self) {
        let send_cancel = self.state.lock_mut().is_some_and(|mut state| {
            let send = !(self.stream_done && state.ok.is_some())
                && state.closed.is_ok()
                && !state.cancel_sent;
            state.cancel_sent |= send;
            send
        });
        if send_cancel {
            self.subscriber
                .send_message(message::FetchCancel { id: self.id });
        }
        self.subscriber.remove_fetch(self.id);
    }
}

impl FetchRecv {
    pub fn recv_ok(&mut self, ok: &FetchOk) -> Result<(), ServeError> {
        let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
        if state.ok.is_some() {
            return Err(ServeError::Duplicate);
        }
        state.ok = Some(ok.clone());
        Ok(())
    }

    pub fn recv_error(&mut self, error: &message::RequestError) -> Result<(), ServeError> {
        let Some(mut state) = self.state.lock_mut() else {
            return Ok(());
        };
        state.request_error = Some(error.clone());
        state.closed = Err(ServeError::Closed(error.error_code));
        Ok(())
    }

    pub fn recv_timeout(&mut self, err: ServeError) -> Result<bool, ServeError> {
        let Some(mut state) = self.state.lock_mut() else {
            return Ok(false);
        };
        state.closed = Err(err);
        let send_cancel = !state.cancel_sent;
        state.cancel_sent = true;
        Ok(send_cancel)
    }

    pub fn recv_stream(&mut self, reader: Reader) -> Result<(), ServeError> {
        let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
        if state.stream_received {
            return Err(ServeError::Duplicate);
        }
        state.stream_received = true;
        state.reader = Some(reader);
        Ok(())
    }
}

async fn wait_closed(state: State<FetchState>) -> ServeError {
    loop {
        let notify = {
            let state = state.lock();
            if let Err(err) = &state.closed {
                return err.clone();
            }
            state.modified()
        };
        match notify {
            Some(notify) => notify.await,
            None => return ServeError::Done,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use crate::{
        coding::{KeyValuePairs, Location, TrackNamespace},
        message::{Message, StandaloneFetch},
        session::{PendingRequest, PendingRequests, RequestId, SessionId},
        watch::Queue,
    };

    use super::*;

    #[test]
    fn request_error_after_fetch_drop_is_benign() {
        let (fetch, state) = State::<FetchState>::default().split();
        let mut recv = FetchRecv {
            state,
            start: Default::default(),
        };
        drop(fetch);

        let error =
            message::RequestError::new(0, message::RequestErrorCode::InternalError, 0, "failed");
        assert!(recv.recv_error(&error).is_ok());
    }

    #[test]
    fn timeout_after_fetch_drop_is_benign() {
        let (fetch, state) = State::<FetchState>::default().split();
        let mut recv = FetchRecv {
            state,
            start: Default::default(),
        };
        drop(fetch);

        assert_eq!(recv.recv_timeout(ServeError::Cancel), Ok(false));
    }

    #[tokio::test]
    async fn timeout_racing_fetch_drop_sends_one_cancel() {
        for _ in 0..100 {
            let (outgoing, mut receiver) = Queue::default().split();
            let keepalive = outgoing.clone();
            let mut subscriber = Subscriber::new(
                outgoing,
                Queue::default(),
                None,
                RequestId::new(0, 100, 100, 0),
                PendingRequests::default(),
                SessionId::generate(),
            );
            let fetch = subscriber
                .fetch(
                    StandaloneFetch {
                        track_namespace: TrackNamespace::from_utf8_path("test"),
                        track_name: "video".into(),
                        start_location: Location::new(0, 0),
                        end_location: Location::new(1, 0),
                    },
                    KeyValuePairs::default(),
                )
                .unwrap();
            let Message::Fetch(request) = receiver.pop().await.unwrap() else {
                panic!("expected FETCH");
            };
            let barrier = Arc::new(Barrier::new(3));
            let drop_barrier = barrier.clone();
            let dropper = std::thread::spawn(move || {
                drop_barrier.wait();
                drop(fetch);
            });
            let timeout_barrier = barrier.clone();
            let timeout = std::thread::spawn(move || {
                timeout_barrier.wait();
                subscriber
                    .recv_request_timeout(request.id, PendingRequest::Fetch)
                    .unwrap();
            });
            barrier.wait();
            dropper.join().unwrap();
            timeout.join().unwrap();

            let cancels = receiver
                .close()
                .into_iter()
                .filter(|message| matches!(message, Message::FetchCancel(_)))
                .count();
            assert_eq!(cancels, 1);
            drop(keepalive);
        }
    }
}
