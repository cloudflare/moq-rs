// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::{
    sync::Notify,
    time::{Duration, Instant},
};

use super::SessionError;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
pub(crate) struct PendingRequests {
    inner: Arc<Mutex<HashMap<u64, PendingRequestState>>>,
    notify: Arc<Notify>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingRequest {
    PublishNamespace,
    Publish,
    Subscribe,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingResponse {
    RequestOk,
    RequestError,
    PublishOk,
    SubscribeOk,
}

#[derive(Clone, Copy, Debug)]
struct PendingRequestState {
    request: PendingRequest,
    deadline: Instant,
}

impl PendingRequest {
    fn accepts(self, response: PendingResponse) -> bool {
        match (self, response) {
            (
                Self::PublishNamespace,
                PendingResponse::RequestOk | PendingResponse::RequestError,
            ) => true,
            (Self::Publish, PendingResponse::PublishOk | PendingResponse::RequestError) => true,
            (Self::Subscribe, PendingResponse::SubscribeOk | PendingResponse::RequestError) => true,
            _ => false,
        }
    }
}

impl PendingRequests {
    pub(crate) fn insert(&self, id: u64, request: PendingRequest) -> Result<(), SessionError> {
        let mut pending = self.inner.lock().map_err(|_| SessionError::Internal)?;
        if pending.contains_key(&id) {
            return Err(SessionError::Duplicate);
        }

        pending.insert(
            id,
            PendingRequestState {
                request,
                deadline: Instant::now() + RESPONSE_TIMEOUT,
            },
        );
        self.notify.notify_one();
        Ok(())
    }

    pub(crate) fn remove(&self, id: u64) -> Result<Option<PendingRequest>, SessionError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| SessionError::Internal)?
            .remove(&id)
            .map(|state| state.request))
    }

    pub(crate) fn complete(
        &self,
        id: u64,
        response: PendingResponse,
    ) -> Result<Option<PendingRequest>, SessionError> {
        let mut pending = self.inner.lock().map_err(|_| SessionError::Internal)?;
        let Some(state) = pending.get(&id).copied() else {
            return Ok(None);
        };

        if !state.request.accepts(response) {
            return Err(SessionError::ProtocolViolation(format!(
                "received {:?} for pending {:?} request {}",
                response, state.request, id
            )));
        }

        pending.remove(&id);
        Ok(Some(state.request))
    }

    pub(crate) fn next_deadline(&self) -> Result<Option<Instant>, SessionError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| SessionError::Internal)?
            .values()
            .map(|state| state.deadline)
            .min())
    }

    pub(crate) fn expire(&self) -> Result<Vec<(u64, PendingRequest)>, SessionError> {
        let now = Instant::now();
        let mut pending = self.inner.lock().map_err(|_| SessionError::Internal)?;
        let expired = pending
            .iter()
            .filter_map(|(id, state)| (state.deadline <= now).then_some((*id, state.request)))
            .collect::<Vec<_>>();

        for (id, _) in &expired {
            pending.remove(id);
        }

        Ok(expired)
    }

    pub(crate) async fn changed(&self) {
        self.notify.notified().await;
    }
}
