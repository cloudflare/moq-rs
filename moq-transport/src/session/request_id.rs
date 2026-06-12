// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Request ID allocation per draft-ietf-moq-transport-18 §10.1.
//!
//! Draft-18 removed MAX_REQUEST_ID and REQUESTS_BLOCKED. Request IDs are
//! simple incrementing counters: clients use even IDs starting at 0,
//! servers use odd IDs starting at 1, each incremented by 2.
//!
//! This module validates inbound request ID sequencing (correct parity
//! and strictly sequential) and allocates outbound IDs.

use std::sync::{Arc, Mutex};

use crate::session::SessionError;

#[derive(Clone, Debug)]
pub struct RequestId {
    inner: Arc<RequestIdInner>,
}

#[derive(Debug)]
struct RequestIdInner {
    send: Mutex<SendState>,
    recv: Mutex<RecvState>,
}

#[derive(Debug)]
struct SendState {
    next: u64,
}

#[derive(Debug)]
struct RecvState {
    next_expected: u64,
}

impl RequestId {
    /// Create a request-ID manager for one endpoint in a session.
    ///
    /// `local_first_id` is 0 for clients and 1 for servers.
    /// `peer_first_id` is 0 if the peer is client, 1 if the peer is server.
    pub fn new(local_first_id: u64, peer_first_id: u64) -> Self {
        Self {
            inner: Arc::new(RequestIdInner {
                send: Mutex::new(SendState {
                    next: local_first_id,
                }),
                recv: Mutex::new(RecvState {
                    next_expected: peer_first_id,
                }),
            }),
        }
    }

    /// Allocate the next outbound request ID.
    pub fn allocate(&self) -> Result<u64, SessionError> {
        let mut send = self.inner.send.lock().map_err(|_| SessionError::Internal)?;

        let id = send.next;
        send.next = send
            .next
            .checked_add(2)
            .ok_or(SessionError::TooManyRequests)?;
        Ok(id)
    }

    /// Validate an incoming new request ID from the peer.
    pub fn validate_incoming(&self, id: u64) -> Result<(), SessionError> {
        let mut recv = self.inner.recv.lock().map_err(|_| SessionError::Internal)?;

        if id != recv.next_expected {
            return Err(SessionError::InvalidRequestId);
        }

        recv.next_expected = recv
            .next_expected
            .checked_add(2)
            .ok_or(SessionError::InvalidRequestId)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client_ids() -> RequestId {
        // local client sends even (0), peer server sends odd (1)
        RequestId::new(0, 1)
    }

    fn server_ids() -> RequestId {
        // local server sends odd (1), peer client sends even (0)
        RequestId::new(1, 0)
    }

    #[test]
    fn client_allocates_even_ids() {
        let ids = client_ids();
        assert_eq!(ids.allocate().unwrap(), 0);
        assert_eq!(ids.allocate().unwrap(), 2);
        assert_eq!(ids.allocate().unwrap(), 4);
    }

    #[test]
    fn server_allocates_odd_ids() {
        let ids = server_ids();
        assert_eq!(ids.allocate().unwrap(), 1);
        assert_eq!(ids.allocate().unwrap(), 3);
        assert_eq!(ids.allocate().unwrap(), 5);
    }

    #[test]
    fn validates_client_peer_sequence() {
        let ids = server_ids();
        ids.validate_incoming(0).unwrap();
        ids.validate_incoming(2).unwrap();
        ids.validate_incoming(4).unwrap();
    }

    #[test]
    fn validates_server_peer_sequence() {
        let ids = client_ids();
        ids.validate_incoming(1).unwrap();
        ids.validate_incoming(3).unwrap();
        ids.validate_incoming(5).unwrap();
    }

    #[test]
    fn rejects_wrong_first_id() {
        let ids = server_ids();
        assert!(matches!(
            ids.validate_incoming(2).unwrap_err(),
            SessionError::InvalidRequestId
        ));
    }

    #[test]
    fn rejects_skipped_id() {
        let ids = server_ids();
        ids.validate_incoming(0).unwrap();
        assert!(matches!(
            ids.validate_incoming(4).unwrap_err(),
            SessionError::InvalidRequestId
        ));
    }

    #[test]
    fn rejects_repeated_id() {
        let ids = server_ids();
        ids.validate_incoming(0).unwrap();
        assert!(matches!(
            ids.validate_incoming(0).unwrap_err(),
            SessionError::InvalidRequestId
        ));
    }

    #[test]
    fn send_and_receive_state_do_not_block_each_other() {
        let ids = client_ids();
        let send_ids = ids.clone();
        let recv_ids = ids.clone();

        assert_eq!(send_ids.allocate().unwrap(), 0);
        recv_ids.validate_incoming(1).unwrap();
        assert_eq!(send_ids.allocate().unwrap(), 2);
        recv_ids.validate_incoming(3).unwrap();
    }
}
