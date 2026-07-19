// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Request ID allocation per draft-ietf-moq-transport-18 §10.1.
//!
//! Draft-18 removed MAX_REQUEST_ID and REQUESTS_BLOCKED. Request IDs are
//! simple incrementing counters: clients use even IDs starting at 0,
//! servers use odd IDs starting at 1, each incremented by 2.
//!
//! This module validates inbound request IDs (correct parity and no
//! duplicates — out-of-order arrival is permitted because bidi streams
//! may arrive in any order) and allocates outbound IDs.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::session::SessionError;

/// Maximum number of distinct request IDs tracked per session (~512 KB).
const MAX_REQUEST_IDS_PER_SESSION: usize = 65536;

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
    /// Expected parity bit (0 for client-originated, 1 for server-originated).
    expected_parity: u64,
    /// Request IDs already received, for duplicate detection per §10.1.
    /// Bounded to MAX_REQUEST_IDS_PER_SESSION entries (~512 KB). Never
    /// remove entries: doing so would allow request ID reuse, which
    /// draft-18 §10.1 forbids.
    seen: HashSet<u64>,
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
                    expected_parity: peer_first_id & 1,
                    seen: HashSet::new(),
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

    /// Validate an incoming request ID from the peer.
    ///
    /// Draft-18 §10.1: checks correct parity and rejects duplicates.
    /// Does NOT enforce strict sequential order because bidi request
    /// streams can arrive concurrently in any order.
    pub fn validate_incoming(&self, id: u64) -> Result<(), SessionError> {
        let mut recv = self.inner.recv.lock().map_err(|_| SessionError::Internal)?;

        // Wrong parity (e.g. server sent an even ID).
        if (id & 1) != recv.expected_parity {
            return Err(SessionError::InvalidRequestId);
        }

        // Cap to prevent unbounded memory growth.
        if recv.seen.len() >= MAX_REQUEST_IDS_PER_SESSION {
            return Err(SessionError::TooManyRequests);
        }

        // Duplicate request ID.
        if !recv.seen.insert(id) {
            return Err(SessionError::InvalidRequestId);
        }

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
    fn rejects_wrong_parity() {
        let ids = server_ids();
        // Server expects even IDs from client peer; odd ID is rejected.
        assert!(matches!(
            ids.validate_incoming(1).unwrap_err(),
            SessionError::InvalidRequestId
        ));
    }

    #[test]
    fn accepts_out_of_order_ids() {
        // Draft-18: bidi streams can arrive in any order.
        let ids = server_ids();
        ids.validate_incoming(4).unwrap();
        ids.validate_incoming(0).unwrap();
        ids.validate_incoming(2).unwrap();
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

    #[test]
    fn rejects_at_seen_cap() {
        let ids = server_ids();
        // Fill to the cap with even IDs (peer is client).
        for i in 0..MAX_REQUEST_IDS_PER_SESSION {
            ids.validate_incoming((i as u64) * 2).unwrap();
        }
        // Next ID exceeds the cap.
        assert!(matches!(
            ids.validate_incoming((MAX_REQUEST_IDS_PER_SESSION as u64) * 2)
                .unwrap_err(),
            SessionError::TooManyRequests
        ));
    }
}
