// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use cat_token::MoqtAction;
use moq_auth::AuthzOperation;
use moq_transport::coding::TrackNamespace;

/// Maps an AuthzOperation to the (MoqtAction, namespace_tuple, track) triple
/// expected by cat-token's MoqtAuthRequest.
pub(crate) fn map_operation(op: &AuthzOperation<'_>) -> (MoqtAction, Vec<Vec<u8>>, Vec<u8>) {
    match op {
        AuthzOperation::Publish { namespace, track } => {
            (MoqtAction::Publish, ns_to_tuple(namespace), track.to_vec())
        }
        AuthzOperation::PublishNamespace { namespace } => {
            (MoqtAction::PublishNamespace, ns_to_tuple(namespace), vec![])
        }
        AuthzOperation::PublishNamespaceDone { namespace } => {
            (MoqtAction::PublishNamespace, ns_to_tuple(namespace), vec![])
        }
        AuthzOperation::Subscribe { namespace, track } => {
            (MoqtAction::Subscribe, ns_to_tuple(namespace), track.to_vec())
        }
        AuthzOperation::SubscribeNamespace { prefix } => {
            (MoqtAction::SubscribeNamespace, ns_to_tuple(prefix), vec![])
        }
        AuthzOperation::Fetch { namespace, track } => {
            (MoqtAction::Fetch, ns_to_tuple(namespace), track.to_vec())
        }
        AuthzOperation::TrackStatus { namespace, track } => {
            (MoqtAction::TrackStatus, ns_to_tuple(namespace), track.to_vec())
        }
        AuthzOperation::RequestUpdate { .. } => (MoqtAction::RequestUpdate, vec![], vec![]),
        _ => (MoqtAction::ClientSetup, vec![], vec![]),
    }
}

fn ns_to_tuple(ns: &TrackNamespace) -> Vec<Vec<u8>> {
    ns.fields.iter().map(|f| f.value.clone()).collect()
}
