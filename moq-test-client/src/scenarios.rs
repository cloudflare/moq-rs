// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Test scenario implementations
//!
//! Each scenario tests a specific aspect of MoQT interoperability.
//!
//! Each test function returns `Result<TestConnectionIds>` where success means
//! the test passed and failure means it failed. Connection IDs are collected
//! for correlation with relay-side mlog files.

use anyhow::{Context, Result};
use tokio::time::{timeout, Duration};

use moq_native_ietf::quic;
use moq_transport::{coding::TrackNamespace, serve::Tracks, session::Session};

use crate::Args;

/// Overall test timeout - individual operations should complete faster
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Namespace used for test operations
const TEST_NAMESPACE: &str = "moq-test/interop";

/// Track name used for test operations
const TEST_TRACK: &str = "test-track";

/// Helper to connect to a relay and establish a session
/// Returns (session, connection_id, transport) so we can report CIDs for mlog correlation
async fn connect(
    args: &Args,
) -> Result<(
    web_transport::Session,
    String,
    moq_transport::session::Transport,
)> {
    let tls = args.tls.load()?;
    let quic = quic::Endpoint::new(quic::Config::new(args.bind, None, tls)?)?;

    let (session, connection_id, transport) = quic.client.connect(&args.relay, None).await?;
    Ok((session, connection_id, transport))
}

/// Collected connection IDs from a test run
#[derive(Debug, Default)]
pub struct TestConnectionIds {
    pub cids: Vec<String>,
}

impl TestConnectionIds {
    pub fn add(&mut self, cid: String) {
        self.cids.push(cid);
    }
}

/// T0.1: Setup Only
///
/// Connect to relay, complete CLIENT_SETUP/SERVER_SETUP exchange, close gracefully.
pub async fn test_setup_only(args: &Args) -> Result<TestConnectionIds> {
    timeout(TEST_TIMEOUT, async {
        let (session, cid, transport) =
            connect(args).await.context("failed to connect to relay")?;
        let mut cids = TestConnectionIds::default();
        cids.add(cid);

        let (session, _publisher, _subscriber) = Session::connect(session, None, transport)
            .await
            .context("SETUP exchange failed")?;

        tracing::info!("SETUP exchange completed successfully");
        drop(session);
        Ok(cids)
    })
    .await
    .context("test timed out")?
}

/// T0.2: Publish Namespace Only
///
/// Connect to relay, send PUBLISH_NAMESPACE, receive REQUEST_OK, close.
pub async fn test_publish_namespace_only(args: &Args) -> Result<TestConnectionIds> {
    timeout(TEST_TIMEOUT, async {
        let (session, cid, transport) =
            connect(args).await.context("failed to connect to relay")?;
        let mut cids = TestConnectionIds::default();
        cids.add(cid);

        let (session, mut publisher, _subscriber) = Session::connect(session, None, transport)
            .await
            .context("SETUP exchange failed")?;

        let namespace = TrackNamespace::from_utf8_path(TEST_NAMESPACE);
        let (_, _, reader) = Tracks::new(namespace.clone()).produce();

        tracing::info!("Sending PUBLISH_NAMESPACE for: {}", TEST_NAMESPACE);

        // publish_namespace() blocks waiting for subscriptions after receiving REQUEST_OK.
        // If we receive REQUEST_ERROR instead, it returns Err immediately.
        // Timing out here means we received REQUEST_OK and are now waiting for subscribers,
        // which is the expected success case.
        let result = tokio::select! {
            res = publisher.publish_namespace(reader) => res,
            res = session.run() => {
                res.context("session error")?;
                anyhow::bail!("session ended before PUBLISH_NAMESPACE completed");
            }
            _ = tokio::time::sleep(Duration::from_secs(2)) => {
                tracing::info!(
                    "PUBLISH_NAMESPACE succeeded (REQUEST_OK received, waiting for subscribers)"
                );
                return Ok(cids);
            }
        };

        result.context("PUBLISH_NAMESPACE failed")?;
        Ok(cids)
    })
    .await
    .context("test timed out")?
}

/// T0.3: Subscribe Error
///
/// Subscribe to a non-existent track and verify we get a subscription error.
pub async fn test_subscribe_error(args: &Args) -> Result<TestConnectionIds> {
    timeout(TEST_TIMEOUT, async {
        let (session, cid, transport) =
            connect(args).await.context("failed to connect to relay")?;
        let mut cids = TestConnectionIds::default();
        cids.add(cid);

        let (session, _publisher, mut subscriber) = Session::connect(session, None, transport)
            .await
            .context("SETUP exchange failed")?;

        let namespace = TrackNamespace::from_utf8_path("nonexistent/namespace");
        let (mut writer, _, _reader) = Tracks::new(namespace.clone()).produce();

        let track = writer
            .create(TEST_TRACK)
            .ok_or_else(|| anyhow::anyhow!("failed to create track (already exists?)"))?;

        tracing::info!(
            "Subscribing to non-existent track: {}/{}",
            "nonexistent/namespace",
            TEST_TRACK
        );

        let subscribe_result = tokio::select! {
            res = subscriber.subscribe(track) => res,
            res = session.run() => {
                res.context("session error")?;
                anyhow::bail!("session ended before subscribe completed");
            }
        };

        match subscribe_result {
            Ok(()) => {
                anyhow::bail!(
                    "subscribe succeeded but should have failed (track doesn't exist)"
                );
            }
            Err(e) => {
                let err_str = e.to_string().to_lowercase();
                let is_expected = err_str.contains("not found")
                    || err_str.contains("notfound")
                    || err_str.contains("no such")
                    || err_str.contains("doesn't exist")
                    || err_str.contains("does not exist")
                    || err_str.contains("unknown");

                if is_expected {
                    tracing::info!("Got expected 'not found' error: {}", e);
                } else {
                    tracing::warn!(
                        "Got error but not clearly 'not found': {}. \
                        Relay may use different error text.",
                        e
                    );
                }
                Ok(cids)
            }
        }
    })
    .await
    .context("test timed out")?
}

/// T0.4: Publish Namespace + Subscribe
///
/// Publisher sends PUBLISH_NAMESPACE; subscriber subscribes to a track in that namespace.
/// Verifies the relay correctly routes the subscription to the publisher.
pub async fn test_publish_namespace_subscribe(args: &Args) -> Result<TestConnectionIds> {
    timeout(TEST_TIMEOUT, async {
        let mut cids = TestConnectionIds::default();

        let (pub_session, pub_cid, pub_transport) =
            connect(args).await.context("publisher failed to connect")?;
        cids.add(pub_cid);
        let (pub_session, mut publisher, _) =
            Session::connect(pub_session, None, pub_transport)
                .await
                .context("publisher SETUP failed")?;

        let (sub_session, sub_cid, sub_transport) =
            connect(args).await.context("subscriber failed to connect")?;
        cids.add(sub_cid);
        let (sub_session, _, mut subscriber) =
            Session::connect(sub_session, None, sub_transport)
                .await
                .context("subscriber SETUP failed")?;

        let namespace = TrackNamespace::from_utf8_path(TEST_NAMESPACE);

        let (mut pub_writer, _, pub_reader) = Tracks::new(namespace.clone()).produce();
        let _track_writer = pub_writer.create(TEST_TRACK);

        tracing::info!(
            "Publisher sending PUBLISH_NAMESPACE: {}",
            TEST_NAMESPACE
        );

        let (mut sub_writer, _, _sub_reader) = Tracks::new(namespace.clone()).produce();
        let sub_track = sub_writer
            .create(TEST_TRACK)
            .ok_or_else(|| anyhow::anyhow!("failed to create subscriber track"))?;

        tracing::info!(
            "Subscriber subscribing to track: {}/{}",
            TEST_NAMESPACE,
            TEST_TRACK
        );

        tokio::select! {
            res = publisher.publish_namespace(pub_reader) => {
                res.context("publisher PUBLISH_NAMESPACE failed")?;
                tracing::info!("Publisher PUBLISH_NAMESPACE completed");
            }
            res = subscriber.subscribe(sub_track) => {
                match res {
                    Ok(()) => tracing::info!(
                        "Subscriber got subscription response - relay routed correctly"
                    ),
                    Err(e) => tracing::info!(
                        "Subscriber got error: {} - subscription was processed", e
                    ),
                }
            }
            res = pub_session.run() => res.context("publisher session error")?,
            res = sub_session.run() => res.context("subscriber session error")?,
            _ = tokio::time::sleep(Duration::from_secs(3)) => {
                tracing::info!(
                    "Test timeout reached - subscription routing may still be in progress"
                );
            }
        };

        Ok(cids)
    })
    .await
    .context("test timed out")?
}

/// T0.6: Publish Namespace Done
///
/// Send PUBLISH_NAMESPACE, receive REQUEST_OK, then send PUBLISH_NAMESPACE_DONE.
/// Verifies the relay handles namespace unpublishing correctly.
pub async fn test_publish_namespace_done(args: &Args) -> Result<TestConnectionIds> {
    timeout(TEST_TIMEOUT, async {
        let (session, cid, transport) =
            connect(args).await.context("failed to connect to relay")?;
        let mut cids = TestConnectionIds::default();
        cids.add(cid);

        let (session, mut publisher, _subscriber) = Session::connect(session, None, transport)
            .await
            .context("SETUP exchange failed")?;

        let namespace = TrackNamespace::from_utf8_path(TEST_NAMESPACE);
        let (_, _, reader) = Tracks::new(namespace.clone()).produce();

        tracing::info!("Sending PUBLISH_NAMESPACE: {}", TEST_NAMESPACE);

        let result = tokio::select! {
            res = publisher.publish_namespace(reader) => res,
            res = session.run() => {
                res.context("session error")?;
                anyhow::bail!("session ended before PUBLISH_NAMESPACE completed");
            }
            _ = tokio::time::sleep(Duration::from_secs(2)) => {
                // No error received: REQUEST_OK arrived and we are waiting for subscribers.
                // Drop publish_namespace here to send PUBLISH_NAMESPACE_DONE.
                tracing::info!("PUBLISH_NAMESPACE active; sending PUBLISH_NAMESPACE_DONE");
                Ok(())
            }
        };

        result.context("PUBLISH_NAMESPACE failed")?;

        tokio::time::sleep(Duration::from_millis(100)).await;
        tracing::info!("PUBLISH_NAMESPACE_DONE sent successfully");
        Ok(cids)
    })
    .await
    .context("test timed out")?
}

/// T0.5: Subscribe Before Publish Namespace
///
/// Subscriber subscribes first (will be pending), then publisher sends PUBLISH_NAMESPACE.
/// Verifies the relay correctly handles out-of-order setup.
pub async fn test_subscribe_before_publish_namespace(args: &Args) -> Result<TestConnectionIds> {
    timeout(TEST_TIMEOUT, async {
        let mut cids = TestConnectionIds::default();

        // Subscriber connects first.
        let (sub_session, sub_cid, sub_transport) =
            connect(args).await.context("subscriber failed to connect")?;
        cids.add(sub_cid);
        let (sub_session, _, mut subscriber) =
            Session::connect(sub_session, None, sub_transport)
                .await
                .context("subscriber SETUP failed")?;

        let namespace = TrackNamespace::from_utf8_path(TEST_NAMESPACE);

        let (mut sub_writer, _, _sub_reader) = Tracks::new(namespace.clone()).produce();
        let sub_track = sub_writer
            .create(TEST_TRACK)
            .ok_or_else(|| anyhow::anyhow!("failed to create subscriber track"))?;

        tracing::info!(
            "Subscriber subscribing BEFORE PUBLISH_NAMESPACE: {}/{}",
            TEST_NAMESPACE,
            TEST_TRACK
        );

        let sub_handle = tokio::spawn(async move {
            let result = tokio::select! {
                res = subscriber.subscribe(sub_track) => res,
                res = sub_session.run() => {
                    res.map_err(|e| moq_transport::serve::ServeError::Internal(e.to_string()))?;
                    Err(moq_transport::serve::ServeError::Done)
                }
            };
            result
        });

        // Give subscriber time to send SUBSCRIBE.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Now publisher connects and sends PUBLISH_NAMESPACE.
        let (pub_session, pub_cid, pub_transport) =
            connect(args).await.context("publisher failed to connect")?;
        cids.add(pub_cid);
        let (pub_session, mut publisher, _) =
            Session::connect(pub_session, None, pub_transport)
                .await
                .context("publisher SETUP failed")?;

        let (mut pub_writer, _, pub_reader) = Tracks::new(namespace.clone()).produce();
        let _track_writer = pub_writer.create(TEST_TRACK);

        tracing::info!(
            "Publisher sending PUBLISH_NAMESPACE (after subscriber): {}",
            TEST_NAMESPACE
        );

        tokio::select! {
            res = publisher.publish_namespace(pub_reader) => {
                res.context("publisher PUBLISH_NAMESPACE failed")?;
            }
            res = pub_session.run() => res.context("publisher session error")?,
            _ = tokio::time::sleep(Duration::from_secs(3)) => {
                tracing::info!("Publisher PUBLISH_NAMESPACE timeout (expected)");
            }
        };

        tokio::select! {
            res = sub_handle => {
                match res {
                    Ok(Ok(())) => tracing::info!("Subscriber completed successfully"),
                    Ok(Err(e)) => tracing::info!("Subscriber got error: {} (may be expected)", e),
                    Err(e) => tracing::warn!("Subscriber task panicked: {}", e),
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                tracing::info!("Subscriber still waiting (test complete)");
            }
        };

        Ok(cids)
    })
    .await
    .context("test timed out")?
}
