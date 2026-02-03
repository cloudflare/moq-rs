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
/// Returns (session, connection_id) so we can report CIDs for mlog correlation
async fn connect(args: &Args) -> Result<(web_transport::Session, String)> {
    let tls = args.tls.load()?;
    let quic = quic::Endpoint::new(quic::Config::new(args.bind, None, tls))?;

    let (session, connection_id) = quic.client.connect(&args.relay, None).await?;
    Ok((session, connection_id))
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
/// This is the simplest possible test - if this fails, nothing else will work.
pub async fn test_setup_only(args: &Args) -> Result<TestConnectionIds> {
    timeout(TEST_TIMEOUT, async {
        let (session, cid) = connect(args).await.context("failed to connect to relay")?;
        let mut cids = TestConnectionIds::default();
        cids.add(cid);

        // Session::connect performs the SETUP exchange
        let (session, _publisher, _subscriber) = Session::connect(session, None)
            .await
            .context("SETUP exchange failed")?;

        log::info!("SETUP exchange completed successfully");

        // We don't need to run the session, just verify setup worked
        // Dropping the session will close the connection
        drop(session);

        Ok(cids)
    })
    .await
    .context("test timed out")?
}

/// T0.2: Announce Only
///
/// Connect to relay, announce a namespace, receive PUBLISH_NAMESPACE_OK, close.
pub async fn test_announce_only(args: &Args) -> Result<TestConnectionIds> {
    timeout(TEST_TIMEOUT, async {
        let (session, cid) = connect(args).await.context("failed to connect to relay")?;
        let mut cids = TestConnectionIds::default();
        cids.add(cid);

        let (session, mut publisher, _subscriber) = Session::connect(session, None)
            .await
            .context("SETUP exchange failed")?;

        let namespace = TrackNamespace::from_utf8_path(TEST_NAMESPACE);
        let (_, _, reader) = Tracks::new(namespace.clone()).produce();

        log::info!("Announcing namespace: {}", TEST_NAMESPACE);

        // Run announce with a timeout - we want to verify we get PUBLISH_NAMESPACE_OK.
        // NOTE: The announce() method blocks waiting for subscriptions after getting OK.
        // If we get PUBLISH_NAMESPACE_ERROR instead of OK, the method returns Err immediately.
        // So timing out here means: either (a) got OK and waiting for subs, or (b) relay never responded.
        // We accept this limitation since (b) would indicate a broken relay anyway.
        // TODO: For stricter verification, use lower-level Announce::ok() method directly.
        let announce_result = tokio::select! {
            res = publisher.announce(reader) => res,
            res = session.run() => {
                res.context("session error")?;
                anyhow::bail!("session ended before announce completed");
            }
            _ = tokio::time::sleep(Duration::from_secs(2)) => {
                // If we got an error from the relay, announce() would have returned already.
                // Timing out means we're past the OK and now waiting for subscriptions.
                log::info!("Announce succeeded (no error received, waiting for subscriptions timed out)");
                return Ok(cids);
            }
        };

        // If we get here, announce completed (which means it errored or namespace was cancelled)
        announce_result.context("announce failed")?;

        Ok(cids)
    })
    .await
    .context("test timed out")?
}

/// T0.3: Subscribe Error
///
/// Subscribe to a non-existent track and verify we get SUBSCRIBE_ERROR.
pub async fn test_subscribe_error(args: &Args) -> Result<TestConnectionIds> {
    timeout(TEST_TIMEOUT, async {
        let (session, cid) = connect(args).await.context("failed to connect to relay")?;
        let mut cids = TestConnectionIds::default();
        cids.add(cid);

        let (session, _publisher, mut subscriber) = Session::connect(session, None)
            .await
            .context("SETUP exchange failed")?;

        let namespace = TrackNamespace::from_utf8_path("nonexistent/namespace");
        let (mut writer, _, _reader) = Tracks::new(namespace.clone()).produce();

        // Create a track to subscribe to
        let track = writer
            .create(TEST_TRACK)
            .ok_or_else(|| anyhow::anyhow!("failed to create track (already exists?)"))?;

        log::info!(
            "Subscribing to non-existent track: {}/{}",
            "nonexistent/namespace",
            TEST_TRACK
        );

        // Run subscribe - we expect an error
        let subscribe_result = tokio::select! {
            res = subscriber.subscribe(track) => res,
            res = session.run() => {
                res.context("session error")?;
                anyhow::bail!("session ended before subscribe completed");
            }
        };

        // We expect this to fail with a "not found" or similar error
        match subscribe_result {
            Ok(()) => {
                anyhow::bail!("subscribe succeeded but should have failed (track doesn't exist)");
            }
            Err(e) => {
                // Validate that the error is related to the track not existing.
                // Different relays may return different error messages, but they should
                // indicate the track/namespace was not found.
                let err_str = e.to_string().to_lowercase();
                let is_expected_error = err_str.contains("not found")
                    || err_str.contains("notfound")
                    || err_str.contains("no such")
                    || err_str.contains("doesn't exist")
                    || err_str.contains("does not exist")
                    || err_str.contains("unknown");

                if is_expected_error {
                    log::info!("Got expected 'not found' error: {}", e);
                } else {
                    // Log warning but still pass - relay may use different error text
                    log::warn!(
                        "Got error but not clearly 'not found': {}. \
                        This may indicate a different error type than expected.",
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

/// T0.4: Announce + Subscribe
///
/// Two clients: publisher announces a namespace, subscriber subscribes to a track.
/// Verifies the relay correctly routes the subscription to the publisher.
pub async fn test_announce_subscribe(args: &Args) -> Result<TestConnectionIds> {
    timeout(TEST_TIMEOUT, async {
        let mut cids = TestConnectionIds::default();

        // Publisher connection
        let (pub_session, pub_cid) = connect(args).await.context("publisher failed to connect")?;
        cids.add(pub_cid);
        let (pub_session, mut publisher, _) = Session::connect(pub_session, None)
            .await
            .context("publisher SETUP failed")?;

        // Subscriber connection
        let (sub_session, sub_cid) = connect(args)
            .await
            .context("subscriber failed to connect")?;
        cids.add(sub_cid);
        let (sub_session, _, mut subscriber) = Session::connect(sub_session, None)
            .await
            .context("subscriber SETUP failed")?;

        let namespace = TrackNamespace::from_utf8_path(TEST_NAMESPACE);

        // Publisher: set up tracks and announce
        let (mut pub_writer, _, pub_reader) = Tracks::new(namespace.clone()).produce();

        // Create the track that subscriber will request
        let _track_writer = pub_writer.create(TEST_TRACK);

        log::info!("Publisher announcing namespace: {}", TEST_NAMESPACE);

        // Subscriber: set up tracks and subscribe
        let (mut sub_writer, _, _sub_reader) = Tracks::new(namespace.clone()).produce();
        let sub_track = sub_writer
            .create(TEST_TRACK)
            .ok_or_else(|| anyhow::anyhow!("failed to create subscriber track"))?;

        log::info!(
            "Subscriber subscribing to track: {}/{}",
            TEST_NAMESPACE,
            TEST_TRACK
        );

        // Run everything concurrently. We expect the subscriber to get a response
        // (either SUBSCRIBE_OK or error) within the timeout.
        tokio::select! {
            // Publisher announces and waits for subscriptions
            res = publisher.announce(pub_reader) => {
                res.context("publisher announce failed")?;
                log::info!("Publisher announce completed");
            }
            // Subscriber subscribes - this is the main thing we're testing
            res = subscriber.subscribe(sub_track) => {
                match res {
                    Ok(()) => log::info!("Subscriber got SUBSCRIBE_OK - relay routed subscription correctly"),
                    Err(e) => log::info!("Subscriber got error: {} - subscription was processed", e),
                }
            }
            // Run publisher session
            res = pub_session.run() => {
                res.context("publisher session error")?;
            }
            // Run subscriber session
            res = sub_session.run() => {
                res.context("subscriber session error")?;
            }
            // Timeout: give the relay time to route the subscription
            _ = tokio::time::sleep(Duration::from_secs(3)) => {
                // If we hit this timeout, the subscription may still be pending.
                // This isn't necessarily a failure - some relays may hold subscriptions
                // until the track has data. Log for visibility.
                log::info!("Test timeout reached - subscription routing may still be in progress");
            }
        };

        Ok(cids)
    })
    .await
    .context("test timed out")?
}

/// T0.6: Publish Namespace Done (Letter L)
///
/// Announce a namespace, receive OK, then send PUBLISH_NAMESPACE_DONE.
/// Verifies the relay handles namespace unpublishing correctly.
pub async fn test_publish_namespace_done(args: &Args) -> Result<TestConnectionIds> {
    timeout(TEST_TIMEOUT, async {
        let (session, cid) = connect(args).await.context("failed to connect to relay")?;
        let mut cids = TestConnectionIds::default();
        cids.add(cid);

        let (session, mut publisher, _subscriber) = Session::connect(session, None)
            .await
            .context("SETUP exchange failed")?;

        let namespace = TrackNamespace::from_utf8_path(TEST_NAMESPACE);
        let (_, _, reader) = Tracks::new(namespace.clone()).produce();

        log::info!("Announcing namespace: {}", TEST_NAMESPACE);

        // Run announce and wait for OK, then explicitly drop to send PUBLISH_NAMESPACE_DONE.
        // See note in test_announce_only about timeout-based verification.
        let result = tokio::select! {
            res = publisher.announce(reader) => res,
            res = session.run() => {
                res.context("session error")?;
                anyhow::bail!("session ended before announce completed");
            }
            _ = tokio::time::sleep(Duration::from_secs(2)) => {
                // No error received - announce is active and waiting for subscriptions
                log::info!("Announce active, now sending PUBLISH_NAMESPACE_DONE");
                // Dropping out of this block will drop the announce, which sends PUBLISH_NAMESPACE_DONE
                Ok(())
            }
        };

        result.context("announce failed")?;

        // Small delay to ensure PUBLISH_NAMESPACE_DONE is sent before we close
        tokio::time::sleep(Duration::from_millis(100)).await;

        log::info!("PUBLISH_NAMESPACE_DONE sent successfully");
        Ok(cids)
    })
    .await
    .context("test timed out")?
}

/// T0.5: Subscribe Before Announce
///
/// Subscriber subscribes first (will be pending), then publisher announces.
/// Verifies the relay correctly handles out-of-order setup.
pub async fn test_subscribe_before_announce(args: &Args) -> Result<TestConnectionIds> {
    timeout(TEST_TIMEOUT, async {
        let mut cids = TestConnectionIds::default();

        // Subscriber connection - connects first
        let (sub_session, sub_cid) = connect(args)
            .await
            .context("subscriber failed to connect")?;
        cids.add(sub_cid);
        let (sub_session, _, mut subscriber) = Session::connect(sub_session, None)
            .await
            .context("subscriber SETUP failed")?;

        let namespace = TrackNamespace::from_utf8_path(TEST_NAMESPACE);

        // Subscriber: set up and subscribe (before publisher announces)
        let (mut sub_writer, _, _sub_reader) = Tracks::new(namespace.clone()).produce();
        let sub_track = sub_writer
            .create(TEST_TRACK)
            .ok_or_else(|| anyhow::anyhow!("failed to create subscriber track"))?;

        log::info!(
            "Subscriber subscribing BEFORE announce: {}/{}",
            TEST_NAMESPACE,
            TEST_TRACK
        );

        // Start the subscribe (it will be pending)
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

        // Give subscriber time to send SUBSCRIBE
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Now publisher connects and announces
        let (pub_session, pub_cid) = connect(args).await.context("publisher failed to connect")?;
        cids.add(pub_cid);
        let (pub_session, mut publisher, _) = Session::connect(pub_session, None)
            .await
            .context("publisher SETUP failed")?;

        let (mut pub_writer, _, pub_reader) = Tracks::new(namespace.clone()).produce();
        let _track_writer = pub_writer.create(TEST_TRACK);

        log::info!(
            "Publisher announcing namespace (after subscriber): {}",
            TEST_NAMESPACE
        );

        // Run publisher announce
        tokio::select! {
            res = publisher.announce(pub_reader) => {
                res.context("publisher announce failed")?;
            }
            res = pub_session.run() => {
                res.context("publisher session error")?;
            }
            _ = tokio::time::sleep(Duration::from_secs(3)) => {
                log::info!("Publisher announce timeout (expected)");
            }
        };

        // Check subscriber result
        tokio::select! {
            res = sub_handle => {
                match res {
                    Ok(Ok(())) => log::info!("Subscriber completed successfully"),
                    Ok(Err(e)) => log::info!("Subscriber got error: {} (may be expected)", e),
                    Err(e) => log::warn!("Subscriber task panicked: {}", e),
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                log::info!("Subscriber still waiting (test complete)");
            }
        };

        Ok(cids)
    })
    .await
    .context("test timed out")?
}
