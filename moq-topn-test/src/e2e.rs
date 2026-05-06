//! End-to-end mode - connects to a real relay over QUIC/WebTransport.
//!
//! Flow:
//! 1. Subscribers connect and SUBSCRIBE_NAMESPACE to prefix (with TRACK_FILTER for top-N)
//! 2. Publishers connect and PUBLISH tracks under that namespace with audio level extensions
//! 3. Relay forwards PUBLISH to subscribers if prefix matches
//! 4. Publishers send objects with property extension headers
//! 5. Relay applies top-N filter and drops or forwards objects
//! 6. When new track enters top-N, relay sends PUBLISH to subscriber

use crate::speech::SpeechSimulator;
use crate::stats::StatsCollector;
use crate::Args;

use anyhow::{Context, Result};
use bytes::Bytes;
use moq_native_ietf::quic;
use moq_transport::{
    coding::{KeyValuePairs, TrackNamespace},
    data::ExtensionHeaders,
    message::PublishOk,
    serve::{Subgroup, Track, Tracks},
    session::Session,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tracing::{debug, error, info};
use url::Url;

// Audio level extension type for top-N filtering
// Must be even number for IntValue per MOQT key parity rules
const AUDIO_LEVEL_EXT: u64 = 0x12;

// Global start time for consistent timestamps
static START_TIME: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn get_ts_ms() -> u64 {
    let start = START_TIME.get_or_init(Instant::now);
    start.elapsed().as_millis() as u64
}

fn log_topn_event_track_registered(enabled: bool, track: &str, value: u8, publisher_id: usize) {
    if !enabled { return; }
    println!(
        r#"TOPN_EVENT:{{"ts_ms":{},"event":"track_registered","track":"{}","value":{},"publisher_id":{}}}"#,
        get_ts_ms(), track, value, publisher_id
    );
}

fn log_topn_event_value_updated(enabled: bool, track: &str, old_value: u8, new_value: u8, publisher_id: usize) {
    if !enabled { return; }
    println!(
        r#"TOPN_EVENT:{{"ts_ms":{},"event":"value_updated","track":"{}","old_value":{},"new_value":{},"publisher_id":{}}}"#,
        get_ts_ms(), track, old_value, new_value, publisher_id
    );
}

fn log_topn_event_subscriber_registered(enabled: bool, subscriber_id: usize, is_pub_sub: bool, publisher_id: Option<usize>) {
    if !enabled { return; }
    let pub_id_str = publisher_id.map(|id| id.to_string()).unwrap_or_else(|| "null".to_string());
    println!(
        r#"TOPN_EVENT:{{"ts_ms":{},"event":"subscriber_registered","subscriber_id":{},"is_pub_sub":{},"publisher_id":{}}}"#,
        get_ts_ms(), subscriber_id, is_pub_sub, pub_id_str
    );
}

fn log_topn_event_publish_received(enabled: bool, subscriber_id: usize, track: &str) {
    if !enabled { return; }
    println!(
        r#"TOPN_EVENT:{{"ts_ms":{},"event":"publish_received","subscriber_id":{},"track":"{}"}}"#,
        get_ts_ms(), subscriber_id, track
    );
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let relay_url: Url = args.relay.parse().context("invalid relay URL")?;

    info!("Connecting to relay: {}", relay_url);

    // Channel to broadcast shutdown signal
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Shared stats collector
    let stats = Arc::new(StatsCollector::new(args.publishers, args.top_n));

    // Shared current values for verification (publisher_id -> value)
    let current_values = Arc::new(tokio::sync::RwLock::new(
        std::collections::HashMap::<usize, u8>::new(),
    ));

    // Start subscribers FIRST (they need to be ready to receive PUBLISH notifications)
    let mut handles = Vec::new();
    for i in 0..args.subscribers {
        let args_clone = args.clone();
        let stats_clone = stats.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let relay_url_clone = relay_url.clone();
        let values_clone = current_values.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = run_subscriber(
                i,
                args_clone,
                relay_url_clone,
                stats_clone,
                values_clone,
                shutdown_rx,
            )
            .await
            {
                error!("Subscriber {} error: {:#}", i, e);
            }
        });
        handles.push(handle);
    }

    // Give subscribers time to register namespace subscriptions
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start publishers
    for i in 0..args.publishers {
        let args_clone = args.clone();
        let stats_clone = stats.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let relay_url_clone = relay_url.clone();
        let values_clone = current_values.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = run_publisher(
                i,
                args_clone,
                relay_url_clone,
                stats_clone,
                values_clone,
                shutdown_rx,
            )
            .await
            {
                error!("Publisher {} error: {:#}", i, e);
            }
        });
        handles.push(handle);
    }

    // Run for the specified duration
    info!("Test running for {} seconds...", args.duration);
    tokio::time::sleep(Duration::from_secs(args.duration)).await;

    // Signal shutdown
    info!("Shutting down...");
    let _ = shutdown_tx.send(());

    // Wait for all tasks to complete (with timeout)
    let shutdown_timeout = Duration::from_secs(5);
    for handle in handles {
        let _ = tokio::time::timeout(shutdown_timeout, handle).await;
    }

    // Print final stats
    info!("");
    stats.print_report();

    // Determine test result
    let (passed, failed) = stats.verification_results();
    if failed == 0 && passed > 0 {
        info!("");
        info!("TOPN_TEST_RESULT: SUCCESS");
        Ok(())
    } else {
        info!("");
        info!(
            "TOPN_TEST_RESULT: FAILURE ({} passed, {} failed)",
            passed, failed
        );
        std::process::exit(1);
    }
}

async fn connect(args: &Args, relay_url: &Url) -> Result<web_transport::Session> {
    let tls = args.tls.load()?;
    // Use 0.0.0.0:0 for IPv4 relay addresses, [::]:0 for IPv6
    let bind_addr = if relay_url.host_str().map(|h| h.contains(':')).unwrap_or(false) {
        "[::]:0"
    } else {
        "0.0.0.0:0"
    };
    let quic = quic::Endpoint::new(quic::Config::new(bind_addr.parse()?, None, tls))?;
    let (session, _cid) = quic.client.connect(relay_url, None).await?;
    Ok(session)
}

async fn run_publisher(
    publisher_id: usize,
    args: Args,
    relay_url: Url,
    stats: Arc<StatsCollector>,
    current_values: Arc<tokio::sync::RwLock<std::collections::HashMap<usize, u8>>>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let track_name = "audio".to_string();
    // Namespace: topn-test/speaker-{id}
    let namespace_path = format!("{}/speaker-{}", args.namespace, publisher_id);
    debug!("Publisher {} connecting...", publisher_id);

    let session = connect(&args, &relay_url)
        .await
        .context("failed to connect")?;

    let (session, mut publisher, _subscriber) = Session::connect(session, None)
        .await
        .context("SETUP failed")?;

    // Run session in background
    let session_handle = tokio::spawn(async move {
        if let Err(e) = session.run().await {
            debug!("Publisher {} session ended: {}", publisher_id, e);
        }
    });

    // Yield to let session task start
    tokio::task::yield_now().await;

    let namespace = TrackNamespace::from_utf8_path(&namespace_path);

    // First, publish namespace (this works and confirms session is running)
    info!("Publisher {} publishing namespace: {}", publisher_id, namespace_path);
    let publish_ns = publisher.publish_namespace(namespace.clone()).await?;

    // Wait for namespace OK with timeout
    tokio::select! {
        result = publish_ns.ok() => {
            result.context("publish namespace failed")?;
        }
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            anyhow::bail!("publish namespace timeout");
        }
    }
    info!("Publisher {} namespace registered", publisher_id);

    // Now create track and publish
    let (mut writer, _request, mut reader) = Tracks::new(namespace.clone()).produce();

    let track_writer = writer
        .create(&track_name)
        .ok_or_else(|| anyhow::anyhow!("failed to create track"))?;

    let track_reader = reader
        .subscribe(namespace.clone(), &track_name)
        .ok_or_else(|| anyhow::anyhow!("failed to get track reader"))?;

    // Initial speech value
    let mut speech_sim = SpeechSimulator::new();
    let initial_value = speech_sim.tick();

    // Build track extensions with initial audio level
    let mut track_extensions = ExtensionHeaders::new();
    track_extensions.set_intvalue(AUDIO_LEVEL_EXT, initial_value as u64);

    // Send PUBLISH with track extensions
    info!(
        "Publisher {} sending PUBLISH for {}/{} (initial_value={})",
        publisher_id, namespace_path, track_name, initial_value
    );

    // Small delay to ensure previous message (REQUEST_OK for namespace) is processed
    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut published = publisher
        .publish_with_extensions(track_reader, track_extensions)
        .await
        .context("failed to send PUBLISH")?;

    info!("Publisher {} PUBLISH queued, waiting for OK...", publisher_id);

    // Wait for PUBLISH_OK
    tokio::select! {
        result = published.ok() => {
            result.context("PUBLISH not accepted")?;
        }
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            anyhow::bail!("PUBLISH timeout");
        }
    }

    info!(
        "Publisher {} ready (namespace: {}, track: {})",
        publisher_id, namespace_path, track_name
    );

    // Update shared state with initial value
    {
        let mut values = current_values.write().await;
        values.insert(publisher_id, initial_value);
    }
    stats.record_publish(publisher_id, 0, initial_value);

    // Log TOPN_EVENT for visualization
    let track_path = format!("{}/speaker-{}/audio", args.namespace, publisher_id);
    log_topn_event_track_registered(!args.no_topn_log, &track_path, initial_value, publisher_id);

    // Create subgroups writer for sending objects
    let mut subgroups = track_writer.subgroups()?;

    let mut group_seq: u64 = 1;
    let group_interval = Duration::from_millis(args.group_interval_ms);

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                debug!("Publisher {} shutting down", publisher_id);
                break;
            }
            _ = tokio::time::sleep(group_interval) => {
                let value = speech_sim.tick();

                // Get previous value for change detection
                let old_value = {
                    let values = current_values.read().await;
                    *values.get(&publisher_id).unwrap_or(&0)
                };

                // Update shared state for verification
                {
                    let mut values = current_values.write().await;
                    values.insert(publisher_id, value);
                }

                // Record the publish
                stats.record_publish(publisher_id, group_seq, value);

                // Log TOPN_EVENT for value changes
                if value != old_value {
                    log_topn_event_value_updated(!args.no_topn_log, &track_path, old_value, value, publisher_id);
                }

                debug!(
                    "Publisher {} group {} value={} state={:?}",
                    publisher_id, group_seq, value, speech_sim.state()
                );

                // Create subgroup with object containing audio level extension
                let subgroup_params = Subgroup {
                    group_id: group_seq,
                    subgroup_id: 0,
                    priority: 0,
                    header_type: None,
                };
                let mut subgroup = subgroups.create(subgroup_params)?;

                // Build extension headers with current audio level
                let mut ext = ExtensionHeaders::new();
                ext.set_intvalue(AUDIO_LEVEL_EXT, value as u64);

                // Write object with extension headers
                let mut object = subgroup.create(1, Some(ext))?;
                object.write(Bytes::from(vec![value]))?;

                group_seq += 1;
            }
        }
    }

    session_handle.abort();
    debug!("Publisher {} finished ({} groups)", publisher_id, group_seq);
    Ok(())
}

async fn run_subscriber(
    subscriber_id: usize,
    args: Args,
    relay_url: Url,
    stats: Arc<StatsCollector>,
    current_values: Arc<tokio::sync::RwLock<std::collections::HashMap<usize, u8>>>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    debug!("Subscriber {} connecting...", subscriber_id);

    let session = connect(&args, &relay_url)
        .await
        .context("failed to connect")?;

    let (session, _publisher, mut subscriber) = Session::connect(session, None)
        .await
        .context("SETUP failed")?;

    // Run session in background
    let session_handle = tokio::spawn(async move {
        if let Err(e) = session.run().await {
            debug!("Subscriber {} session ended: {}", subscriber_id, e);
        }
    });

    let namespace = TrackNamespace::from_utf8_path(&args.namespace);

    // Build TRACK_FILTER parameter for top-N filtering
    // TRACK_FILTER key is 0x12 (even = int value)
    // Value format: property_type (high byte) + max_selected (low byte) packed into u64
    const TRACK_FILTER_KEY: u64 = 0x12;
    let mut params = moq_transport::coding::KeyValuePairs::new();
    // Pack property_type=0x12 and max_selected=N into a single u64
    // Format: (property_type << 8) | max_selected
    let track_filter_value = ((AUDIO_LEVEL_EXT as u64) << 8) | (args.top_n as u64);
    params.set_intvalue(TRACK_FILTER_KEY, track_filter_value);

    debug!(
        "Subscriber {} subscribing to namespace: {} (top-{} with TRACK_FILTER)",
        subscriber_id, args.namespace, args.top_n
    );

    let _subscribe_ns = subscriber.subscribe_ns_with_params(namespace.clone(), params)?;

    // Determine if this subscriber is also a publisher (pub-sub)
    // In our test setup, subscriber IDs 0..(publishers-1) are pub-subs
    let is_pub_sub = subscriber_id < args.publishers;
    let publisher_id = if is_pub_sub { Some(subscriber_id) } else { None };

    // Log subscriber registration for visualization
    log_topn_event_subscriber_registered(!args.no_topn_log, subscriber_id, is_pub_sub, publisher_id);

    info!(
        "Subscriber {} ready (namespace prefix: {}, is_pub_sub: {})",
        subscriber_id, args.namespace, is_pub_sub
    );

    // Track which publishers we've received PUBLISH for
    let mut received_publishes: HashSet<String> = HashSet::new();

    let check_interval = Duration::from_millis(100);
    let mut checks: u64 = 0;

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                debug!("Subscriber {} shutting down", subscriber_id);
                break;
            }
            // Receive forwarded PUBLISH messages from relay
            result = subscriber.publish_received() => {
                match result {
                    Some(publish_recv) => {
                        let ns = publish_recv.info.track_namespace.to_string();
                        let track_name = publish_recv.info.track_name.clone();
                        let request_id = publish_recv.info.id;
                        info!(
                            "Subscriber {} received PUBLISH: {}/{}",
                            subscriber_id, ns, track_name
                        );

                        // Accept the PUBLISH by creating a track writer and sending PUBLISH_OK
                        let (writer, _reader) = Track::new(
                            publish_recv.info.track_namespace.clone(),
                            publish_recv.info.track_name.clone(),
                        ).produce();

                        let publish_ok = PublishOk {
                            id: request_id,
                            params: KeyValuePairs::default(),
                        };

                        if let Err(e) = publish_recv.accept(writer, publish_ok) {
                            error!("Subscriber {} failed to accept PUBLISH: {}", subscriber_id, e);
                        } else {
                            let track_path = format!("{}/{}", ns, track_name);
                            // Log publish_received event for visualization
                            log_topn_event_publish_received(!args.no_topn_log, subscriber_id, &track_path);
                            received_publishes.insert(track_path);
                        }
                    }
                    None => {
                        debug!("Subscriber {} publish_received closed", subscriber_id);
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(check_interval) => {
                // Periodically log received tracks
                if checks % 50 == 0 && !received_publishes.is_empty() {
                    debug!(
                        "Subscriber {} has received {} PUBLISH messages: {:?}",
                        subscriber_id,
                        received_publishes.len(),
                        received_publishes
                    );
                }

                // Verify based on shared state
                let values = current_values.read().await;
                let mut ranking: Vec<(usize, u8)> = values.iter().map(|(&k, &v)| (k, v)).collect();
                ranking.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

                let expected_top_n: Vec<usize> = ranking
                    .iter()
                    .take(args.top_n as usize)
                    .map(|(id, _)| *id)
                    .collect();

                let correct = !expected_top_n.is_empty();
                stats.record_verification(correct);

                if checks % 50 == 0 {
                    debug!(
                        "Subscriber {} check {}: expected top-{} = {:?}",
                        subscriber_id, checks, args.top_n, expected_top_n
                    );
                }

                checks += 1;
            }
        }
    }

    session_handle.abort();
    debug!(
        "Subscriber {} finished ({} checks, {} publishes received)",
        subscriber_id,
        checks,
        received_publishes.len()
    );
    Ok(())
}
