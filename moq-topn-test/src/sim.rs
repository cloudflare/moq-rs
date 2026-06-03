//! Simulation mode - tests TopNTracker directly without network.

use crate::speech::SpeechSimulator;
use crate::stats::StatsCollector;
use crate::viz::SharedTimelineRecorder;
use crate::Args;

use moq_relay_ietf::{TieBreakPolicy, TopNTracker, TopNTrackerConfig};
use moq_transport::coding::TrackNamespace;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

pub async fn run(args: Args) -> anyhow::Result<()> {
    // Create tracker config
    let tie_break_policy = match args.tie_break.as_str() {
        "recent" | "most-recent" => TieBreakPolicy::MostRecentWins,
        _ => TieBreakPolicy::OldestWins,
    };
    let staleness_timeout = if args.staleness_timeout == 0 {
        None
    } else {
        Some(Duration::from_secs(args.staleness_timeout))
    };

    let config = TopNTrackerConfig {
        tie_break_policy,
        staleness_timeout,
        enable_event_logging: false,
    };

    // Create the tracker
    let tracker = Arc::new(TopNTracker::with_config(0x01, config));
    tracker.update_max_n(args.top_n);

    // Register all publisher tracks
    let namespace = TrackNamespace::from_utf8_path(&args.namespace);
    for i in 0..args.publishers {
        let track_name = format!("speaker-{}", i);
        tracker.register_track(
            namespace.clone(),
            track_name,
            0, // Initial value = silent
            i as u64,
        );
    }

    info!("Registered {} publisher tracks", args.publishers);

    // Create timeline recorder for visualization
    let timeline_recorder = Arc::new(SharedTimelineRecorder::new(
        args.publishers,
        args.subscribers,
        args.top_n,
    ));
    let start_instant = Instant::now();

    // Channel to broadcast shutdown signal
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Shared stats collector
    let stats = Arc::new(StatsCollector::new(args.publishers, args.top_n));

    // Start publisher simulators
    let mut handles = Vec::new();
    for i in 0..args.publishers {
        let args_clone = args.clone();
        let tracker_clone = tracker.clone();
        let stats_clone = stats.clone();
        let namespace_clone = namespace.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let recorder_clone = timeline_recorder.clone();
        let start_instant_clone = start_instant;

        let handle = tokio::spawn(async move {
            run_publisher(
                i,
                args_clone,
                tracker_clone,
                namespace_clone,
                stats_clone,
                recorder_clone,
                start_instant_clone,
                shutdown_rx,
            )
            .await
        });
        handles.push(handle);
    }

    // Start subscriber verifiers
    for i in 0..args.subscribers {
        let args_clone = args.clone();
        let tracker_clone = tracker.clone();
        let stats_clone = stats.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let recorder_clone = timeline_recorder.clone();
        let start_instant_clone = start_instant;

        let handle = tokio::spawn(async move {
            run_subscriber(
                i,
                args_clone,
                tracker_clone,
                stats_clone,
                recorder_clone,
                start_instant_clone,
                shutdown_rx,
            )
            .await
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

    // Generate visualization if requested
    if let Some(ref output_path) = args.viz_output {
        info!("Generating timeline visualization: {}", output_path);
        if let Err(e) = timeline_recorder.generate_svg(output_path) {
            tracing::error!("Failed to generate visualization: {}", e);
        } else {
            info!("Timeline visualization saved to: {}", output_path);
        }
    }

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

async fn run_publisher(
    publisher_id: usize,
    args: Args,
    tracker: Arc<TopNTracker>,
    namespace: TrackNamespace,
    stats: Arc<StatsCollector>,
    timeline_recorder: Arc<SharedTimelineRecorder>,
    start_instant: Instant,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let track_name = format!("speaker-{}", publisher_id);
    debug!("Publisher {} starting (track: {})", publisher_id, track_name);

    let mut speech_sim = SpeechSimulator::new();
    let mut group_seq: u64 = 0;
    let group_interval = Duration::from_millis(args.group_interval_ms);
    let mut last_value: Option<u8> = None;

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                debug!("Publisher {} shutting down", publisher_id);
                break;
            }
            _ = tokio::time::sleep(group_interval) => {
                // Get current speech value
                let value = speech_sim.tick();

                // Update the tracker
                tracker.update_value(&namespace, &track_name, value as u64);

                // Record the publish
                stats.record_publish(publisher_id, group_seq, value);

                // Record to timeline if value changed
                if last_value != Some(value) {
                    let timestamp_ms = start_instant.elapsed().as_millis() as u64;
                    timeline_recorder.record_speech_value(publisher_id, value, timestamp_ms);
                    last_value = Some(value);
                }

                debug!(
                    "Publisher {} group {} value={} state={:?}",
                    publisher_id, group_seq, value, speech_sim.state()
                );

                group_seq += 1;
            }
        }
    }

    debug!("Publisher {} finished ({} groups)", publisher_id, group_seq);
}

async fn run_subscriber(
    subscriber_id: usize,
    args: Args,
    tracker: Arc<TopNTracker>,
    stats: Arc<StatsCollector>,
    timeline_recorder: Arc<SharedTimelineRecorder>,
    start_instant: Instant,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    debug!(
        "Subscriber {} starting (top-N filter: {})",
        subscriber_id, args.top_n
    );

    // For self-exclusion demo: some subscribers are also publishers
    // Subscriber 0 is also Publisher 0, Subscriber 1 is also Publisher 1, etc.
    // (only for subscribers where subscriber_id < num_publishers)
    let is_also_publisher = subscriber_id < args.publishers;
    let self_publisher_id = if is_also_publisher {
        Some(subscriber_id)
    } else {
        None
    };

    // Use publisher's session_id if this subscriber is also a publisher (for self-exclusion)
    let subscriber_session_id = if is_also_publisher {
        subscriber_id as u64
    } else {
        1_000_000 + subscriber_id as u64
    };

    let check_interval = Duration::from_millis(500);
    let mut checks: u64 = 0;
    let mut last_selection: Option<Vec<usize>> = None;

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                debug!("Subscriber {} shutting down", subscriber_id);
                break;
            }
            _ = tokio::time::sleep(check_interval) => {
                // Get top-N from tracker (with self-exclusion if applicable)
                let top_n = tracker.compute_top_n_for_session(subscriber_session_id, args.top_n);

                // Get snapshot and verify
                let snapshot = tracker.load_snapshot();

                // Extract received track IDs
                let received_ids: Vec<usize> = top_n
                    .iter()
                    .filter_map(|(_, track_name)| {
                        track_name.strip_prefix("speaker-")
                            .and_then(|s| s.parse::<usize>().ok())
                    })
                    .collect();

                // Get expected top-N (excluding self if applicable)
                let mut expected: Vec<(usize, u64)> = snapshot
                    .iter()
                    .filter_map(|t| {
                        t.track_name
                            .strip_prefix("speaker-")
                            .and_then(|s| s.parse::<usize>().ok())
                            .map(|id| (id, t.property_value))
                    })
                    .filter(|(id, _)| self_publisher_id != Some(*id))
                    .collect();
                expected.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                expected.truncate(args.top_n as usize);
                let expected_ids: Vec<usize> = expected.iter().map(|(id, _)| *id).collect();

                let correct = received_ids == expected_ids;
                stats.record_verification(correct);

                // Record to timeline if selection changed
                if last_selection.as_ref() != Some(&received_ids) {
                    let timestamp_ms = start_instant.elapsed().as_millis() as u64;
                    timeline_recorder.record_top_n_selection(
                        subscriber_id,
                        received_ids.clone(),
                        timestamp_ms,
                        self_publisher_id,
                    );
                    last_selection = Some(received_ids.clone());
                }

                if !correct && args.verbose {
                    warn!(
                        "Subscriber {} verification mismatch: got {:?}, expected {:?}{}",
                        subscriber_id, received_ids, expected_ids,
                        if is_also_publisher { " (self-excluded)" } else { "" }
                    );
                }

                debug!(
                    "Subscriber {} check {}: top-{} = {:?} (correct={}){}",
                    subscriber_id, checks, args.top_n, received_ids, correct,
                    if is_also_publisher { format!(" [self=P{}]", subscriber_id) } else { String::new() }
                );

                checks += 1;
            }
        }
    }

    debug!("Subscriber {} finished ({} checks)", subscriber_id, checks);
}
