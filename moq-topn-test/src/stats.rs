//! Statistics collection and reporting for Top-N test.

use hdrhistogram::Histogram;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;
use tracing::info;

/// Collected statistics for the test run
pub struct StatsCollector {
    /// Number of publishers
    num_publishers: usize,
    /// Top-N value
    top_n: u8,

    /// Total groups published (per publisher)
    groups_published: Vec<AtomicU64>,
    /// Total groups received (per subscriber)
    groups_received: AtomicU64,

    /// Ranking change latency histogram (microseconds)
    ranking_latency: Mutex<Histogram<u64>>,

    /// Current ranking state (publisher_id -> current_value)
    current_values: Mutex<HashMap<usize, u8>>,
    /// Timestamps of value changes for latency measurement
    value_change_times: Mutex<HashMap<(usize, u64), Instant>>,

    /// Verification results
    correct_deliveries: AtomicUsize,
    incorrect_deliveries: AtomicUsize,

    /// Throughput tracking
    start_time: Instant,
}

impl StatsCollector {
    pub fn new(num_publishers: usize, top_n: u8) -> Self {
        let mut groups_published = Vec::with_capacity(num_publishers);
        for _ in 0..num_publishers {
            groups_published.push(AtomicU64::new(0));
        }

        Self {
            num_publishers,
            top_n,
            groups_published,
            groups_received: AtomicU64::new(0),
            ranking_latency: Mutex::new(Histogram::new(3).unwrap()),
            current_values: Mutex::new(HashMap::new()),
            value_change_times: Mutex::new(HashMap::new()),
            correct_deliveries: AtomicUsize::new(0),
            incorrect_deliveries: AtomicUsize::new(0),
            start_time: Instant::now(),
        }
    }

    /// Record a group published by a publisher
    pub fn record_publish(&self, publisher_id: usize, group_seq: u64, value: u8) {
        if publisher_id < self.groups_published.len() {
            self.groups_published[publisher_id].fetch_add(1, Ordering::Relaxed);
        }

        // Track value changes for latency measurement
        let mut values = self.current_values.lock().unwrap();
        let prev_value = values.get(&publisher_id).copied();

        if prev_value != Some(value) {
            values.insert(publisher_id, value);

            // Record timestamp of this change
            let mut times = self.value_change_times.lock().unwrap();
            times.insert((publisher_id, group_seq), Instant::now());
        }
    }

    /// Record a group received by a subscriber and verify correctness
    pub fn record_receive(
        &self,
        _subscriber_id: usize,
        publisher_id: usize,
        group_seq: u64,
        value: u8,
        expected_in_top_n: bool,
    ) {
        self.groups_received.fetch_add(1, Ordering::Relaxed);

        // Check latency if this was a value change
        let times = self.value_change_times.lock().unwrap();
        if let Some(send_time) = times.get(&(publisher_id, group_seq)) {
            let latency_us = send_time.elapsed().as_micros() as u64;
            if let Ok(mut hist) = self.ranking_latency.lock() {
                let _ = hist.record(latency_us.min(u64::MAX - 1));
            }
        }

        // Verify correctness
        if expected_in_top_n {
            self.correct_deliveries.fetch_add(1, Ordering::Relaxed);
        } else {
            self.incorrect_deliveries.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a verification result directly
    pub fn record_verification(&self, correct: bool) {
        if correct {
            self.correct_deliveries.fetch_add(1, Ordering::Relaxed);
        } else {
            self.incorrect_deliveries.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get current ranking based on values (for verification)
    pub fn get_current_ranking(&self) -> Vec<(usize, u8)> {
        let values = self.current_values.lock().unwrap();
        let mut ranking: Vec<_> = values.iter().map(|(&k, &v)| (k, v)).collect();
        // Sort by value descending, then by publisher_id ascending for ties
        ranking.sort_by(|a, b| {
            b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0))
        });
        ranking
    }

    /// Get the top-N publisher IDs based on current values
    pub fn get_top_n_publishers(&self) -> Vec<usize> {
        let ranking = self.get_current_ranking();
        ranking.iter()
            .take(self.top_n as usize)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get verification results (passed, failed)
    pub fn verification_results(&self) -> (usize, usize) {
        (
            self.correct_deliveries.load(Ordering::Relaxed),
            self.incorrect_deliveries.load(Ordering::Relaxed),
        )
    }

    /// Print the final report
    pub fn print_report(&self) {
        let elapsed = self.start_time.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        info!("=== Test Results ===");
        info!("");

        // Throughput
        let total_published: u64 = self.groups_published
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum();
        let total_received = self.groups_received.load(Ordering::Relaxed);

        info!("Throughput:");
        info!("  Groups published: {} ({:.1}/s)", total_published, total_published as f64 / elapsed_secs);
        info!("  Groups received:  {} ({:.1}/s)", total_received, total_received as f64 / elapsed_secs);
        info!("");

        // Per-publisher stats
        info!("Per-publisher groups:");
        for (i, counter) in self.groups_published.iter().enumerate() {
            let count = counter.load(Ordering::Relaxed);
            info!("  Publisher {}: {}", i, count);
        }
        info!("");

        // Latency
        if let Ok(hist) = self.ranking_latency.lock() {
            if hist.len() > 0 {
                info!("Ranking Change Latency:");
                info!("  p50:  {:>8} µs", hist.value_at_quantile(0.50));
                info!("  p90:  {:>8} µs", hist.value_at_quantile(0.90));
                info!("  p99:  {:>8} µs", hist.value_at_quantile(0.99));
                info!("  max:  {:>8} µs", hist.max());
                info!("");
            }
        }

        // Verification
        let (correct, incorrect) = self.verification_results();
        let total_verified = correct + incorrect;
        let accuracy = if total_verified > 0 {
            100.0 * correct as f64 / total_verified as f64
        } else {
            0.0
        };

        info!("Verification:");
        info!("  Correct deliveries:   {}", correct);
        info!("  Incorrect deliveries: {}", incorrect);
        info!("  Accuracy: {:.2}%", accuracy);
        info!("");

        // Current ranking
        let ranking = self.get_current_ranking();
        info!("Final Ranking (top {}):", self.top_n);
        for (i, (pub_id, value)) in ranking.iter().take(self.top_n as usize).enumerate() {
            info!("  {}: Publisher {} (value={})", i + 1, pub_id, value);
        }
    }
}
