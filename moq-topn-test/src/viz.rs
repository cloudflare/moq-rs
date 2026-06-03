//! Timeline visualization for Top-N filtering test.
//!
//! Generates an SVG image showing:
//! - Publishers as horizontal lanes with speech activity bars
//! - Subscribers with their top-N selections over time
//! - Self-exclusion clearly visible

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;

/// Event types for the timeline
#[derive(Debug, Clone)]
pub enum TimelineEvent {
    /// Publisher speech value changed
    SpeechValue {
        publisher_id: usize,
        value: u8,
        timestamp_ms: u64,
    },
    /// Subscriber top-N selection updated
    TopNSelection {
        subscriber_id: usize,
        selected_publishers: Vec<usize>,
        timestamp_ms: u64,
        excluded_self: Option<usize>,
    },
}

/// Collects timeline events for visualization
pub struct TimelineRecorder {
    events: Vec<TimelineEvent>,
    start_time_ms: u64,
    num_publishers: usize,
    num_subscribers: usize,
    top_n: u8,
}

impl TimelineRecorder {
    pub fn new(num_publishers: usize, num_subscribers: usize, top_n: u8) -> Self {
        Self {
            events: Vec::new(),
            start_time_ms: 0,
            num_publishers,
            num_subscribers,
            top_n,
        }
    }

    pub fn set_start_time(&mut self, start_ms: u64) {
        self.start_time_ms = start_ms;
    }

    pub fn record_speech_value(&mut self, publisher_id: usize, value: u8, timestamp_ms: u64) {
        self.events.push(TimelineEvent::SpeechValue {
            publisher_id,
            value,
            timestamp_ms: timestamp_ms.saturating_sub(self.start_time_ms),
        });
    }

    pub fn record_top_n_selection(
        &mut self,
        subscriber_id: usize,
        selected_publishers: Vec<usize>,
        timestamp_ms: u64,
        excluded_self: Option<usize>,
    ) {
        self.events.push(TimelineEvent::TopNSelection {
            subscriber_id,
            selected_publishers,
            timestamp_ms: timestamp_ms.saturating_sub(self.start_time_ms),
            excluded_self,
        });
    }

    /// Generate SVG visualization
    pub fn generate_svg(&self, output_path: &str) -> std::io::Result<()> {
        let max_time_ms = self
            .events
            .iter()
            .map(|e| match e {
                TimelineEvent::SpeechValue { timestamp_ms, .. } => *timestamp_ms,
                TimelineEvent::TopNSelection { timestamp_ms, .. } => *timestamp_ms,
            })
            .max()
            .unwrap_or(1000);

        let margin_left = 120.0;
        let margin_right = 40.0;
        let margin_top = 60.0;
        let margin_bottom = 40.0;

        let publisher_lane_height = 40.0;
        let subscriber_lane_height = 50.0;
        let section_gap = 30.0;
        let legend_height = 80.0;

        let timeline_width = 1000.0;
        let publishers_height = self.num_publishers as f64 * publisher_lane_height;
        let subscribers_height = self.num_subscribers as f64 * subscriber_lane_height;

        let total_width = margin_left + timeline_width + margin_right;
        let total_height =
            margin_top + publishers_height + section_gap + subscribers_height + section_gap + legend_height + margin_bottom;

        let mut svg = String::new();
        svg.push_str(&format!(
            r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {} {}" width="{}" height="{}">"#,
            total_width, total_height, total_width, total_height
        ));
        svg.push('\n');

        // Styles
        svg.push_str(r#"<defs>
  <style>
    .title { font: bold 18px sans-serif; fill: #333; }
    .section-label { font: bold 14px sans-serif; fill: #555; }
    .lane-label { font: 12px sans-serif; fill: #666; }
    .time-label { font: 10px sans-serif; fill: #888; }
    .legend-text { font: 11px sans-serif; fill: #555; }
    .speech-silent { fill: #e0e0e0; }
    .speech-active { fill: #4CAF50; }
    .speech-start { fill: #FF9800; }
    .selected { fill: #2196F3; opacity: 0.7; }
    .excluded { fill: #f44336; opacity: 0.5; }
    .grid-line { stroke: #eee; stroke-width: 1; }
    .axis-line { stroke: #ccc; stroke-width: 1; }
  </style>
</defs>
"#);

        // Title
        svg.push_str(&format!(
            r#"<text x="{}" y="30" class="title">Top-N Filtering Timeline ({} publishers, {} subscribers, N={})</text>"#,
            total_width / 2.0 - 150.0,
            self.num_publishers,
            self.num_subscribers,
            self.top_n
        ));
        svg.push('\n');

        // Build speech value timeline per publisher
        let mut publisher_speech: HashMap<usize, Vec<(u64, u8)>> = HashMap::new();
        for i in 0..self.num_publishers {
            publisher_speech.insert(i, vec![(0, 0)]);
        }
        for event in &self.events {
            if let TimelineEvent::SpeechValue {
                publisher_id,
                value,
                timestamp_ms,
            } = event
            {
                if let Some(v) = publisher_speech.get_mut(publisher_id) {
                    v.push((*timestamp_ms, *value));
                }
            }
        }
        for (_, v) in publisher_speech.iter_mut() {
            v.push((max_time_ms, v.last().map(|(_, val)| *val).unwrap_or(0)));
        }

        // Publishers section label
        svg.push_str(&format!(
            r#"<text x="10" y="{}" class="section-label">Publishers</text>"#,
            margin_top - 10.0
        ));
        svg.push('\n');

        // Draw publisher lanes
        for pub_id in 0..self.num_publishers {
            let lane_y = margin_top + pub_id as f64 * publisher_lane_height;

            // Lane label
            svg.push_str(&format!(
                r#"<text x="{}" y="{}" class="lane-label">P{}</text>"#,
                margin_left - 40.0,
                lane_y + publisher_lane_height / 2.0 + 4.0,
                pub_id
            ));
            svg.push('\n');

            // Draw speech activity bars
            if let Some(timeline) = publisher_speech.get(&pub_id) {
                for i in 0..timeline.len() - 1 {
                    let (t1, v1) = timeline[i];
                    let (t2, _) = timeline[i + 1];

                    let x1 = margin_left + (t1 as f64 / max_time_ms as f64) * timeline_width;
                    let x2 = margin_left + (t2 as f64 / max_time_ms as f64) * timeline_width;
                    let width = (x2 - x1).max(1.0);

                    let class = match v1 {
                        0 => "speech-silent",
                        1 => "speech-active",
                        2 => "speech-start",
                        _ => "speech-active",
                    };

                    svg.push_str(&format!(
                        r#"<rect x="{}" y="{}" width="{}" height="{}" class="{}" />"#,
                        x1,
                        lane_y + 5.0,
                        width,
                        publisher_lane_height - 10.0,
                        class
                    ));
                    svg.push('\n');
                }
            }

            // Lane border
            svg.push_str(&format!(
                r#"<line x1="{}" y1="{}" x2="{}" y2="{}" class="axis-line" />"#,
                margin_left,
                lane_y + publisher_lane_height,
                margin_left + timeline_width,
                lane_y + publisher_lane_height
            ));
            svg.push('\n');
        }

        // Subscribers section
        let subscribers_y = margin_top + publishers_height + section_gap;
        svg.push_str(&format!(
            r#"<text x="10" y="{}" class="section-label">Subscribers (Top-N Selection)</text>"#,
            subscribers_y - 10.0
        ));
        svg.push('\n');

        // Build top-N selection timeline per subscriber
        let mut subscriber_selections: HashMap<usize, Vec<(u64, Vec<usize>, Option<usize>)>> = HashMap::new();
        for i in 0..self.num_subscribers {
            subscriber_selections.insert(i, vec![(0, vec![], None)]);
        }
        for event in &self.events {
            if let TimelineEvent::TopNSelection {
                subscriber_id,
                selected_publishers,
                timestamp_ms,
                excluded_self,
            } = event
            {
                if let Some(v) = subscriber_selections.get_mut(subscriber_id) {
                    v.push((*timestamp_ms, selected_publishers.clone(), *excluded_self));
                }
            }
        }
        for (_, v) in subscriber_selections.iter_mut() {
            let last_selection = v.last().map(|(_, s, e)| (s.clone(), *e)).unwrap_or((vec![], None));
            v.push((max_time_ms, last_selection.0, last_selection.1));
        }

        // Draw subscriber lanes
        for sub_id in 0..self.num_subscribers {
            let lane_y = subscribers_y + sub_id as f64 * subscriber_lane_height;

            // Lane label
            svg.push_str(&format!(
                r#"<text x="{}" y="{}" class="lane-label">S{}</text>"#,
                margin_left - 40.0,
                lane_y + subscriber_lane_height / 2.0 + 4.0,
                sub_id
            ));
            svg.push('\n');

            // Draw selection segments
            if let Some(timeline) = subscriber_selections.get(&sub_id) {
                for i in 0..timeline.len() - 1 {
                    let (t1, ref selected, excluded) = timeline[i];
                    let (t2, _, _) = timeline[i + 1];

                    let x1 = margin_left + (t1 as f64 / max_time_ms as f64) * timeline_width;
                    let x2 = margin_left + (t2 as f64 / max_time_ms as f64) * timeline_width;
                    let width = (x2 - x1).max(1.0);

                    // Background
                    svg.push_str(&format!(
                        r##"<rect x="{}" y="{}" width="{}" height="{}" fill="#f5f5f5" />"##,
                        x1,
                        lane_y + 2.0,
                        width,
                        subscriber_lane_height - 4.0
                    ));
                    svg.push('\n');

                    // Draw mini bars for each selected publisher
                    let bar_height = (subscriber_lane_height - 8.0) / self.num_publishers as f64;
                    let bar_width = (width - 2.0).max(1.0);
                    for &pub_id in selected {
                        let bar_y = lane_y + 4.0 + pub_id as f64 * bar_height;
                        svg.push_str(&format!(
                            r#"<rect x="{}" y="{}" width="{}" height="{}" class="selected" />"#,
                            x1 + 1.0,
                            bar_y,
                            bar_width,
                            bar_height - 1.0
                        ));
                        svg.push('\n');
                    }

                    // Mark excluded self
                    if let Some(excl_id) = excluded {
                        let bar_y = lane_y + 4.0 + excl_id as f64 * bar_height;
                        svg.push_str(&format!(
                            r#"<rect x="{}" y="{}" width="{}" height="{}" class="excluded" />"#,
                            x1 + 1.0,
                            bar_y,
                            bar_width,
                            bar_height - 1.0
                        ));
                        svg.push('\n');
                    }
                }
            }

            // Lane border
            svg.push_str(&format!(
                r#"<line x1="{}" y1="{}" x2="{}" y2="{}" class="axis-line" />"#,
                margin_left,
                lane_y + subscriber_lane_height,
                margin_left + timeline_width,
                lane_y + subscriber_lane_height
            ));
            svg.push('\n');
        }

        // Time axis with labels
        let axis_y = subscribers_y + subscribers_height + 20.0;
        svg.push_str(&format!(
            r#"<line x1="{}" y1="{}" x2="{}" y2="{}" class="axis-line" />"#,
            margin_left, axis_y, margin_left + timeline_width, axis_y
        ));
        svg.push('\n');

        // Time ticks (every 5 seconds)
        let tick_interval_ms = 5000u64;
        let mut t = 0u64;
        while t <= max_time_ms {
            let x = margin_left + (t as f64 / max_time_ms as f64) * timeline_width;
            svg.push_str(&format!(
                r#"<line x1="{}" y1="{}" x2="{}" y2="{}" class="axis-line" />"#,
                x, axis_y, x, axis_y + 5.0
            ));
            svg.push_str(&format!(
                r#"<text x="{}" y="{}" class="time-label" text-anchor="middle">{}s</text>"#,
                x,
                axis_y + 18.0,
                t / 1000
            ));
            svg.push('\n');
            t += tick_interval_ms;
        }

        // Legend
        let legend_y = axis_y + 35.0;
        svg.push_str(&format!(
            r#"<text x="{}" y="{}" class="section-label">Legend</text>"#,
            margin_left, legend_y
        ));
        svg.push('\n');

        let legend_items = [
            ("speech-silent", "Silent (0)"),
            ("speech-active", "Speaking (1)"),
            ("speech-start", "Speech Start (2)"),
            ("selected", "Selected in Top-N"),
            ("excluded", "Self-Excluded"),
        ];

        let mut lx = margin_left;
        for (class, label) in legend_items {
            svg.push_str(&format!(
                r#"<rect x="{}" y="{}" width="20" height="12" class="{}" />"#,
                lx,
                legend_y + 10.0,
                class
            ));
            svg.push_str(&format!(
                r#"<text x="{}" y="{}" class="legend-text">{}</text>"#,
                lx + 25.0,
                legend_y + 20.0,
                label
            ));
            svg.push('\n');
            lx += 150.0;
        }

        svg.push_str("</svg>\n");

        let mut file = File::create(output_path)?;
        file.write_all(svg.as_bytes())?;

        Ok(())
    }
}

/// Wrapper for synchronized access
pub struct SharedTimelineRecorder {
    inner: std::sync::Mutex<TimelineRecorder>,
}

impl SharedTimelineRecorder {
    pub fn new(num_publishers: usize, num_subscribers: usize, top_n: u8) -> Self {
        Self {
            inner: std::sync::Mutex::new(TimelineRecorder::new(
                num_publishers,
                num_subscribers,
                top_n,
            )),
        }
    }

    pub fn set_start_time(&self, start_ms: u64) {
        self.inner.lock().unwrap().set_start_time(start_ms);
    }

    pub fn record_speech_value(&self, publisher_id: usize, value: u8, timestamp_ms: u64) {
        self.inner
            .lock()
            .unwrap()
            .record_speech_value(publisher_id, value, timestamp_ms);
    }

    pub fn record_top_n_selection(
        &self,
        subscriber_id: usize,
        selected_publishers: Vec<usize>,
        timestamp_ms: u64,
        excluded_self: Option<usize>,
    ) {
        self.inner.lock().unwrap().record_top_n_selection(
            subscriber_id,
            selected_publishers,
            timestamp_ms,
            excluded_self,
        );
    }

    pub fn generate_svg(&self, output_path: &str) -> std::io::Result<()> {
        self.inner.lock().unwrap().generate_svg(output_path)
    }
}
