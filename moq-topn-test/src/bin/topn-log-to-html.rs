//! Converts TOPN_EVENT logs from relay into an interactive HTML visualization.
//!
//! Usage:
//!   topn-log-to-html <input.log> <output.html>
//!
//! The input file should contain lines with TOPN_EVENT:{json} format.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};

#[derive(Debug, Deserialize)]
#[serde(tag = "event")]
enum TopNEvent {
    #[serde(rename = "track_registered")]
    TrackRegistered {
        ts_ms: u64,
        track: String,
        value: u64,
        publisher_id: u64,
    },
    #[serde(rename = "value_updated")]
    ValueUpdated {
        ts_ms: u64,
        track: String,
        old_value: u64,
        new_value: u64,
        publisher_id: u64,
    },
    #[serde(rename = "top_n_query")]
    TopNQuery {
        ts_ms: u64,
        subscriber_id: u64,
        n: u8,
        selected: Vec<SelectedTrack>,
        excluded_self: Option<u64>,
    },
    #[serde(rename = "track_removed")]
    TrackRemoved {
        ts_ms: u64,
        track: String,
        publisher_id: u64,
    },
    #[serde(rename = "subscriber_registered")]
    SubscriberRegistered {
        ts_ms: u64,
        subscriber_id: u64,
        is_pub_sub: bool,
        publisher_id: Option<u64>,
    },
    #[serde(rename = "publish_received")]
    PublishReceived {
        ts_ms: u64,
        subscriber_id: u64,
        track: String,
    },
}

#[derive(Debug, Deserialize)]
struct SelectedTrack {
    track: String,
    value: u64,
}

#[derive(Debug, Serialize)]
struct TimelineBlock {
    start: u64,
    end: u64,
    value: u64,
}

#[derive(Debug, Serialize)]
struct Publisher {
    id: u64,
    track: String,
    timeline: Vec<TimelineBlock>,
}

#[derive(Debug, Serialize)]
struct Subscriber {
    id: u64,
    is_publisher: bool,
    publisher_id: Option<u64>,
}

#[derive(Debug, Serialize)]
struct TopNQueryData {
    ts_ms: u64,
    subscriber_id: u64,
    selected: Vec<u64>,
    excluded_self: Option<u64>,
}

#[derive(Debug, Serialize)]
struct VisualizationData {
    duration: u64,
    top_n: u8,
    publishers: Vec<Publisher>,
    subscribers: Vec<Subscriber>,
    queries: Vec<TopNQueryData>,
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <input.log> <output.html>", args[0]);
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];

    println!("Reading events from: {}", input_path);

    let file = fs::File::open(input_path)?;
    let reader = BufReader::new(file);

    let mut events = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if let Some(json_str) = line.strip_prefix("TOPN_EVENT:") {
            match serde_json::from_str::<TopNEvent>(json_str) {
                Ok(event) => events.push(event),
                Err(e) => eprintln!("Failed to parse event: {} - {}", json_str, e),
            }
        }
    }

    println!("Parsed {} events", events.len());

    if events.is_empty() {
        eprintln!("No TOPN_EVENT entries found in the log file.");
        eprintln!("Make sure the relay was started with --topn-log flag.");
        std::process::exit(1);
    }

    // Process events to build visualization data
    let viz_data = process_events(events);

    // Generate HTML
    let html = generate_html(&viz_data);

    fs::write(output_path, html)?;
    println!("Generated interactive HTML: {}", output_path);

    Ok(())
}

fn process_events(events: Vec<TopNEvent>) -> VisualizationData {
    // Track publisher states: track_name -> (publisher_id, current_value, value_changes)
    let mut publisher_tracks: HashMap<String, (u64, Vec<(u64, u64)>)> = HashMap::new();
    let mut subscriber_map: HashMap<u64, Subscriber> = HashMap::new();
    let mut publisher_ids: HashSet<u64> = HashSet::new();
    let mut queries: Vec<TopNQueryData> = Vec::new();
    let mut top_n: u8 = 2;
    let mut min_ts: u64 = u64::MAX;
    let mut max_ts: u64 = 0;

    // Track name to publisher ID mapping
    let mut track_to_publisher: HashMap<String, u64> = HashMap::new();

    for event in events {
        match event {
            TopNEvent::TrackRegistered {
                ts_ms,
                track,
                value,
                publisher_id,
            } => {
                min_ts = min_ts.min(ts_ms);
                max_ts = max_ts.max(ts_ms);
                publisher_ids.insert(publisher_id);
                track_to_publisher.insert(track.clone(), publisher_id);
                publisher_tracks
                    .entry(track)
                    .or_insert_with(|| (publisher_id, Vec::new()))
                    .1
                    .push((ts_ms, value));
            }
            TopNEvent::ValueUpdated {
                ts_ms,
                track,
                new_value,
                publisher_id,
                ..
            } => {
                min_ts = min_ts.min(ts_ms);
                max_ts = max_ts.max(ts_ms);
                publisher_ids.insert(publisher_id);
                track_to_publisher.insert(track.clone(), publisher_id);
                publisher_tracks
                    .entry(track)
                    .or_insert_with(|| (publisher_id, Vec::new()))
                    .1
                    .push((ts_ms, new_value));
            }
            TopNEvent::TopNQuery {
                ts_ms,
                subscriber_id,
                n,
                selected,
                excluded_self,
            } => {
                min_ts = min_ts.min(ts_ms);
                max_ts = max_ts.max(ts_ms);
                top_n = n;

                // Convert selected tracks to publisher IDs
                let selected_ids: Vec<u64> = selected
                    .iter()
                    .filter_map(|s| track_to_publisher.get(&s.track).copied())
                    .collect();

                queries.push(TopNQueryData {
                    ts_ms,
                    subscriber_id,
                    selected: selected_ids,
                    excluded_self,
                });
            }
            TopNEvent::TrackRemoved { ts_ms, .. } => {
                max_ts = max_ts.max(ts_ms);
            }
            TopNEvent::SubscriberRegistered {
                ts_ms,
                subscriber_id,
                is_pub_sub,
                publisher_id,
            } => {
                min_ts = min_ts.min(ts_ms);
                max_ts = max_ts.max(ts_ms);
                subscriber_map.insert(
                    subscriber_id,
                    Subscriber {
                        id: subscriber_id,
                        is_publisher: is_pub_sub,
                        publisher_id,
                    },
                );
            }
            TopNEvent::PublishReceived { ts_ms, .. } => {
                min_ts = min_ts.min(ts_ms);
                max_ts = max_ts.max(ts_ms);
            }
        }
    }

    // Normalize timestamps to start from 0
    let base_ts = min_ts;
    let duration = if max_ts > min_ts { max_ts - min_ts } else { 1000 };

    // Build publisher timelines
    let mut publishers: Vec<Publisher> = Vec::new();
    for (track, (pub_id, changes)) in publisher_tracks {
        let mut timeline = Vec::new();
        let mut sorted_changes: Vec<(u64, u64)> = changes;
        sorted_changes.sort_by_key(|(ts, _)| *ts);

        for i in 0..sorted_changes.len() {
            let (ts, value) = sorted_changes[i];
            let end_ts = if i + 1 < sorted_changes.len() {
                sorted_changes[i + 1].0
            } else {
                max_ts
            };

            timeline.push(TimelineBlock {
                start: ts.saturating_sub(base_ts),
                end: end_ts.saturating_sub(base_ts),
                value,
            });
        }

        publishers.push(Publisher {
            id: pub_id,
            track,
            timeline,
        });
    }

    // Sort publishers by ID
    publishers.sort_by_key(|p| p.id);

    // Build subscriber list from subscriber_map (populated by subscriber_registered events)
    let mut subs: Vec<Subscriber> = subscriber_map.into_values().collect();
    subs.sort_by_key(|s| s.id);

    // Adjust query timestamps
    let adjusted_queries: Vec<TopNQueryData> = queries
        .into_iter()
        .map(|mut q| {
            q.ts_ms = q.ts_ms.saturating_sub(base_ts);
            q
        })
        .collect();

    VisualizationData {
        duration,
        top_n,
        publishers,
        subscribers: subs,
        queries: adjusted_queries,
    }
}

fn generate_html(data: &VisualizationData) -> String {
    let data_json = serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string());

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Top-N Filtering Interactive Timeline</title>
    <style>
        * {{ box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0; padding: 20px; background: #f5f5f5;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{ color: #333; margin-bottom: 5px; }}
        .subtitle {{ color: #666; margin-bottom: 20px; }}

        .controls {{
            background: white; padding: 15px; border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px;
            display: flex; flex-wrap: wrap; align-items: center; gap: 10px;
        }}
        .controls label {{ font-weight: 600; }}
        .pub-btn {{
            padding: 10px 20px; margin: 2px; border: 2px solid #ddd;
            background: white; border-radius: 6px; cursor: pointer;
            font-size: 14px; transition: all 0.2s;
        }}
        .pub-btn:hover {{ border-color: #2196F3; }}
        .pub-btn.active {{ background: #2196F3; color: white; border-color: #2196F3; }}

        .timeline-container {{
            background: white; padding: 20px; border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}

        .publisher-timeline {{ margin-bottom: 30px; }}
        .timeline-header {{
            display: flex; align-items: center; margin-bottom: 10px; flex-wrap: wrap; gap: 10px;
        }}
        .timeline-header h3 {{ margin: 0; margin-right: 20px; }}
        .legend {{ display: flex; gap: 15px; font-size: 12px; flex-wrap: wrap; }}
        .legend-item {{ display: flex; align-items: center; gap: 5px; }}
        .legend-box {{ width: 16px; height: 16px; border-radius: 3px; }}

        .timeline-track {{
            height: 40px; display: flex; border-radius: 6px; overflow: hidden;
            border: 1px solid #ddd; position: relative; flex: 1;
        }}
        .timeline-track.selected {{
            height: 60px; border-width: 2px; border-color: #2196F3;
        }}
        .timeline-track.mini {{
            height: 28px; border-radius: 4px; opacity: 0.85;
        }}
        .time-block {{
            display: flex; align-items: center; justify-content: center;
            color: white; font-weight: 600; font-size: 14px;
            cursor: pointer; transition: all 0.2s;
            position: relative; min-width: 2px;
        }}
        .time-block:hover {{ filter: brightness(1.1); }}
        .time-block.selected-block {{ box-shadow: inset 0 0 0 3px rgba(0,0,0,0.4); }}
        .time-block.silent {{ background: #9e9e9e; }}
        .time-block.speaking {{ background: #4caf50; }}
        .time-block.start {{ background: #ff9800; }}
        .time-block .value-label {{
            position: absolute; font-size: 11px;
            background: rgba(0,0,0,0.3); padding: 2px 6px; border-radius: 3px;
        }}

        .time-axis {{
            display: flex; margin-top: 5px; margin-left: 40px; font-size: 11px; color: #888;
        }}
        .time-label {{ text-align: left; padding-left: 4px; }}

        .block-detail {{
            margin-top: 20px; padding: 15px; background: #f8f9fa;
            border-radius: 6px; border-left: 4px solid #2196F3;
        }}
        .block-detail h4 {{ margin: 0 0 10px 0; }}
        .stats {{ display: flex; gap: 30px; margin-bottom: 15px; flex-wrap: wrap; }}
        .stat {{ text-align: center; min-width: 80px; }}
        .stat-value {{ font-size: 24px; font-weight: 700; color: #2196F3; }}
        .stat-label {{ font-size: 12px; color: #666; }}

        .subscriber-section {{ margin-top: 20px; }}
        .subscriber-section h4 {{ margin: 0 0 10px 0; color: #555; }}

        .subscriber-grid {{
            display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
            gap: 10px;
        }}
        .sub-card {{
            padding: 10px; border-radius: 6px; text-align: center;
            font-size: 13px; transition: all 0.3s;
        }}
        .sub-card.sees {{ background: #e3f2fd; border: 2px solid #2196F3; }}
        .sub-card.not-sees {{ background: #fafafa; border: 2px solid #eee; color: #999; }}
        .sub-card.self-excluded {{ background: #ffebee; border: 2px solid #f44336; }}
        .sub-card .sub-name {{ font-weight: 600; }}
        .sub-card .sub-info {{ font-size: 11px; margin-top: 4px; color: #666; }}

        .stacked-timelines {{ margin-top: 10px; }}
        .stacked-row {{ display: flex; align-items: center; margin-bottom: 6px; }}
        .stacked-row.selected {{ margin-bottom: 10px; }}
        .stacked-label {{ width: 45px; font-size: 12px; color: #666; font-weight: 600; flex-shrink: 0; }}
        .stacked-label.selected {{ color: #2196F3; font-size: 14px; font-weight: 700; }}

        .scrubber {{
            position: absolute; top: 0; bottom: 0; width: 2px;
            background: #f44336; pointer-events: none; z-index: 10;
        }}
        .scrubber::after {{
            content: ''; position: absolute; top: -5px; left: -4px;
            width: 10px; height: 10px; background: #f44336;
            border-radius: 50%;
        }}

        .time-display {{
            font-size: 14px; color: #333; font-weight: 600;
            background: #fff; padding: 5px 10px; border-radius: 4px;
            border: 1px solid #ddd;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Top-N Filtering Interactive Timeline</h1>
        <p class="subtitle" id="subtitle">Loading...</p>

        <div class="controls">
            <label>Select Publisher:</label>
            <div id="pub-buttons"></div>
            <div class="time-display" id="time-display">Time: 0.0s</div>
        </div>

        <div class="timeline-container">
            <div class="publisher-timeline">
                <div class="timeline-header">
                    <h3 id="pub-title">Publisher Timelines</h3>
                    <div class="legend">
                        <div class="legend-item"><div class="legend-box" style="background:#9e9e9e"></div> Silent (0)</div>
                        <div class="legend-item"><div class="legend-box" style="background:#ff9800"></div> Speech Start (2)</div>
                        <div class="legend-item"><div class="legend-box" style="background:#4caf50"></div> Speaking (1)</div>
                    </div>
                </div>
                <div class="stacked-timelines" id="stacked-timelines"></div>
                <div class="time-axis" id="time-axis"></div>
            </div>

            <div class="block-detail" id="block-detail">
                <h4>Click a time block or drag to scrub through time</h4>
            </div>

            <div class="subscriber-section">
                <h4 id="sub-header">Subscriber visibility:</h4>
                <div class="subscriber-grid" id="subscriber-grid"></div>
            </div>
        </div>
    </div>

    <script>
    const rawData = {data_json};

    // Process the data
    const testData = {{
        duration: rawData.duration || 10000,
        topN: rawData.top_n || 2,
        publishers: rawData.publishers || [],
        subscribers: rawData.subscribers || [],
        queries: rawData.queries || []
    }};

    let selectedPublisher = testData.publishers.length > 0 ? testData.publishers[0].id : 0;
    let currentTime = 0;
    let isDragging = false;

    function getValueClass(value) {{
        if (value === 0) return 'silent';
        if (value >= 2) return 'start';
        return 'speaking';
    }}

    function getPublisherValue(pubId, time) {{
        const pub = testData.publishers.find(p => p.id === pubId);
        if (!pub) return 0;
        for (const block of pub.timeline) {{
            if (time >= block.start && time < block.end) {{
                return block.value;
            }}
        }}
        // Check last block
        if (pub.timeline.length > 0) {{
            const last = pub.timeline[pub.timeline.length - 1];
            if (time >= last.start) return last.value;
        }}
        return 0;
    }}

    function computeTopN(time, excludePubId = null) {{
        let values = testData.publishers.map(p => ({{
            id: p.id,
            value: getPublisherValue(p.id, time)
        }}));

        if (excludePubId !== null) {{
            values = values.filter(v => v.id !== excludePubId);
        }}

        // Only include publishers with value > 0 (not silent)
        values = values.filter(v => v.value > 0);

        values.sort((a, b) => {{
            if (b.value !== a.value) return b.value - a.value;
            return a.id - b.id;
        }});

        return values.slice(0, testData.topN).map(v => v.id);
    }}

    function canSubscriberSee(subId, pubId, time) {{
        const sub = testData.subscribers.find(s => s.id === subId);
        if (!sub) return 'not-sees';

        const excludeId = sub.is_publisher ? sub.publisher_id : null;

        if (sub.is_publisher && sub.publisher_id === pubId) {{
            return 'self-excluded';
        }}

        const topN = computeTopN(time, excludeId);
        return topN.includes(pubId) ? 'sees' : 'not-sees';
    }}

    function selectPublisher(pubId) {{
        selectedPublisher = pubId;
        document.querySelectorAll('.pub-btn').forEach(btn => {{
            btn.classList.toggle('active', parseInt(btn.dataset.pubId) === pubId);
        }});
        renderTimeline();
    }}

    function setTime(time) {{
        currentTime = Math.max(0, Math.min(time, testData.duration));
        document.getElementById('time-display').textContent = `Time: ${{(currentTime/1000).toFixed(1)}}s`;
        renderBlockDetail();
        renderSubscribers();
        updateScrubbers();
    }}

    function renderPubButtons() {{
        const container = document.getElementById('pub-buttons');
        container.innerHTML = '';
        testData.publishers.forEach((pub, i) => {{
            const btn = document.createElement('button');
            btn.className = 'pub-btn' + (pub.id === selectedPublisher ? ' active' : '');
            btn.textContent = `P${{pub.id}} (${{pub.track}})`;
            btn.dataset.pubId = pub.id;
            btn.onclick = () => selectPublisher(pub.id);
            container.appendChild(btn);
        }});
    }}

    function renderTimeline() {{
        const pub = testData.publishers.find(p => p.id === selectedPublisher);
        if (!pub) return;

        document.getElementById('pub-title').textContent = `Publisher Timelines (selected: P${{pub.id}})`;
        document.getElementById('sub-header').textContent = `Who sees Publisher ${{pub.id}}:`;

        const container = document.getElementById('stacked-timelines');
        const timeAxis = document.getElementById('time-axis');
        container.innerHTML = '';
        timeAxis.innerHTML = '';

        // Render all publishers stacked, with selected one highlighted
        testData.publishers.forEach(p => {{
            const isSelected = p.id === selectedPublisher;
            const row = document.createElement('div');
            row.className = 'stacked-row' + (isSelected ? ' selected' : '');

            const label = document.createElement('div');
            label.className = 'stacked-label' + (isSelected ? ' selected' : '');
            label.textContent = `P${{p.id}}`;
            row.appendChild(label);

            const track = document.createElement('div');
            track.className = 'timeline-track' + (isSelected ? ' selected' : ' mini');
            track.innerHTML = '<div class="scrubber"></div>';

            p.timeline.forEach(block => {{
                const width = ((block.end - block.start) / testData.duration) * 100;
                const div = document.createElement('div');
                div.className = `time-block ${{getValueClass(block.value)}}`;
                div.style.width = `${{Math.max(width, 0.5)}}%`;
                if (isSelected) {{
                    div.innerHTML = `<span class="value-label">${{block.value}}</span>`;
                }}

                div.onmousedown = (e) => {{
                    isDragging = true;
                    const rect = track.getBoundingClientRect();
                    const x = e.clientX - rect.left;
                    const time = (x / rect.width) * testData.duration;
                    setTime(time);
                }};

                track.appendChild(div);
            }});

            track.onmousemove = (e) => {{
                if (!isDragging) return;
                const rect = track.getBoundingClientRect();
                const x = Math.max(0, Math.min(e.clientX - rect.left, rect.width));
                const time = (x / rect.width) * testData.duration;
                setTime(time);
            }};

            row.appendChild(track);
            container.appendChild(row);
        }});

        document.onmouseup = () => {{ isDragging = false; }};

        // Add time axis labels based on selected publisher blocks
        pub.timeline.forEach(block => {{
            const width = ((block.end - block.start) / testData.duration) * 100;
            const label = document.createElement('div');
            label.className = 'time-label';
            label.style.width = `${{Math.max(width, 0.5)}}%`;
            label.textContent = `${{(block.start/1000).toFixed(1)}}s`;
            timeAxis.appendChild(label);
        }});

        setTime(currentTime);
    }}

    function updateScrubbers() {{
        const pos = (currentTime / testData.duration) * 100;
        document.querySelectorAll('.scrubber').forEach(s => {{
            s.style.left = `${{pos}}%`;
        }});
    }}

    function renderBlockDetail() {{
        const pub = testData.publishers.find(p => p.id === selectedPublisher);
        if (!pub) return;

        const value = getPublisherValue(selectedPublisher, currentTime);

        let seesCount = 0, notSeesCount = 0, excludedCount = 0;
        testData.subscribers.forEach(sub => {{
            const status = canSubscriberSee(sub.id, selectedPublisher, currentTime);
            if (status === 'sees') seesCount++;
            else if (status === 'not-sees') notSeesCount++;
            else excludedCount++;
        }});

        const allValues = testData.publishers.map(p => ({{
            id: p.id,
            value: getPublisherValue(p.id, currentTime)
        }})).sort((a,b) => b.value - a.value || a.id - b.id);

        const detail = document.getElementById('block-detail');
        detail.innerHTML = `
            <h4>Time ${{(currentTime/1000).toFixed(1)}}s: P${{selectedPublisher}} value=${{value}} (${{getValueClass(value)}})</h4>
            <div class="stats">
                <div class="stat">
                    <div class="stat-value">${{seesCount}}</div>
                    <div class="stat-label">Can See</div>
                </div>
                <div class="stat">
                    <div class="stat-value" style="color:#f44336">${{excludedCount}}</div>
                    <div class="stat-label">Self-Excluded</div>
                </div>
                <div class="stat">
                    <div class="stat-value" style="color:#999">${{notSeesCount}}</div>
                    <div class="stat-label">Can't See</div>
                </div>
            </div>
            <div style="font-size:13px; color:#555;">
                <strong>All publishers:</strong> ${{allValues.map(v => `P${{v.id}}=${{v.value}}`).join(', ')}}
            </div>
        `;
    }}

    function renderSubscribers() {{
        const grid = document.getElementById('subscriber-grid');
        grid.innerHTML = '';

        testData.subscribers.forEach(sub => {{
            const status = canSubscriberSee(sub.id, selectedPublisher, currentTime);
            const excludeId = sub.is_publisher ? sub.publisher_id : null;
            const topN = computeTopN(currentTime, excludeId);

            const card = document.createElement('div');
            card.className = `sub-card ${{status}}`;

            let info = '';
            if (status === 'self-excluded') {{
                info = `Is P${{sub.publisher_id}} (self)<br>Sees: [${{topN.map(id => 'P'+id).join(', ')}}]`;
            }} else {{
                info = `Top-${{testData.topN}}: [${{topN.map(id => 'P'+id).join(', ')}}]`;
            }}

            const subLabel = sub.is_publisher ? `S${{sub.id}} (=P${{sub.publisher_id}})` : `S${{sub.id}}`;

            card.innerHTML = `
                <div class="sub-name">${{subLabel}}</div>
                <div class="sub-info">${{info}}</div>
            `;
            grid.appendChild(card);
        }});
    }}

    // Initialize
    document.getElementById('subtitle').textContent =
        `${{testData.publishers.length}} Publishers, ${{testData.subscribers.length}} Subscribers, Top-${{testData.topN}} (${{(testData.duration/1000).toFixed(1)}}s duration)`;

    if (testData.publishers.length > 0) {{
        renderPubButtons();
        renderTimeline();
    }} else {{
        document.getElementById('block-detail').innerHTML = '<h4>No data found. Make sure TOPN_EVENT logs were captured.</h4>';
    }}
    </script>
</body>
</html>
"##,
        data_json = data_json
    )
}
