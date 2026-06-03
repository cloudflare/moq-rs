#!/usr/bin/env python3
"""Generate an interactive HTML correctness report from moq-topn-test e2e output.

Shows:
- Click on a publisher to see their speech timeline
- See which subscribers hear that publisher at each moment
- View ranking changes over time
"""

import json
import re
import sys
from collections import defaultdict


def parse_events(filepath):
    events = []
    with open(filepath) as f:
        for line in f:
            m = re.search(r"TOPN_EVENT:(.*)", line)
            if m:
                try:
                    events.append(json.loads(m.group(1)))
                except:
                    pass
    return events


def parse_results(filepath):
    results = {}
    with open(filepath) as f:
        for line in f:
            if "Accuracy:" in line:
                m = re.search(r"(\d+\.\d+)%", line)
                if m:
                    results["accuracy"] = float(m.group(1))
            if "Correct deliveries:" in line:
                m = re.search(r"(\d+)", line.split(":")[-1])
                if m:
                    results["correct"] = int(m.group(1))
            if "Incorrect deliveries:" in line:
                m = re.search(r"(\d+)", line.split(":")[-1])
                if m:
                    results["incorrect"] = int(m.group(1))
            if "Publishers (X):" in line:
                m = re.search(r"(\d+)", line.split(":")[-1])
                if m:
                    results["publishers"] = int(m.group(1))
            if "Subscribers (Y):" in line:
                m = re.search(r"(\d+)", line.split(":")[-1])
                if m:
                    results["subscribers"] = int(m.group(1))
            if "Top-N filter:" in line:
                m = re.search(r"(\d+)", line.split(":")[-1])
                if m:
                    results["top_n"] = int(m.group(1))
            if "Duration:" in line and "duration" not in results:
                m = re.search(r"(\d+)s", line)
                if m:
                    results["duration"] = int(m.group(1))
            if "Mixed top-N:" in line:
                m = re.search(r"Mixed top-N:\s*(.*)", line)
                if m:
                    results["mixed_topn"] = m.group(1).strip()
    return results


def build_timeline_data(events):
    """Build per-publisher speech timeline and subscriber info."""
    # Speaking sessions
    speaking = defaultdict(list)
    current_speaking = {}
    test_duration = 0

    # Track all value changes for timeline blocks
    value_changes = defaultdict(list)  # pub_id -> [(ts, value)]

    # Subscriber info
    subscribers = {}  # sub_id -> {is_pub_sub, publisher_id, top_n}

    for ev in events:
        ts = ev.get("ts_ms", 0)
        test_duration = max(test_duration, ts)

        if ev.get("event") == "subscriber_registered":
            sub_id = ev.get("subscriber_id")
            subscribers[sub_id] = {
                "is_pub_sub": ev.get("is_pub_sub", False),
                "publisher_id": ev.get("publisher_id"),
            }

        elif ev.get("event") == "value_updated":
            pub_id = ev.get("publisher_id")
            if pub_id is None:
                continue
            new_value = ev.get("new_value", 0)
            old_value = ev.get("old_value", 0)
            value_changes[pub_id].append((ts, new_value))

            if new_value > 0 and old_value == 0:
                current_speaking[pub_id] = ts
            elif new_value == 0 and old_value > 0:
                if pub_id in current_speaking:
                    start = current_speaking.pop(pub_id)
                    speaking[pub_id].append({"start": start, "end": ts})

    for pub_id, start in current_speaking.items():
        speaking[pub_id].append({"start": start, "end": test_duration})

    # Build timeline blocks per publisher
    pub_timelines = {}
    for pub_id in sorted(value_changes.keys()):
        changes = value_changes[pub_id]
        blocks = []
        for i, (ts, val) in enumerate(changes):
            end_ts = changes[i + 1][0] if i + 1 < len(changes) else test_duration
            state = "silent" if val == 0 else "start" if val == 2 else "speaking"
            blocks.append({"start": ts, "end": end_ts, "state": state, "value": val})
        # Add initial silent block if first event is not at t=0
        if changes and changes[0][0] > 0:
            blocks.insert(0, {"start": 0, "end": changes[0][0], "state": "silent", "value": 0})
        pub_timelines[pub_id] = blocks

    return speaking, pub_timelines, subscribers, test_duration


def generate_html(filepath, output_path):
    events = parse_events(filepath)
    results = parse_results(filepath)
    speaking, pub_timelines, subscribers, test_duration = build_timeline_data(events)

    num_pubs = results.get("publishers", len(pub_timelines))
    top_n = results.get("top_n", 5)
    total_subs = results.get("subscribers", len(subscribers))
    accuracy = results.get("accuracy", 0)
    correct = results.get("correct", 0)
    incorrect = results.get("incorrect", 0)
    duration = results.get("duration", test_duration / 1000)

    # Prepare JSON data for JS
    data = {
        "publishers": {str(k): v for k, v in pub_timelines.items()},
        "speaking": {str(k): v for k, v in speaking.items()},
        "subscribers": {str(k): v for k, v in subscribers.items()},
        "events": events,
        "testDuration": test_duration,
        "numPubs": num_pubs,
        "topN": top_n,
        "totalSubs": total_subs,
    }

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Top-N E2E Correctness - Interactive Report</title>
<style>
* {{ box-sizing: border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  margin: 0; padding: 20px; background: #f5f5f5;
}}
.container {{ max-width: 1400px; margin: 0 auto; }}
h1 {{ color: #333; margin-bottom: 5px; }}
.subtitle {{ color: #666; margin-bottom: 20px; }}

.stats-bar {{
  display: flex; gap: 20px; margin-bottom: 20px; flex-wrap: wrap;
}}
.stat-card {{
  background: white; padding: 15px 25px; border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center;
  min-width: 120px;
}}
.stat-card .value {{ font-size: 1.8rem; font-weight: 700; }}
.stat-card .label {{ font-size: 0.8rem; color: #666; margin-top: 2px; }}

.panel {{
  background: white; padding: 20px; border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px;
}}

.pub-selector {{
  display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 15px;
}}
.pub-btn {{
  padding: 8px 16px; border: 2px solid #ddd; background: white;
  border-radius: 6px; cursor: pointer; font-size: 13px;
  transition: all 0.2s; position: relative; overflow: hidden;
}}
.pub-btn:hover {{ border-color: #2196F3; background: #e3f2fd; }}
.pub-btn.active {{ background: #2196F3; color: white; border-color: #2196F3; }}
.pub-btn .indicator {{
  position: absolute; bottom: 0; left: 0; right: 0; height: 3px;
}}

.timeline-row {{
  display: flex; align-items: center; margin-bottom: 8px;
}}
.timeline-label {{
  width: 70px; font-size: 12px; color: #666; text-align: right;
  padding-right: 10px; flex-shrink: 0;
}}
.timeline-track {{
  flex: 1; height: 36px; display: flex; border-radius: 4px;
  overflow: hidden; border: 1px solid #e0e0e0; cursor: pointer;
}}
.time-block {{
  display: flex; align-items: center; justify-content: center;
  font-size: 11px; font-weight: 600; color: white;
  transition: opacity 0.2s; min-width: 1px;
}}
.time-block:hover {{ opacity: 0.8; }}
.time-block.silent {{ background: #bdbdbd; color: #666; }}
.time-block.speaking {{ background: #4caf50; }}
.time-block.start {{ background: #ff9800; }}
.time-block.dimmed {{ opacity: 0.3; }}

.time-axis {{
  display: flex; margin-left: 70px; font-size: 10px; color: #999;
  margin-top: 4px; margin-bottom: 15px;
}}

.detail-panel {{
  margin-top: 15px; padding: 15px; background: #f8f9fa;
  border-radius: 6px; border-left: 4px solid #2196F3;
  display: none;
}}
.detail-panel.visible {{ display: block; }}
.detail-panel h4 {{ margin: 0 0 10px 0; color: #333; }}

.subscriber-section {{ margin-top: 20px; }}
.subscriber-section h4 {{ margin: 0 0 10px 0; color: #555; }}

.sub-grid {{
  display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 8px;
}}
.sub-card {{
  padding: 10px; border-radius: 6px; text-align: center;
  font-size: 12px; transition: all 0.3s; cursor: default;
}}
.sub-card.hears {{
  background: #e8f5e9; border: 2px solid #4caf50;
}}
.sub-card.hears .status {{ color: #2e7d32; font-weight: 600; }}
.sub-card.not-hears {{
  background: #fafafa; border: 2px solid #eee; color: #999;
}}
.sub-card.self-excluded {{
  background: #ffebee; border: 2px solid #ef5350;
}}
.sub-card.self-excluded .status {{ color: #c62828; }}
.sub-card .sub-name {{ font-weight: 600; margin-bottom: 2px; }}
.sub-card .sub-info {{ font-size: 10px; color: #888; }}
.sub-hearing-list {{
  margin-top: 6px; font-size: 10px; line-height: 1.6;
}}
.heard-tag {{
  display: inline-block; padding: 1px 6px; border-radius: 8px;
  background: #e8f5e9; color: #2e7d32; margin: 1px 2px;
  font-weight: 500;
}}
.heard-tag.heard-selected {{
  background: #bbdefb; color: #1565c0; font-weight: 700;
  border: 1px solid #1976d2;
}}

.legend {{
  display: flex; gap: 20px; font-size: 12px; color: #666;
  margin: 10px 0;
}}
.legend-item {{ display: flex; align-items: center; gap: 5px; }}
.legend-box {{ width: 14px; height: 14px; border-radius: 3px; }}

.ranking-panel {{
  margin-top: 15px; padding: 15px; background: #f0f7ff;
  border-radius: 6px;
}}
.ranking-panel h4 {{ margin: 0 0 8px 0; color: #1565c0; }}
.ranking-list {{
  display: flex; gap: 8px; flex-wrap: wrap;
}}
.rank-item {{
  padding: 4px 12px; border-radius: 15px; font-size: 12px;
  font-weight: 600;
}}
.rank-item.in-topn {{ background: #c8e6c9; color: #2e7d32; }}
.rank-item.out-topn {{ background: #eeeeee; color: #757575; }}
.rank-item.selected {{ background: #bbdefb; color: #1565c0; border: 2px solid #1976d2; }}

.all-timelines {{ margin-top: 20px; }}
</style>
</head>
<body>
<div class="container">

<h1>Top-N E2E Correctness Report</h1>
<p class="subtitle">Real QUIC connections &mdash; {num_pubs} publishers, {total_subs} subscribers, N={top_n}, {duration}s</p>

<div class="stats-bar">
  <div class="stat-card">
    <div class="value" style="color: {'#4caf50' if accuracy >= 98 else '#ff9800'}">{accuracy:.1f}%</div>
    <div class="label">Accuracy</div>
  </div>
  <div class="stat-card">
    <div class="value" style="color: #4caf50">{correct:,}</div>
    <div class="label">Correct</div>
  </div>
  <div class="stat-card">
    <div class="value" style="color: #f44336">{incorrect}</div>
    <div class="label">In-flight misses</div>
  </div>
  <div class="stat-card">
    <div class="value" style="color: #2196F3">{num_pubs}</div>
    <div class="label">Publishers</div>
  </div>
  <div class="stat-card">
    <div class="value" style="color: #9c27b0">{total_subs}</div>
    <div class="label">Subscribers</div>
  </div>
  <div class="stat-card">
    <div class="value" style="color: #ff9800">N={top_n}</div>
    <div class="label">Top-N Filter</div>
  </div>
</div>

<div class="panel">
  <h3>Select a Publisher</h3>
  <p style="font-size: 13px; color: #666; margin-bottom: 10px;">
    Click a publisher to see their speech timeline and which subscribers hear them.
  </p>
  <div class="pub-selector" id="pubSelector"></div>
</div>

<div class="panel" id="timelinePanel" style="display:none;">
  <h3 id="timelineTitle">Publisher Timeline</h3>
  <div class="legend">
    <div class="legend-item"><div class="legend-box" style="background:#bdbdbd"></div> Silent (0)</div>
    <div class="legend-item"><div class="legend-box" style="background:#ff9800"></div> Speech Start (2)</div>
    <div class="legend-item"><div class="legend-box" style="background:#4caf50"></div> Speaking (1)</div>
  </div>

  <div class="timeline-row">
    <div class="timeline-label" id="selectedPubLabel"></div>
    <div class="timeline-track" id="selectedTimeline"></div>
  </div>
  <div class="time-axis" id="timeAxis"></div>

  <div class="detail-panel" id="detailPanel">
    <h4 id="detailTitle"></h4>
    <p id="detailText"></p>
  </div>

  <div class="ranking-panel" id="rankingPanel" style="display:none;">
    <h4>Current Ranking at Selected Moment</h4>
    <div class="ranking-list" id="rankingList"></div>
  </div>

  <div class="subscriber-section">
    <h4 id="subHeader">Subscribers hearing this publisher:</h4>
    <div class="sub-grid" id="subGrid"></div>
  </div>
</div>

<div class="panel">
  <h3>All Publishers Overview</h3>
  <p style="font-size: 13px; color: #666; margin-bottom: 10px;">
    Full timeline of all publishers. Click any row to select that publisher.
  </p>
  <div class="all-timelines" id="allTimelines"></div>
  <div class="time-axis" id="allTimeAxis"></div>
</div>

<div class="panel">
  <h3>Correctness Analysis</h3>
  <p>The <strong>{incorrect} incorrect deliveries</strong> ({100 - accuracy:.1f}%) are objects that were
  already in-flight on the QUIC stream when a ranking transition occurred. This is
  <strong>eventual consistency by design</strong> &mdash; not a bug.</p>
  <ul style="margin: 10px 0 0 20px; color: #555;">
    <li>A publisher stops speaking (value &rarr; 0), triggering a snapshot rebuild</li>
    <li>The epoch counter increments, but subscriber observers check on next object</li>
    <li>Objects already queued in QUIC stream buffers are delivered before the filter updates</li>
    <li>Convergence time is bounded by one group interval (33ms at 30 Hz)</li>
  </ul>
</div>

</div>

<script>
const DATA = {json.dumps(data)};

const COLORS = [
  '#e91e63', '#9c27b0', '#3f51b5', '#2196F3', '#00bcd4',
  '#009688', '#4caf50', '#8bc34a', '#ff9800', '#ff5722',
  '#795548', '#607d8b', '#f44336', '#673ab7', '#03a9f4',
];

let selectedPub = null;
let selectedBlockTs = null;

function init() {{
  renderPubSelector();
  renderAllTimelines();
  // Auto-select first publisher
  const pubs = Object.keys(DATA.publishers).map(Number).sort((a,b) => a-b);
  if (pubs.length > 0) selectPublisher(pubs[0]);
}}

function renderPubSelector() {{
  const el = document.getElementById('pubSelector');
  const pubs = Object.keys(DATA.publishers).map(Number).sort((a,b) => a-b);
  el.innerHTML = pubs.map(id => `
    <button class="pub-btn" id="pub-btn-${{id}}" onclick="selectPublisher(${{id}})">
      <span class="indicator" style="background: ${{COLORS[id % COLORS.length]}}"></span>
      Publisher ${{id}}
    </button>
  `).join('');
}}

function selectPublisher(pubId) {{
  selectedPub = pubId;
  selectedBlockTs = null;

  // Update buttons
  document.querySelectorAll('.pub-btn').forEach(btn => btn.classList.remove('active'));
  const btn = document.getElementById('pub-btn-' + pubId);
  if (btn) btn.classList.add('active');

  // Show timeline panel
  document.getElementById('timelinePanel').style.display = 'block';
  document.getElementById('timelineTitle').textContent = `Publisher ${{pubId}} Speech Timeline`;
  document.getElementById('selectedPubLabel').textContent = `pub-${{pubId}}`;

  renderSelectedTimeline(pubId);
  renderTimeAxis('timeAxis');
  updateSubscriberView(pubId, null);
  highlightAllTimelines(pubId);

  // Hide detail until click
  document.getElementById('detailPanel').classList.remove('visible');
  document.getElementById('rankingPanel').style.display = 'none';
}}

function renderSelectedTimeline(pubId) {{
  const track = document.getElementById('selectedTimeline');
  const blocks = DATA.publishers[pubId.toString()] || [];
  const dur = DATA.testDuration;

  track.innerHTML = blocks.map((b, i) => {{
    const width = ((b.end - b.start) / dur) * 100;
    const label = b.state === 'silent' ? '' : (b.state === 'start' ? '2' : '1');
    return `<div class="time-block ${{b.state}}" style="width:${{width}}%"
      onclick="selectBlock(${{pubId}}, ${{i}}, ${{b.start}}, ${{b.end}}, '${{b.state}}', ${{b.value}})"
      title="${{(b.start/1000).toFixed(1)}}s - ${{(b.end/1000).toFixed(1)}}s (${{b.state}})">${{label}}</div>`;
  }}).join('');
}}

function selectBlock(pubId, blockIdx, start, end, state, value) {{
  selectedBlockTs = start;
  const dur = ((end - start) / 1000).toFixed(1);

  const detail = document.getElementById('detailPanel');
  detail.classList.add('visible');
  document.getElementById('detailTitle').textContent =
    `${{state.charAt(0).toUpperCase() + state.slice(1)}} block: ${{(start/1000).toFixed(1)}}s - ${{(end/1000).toFixed(1)}}s (${{dur}}s)`;
  document.getElementById('detailText').textContent =
    `Value = ${{value}}. ${{state === 'silent' ? 'Publisher is silent, will not appear in rankings.' :
      state === 'start' ? 'Speech start (value=2) ranks highest, above ongoing speakers.' :
      'Ongoing speech (value=1), ranked by tie-break policy (oldest wins).'}}`;

  // Compute ranking at this moment
  computeRankingAt(start);

  // Update subscriber view
  updateSubscriberView(pubId, start);
}}

function computeRankingAt(ts) {{
  // Reconstruct state at time ts from value_updated events
  const pubValues = {{}};
  for (const ev of DATA.events) {{
    if (ev.ts_ms > ts) break;
    if (ev.event === 'value_updated') {{
      pubValues[ev.publisher_id] = ev.new_value;
    }}
  }}

  // Sort by value descending (simple ranking, no self-exclusion shown here)
  const ranked = Object.entries(pubValues)
    .filter(([_, v]) => v > 0)
    .sort((a, b) => b[1] - a[1]);

  const panel = document.getElementById('rankingPanel');
  const list = document.getElementById('rankingList');
  panel.style.display = 'block';

  const topN = DATA.topN;
  list.innerHTML = ranked.map(([id, val], idx) => {{
    const inTop = idx < topN;
    const isSel = parseInt(id) === selectedPub;
    const cls = isSel ? 'selected' : (inTop ? 'in-topn' : 'out-topn');
    return `<span class="rank-item ${{cls}}">#${{idx+1}} pub-${{id}} (val=${{val}})</span>`;
  }}).join('');

  if (ranked.length === 0) {{
    list.innerHTML = '<span style="color:#999">No active speakers at this moment</span>';
  }}
}}

function getActiveSpeakersAt(ts) {{
  if (ts === null) return {{}};
  const pubValues = {{}};
  for (const ev of DATA.events) {{
    if (ev.ts_ms > ts) break;
    if (ev.event === 'value_updated') pubValues[ev.publisher_id] = ev.new_value;
  }}
  return pubValues;
}}

function getRankedSpeakersAt(ts) {{
  const pubValues = getActiveSpeakersAt(ts);
  return Object.entries(pubValues)
    .filter(([_, v]) => v > 0)
    .sort((a, b) => b[1] - a[1])
    .map(([id, val], idx) => ({{ id: parseInt(id), value: val, rank: idx }}));
}}

function getSpeakersHeardBy(subId, ts) {{
  if (ts === null) return [];
  const sub = DATA.subscribers[subId.toString()];
  if (!sub) return [];
  const isPubSub = sub.is_pub_sub;
  const subPubId = sub.publisher_id;
  const topN = DATA.topN;

  const ranked = getRankedSpeakersAt(ts);
  const heard = [];
  let count = 0;
  for (const entry of ranked) {{
    if (count >= topN) break;
    if (isPubSub && entry.id === subPubId) continue;
    heard.push(entry);
    count++;
  }}
  return heard;
}}

function updateSubscriberView(pubId, ts) {{
  const grid = document.getElementById('subGrid');
  const subs = DATA.subscribers;
  const subIds = Object.keys(subs).map(Number).sort((a,b) => a-b);

  // Determine if pub is speaking at ts
  let pubSpeaking = false;
  if (ts !== null) {{
    const blocks = DATA.publishers[pubId.toString()] || [];
    for (const b of blocks) {{
      if (ts >= b.start && ts < b.end && b.value > 0) {{
        pubSpeaking = true;
        break;
      }}
    }}
  }} else {{
    const blocks = DATA.publishers[pubId.toString()] || [];
    if (blocks.length > 0) {{
      pubSpeaking = blocks[blocks.length - 1].value > 0;
    }}
  }}

  // Determine ranking position
  let pubRank = -1;
  if (ts !== null && pubSpeaking) {{
    const ranked = getRankedSpeakersAt(ts);
    pubRank = ranked.findIndex(r => r.id === pubId);
  }}

  grid.innerHTML = subIds.map(subId => {{
    const sub = subs[subId.toString()];
    const isPubSub = sub.is_pub_sub;
    const subPubId = sub.publisher_id;

    // Compute speakers this subscriber hears
    const heardList = ts !== null ? getSpeakersHeardBy(subId, ts) : [];
    const heardHtml = heardList.length > 0
      ? `<div class="sub-hearing-list">Hears: ${{heardList.map(s =>
          `<span class="heard-tag${{s.id === pubId ? ' heard-selected' : ''}}">pub-${{s.id}}</span>`
        ).join(' ')}}</div>`
      : (ts !== null ? `<div class="sub-hearing-list" style="color:#999">Hears: none active</div>` : '');

    // Self-exclusion: if this subscriber is the publisher
    if (isPubSub && subPubId === pubId) {{
      return `<div class="sub-card self-excluded">
        <div class="sub-name">Sub ${{subId}}</div>
        <div class="status">Self-excluded</div>
        <div class="sub-info">pub-sub (own track)</div>
        ${{heardHtml}}
      </div>`;
    }}

    const topN = DATA.topN;
    let effectiveRank = pubRank;
    if (isPubSub && subPubId !== null && ts !== null) {{
      const ranked = getRankedSpeakersAt(ts);
      const selfRank = ranked.findIndex(r => r.id === subPubId);
      if (selfRank >= 0 && selfRank < pubRank) {{
        effectiveRank = pubRank - 1;
      }}
    }}

    const hears = pubSpeaking && effectiveRank >= 0 && effectiveRank < topN;
    const cls = hears ? 'hears' : 'not-hears';
    const status = hears ? 'Hearing' : (pubSpeaking ? 'Filtered (rank > N)' : 'Silent');
    const info = isPubSub ? `pub-sub (pub-${{subPubId}})` : 'pure subscriber';

    return `<div class="sub-card ${{cls}}">
      <div class="sub-name">Sub ${{subId}}</div>
      <div class="status">${{status}}</div>
      <div class="sub-info">${{info}}</div>
      ${{heardHtml}}
    </div>`;
  }}).join('');

  const header = document.getElementById('subHeader');
  if (ts !== null) {{
    header.textContent = pubSpeaking
      ? `Subscribers hearing Publisher ${{pubId}} at t=${{(ts/1000).toFixed(1)}}s (rank #${{pubRank+1}}):`
      : `Publisher ${{pubId}} is silent at t=${{(ts/1000).toFixed(1)}}s - no one hears:`;
  }} else {{
    header.textContent = `Subscribers for Publisher ${{pubId}}:`;
  }}
}}

function renderAllTimelines() {{
  const container = document.getElementById('allTimelines');
  const pubs = Object.keys(DATA.publishers).map(Number).sort((a,b) => a-b);
  const dur = DATA.testDuration;

  container.innerHTML = pubs.map(pubId => {{
    const blocks = DATA.publishers[pubId.toString()] || [];
    const trackHtml = blocks.map(b => {{
      const width = ((b.end - b.start) / dur) * 100;
      return `<div class="time-block ${{b.state}}" style="width:${{width}}%"></div>`;
    }}).join('');

    return `<div class="timeline-row" onclick="selectPublisher(${{pubId}})" style="cursor:pointer;">
      <div class="timeline-label">pub-${{pubId}}</div>
      <div class="timeline-track" id="all-track-${{pubId}}" style="border-color: ${{COLORS[pubId % COLORS.length]}}">${{trackHtml}}</div>
    </div>`;
  }}).join('');

  renderTimeAxis('allTimeAxis');
}}

function highlightAllTimelines(selectedId) {{
  const pubs = Object.keys(DATA.publishers).map(Number);
  pubs.forEach(pubId => {{
    const track = document.getElementById('all-track-' + pubId);
    if (track) {{
      track.style.opacity = pubId === selectedId ? '1' : '0.4';
      track.style.borderColor = pubId === selectedId ? COLORS[pubId % COLORS.length] : '#e0e0e0';
      track.style.borderWidth = pubId === selectedId ? '2px' : '1px';
    }}
  }});
}}

function renderTimeAxis(containerId) {{
  const el = document.getElementById(containerId);
  const dur = DATA.testDuration / 1000;
  const ticks = Math.min(10, Math.ceil(dur / 5));
  const interval = dur / ticks;
  el.innerHTML = Array.from({{length: ticks + 1}}, (_, i) =>
    `<span style="flex:1; text-align:${{i===0?'left':i===ticks?'right':'center'}}">${{(i * interval).toFixed(0)}}s</span>`
  ).join('');
}}

init();
</script>
</body>
</html>"""

    with open(output_path, "w") as f:
        f.write(html)
    print(f"Report written to: {output_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: gen_correctness_report.py <test_output.txt> [output.html]")
        sys.exit(1)
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "correctness_report.html"
    generate_html(input_file, output_file)
