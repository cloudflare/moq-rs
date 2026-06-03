#!/usr/bin/env python3
import json, sys, re
from collections import defaultdict

events = []
with open(sys.argv[1]) as f:
    for line in f:
        m = re.search(r"TOPN_EVENT:(.*)", line)
        if m:
            try:
                events.append(json.loads(m.group(1)))
            except:
                pass

# Track speaking sessions per publisher
speaking_sessions = defaultdict(list)
current_speaking = {}
test_duration_ms = 0

for ev in events:
    ts = ev.get("ts_ms", 0)
    test_duration_ms = max(test_duration_ms, ts)

    if ev.get("event") == "value_updated":
        pub_id = ev.get("publisher_id")
        if pub_id is None:
            track = ev.get("track", "")
            m2 = re.search(r"speaker-(\d+)", track)
            if not m2:
                continue
            pub_id = int(m2.group(1))
        new_value = ev.get("new_value", 0)
        old_value = ev.get("old_value", 0)

        if new_value > 0 and old_value == 0:
            current_speaking[pub_id] = ts
        elif new_value == 0 and old_value > 0:
            if pub_id in current_speaking:
                start = current_speaking.pop(pub_id)
                speaking_sessions[pub_id].append((start, ts))

# Close any still-speaking
for pub_id, start in current_speaking.items():
    speaking_sessions[pub_id].append((start, test_duration_ms))

total_speakers = len(speaking_sessions)
all_sessions = []
for pub_id, sessions in speaking_sessions.items():
    for s in sessions:
        all_sessions.append((pub_id, s[0], s[1]))
all_sessions.sort(key=lambda x: x[1])

# Silence analysis
timeline = []
for pub_id, sessions in speaking_sessions.items():
    for start, end in sessions:
        timeline.append((start, 1))
        timeline.append((end, -1))
timeline.sort()

silence_periods = []
active_count = 0
last_ts = 0
total_silence_ms = 0
max_concurrent = 0

for ts, delta in timeline:
    if active_count == 0 and ts > last_ts:
        silence_periods.append((last_ts, ts))
        total_silence_ms += (ts - last_ts)
    active_count += delta
    max_concurrent = max(max_concurrent, active_count)
    last_ts = ts

if active_count == 0 and last_ts < test_duration_ms:
    silence_periods.append((last_ts, test_duration_ms))
    total_silence_ms += (test_duration_ms - last_ts)

# Per-speaker stats
speaker_stats = []
for pub_id in sorted(speaking_sessions.keys()):
    sessions = speaking_sessions[pub_id]
    total_speaking = sum(end - start for start, end in sessions)
    speaker_stats.append((pub_id, len(sessions), total_speaking))

print("=" * 60)
print("         SPEECH ACTIVITY ANALYSIS")
print("=" * 60)
print()
print(f"Test duration: {test_duration_ms/1000:.1f}s")
print(f"Total unique speakers: {total_speakers} / 80 publishers")
print(f"Total speaking sessions: {len(all_sessions)}")
print(f"Max concurrent speakers: {max_concurrent}")
avg_sessions = len(all_sessions) / max(1, total_speakers)
avg_duration = sum(s[2]-s[1] for s in all_sessions) / max(1, len(all_sessions))
print(f"Avg sessions per speaker: {avg_sessions:.1f}")
print(f"Avg speaking duration: {avg_duration/1000:.1f}s")
print()
print(f"Total silence (nobody speaking): {total_silence_ms/1000:.1f}s ({100*total_silence_ms/max(1,test_duration_ms):.1f}%)")
print(f"Number of silence gaps: {len(silence_periods)}")
if silence_periods:
    longest_silence = max(end - start for start, end in silence_periods)
    print(f"Longest silence gap: {longest_silence/1000:.1f}s")
print()
print("-" * 60)
print("PER-SPEAKER BREAKDOWN (sorted by total speaking time)")
print("-" * 60)
hdr = f"{'Pub':>4} {'Sessions':>8} {'Total':>8} {'Avg':>6}"
print(hdr)
for pub_id, num_sessions, total_ms in sorted(speaker_stats, key=lambda x: -x[2]):
    avg = total_ms / max(1, num_sessions)
    print(f"{pub_id:>4} {num_sessions:>8} {total_ms/1000:>7.1f}s {avg/1000:>5.1f}s")
print()
print("-" * 60)
print("SILENCE PERIODS (gaps where no publisher was speaking)")
print("-" * 60)
if not silence_periods:
    print("  None - at least one speaker active throughout the test")
else:
    for i, (start, end) in enumerate(silence_periods[:20]):
        dur = (end - start) / 1000
        print(f"  {start/1000:>7.1f}s - {end/1000:>7.1f}s  ({dur:.1f}s)")
    if len(silence_periods) > 20:
        print(f"  ... and {len(silence_periods) - 20} more gaps")
