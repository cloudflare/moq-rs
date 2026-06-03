#!/bin/bash
set -euo pipefail

# Usage: ./run_perf_e2e.sh <pub_subs> <pure_subs> [top_n] [duration_sec] [output_dir] [mixed_topn]
# Example: ./run_perf_e2e.sh 80 720 45 120 ./perf-results
# Example: ./run_perf_e2e.sh 80 800 25 180 ./perf-results "1,2,4,8,16,32,64,75"

PUB_SUBS=${1:?Usage: $0 <pub_subs> <pure_subs> [top_n] [duration_sec] [output_dir] [mixed_topn]}
PURE_SUBS=${2:?Usage: $0 <pub_subs> <pure_subs> [top_n] [duration_sec] [output_dir] [mixed_topn]}
TOP_N=${3:-45}
DURATION=${4:-120}
OUTPUT_DIR=${5:-./perf-results}
MIXED_TOPN=${6:-}

TOTAL_SUBS=$((PUB_SUBS + PURE_SUBS))
SETUP_WAIT=$((TOTAL_SUBS / 20 > 30 ? TOTAL_SUBS / 20 : 30))
STEADY_DURATION=$((DURATION - SETUP_WAIT - 10))
REMOTE="admin@snk-dev-1.m10x.org"
SSH_KEY="$HOME/.ssh/keys/snk-dev-server.pem"
SSH="ssh -i $SSH_KEY $REMOTE"
SCP="scp -i $SSH_KEY"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
REMOTE_DIR="/tmp/moq-rs-perf-$TIMESTAMP"
LOCAL_DIR="$OUTPUT_DIR/$TIMESTAMP"

mkdir -p "$LOCAL_DIR"

echo "=== MOQ-RS E2E Performance Test ==="
echo "  Publishers (pub-sub): $PUB_SUBS"
echo "  Pure subscribers:     $PURE_SUBS"
echo "  Total subscribers:    $TOTAL_SUBS"
echo "  Top-N filter:         $TOP_N"
if [ -n "$MIXED_TOPN" ]; then
echo "  Mixed top-N values:   $MIXED_TOPN"
fi
echo "  Duration:             ${DURATION}s"
echo "  Setup wait:           ${SETUP_WAIT}s"
echo "  Steady-state capture: ${STEADY_DURATION}s"
echo "  Remote:               $REMOTE"
echo "  Remote dir:           $REMOTE_DIR"
echo "  Local output:         $LOCAL_DIR"
echo ""

# Step 1: Collect system info
echo "[1/7] Collecting system configuration..."
$SSH "bash -s" > "$LOCAL_DIR/sysinfo.txt" <<'SYSINFO'
echo "=== System Configuration ==="
echo "Hostname: $(hostname)"
echo "Date: $(date -u)"
echo "Kernel: $(uname -r)"
echo "Arch: $(uname -m)"
echo "CPU:"
lscpu | grep -E "^(Model name|CPU\(s\)|Thread|Core|Socket|CPU max)"
echo ""
echo "Memory:"
free -h | head -2
echo ""
echo "OS:"
cat /etc/os-release 2>/dev/null | grep -E "^(NAME|VERSION)" || true
echo ""
echo "Rust:"
source ~/.cargo/env 2>/dev/null
rustc --version 2>/dev/null || echo "unknown"
echo ""
echo "Git commit:"
cd ~/moq-rs-top-n && git log --oneline -1
echo "Git branch:"
cd ~/moq-rs-top-n && git branch --show-current
SYSINFO

echo "  Done."

# Step 2: Build on remote
echo "[2/7] Building on remote..."
$SSH "cd ~/moq-rs-top-n && source ~/.cargo/env && cargo build --release --bin moq-relay-ietf --bin moq-topn-test 2>&1 | tail -3"
echo "  Done."

# Step 3: Run perf test (steady-state only)
echo "[3/8] Running perf test (${DURATION}s total, profiling last ${STEADY_DURATION}s)..."

MIXED_ARG=""
if [ -n "$MIXED_TOPN" ]; then
  MIXED_ARG="--mixed-topn \"$MIXED_TOPN\""
fi

$SSH "bash -s" <<PERF_SCRIPT
set -e
cd ~/moq-rs-top-n
source ~/.cargo/env
mkdir -p $REMOTE_DIR

pkill -f moq-relay-ietf 2>/dev/null || true
sleep 1

# Start relay
./target/release/moq-relay-ietf --bind "[::]:4443" --tls-cert cert.pem --tls-key key.pem > $REMOTE_DIR/relay.log 2>&1 &
RELAY_PID=\$!
sleep 2

# Capture memory baseline
ps -o rss= -p \$RELAY_PID > $REMOTE_DIR/mem_before.txt

# Run e2e test in background
./target/release/moq-topn-test -m e2e \\
  --relay https://localhost:4443 \\
  --tls-disable-verify \\
  -x $PUB_SUBS \\
  -y $TOTAL_SUBS \\
  -n $TOP_N \\
  $MIXED_ARG \\
  -d $DURATION \\
  --group-interval-ms 33 \\
  --connection-batch-size 50 \\
  > $REMOTE_DIR/test_output.txt 2>&1 &
TEST_PID=\$!

# Wait for connections to establish before profiling
sleep $SETUP_WAIT

# Start perf recording for steady-state only
perf record -F 999 -p \$RELAY_PID -g -o $REMOTE_DIR/perf.data -- sleep $STEADY_DURATION &
PERF_PID=\$!

# Wait for test to finish
wait \$TEST_PID 2>/dev/null || true

# Capture memory after test
ps -o rss= -p \$RELAY_PID > $REMOTE_DIR/mem_after.txt 2>/dev/null || echo "0" > $REMOTE_DIR/mem_after.txt

wait \$PERF_PID 2>/dev/null || true

# Generate flamegraph
perf script -i $REMOTE_DIR/perf.data | ~/FlameGraph/stackcollapse-perf.pl > $REMOTE_DIR/collapsed.txt
~/FlameGraph/flamegraph.pl $REMOTE_DIR/collapsed.txt > $REMOTE_DIR/flamegraph.svg

# Run analysis
python3 ~/moq-rs-top-n/tools/analyze_flamegraph.py $REMOTE_DIR/collapsed.txt $REMOTE_DIR/analysis.txt

kill \$RELAY_PID 2>/dev/null || true
echo "DONE" > $REMOTE_DIR/status.txt
PERF_SCRIPT

echo "  Done."

# Step 4: Wait for completion and verify
echo "[4/8] Verifying completion..."
STATUS=$($SSH "cat $REMOTE_DIR/status.txt 2>/dev/null || echo FAILED")
if [ "$STATUS" != "DONE" ]; then
  echo "  ERROR: Test did not complete successfully"
  exit 1
fi
echo "  Done."

# Step 5: Collect results
echo "[5/8] Collecting results from remote..."
$SCP "$REMOTE:$REMOTE_DIR/flamegraph.svg" "$LOCAL_DIR/flamegraph.svg"
$SCP "$REMOTE:$REMOTE_DIR/collapsed.txt" "$LOCAL_DIR/collapsed.txt"
$SCP "$REMOTE:$REMOTE_DIR/analysis.txt" "$LOCAL_DIR/analysis.txt"
$SCP "$REMOTE:$REMOTE_DIR/test_output.txt" "$LOCAL_DIR/test_output.txt"
$SCP "$REMOTE:$REMOTE_DIR/relay.log" "$LOCAL_DIR/relay.log"
$SCP "$REMOTE:$REMOTE_DIR/mem_before.txt" "$LOCAL_DIR/mem_before.txt"
$SCP "$REMOTE:$REMOTE_DIR/mem_after.txt" "$LOCAL_DIR/mem_after.txt"
echo "  Done."

# Step 6: Run speech activity analysis
echo "[6/8] Running speech activity analysis..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "$SCRIPT_DIR/analyze_speech.py" "$LOCAL_DIR/test_output.txt" > "$LOCAL_DIR/speech_analysis.txt" 2>/dev/null || true
echo "  Done."

# Step 7: Extract test metrics from output
echo "[7/8] Extracting metrics..."
TEST_OUTPUT="$LOCAL_DIR/test_output.txt"

# Memory
MEM_BEFORE=$(cat "$LOCAL_DIR/mem_before.txt" | tr -d ' ')
MEM_AFTER=$(cat "$LOCAL_DIR/mem_after.txt" | tr -d ' ')
MEM_BEFORE_MB=$(echo "scale=1; $MEM_BEFORE / 1024" | bc 2>/dev/null || echo "N/A")
MEM_AFTER_MB=$(echo "scale=1; $MEM_AFTER / 1024" | bc 2>/dev/null || echo "N/A")
MEM_GROWTH_MB=$(echo "scale=1; ($MEM_AFTER - $MEM_BEFORE) / 1024" | bc 2>/dev/null || echo "N/A")

# Extract from analysis
TOPN_SELF=$(grep "Total Top-N self time" "$LOCAL_DIR/analysis.txt" | grep -o "[0-9.]*%" | head -1 || echo "N/A")
TOPN_INCLUSIVE=$(grep "Top-N compute (inclusive)" "$LOCAL_DIR/analysis.txt" | grep -o "[0-9.]*%" | head -1 || echo "N/A")
QUIC_SELF=$(grep "QUIC transport (self)" "$LOCAL_DIR/analysis.txt" | grep -o "[0-9.]*%" | head -1 || echo "N/A")
QUIC_INCLUSIVE=$(grep "QUIC Transport" "$LOCAL_DIR/analysis.txt" | head -1 | grep -o "[0-9.]*%" | head -1 || echo "N/A")
MOQ_INCLUSIVE=$(grep "MOQ protocol (inclusive)" "$LOCAL_DIR/analysis.txt" | grep -o "[0-9.]*%" | head -1 || echo "N/A")
ALLOC_SELF=$(grep "Memory Allocation" "$LOCAL_DIR/analysis.txt" | tail -1 | grep -o "[0-9.]*%" | head -1 || echo "N/A")

echo "  Done."

# Step 8: Generate report
echo "[8/8] Generating report..."
REPORT="$LOCAL_DIR/report.md"
SYSINFO=$(cat "$LOCAL_DIR/sysinfo.txt")
ANALYSIS=$(cat "$LOCAL_DIR/analysis.txt")
SPEECH=$(cat "$LOCAL_DIR/speech_analysis.txt" 2>/dev/null || echo "No speech data available")

MIXED_NOTE=""
if [ -n "$MIXED_TOPN" ]; then
  MIXED_NOTE="| Mixed top-N values | $MIXED_TOPN |"
fi

cat > "$REPORT" <<EOF
# MOQ-RS Relay Performance Report

**Date:** $(date -u '+%Y-%m-%d %H:%M:%S UTC')
**Commit:** $(grep "Git commit:" "$LOCAL_DIR/sysinfo.txt" | cut -d: -f2- | xargs)
**Branch:** $(grep "Git branch:" "$LOCAL_DIR/sysinfo.txt" | cut -d: -f2- | xargs)

## Test Parameters

| Parameter | Value |
|-----------|-------|
| Publishers (pub-sub) | $PUB_SUBS |
| Pure subscribers | $PURE_SUBS |
| Total subscribers | $TOTAL_SUBS |
| Top-N filter | $TOP_N |
$MIXED_NOTE
| Duration | ${DURATION}s |
| Steady-state profiling | ${STEADY_DURATION}s (after ${SETUP_WAIT}s setup) |
| Group interval | 33ms (~30 fps) |
| Connection batch size | 50 |

## System Configuration

\`\`\`
$SYSINFO
\`\`\`

## Results Summary

### CPU Profile (Steady-State Only)

| Component | Self Time | Inclusive Time |
|-----------|-----------|----------------|
| **Top-N** | $TOPN_SELF | $TOPN_INCLUSIVE |
| QUIC Transport | $QUIC_SELF | $QUIC_INCLUSIVE |
| MOQ Protocol | — | $MOQ_INCLUSIVE |
| Memory Allocation | $ALLOC_SELF | — |

### Memory Usage

| Metric | Value |
|--------|-------|
| RSS before test | ${MEM_BEFORE_MB} MB |
| RSS after test | ${MEM_AFTER_MB} MB |
| Growth | ${MEM_GROWTH_MB} MB |
| Per connection | $(echo "scale=0; ($MEM_AFTER - $MEM_BEFORE) / $TOTAL_SUBS" | bc 2>/dev/null || echo "N/A") KB |

### Top-N vs QUIC Relative Cost

- Self vs self: Top-N $TOPN_SELF vs QUIC $QUIC_SELF
- Inclusive vs inclusive: Top-N $TOPN_INCLUSIVE vs QUIC $QUIC_INCLUSIVE

## Methodology

### Profiling Approach

1. **Steady-state isolation**: The relay and all ${TOTAL_SUBS} connections are established
   during a ${SETUP_WAIT}s warm-up period. CPU profiling begins only after setup completes,
   capturing ${STEADY_DURATION}s of pure steady-state operation. This excludes one-time costs
   like TLS handshakes, session setup, SUBSCRIBE exchanges, and filter registration.

2. **Sampling**: Linux \`perf record\` at 999 Hz with call-graph (\`-g\`) on the relay process.
   Stacks are collapsed via FlameGraph's \`stackcollapse-perf.pl\` and categorized by
   function name patterns into: Top-N Compute, Top-N Filter, QUIC Transport, Tokio Runtime,
   MOQ Protocol, and Memory Allocation.

3. **Speech simulation**: Publishers emit audio-level values at 30 Hz (33ms groups).
   Each publisher independently transitions between silent (value=0) and speaking (value>0)
   with p(start)=0.03/tick and random duration 3-10s. This creates realistic dynamic
   top-N ranking churn.

### Speech Simulator State Machine

\`\`\`
                    p=0.03/tick
    ┌─────────┐ ──────────────────► ┌──────────────┐
    │         │                      │              │
    │  SILENT │                      │ SPEECH_START │
    │ value=0 │ ◄──────────────────  │   value=2    │
    │         │   duration expired   │              │
    └─────────┘                      └──────┬───────┘
         ▲                                  │
         │                                  │ next tick
         │                                  ▼
         │          duration expired   ┌──────────┐
         └──────────────────────────── │ SPEAKING │
                                       │ value=1  │
                                       │          │
                                       └──────────┘
                                        (3-10s random)

    Transitions:
      SILENT → SPEECH_START : p=0.03 per tick (30 Hz), emit value=2
      SPEECH_START → SPEAKING: next tick, emit value=1
      SPEAKING → SPEAKING   : each tick until duration expires, emit value=1
      SPEAKING → SILENT     : duration (3-10s uniform) expired, emit value=0

    Values sent as object payload each group interval (33ms):
      0 = silent (not ranked)
      1 = speaking (ranked, maintains position)
      2 = speech_start (ranked, signals new utterance)
\`\`\`

4. **Memory**: RSS measured via \`ps -o rss\` before connections and after steady-state.
   Growth reflects per-connection overhead (QUIC state, stream buffers, filter state).

### What Top-N Does Per Object

- **Ingest observer** (1 per track): reads each published object, calls \`update_track_value\`
  if the audio level changed → mutex lock, BTreeMap update, ArcSwap snapshot rebuild.
- **Subscriber filter** (1 per subscriber per track): atomic epoch load + comparison.
  If epoch unchanged, uses cached decision. If changed, loads ArcSwap snapshot and does
  binary search on pre-sorted Vec.
- Both paths are O(1) amortized for the common case (epoch cache hit).

## Speech Activity Analysis

\`\`\`
$SPEECH
\`\`\`

## Detailed Flamegraph Analysis

\`\`\`
$ANALYSIS
\`\`\`

## Artifacts

- \`flamegraph.svg\` — Interactive flamegraph (open in browser)
- \`collapsed.txt\` — Collapsed stacks for custom analysis
- \`test_output.txt\` — Full test driver output
- \`relay.log\` — Relay server logs
- \`analysis.txt\` — Raw flamegraph analysis
- \`speech_analysis.txt\` — Speech activity breakdown

EOF

echo "  Done."
echo ""
echo "=== Report generated: $REPORT ==="
echo "=== Flamegraph:       $LOCAL_DIR/flamegraph.svg ==="
echo ""

# Open results directory and flamegraph in browser (macOS)
if command -v open &>/dev/null; then
  open "$LOCAL_DIR"
  open -a "Google Chrome" "$LOCAL_DIR/flamegraph.svg" 2>/dev/null || open "$LOCAL_DIR/flamegraph.svg"
fi
