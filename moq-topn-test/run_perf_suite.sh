#!/bin/bash
# Perf profiling suite for moq-rs Top-N performance analysis.
# Runs on Linux with perf support. Produces flamegraphs + automated analysis.
#
# Usage: ./run_perf_suite.sh [--relay-bin PATH] [--test-bin PATH] [--results-dir PATH]
#
# Prerequisites:
#   - Linux with perf installed
#   - ~/FlameGraph (git clone https://github.com/brendangregg/FlameGraph.git ~/FlameGraph)
#   - Release build with debug info: cargo build --release -p moq-relay-ietf -p moq-topn-test
#   - sudo sysctl kernel.perf_event_paranoid=-1
#   - sudo sysctl kernel.kptr_restrict=0
#   - ulimit -n 65536

set -e

RELAY_BIN="${RELAY_BIN:-./target/release/moq-relay-ietf}"
TEST_BIN="${TEST_BIN:-./target/release/moq-topn-test}"
DURATION=120
PANELISTS=80
SUBSCRIBERS=800
RESULTS_DIR="${RESULTS_DIR:-/tmp/moq-rs-perf-results}"
FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-$HOME/FlameGraph}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOOLS_DIR="$(cd "$SCRIPT_DIR/../tools" && pwd)"

mkdir -p "$RESULTS_DIR"

echo "=== moq-rs Top-N Performance Suite ==="
echo "  Relay:       $RELAY_BIN"
echo "  Test binary: $TEST_BIN"
echo "  Duration:    ${DURATION}s"
echo "  Scale:       ${PANELISTS} publishers, ${SUBSCRIBERS} subscribers"
echo "  Results:     $RESULTS_DIR"
echo ""

# Verify prerequisites
if ! command -v perf &>/dev/null; then
    echo "ERROR: 'perf' not found. Install linux-perf."
    exit 1
fi

if [ ! -d "$FLAMEGRAPH_DIR" ]; then
    echo "ERROR: FlameGraph not found at $FLAMEGRAPH_DIR"
    echo "  git clone --depth 1 https://github.com/brendangregg/FlameGraph.git ~/FlameGraph"
    exit 1
fi

if [ ! -f "$RELAY_BIN" ] || [ ! -f "$TEST_BIN" ]; then
    echo "Building release binaries with debug info..."
    cargo build --release -p moq-relay-ietf -p moq-topn-test
fi

# Verify debug info
if ! file "$RELAY_BIN" | grep -q "not stripped"; then
    echo "WARNING: $RELAY_BIN appears to be stripped. Debug info recommended."
    echo "  Ensure [profile.release] debug = true in Cargo.toml"
fi

run_test() {
    local name=$1
    local top_n=$2
    local extra_args=$3

    echo ""
    echo "=== Test: $name (N=$top_n, ${PANELISTS}p/${SUBSCRIBERS}s) ==="
    echo ""

    # Kill any previous relay
    pkill -f moq-relay-ietf 2>/dev/null || true
    sleep 1

    # Start relay
    TOPN_LOG=1 "$RELAY_BIN" --bind "[::]:4443" \
        > "$RESULTS_DIR/relay_${name}.log" 2>&1 &
    RELAY_PID=$!
    echo "  Relay started (PID=$RELAY_PID)"
    sleep 2

    # Start perf recording (duration + 10s for ramp)
    echo "  Starting perf record..."
    perf record -F 999 -p $RELAY_PID -g -o "$RESULTS_DIR/perf_${name}.data" -- sleep $((DURATION + 10)) &
    PERF_PID=$!

    # Run load test
    echo "  Running load test (${DURATION}s)..."
    "$TEST_BIN" -m e2e --relay https://localhost:4443 --tls-disable-verify \
        -x $PANELISTS -y $SUBSCRIBERS -n $top_n -d $DURATION \
        --group-interval-ms 33 $extra_args \
        > "$RESULTS_DIR/result_${name}.txt" 2>&1 || true

    echo "  Load test complete. Waiting for perf..."
    wait $PERF_PID 2>/dev/null || true

    # Generate flamegraph
    echo "  Generating flamegraph..."
    perf script -i "$RESULTS_DIR/perf_${name}.data" | \
        "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" > "$RESULTS_DIR/collapsed_${name}.txt"
    cat "$RESULTS_DIR/collapsed_${name}.txt" | "$FLAMEGRAPH_DIR/flamegraph.pl" \
        --title "moq-rs - $name (${PANELISTS}p/${SUBSCRIBERS}s)" \
        --width 1200 > "$RESULTS_DIR/flamegraph_${name}.svg"

    # Automated analysis
    echo "  Running analysis..."
    python3 "$TOOLS_DIR/analyze_flamegraph.py" \
        "$RESULTS_DIR/collapsed_${name}.txt" "$RESULTS_DIR/analysis_${name}.txt"

    # Extract TOPN_EVENT for visualization (if present)
    if grep -q TOPN_EVENT "$RESULTS_DIR/relay_${name}.log" 2>/dev/null; then
        grep TOPN_EVENT "$RESULTS_DIR/relay_${name}.log" | \
            sed 's/.*TOPN_EVENT: //' > "$RESULTS_DIR/events_${name}.log"
    fi

    kill $RELAY_PID 2>/dev/null || true
    wait $RELAY_PID 2>/dev/null || true

    echo "  Done: $name"
    echo "    Flamegraph: $RESULTS_DIR/flamegraph_${name}.svg"
    echo "    Analysis:   $RESULTS_DIR/analysis_${name}.txt"
    echo "    Test output: $RESULTS_DIR/result_${name}.txt"
}

# Test 1: N=45 (normal case)
run_test "n45" 45 ""

# Test 2: N=80 (degenerate — N equals panelists)
run_test "n80" 80 ""

# Test 3: Mixed N values
run_test "mixed" 45 "--mixed-topn 1,10,25,45,65,77,85"

echo ""
echo "=== All tests complete ==="
echo "Results in: $RESULTS_DIR/"
echo "  Flamegraphs: flamegraph_*.svg"
echo "  Analysis:    analysis_*.txt"
echo "  Test output: result_*.txt"
