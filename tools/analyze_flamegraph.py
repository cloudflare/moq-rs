#!/usr/bin/env python3
"""Analyze collapsed perf stacks and produce a text breakdown of CPU usage for moq-rs."""

import sys
from collections import defaultdict


CATEGORIES = [
    ("TopN Compute", [
        "top_n_tracker",
        "compute_top_n",
        "rebuild_snapshot",
        "update_value",
        "register_track",
        "subscriber_registry",
    ]),
    ("TopN Filter", [
        "track_filter",
        "property_check",
    ]),
    ("QUIC Transport", [
        "quinn::",
        "quic::",
        "quinn_proto",
        "quinn_udp",
    ]),
    ("HTTP/WebTransport", [
        "h3::",
        "web_transport",
        "webtransport",
    ]),
    ("Tokio Runtime", [
        "tokio::",
        "mio::",
        "poll::",
    ]),
    ("MOQ Protocol", [
        "moq_transport",
        "moq_relay",
        "encode",
        "decode",
    ]),
    ("Memory Allocation", [
        "alloc::",
        "malloc",
        "free",
        "realloc",
        "jemalloc",
    ]),
]

TOPN_PATTERNS = {
    "update_value": "top_n_tracker::.*update_value",
    "rebuild_snapshot": "top_n_tracker::.*rebuild_snapshot",
    "register_track": "top_n_tracker::.*register_track",
    "compute_top_n": "top_n_tracker::.*compute_top_n",
    "subscriber_registry": "subscriber_registry::",
}


def parse_collapsed(filepath):
    """Parse a collapsed stack file (output of stackcollapse-perf.pl)."""
    stacks = []
    total_weight = 0
    for line in open(filepath):
        line = line.strip()
        if not line:
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) != 2:
            continue
        stack_str, weight_str = parts
        try:
            weight = int(weight_str)
        except ValueError:
            try:
                weight = int(float(weight_str))
            except ValueError:
                continue
        stacks.append((stack_str, weight))
        total_weight += weight
    return stacks, total_weight


def categorize_inclusive(stacks, total_weight):
    """Categorize by inclusive time (frame appears anywhere in stack)."""
    cat_weights = defaultdict(int)
    for stack_str, weight in stacks:
        matched_cats = set()
        for cat_name, patterns in CATEGORIES:
            for pat in patterns:
                if pat in stack_str:
                    matched_cats.add(cat_name)
                    break
        for cat in matched_cats:
            cat_weights[cat] += weight
    return cat_weights


def categorize_exclusive(stacks, total_weight):
    """Categorize by exclusive time (leaf frame only)."""
    cat_weights = defaultdict(int)
    for stack_str, weight in stacks:
        frames = stack_str.split(";")
        leaf = frames[-1] if frames else ""
        matched = False
        for cat_name, patterns in CATEGORIES:
            for pat in patterns:
                if pat in leaf:
                    cat_weights[cat_name] += weight
                    matched = True
                    break
            if matched:
                break
        if not matched:
            cat_weights["Other"] += weight
    return cat_weights


def top_functions_self(stacks, total_weight, n=25):
    """Get top N functions by self (leaf) time."""
    func_weights = defaultdict(int)
    for stack_str, weight in stacks:
        frames = stack_str.split(";")
        leaf = frames[-1] if frames else ""
        func_weights[leaf] += weight
    sorted_funcs = sorted(func_weights.items(), key=lambda x: -x[1])
    return sorted_funcs[:n]


def topn_specific_breakdown(stacks, total_weight):
    """Detailed breakdown of top-N related work."""
    detail = defaultdict(int)
    for stack_str, weight in stacks:
        frames = stack_str.split(";")
        leaf = frames[-1] if frames else ""
        for label, pat in TOPN_PATTERNS.items():
            if pat.replace("::.*", "::") in leaf or pat.replace(".*", "") in leaf:
                detail[label] += weight
                break
    return detail


def main():
    if len(sys.argv) < 2:
        print("Usage: analyze_flamegraph.py <collapsed_stacks_file> [output_file]")
        print("  Generate collapsed stacks with:")
        print("    perf script | stackcollapse-perf.pl > collapsed.txt")
        sys.exit(1)

    filepath = sys.argv[1]
    output = sys.argv[2] if len(sys.argv) > 2 else None

    stacks, total_weight = parse_collapsed(filepath)
    total_samples = len(stacks)

    lines = []
    def p(s=""):
        lines.append(s)

    p("=" * 80)
    p("              FLAMEGRAPH ANALYSIS - moq-rs relay CPU profile")
    p("=" * 80)
    p()
    p(f"Total unique stacks: {total_samples}")
    p(f"Total weight:        {total_weight:,}")
    p()

    # Inclusive breakdown
    p("-" * 80)
    p("INCLUSIVE CPU TIME (frame appears anywhere in the call stack)")
    p("  Note: percentages overlap because callers include callee time")
    p("-" * 80)
    inclusive = categorize_inclusive(stacks, total_weight)
    for cat_name, _ in CATEGORIES:
        w = inclusive.get(cat_name, 0)
        pct = w / total_weight * 100 if total_weight else 0
        bar = "#" * int(pct / 2)
        p(f"  {cat_name:<35s} {pct:6.2f}%  {bar}")
    p()

    # Exclusive breakdown
    p("-" * 80)
    p("EXCLUSIVE CPU TIME (leaf frame only - where CPU actually spends cycles)")
    p("  Note: percentages do NOT overlap, total = 100%")
    p("-" * 80)
    exclusive = categorize_exclusive(stacks, total_weight)
    sorted_excl = sorted(exclusive.items(), key=lambda x: -x[1])
    for cat_name, w in sorted_excl:
        pct = w / total_weight * 100 if total_weight else 0
        bar = "#" * int(pct / 2)
        p(f"  {cat_name:<35s} {pct:6.2f}%  {bar}")
    p()

    # Top-N specific
    p("-" * 80)
    p("TOP-N SPECIFIC BREAKDOWN (self time in top-N related functions)")
    p("-" * 80)
    topn_detail = topn_specific_breakdown(stacks, total_weight)
    topn_total = sum(topn_detail.values())
    topn_pct = topn_total / total_weight * 100 if total_weight else 0
    p(f"  Total Top-N self time: {topn_pct:.2f}% of all CPU")
    p()
    sorted_topn = sorted(topn_detail.items(), key=lambda x: -x[1])
    for func, w in sorted_topn:
        pct = w / total_weight * 100 if total_weight else 0
        p(f"    {func:<40s} {pct:5.2f}%")
    p()

    # Top functions by self time
    p("-" * 80)
    p("TOP 25 FUNCTIONS BY SELF (LEAF) TIME")
    p("-" * 80)
    top_funcs = top_functions_self(stacks, total_weight)
    for i, (func, w) in enumerate(top_funcs, 1):
        pct = w / total_weight * 100 if total_weight else 0
        name = func if len(func) <= 70 else func[:67] + "..."
        p(f"  {i:2d}. {pct:5.2f}%  {name}")
    p()

    # Summary
    p("=" * 80)
    p("SUMMARY")
    p("=" * 80)
    topn_incl = inclusive.get("TopN Compute", 0) / total_weight * 100 if total_weight else 0
    topn_filt_incl = inclusive.get("TopN Filter", 0) / total_weight * 100 if total_weight else 0
    quic_incl = inclusive.get("QUIC Transport", 0) / total_weight * 100 if total_weight else 0
    quic_excl = exclusive.get("QUIC Transport", 0) / total_weight * 100 if total_weight else 0
    topn_excl = exclusive.get("TopN Compute", 0) / total_weight * 100 if total_weight else 0
    moq_incl = inclusive.get("MOQ Protocol", 0) / total_weight * 100 if total_weight else 0
    p(f"  Top-N compute (inclusive): {topn_incl:.1f}% - ranking decisions & value tracking")
    p(f"  Top-N filter (inclusive):  {topn_filt_incl:.1f}% - property check + filter interception")
    p(f"  Top-N self time:           {topn_pct:.1f}% - actual CPU in top-N code (no callees)")
    p(f"  QUIC transport (self):     {quic_excl:.1f}% - packet I/O & stream management")
    p(f"  MOQ protocol (inclusive):  {moq_incl:.1f}% - encode/decode + session management")
    p()
    p("-" * 80)
    p("TOP-N vs QUIC RELATIVE COST")
    p("-" * 80)
    if quic_excl > 0 and topn_pct > 0:
        self_ratio = topn_pct / quic_excl
        p(f"  Self vs self:           Top-N {topn_pct:.1f}% vs QUIC {quic_excl:.1f}% → Top-N is 1/{int(1/self_ratio)} of QUIC")
    else:
        p(f"  Self vs self:           Top-N {topn_pct:.1f}% vs QUIC {quic_excl:.1f}%")
    if quic_incl > 0 and topn_incl > 0:
        incl_ratio = topn_incl / quic_incl
        p(f"  Inclusive vs inclusive:  Top-N {topn_incl:.1f}% vs QUIC {quic_incl:.1f}% → Top-N is 1/{int(1/incl_ratio)} of QUIC")
    else:
        p(f"  Inclusive vs inclusive:  Top-N {topn_incl:.1f}% vs QUIC {quic_incl:.1f}%")
    p()
    p(f"  Interpretation: Top-N ranking adds ~{topn_pct:.1f}% CPU overhead (self time).")
    p("  The vast majority of CPU goes to QUIC transport and fan-out to subscribers.")
    p()

    report = "\n".join(lines)
    print(report)

    if output:
        with open(output, "w") as f:
            f.write(report + "\n")
        print(f"\nReport written to: {output}")


if __name__ == "__main__":
    main()
