#!/usr/bin/env python3
"""
Run MOQ Top-N E2E test and generate interactive HTML visualization.

Usage:
    ./run_topn_relay.py [options]

Examples:
    # Run local relay + test with defaults
    ./run_topn_relay.py --publishers 3 --subscribers 10 --top-n 2

    # Connect to remote relay (no local relay started)
    ./run_topn_relay.py --relay https://myserver.com:4443 --publishers 3 --subscribers 10 --top-n 2

    # Run for specific duration
    ./run_topn_relay.py --duration 30 --publishers 5 --subscribers 20 --top-n 3
"""

import argparse
import subprocess
import sys
import os
import signal
import threading
import time
import tempfile
from pathlib import Path
from datetime import datetime

# ANSI colors
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def colored(text, color):
    return f"{color}{text}{Colors.ENDC}"

def find_project_root():
    """Find the moq-rs project root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "Cargo.toml").exists() and (current / "moq-relay-ietf").exists():
            return current
        current = current.parent
    return None

def build_if_needed(project_root, need_relay=True, verbose=False):
    """Build the required binaries if they don't exist."""
    relay_bin = project_root / "target" / "release" / "moq-relay-ietf"
    test_bin = project_root / "target" / "debug" / "moq-topn-test"
    html_tool = project_root / "target" / "debug" / "topn-log-to-html"

    needs_build = []
    if need_relay and not relay_bin.exists():
        needs_build.append(("moq-relay-ietf", "--release"))
    if not test_bin.exists() or not html_tool.exists():
        needs_build.append(("moq-topn-test", ""))

    if needs_build:
        for pkg, flags in needs_build:
            print(colored(f"Building {pkg}...", Colors.YELLOW))
            cmd = ["cargo", "build", "-p", pkg]
            if flags:
                cmd.append(flags)
            result = subprocess.run(cmd, cwd=project_root, capture_output=not verbose)
            if result.returncode != 0:
                print(colored(f"Build failed for {pkg}!", Colors.RED))
                if not verbose:
                    print(result.stderr.decode())
                sys.exit(1)
        print(colored("Build complete.", Colors.GREEN))

    return relay_bin, test_bin, html_tool

def start_relay(args, project_root, relay_bin):
    """Start the relay process in background."""
    cmd = [str(relay_bin)]

    if args.cert:
        cmd.extend(["--tls-cert", args.cert])
    else:
        default_cert = project_root / "dev" / "localhost.crt"
        if default_cert.exists():
            cmd.extend(["--tls-cert", str(default_cert)])

    if args.key:
        cmd.extend(["--tls-key", args.key])
    else:
        default_key = project_root / "dev" / "localhost.key"
        if default_key.exists():
            cmd.extend(["--tls-key", str(default_key)])

    cmd.extend(["--bind", f"[::]:{args.port}"])

    print(colored(f"\nStarting relay on port {args.port}...", Colors.CYAN))
    print(f"{colored('Command:', Colors.CYAN)} {' '.join(cmd)}")

    env = os.environ.copy()
    env["RUST_LOG"] = "moq_relay_ietf=info,moq_transport=warn"

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        cwd=project_root
    )

    # Give relay time to start
    time.sleep(2)

    if process.poll() is not None:
        print(colored("Relay failed to start!", Colors.RED))
        print(process.stderr.read().decode())
        sys.exit(1)

    print(colored("Relay started.", Colors.GREEN))
    return process

def run_test(args, project_root, test_bin, relay_url, events_file):
    """Run the e2e test and capture events."""
    cmd = [
        str(test_bin),
        "--mode", "e2e",
        "--relay", relay_url,
        "--tls-disable-verify",
        "--publishers", str(args.publishers),
        "--subscribers", str(args.subscribers),
        "--top-n", str(args.top_n),
        "--duration", str(args.duration),
    ]

    if args.group_interval:
        cmd.extend(["--group-interval-ms", str(args.group_interval)])

    print(colored("\n" + "="*60, Colors.HEADER))
    print(colored(" MOQ Top-N E2E Test", Colors.HEADER + Colors.BOLD))
    print(colored("="*60, Colors.HEADER))
    print(f"\n{colored('Relay:', Colors.CYAN)} {relay_url}")
    print(f"{colored('Publishers:', Colors.CYAN)} {args.publishers}")
    print(f"{colored('Subscribers:', Colors.CYAN)} {args.subscribers}")
    print(f"{colored('Top-N:', Colors.CYAN)} {args.top_n}")
    print(f"{colored('Duration:', Colors.CYAN)} {args.duration}s")
    print(f"{colored('Events file:', Colors.CYAN)} {events_file}")
    print()

    # Run test and capture output
    env = os.environ.copy()
    env["RUST_LOG"] = "moq_topn_test=info"

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        cwd=project_root
    )

    event_count = 0
    with open(events_file, 'w') as f:
        for line in iter(process.stdout.readline, b''):
            line_str = line.decode('utf-8', errors='replace')

            # Write all TOPN events to file
            if "TOPN_EVENT:" in line_str:
                f.write(line_str)
                f.flush()
                event_count += 1
                if args.show_events:
                    print(colored("[EVENT] ", Colors.GREEN) + line_str.strip())
            else:
                # Show test output
                if "INFO" in line_str:
                    # Clean up the log format for display
                    if "===" in line_str or "TOPN_TEST_RESULT" in line_str:
                        print(colored(line_str.rstrip(), Colors.BOLD))
                    elif "Accuracy:" in line_str:
                        print(colored(line_str.rstrip(), Colors.GREEN + Colors.BOLD))
                    else:
                        print(line_str.rstrip())
                elif "ERROR" in line_str or "error" in line_str:
                    print(colored(line_str.rstrip(), Colors.RED))
                elif "WARN" in line_str:
                    print(colored(line_str.rstrip(), Colors.YELLOW))
                elif args.verbose:
                    print(line_str.rstrip())

    process.wait()

    print(f"\n{colored('Events captured:', Colors.GREEN)} {event_count}")
    return event_count, process.returncode

def generate_html(html_tool, events_file, output_file):
    """Generate interactive HTML from events."""
    print(colored("\nGenerating interactive HTML visualization...", Colors.CYAN))

    result = subprocess.run(
        [str(html_tool), events_file, output_file],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(colored("Failed to generate HTML!", Colors.RED))
        print(result.stderr)
        return False

    print(colored(f"Generated: {output_file}", Colors.GREEN))
    return True

def main():
    parser = argparse.ArgumentParser(
        description="Run MOQ Top-N E2E test and generate visualization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Relay options
    parser.add_argument("--relay", "-r", type=str,
                        help="Remote relay URL (e.g., https://server:4443). If not specified, starts local relay.")
    parser.add_argument("--port", "-p", type=int, default=4443,
                        help="Port for local relay (default: 4443)")
    parser.add_argument("--cert", type=str,
                        help="TLS certificate file for local relay (default: dev/localhost.crt)")
    parser.add_argument("--key", type=str,
                        help="TLS key file for local relay (default: dev/localhost.key)")

    # Test options
    parser.add_argument("--publishers", "-x", type=int, default=3,
                        help="Number of publishers (default: 3)")
    parser.add_argument("--subscribers", "-y", type=int, default=10,
                        help="Number of subscribers (default: 10)")
    parser.add_argument("--top-n", "-n", type=int, default=2,
                        help="Top-N filter value (default: 2)")
    parser.add_argument("--duration", "-d", type=int, default=20,
                        help="Test duration in seconds (default: 20)")
    parser.add_argument("--group-interval", type=int,
                        help="Group interval in milliseconds (default: 2000)")

    # Output options
    parser.add_argument("--output", "-o", type=str,
                        help="Output HTML file (default: topn-viz-{timestamp}.html)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show all log output")
    parser.add_argument("--show-events", "-e", action="store_true",
                        help="Print TOPN events as they arrive")
    parser.add_argument("--no-html", action="store_true",
                        help="Don't generate HTML visualization")
    parser.add_argument("--no-open", action="store_true",
                        help="Don't open HTML in browser after generation")

    args = parser.parse_args()

    # Find project root
    project_root = find_project_root()
    if not project_root:
        print(colored("Could not find moq-rs project root!", Colors.RED))
        sys.exit(1)

    # Determine if we need local relay
    use_local_relay = args.relay is None
    relay_url = args.relay if args.relay else f"https://localhost:{args.port}"

    # Build if needed
    relay_bin, test_bin, html_tool = build_if_needed(
        project_root,
        need_relay=use_local_relay,
        verbose=args.verbose
    )

    # Setup output files
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    events_file = tempfile.mktemp(prefix="topn-events-", suffix=".log")

    if args.output:
        output_html = args.output
    else:
        output_html = str(project_root / "moq-topn-test" / f"topn-viz-{timestamp}.html")

    # Start local relay if needed
    relay_process = None
    if use_local_relay:
        relay_process = start_relay(args, project_root, relay_bin)

    try:
        # Run the test
        event_count, test_result = run_test(args, project_root, test_bin, relay_url, events_file)

        # Generate HTML
        if not args.no_html and event_count > 0:
            if generate_html(html_tool, events_file, output_html):
                print(colored(f"\nVisualization saved to: {output_html}", Colors.GREEN + Colors.BOLD))

                if not args.no_open:
                    print(colored("Opening in browser...", Colors.CYAN))
                    if sys.platform == "darwin":
                        subprocess.run(["open", output_html])
                    elif sys.platform == "linux":
                        subprocess.run(["xdg-open", output_html])
                    elif sys.platform == "win32":
                        os.startfile(output_html)
        elif event_count == 0:
            print(colored("\nNo TOPN events captured.", Colors.YELLOW))

    finally:
        # Stop local relay
        if relay_process:
            print(colored("\nStopping relay...", Colors.CYAN))
            relay_process.terminate()
            try:
                relay_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                relay_process.kill()

    print(colored("\nDone!", Colors.GREEN + Colors.BOLD))
    sys.exit(0 if test_result == 0 else 1)

if __name__ == "__main__":
    main()
