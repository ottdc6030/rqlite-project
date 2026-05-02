#!/usr/bin/env python3
"""
rqlite test runner — fuzz or YCSB benchmark.

Usage:
  python3 run_tests.py fuzz --urls HOST:PORT[,...] [options]
  python3 run_tests.py fuzz --ips IP[,...] --ports PORT[,...] [options]
  python3 run_tests.py ycsb --urls HOST:PORT[,...] --workload FILE [options]
  python3 run_tests.py ycsb --ips IP[,...] --ports PORT[,...] --workload FILE [options]

Node addresses can be supplied in two ways:
  --urls  HOST:PORT,...   explicit full list (http:// prepended automatically)
  --ips + --ports        each IP is one cluster; every port listed is added as
                         a node for that cluster, yielding IP:PORT for every
                         (IP, PORT) combination in order.

All options without a default listed below have their defaults controlled by
the underlying Java tool, not by this script.

Examples:
  # Consistency fuzzer: 6 nodes via --urls, replication-factor 3, 8 threads
  python3 run_tests.py fuzz \\
      --urls 10.0.0.1:4001,10.0.0.2:4001,10.0.0.3:4001,\\
             10.0.0.4:4001,10.0.0.5:4001,10.0.0.6:4001 \\
      --replication-factor 3 --threads 8 --num-ops 5000

  # Same 6 nodes expressed with --ips / --ports (2 clusters × 3 ports each)
  python3 run_tests.py fuzz \\
      --ips 10.0.0.1,10.0.0.2 --ports 4001,4002,4003 \\
      --replication-factor 3 --threads 8 --num-ops 5000

  # YCSB workload A: 3 nodes, replication-factor 3, 8 threads
  python3 run_tests.py ycsb \\
      --urls 10.0.0.1:4001,10.0.0.2:4001,10.0.0.3:4001 \\
      --replication-factor 3 --workload a --threads 8
"""

import argparse
import os
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FUZZER_JAR = os.path.join(SCRIPT_DIR, "rqlite-fuzzer", "target", "rqlite-fuzzer.jar")
YCSB_BIN   = os.path.join(SCRIPT_DIR, "ycsb-0.17.0", "bin", "ycsb")

# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def format_urls(raw: str) -> str:
    """Prepend http:// to each comma-separated HOST:PORT entry if needed."""
    parts = [u.strip() for u in raw.split(",") if u.strip()]
    result = []
    for part in parts:
        if part.startswith("http://") or part.startswith("https://"):
            result.append(part)
        else:
            result.append("http://" + part)
    return ",".join(result)


def urls_from_ips_ports(raw_ips: str, raw_ports: str) -> str:
    """Build a comma-separated HOST:PORT list from --ips and --ports.

    Each IP represents one cluster.  Every port is added as a node for
    every IP, preserving the order: all ports for IP[0], then all ports
    for IP[1], etc.  The result is then passed through format_urls so
    http:// is prepended consistently.

    Example:
        ips="10.0.0.1,10.0.0.2", ports="4001,4002"
        → "http://10.0.0.1:4001,http://10.0.0.1:4002,
            http://10.0.0.2:4001,http://10.0.0.2:4002"
    """
    ips   = [ip.strip() for ip in raw_ips.split(",")   if ip.strip()]
    ports = [p.strip()  for p  in raw_ports.split(",") if p.strip()]
    if not ips:
        raise ValueError("--ips contained no valid addresses.")
    if not ports:
        raise ValueError("--ports contained no valid ports.")
    pairs = [f"{ip}:{port}" for ip in ips for port in ports]
    return format_urls(",".join(pairs))

# ---------------------------------------------------------------------------
# Shared property builders
# ---------------------------------------------------------------------------

def _append_shared(cmd: list, args: argparse.Namespace) -> None:
    """Append flags/values shared between fuzz and YCSB to cmd in-place."""
    if args.replication_factor is not None:
        cmd += ["--replication-factor", str(args.replication_factor)]
    if args.threads is not None:
        cmd += ["--threads", str(args.threads)]
    if args.consistency_level is not None:
        cmd += ["--consistency-level", args.consistency_level]
    if args.follow_leader:
        cmd += ["--follow-leader"]
    if args.timeout is not None:
        cmd += ["--timeout", str(args.timeout)]
    if args.connect_timeout is not None:
        cmd += ["--connect-timeout", str(args.connect_timeout)]

# ---------------------------------------------------------------------------
# Fuzz mode
# ---------------------------------------------------------------------------

def run_fuzz(args: argparse.Namespace, urls: str) -> int:
    if not os.path.isfile(FUZZER_JAR):
        print(f"Error: fuzzer JAR not found at {FUZZER_JAR}", file=sys.stderr)
        print("Run ./build.sh first.", file=sys.stderr)
        return 1

    cmd = ["java", "-jar", FUZZER_JAR, "--urls", urls]
    _append_shared(cmd, args)

    if args.num_keys is not None:
        cmd += ["--num-keys", str(args.num_keys)]
    if args.num_ops is not None:
        cmd += ["--num-ops", str(args.num_ops)]
    if args.conflict:
        cmd += ["--conflict"]
    if args.table is not None:
        cmd += ["--table", args.table]
    if args.seed is not None:
        cmd += ["--seed", str(args.seed)]

    print("=== Fuzz Test ===")
    print("  " + " ".join(cmd))
    print()
    sys.stdout.flush()
    return subprocess.run(cmd).returncode

# ---------------------------------------------------------------------------
# YCSB mode
# ---------------------------------------------------------------------------

def _build_ycsb_props(args: argparse.Namespace, urls: str) -> list:
    """Return a flat list of -p key=value tokens for bin/ycsb."""
    props = ["-p", f"rqlite.urls={urls}"]

    if args.replication_factor is not None:
        props += ["-p", f"rqlite.replication.factor={args.replication_factor}"]
    if args.threads is not None:
        props += ["-p", f"threadcount={args.threads}"]
    if args.consistency_level is not None:
        props += ["-p", f"rqlite.read.consistency={args.consistency_level}"]
    if args.follow_leader:
        props += ["-p", "rqlite.follow.leader=true"]
    if args.timeout is not None:
        props += ["-p", f"rqlite.request.timeout={args.timeout}"]
    if args.connect_timeout is not None:
        props += ["-p", f"rqlite.connect.timeout={args.connect_timeout}"]
    if args.fieldnames is not None:
        props += ["-p", f"rqlite.fieldnames={args.fieldnames}"]
    if args.fieldcount is not None:
        props += ["-p", f"rqlite.fieldcount={args.fieldcount}"]
    if args.insert_replace:
        props += ["-p", "rqlite.insert.replace=true"]
    if args.write_transaction:
        props += ["-p", "rqlite.write.transaction=true"]

    return props


def run_ycsb(args: argparse.Namespace, urls: str) -> int:
    if args.workload is None:
        print("Error: --workload is required for ycsb mode.", file=sys.stderr)
        return 1

    if not os.path.isfile(YCSB_BIN):
        print(f"Error: YCSB binary not found at {YCSB_BIN}", file=sys.stderr)
        print("Run ./build.sh first.", file=sys.stderr)
        return 1

    # Resolve workload path: expand bare letter (a-f) to workload name,
    # then check the YCSB workloads dir for a bare name.
    workload_path = args.workload
    if len(workload_path) == 1 and workload_path.lower() in "abcdef":
        workload_path = "workload" + workload_path.lower()
    if not os.path.isabs(workload_path) and not os.path.isfile(workload_path):
        candidate = os.path.join(SCRIPT_DIR, "ycsb-0.17.0", "workloads", workload_path)
        if os.path.isfile(candidate):
            workload_path = candidate

    props = _build_ycsb_props(args, urls)

    # --- Load phase ---
    load_cmd = [YCSB_BIN, "load", "rqlite", "-P", workload_path] + props
    print("=== YCSB Load Phase ===")
    print("  " + " ".join(load_cmd))
    print()
    sys.stdout.flush()
    rc = subprocess.run(load_cmd).returncode
    if rc != 0:
        print(f"\nLoad phase failed (exit {rc}).", file=sys.stderr)
        return rc

    # --- Run phase ---
    run_cmd = [YCSB_BIN, "run", "rqlite", "-P", workload_path] + props
    print("=== YCSB Run Phase ===")
    print("  " + " ".join(run_cmd))
    print()
    sys.stdout.flush()
    return subprocess.run(run_cmd).returncode

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_tests.py",
        description="rqlite test runner — fuzz or YCSB benchmark.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "mode",
        choices=["fuzz", "ycsb"],
        help="'fuzz' for consistency fuzzer; 'ycsb' for YCSB load+run benchmark.",
    )

    # ---- Shared ----
    shared = parser.add_argument_group("shared arguments (fuzz and ycsb)")

    url_group = shared.add_mutually_exclusive_group(required=True)
    url_group.add_argument(
        "--urls",
        metavar="HOST:PORT[,...]",
        help=(
            "Comma-separated list of rqlite node addresses as HOST:PORT or IP:PORT. "
            "http:// is prepended automatically. "
            "Mutually exclusive with --ips/--ports."
        ),
    )
    url_group.add_argument(
        "--ips",
        metavar="IP[,...]",
        help=(
            "Comma-separated list of IP addresses, one per cluster. "
            "Must be used together with --ports. "
            "Mutually exclusive with --urls."
        ),
    )
    shared.add_argument(
        "--ports",
        metavar="PORT[,...]",
        help=(
            "Comma-separated list of ports. Combined with --ips to produce "
            "one HOST:PORT entry per (IP, port) pair. Required when --ips is used."
        ),
    )
    shared.add_argument(
        "--replication-factor",
        type=int,
        dest="replication_factor",
        metavar="N",
        help=(
            "Number of nodes per cluster. "
            "URL count must be divisible by this value. "
            "E.g. 12 URLs with --replication-factor 3 → 4 clusters."
        ),
    )
    shared.add_argument(
        "--threads",
        type=int,
        metavar="N",
        help="Number of concurrent client threads.",
    )
    shared.add_argument(
        "--consistency-level",
        dest="consistency_level",
        metavar="LEVEL",
        help="rqlite read consistency: none | weak | linearizable | strong.",
    )
    shared.add_argument(
        "--follow-leader",
        action="store_true",
        dest="follow_leader",
        help="Discover and route writes to the Raft leader.",
    )
    shared.add_argument(
        "--timeout",
        type=int,
        metavar="SECS",
        help="HTTP request timeout in seconds.",
    )
    shared.add_argument(
        "--connect-timeout",
        type=int,
        dest="connect_timeout",
        metavar="SECS",
        help="HTTP connect timeout in seconds.",
    )

    # ---- Fuzz-only ----
    fuzz = parser.add_argument_group("fuzz-only arguments")

    fuzz.add_argument(
        "--num-keys",
        type=int,
        dest="num_keys",
        metavar="N",
        help="Number of keys in each thread's pool.",
    )
    fuzz.add_argument(
        "--num-ops",
        type=int,
        dest="num_ops",
        metavar="N",
        help="Number of operations per thread.",
    )
    fuzz.add_argument(
        "--conflict",
        action="store_true",
        help="Share one key pool across all threads instead of per-thread pools.",
    )
    fuzz.add_argument(
        "--table",
        metavar="NAME",
        help="SQL table name to use during the fuzz run.",
    )
    fuzz.add_argument(
        "--seed",
        type=int,
        metavar="N",
        help="Base RNG seed (thread i uses seed+i).",
    )

    # ---- YCSB-only ----
    ycsb = parser.add_argument_group("ycsb-only arguments")

    ycsb.add_argument(
        "--workload",
        metavar="WORKLOAD",
        help=(
            "Workload letter (a-f), name (e.g. 'workloada'), or full file path. "
            "A bare letter is expanded to the matching YCSB built-in workload. "
            "Required when mode is 'ycsb'."
        ),
    )
    ycsb.add_argument(
        "--fieldnames",
        metavar="NAMES",
        help="Explicit comma-separated column names (overrides --fieldcount).",
    )
    ycsb.add_argument(
        "--fieldcount",
        type=int,
        metavar="N",
        help="Number of auto-generated fields: field0 … fieldN-1.",
    )
    ycsb.add_argument(
        "--insert-replace",
        action="store_true",
        dest="insert_replace",
        help="Use INSERT OR REPLACE for idempotent load-phase re-runs.",
    )
    ycsb.add_argument(
        "--write-transaction",
        action="store_true",
        dest="write_transaction",
        help="Wrap multi-statement writes in a transaction.",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.urls is not None:
        urls = format_urls(args.urls)
    else:
        if not args.ports:
            parser.error("--ports is required when --ips is used.")
        try:
            urls = urls_from_ips_ports(args.ips, args.ports)
        except ValueError as exc:
            parser.error(str(exc))

    if args.mode == "fuzz":
        sys.exit(run_fuzz(args, urls))
    else:
        sys.exit(run_ycsb(args, urls))


if __name__ == "__main__":
    main()
