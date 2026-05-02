#!/usr/bin/env bash
# cluster.sh — Start / stop / inspect a local rqlite cluster using Podman.
#
# Usage:
#   ./cluster.sh [NODES [BASE_HTTP_PORT [CNI_VERSION]]]   start an N-node cluster
#   ./cluster.sh stop                                      tear down all cluster nodes + network
#   ./cluster.sh status                                    show the state of each node
#   ./cluster.sh urls [NODES [BASE_PORT]]                  print the --urls string for run_tests.py
#
# Node i is accessible on the host at:  localhost : (BASE_HTTP_PORT + i * PORT_STEP)
# Default: 3 nodes, base port 4001, CNI version 1.0.0  →  :4001, :4011, :4021
#
# CNI_VERSION: if anything other than the default (1.0.0) is given, the script
#   patches ~/.config/cni/net.d/$NETWORK.conflist with the requested version and
#   runs `podman system reset --force` before starting any containers.
#
# Examples
#   ./cluster.sh                    3-node cluster on :4001, :4011, :4021
#   ./cluster.sh 5                  5-node cluster on :4001 .. :4041
#   ./cluster.sh 3 5001             3-node cluster on :5001, :5011, :5021
#   ./cluster.sh 3 4001 0.4.0       3-node cluster, set CNI version to 0.4.0
#   ./cluster.sh stop               remove all containers + Podman network
#   ./cluster.sh status             print health of every rqlite container
#   ./cluster.sh urls               print URL list ready to paste into run_tests.py
#
# How it works
#   • A dedicated Podman network (rqlite-net) is created so containers
#     can reach each other by container name via Podman's built-in DNS.
#   • Node 0 is the bootstrap leader.  It starts first and must pass a
#     health check before any follower is launched.
#   • Followers join via  -join http://rqlite-node-0:4001
#   • HTTP_ADV_ADDR / RAFT_ADV_ADDR env vars are set explicitly so rqlite
#     advertises the right container-name address (not the FQDN from
#     `hostname -f`, which can confuse cross-node communication).
#   • Only HTTP port 4001 is mapped to the host.  Raft port 4002 stays
#     internal; nodes talk to each other through the Podman network.
#   • rqlite v9 -join takes a Raft address (host:port 4002), NOT an HTTP URL.

set -euo pipefail

# ── Constants ──────────────────────────────────────────────────────────────────
readonly IMAGE="ghcr.io/rqlite/rqlite:latest"
readonly NETWORK="rqlite-net"
readonly PREFIX="rqlite-node"
readonly PORT_STEP=10        # host port increment between successive nodes
readonly INTERNAL_HTTP=4001  # HTTP port inside every container (fixed)
readonly INTERNAL_RAFT=4002  # Raft port inside every container (fixed)
readonly HEALTH_RETRIES=45   # max number of health-check polls
readonly HEALTH_INTERVAL=1   # seconds between polls

# ── Argument parsing ───────────────────────────────────────────────────────────
SUBCMD="${1:-start}"

case "$SUBCMD" in
    stop)
        NODES=0
        BASE_PORT=4001
        CNI_VERSION="0.4.0"
        ;;
    status)
        NODES=0
        BASE_PORT=4001
        CNI_VERSION="0.4.0"
        ;;
    urls)
        NODES="${2:-3}"
        BASE_PORT="${3:-4001}"
        CNI_VERSION="0.4.0"
        ;;
    [0-9]*)
        NODES="$SUBCMD"
        BASE_PORT="${2:-4001}"
        CNI_VERSION="${3:-0.4.0}"
        SUBCMD="start"
        ;;
    start)
        NODES="${2:-3}"
        BASE_PORT="${3:-4001}"
        CNI_VERSION="${4:-0.4.0}"
        ;;
    -h|--help)
        sed -n '2,/^set -/{ /^set -/d; s/^# \{0,1\}//; p }' "$0"
        exit 0
        ;;
    *)
        echo "Unknown subcommand: $SUBCMD" >&2
        echo "Usage: $0 [NODES [BASE_HTTP_PORT]] | stop | status | urls [NODES [BASE_PORT]]" >&2
        exit 1
        ;;
esac

if [[ "$SUBCMD" == "start" && "$NODES" -lt 1 ]]; then
    echo "Error: NODES must be at least 1." >&2
    exit 1
fi

# ── Helpers ────────────────────────────────────────────────────────────────────

# Container name for node index i
cname() { echo "${PREFIX}-${1}"; }

# Host HTTP port for node index i
host_port() { echo $(( BASE_PORT + $1 * PORT_STEP )); }

# Poll /readyz until the node responds or we time out
wait_ready() {
    local name="$1" port="$2"
    local url="http://localhost:${port}/readyz"
    printf "    Waiting for %s to be ready" "$name"
    local i
    for i in $(seq 1 "$HEALTH_RETRIES"); do
        if curl -sf "$url" &>/dev/null; then
            echo "  [OK]"
            return 0
        fi
        sleep "$HEALTH_INTERVAL"
        printf "."
    done
    echo "  [TIMEOUT]"
    echo "--- Last 30 log lines from ${name} ---" >&2
    podman logs --tail 30 "$name" >&2
    echo "-------------------------------------" >&2
    return 1
}

# ── stop ──────────────────────────────────────────────────────────────────────
stop_cluster() {
    echo "Stopping rqlite cluster..."
    local removed=0

    # Collect all containers whose names start with the cluster prefix
    local names
    names=$(podman ps -a --format '{{.Names}}' 2>/dev/null \
            | grep "^${PREFIX}-" || true)

    while IFS= read -r name; do
        [[ -z "$name" ]] && continue
        echo "  Removing container: $name"
        podman rm -f "$name" &>/dev/null && (( removed++ )) || true
    done <<< "$names"

    if podman network exists "$NETWORK" 2>/dev/null; then
        podman network rm "$NETWORK" &>/dev/null \
            && echo "  Removed Podman network: $NETWORK" || true
    fi

    echo "Done. ($removed container(s) removed)"
}

# ── status ─────────────────────────────────────────────────────────────────────
show_status() {
    echo "rqlite cluster status:"
    local found=0
    local names
    names=$(podman ps -a --format '{{.Names}}' 2>/dev/null \
            | grep "^${PREFIX}-" || true)

    while IFS= read -r name; do
        [[ -z "$name" ]] && continue
        local state ports
        state=$(podman inspect --format '{{.State.Status}}' "$name" 2>/dev/null \
                || echo "unknown")
        # Collect all mapped ports (e.g. "4001/tcp -> 0.0.0.0:4001")
        ports=$(podman inspect \
                  --format '{{range $k,$v := .NetworkSettings.Ports}}{{range $v}}{{$k}}→{{.HostPort}} {{end}}{{end}}' \
                  "$name" 2>/dev/null || echo "")
        printf "  %-22s  state=%-10s  ports=%s\n" "$name" "$state" "$ports"
        (( found++ ))
    done <<< "$names"

    [[ $found -eq 0 ]] && echo "  (no cluster containers found)"
}

# ── print URL list ─────────────────────────────────────────────────────────────
print_urls() {
    local urls=""
    for i in $(seq 0 $(( NODES - 1 ))); do
        [[ -n "$urls" ]] && urls+=","
        urls+="localhost:$(host_port "$i")"
    done
    echo "$urls"
}

# ── start ─────────────────────────────────────────────────────────────────────
start_cluster() {
    echo "Starting ${NODES}-node rqlite cluster (base HTTP port ${BASE_PORT})..."
    echo ""

    # -- Remove stale containers from any prior run --------------------------
    local stale=0
    for i in $(seq 0 $(( NODES - 1 ))); do
        local n; n="$(cname "$i")"
        if podman container exists "$n" 2>/dev/null; then
            echo "  Removing stale container: $n"
            podman rm -f "$n" &>/dev/null
            (( stale++ ))
        fi
    done
    [[ $stale -gt 0 ]] && echo ""

    # -- Reset podman BEFORE network creation when a non-default CNI version --
    # is requested.  The reset wipes all networks (including their conflist
    # files), so the patch must happen AFTER the network is (re-)created.
    if [[ "$CNI_VERSION" != "1.0.0" ]]; then
        echo "  CNI version $CNI_VERSION requested; running podman system reset --force..."
        podman system reset --force
        echo "  Podman reset complete."
    fi

    # -- Ensure the dedicated Podman network exists --------------------------
    if ! podman network exists "$NETWORK" 2>/dev/null; then
        podman network create "$NETWORK"
        echo "  Created Podman network: $NETWORK"
    else
        echo "  Using existing Podman network: $NETWORK"
    fi

    # -- Patch conflist with the requested CNI version (non-default only) ----
    # The conflist is written by `podman network create` above, so it exists
    # at this point and the sed is guaranteed to find the right file.
    if [[ "$CNI_VERSION" != "1.0.0" ]]; then
        local conflist="$HOME/.config/cni/net.d/${NETWORK}.conflist"
        echo "  Patching CNI version to $CNI_VERSION in $conflist"
        sed -i "s/\"cniVersion\"[[:space:]]*:[[:space:]]*\"[^\"]*\"/\"cniVersion\": \"${CNI_VERSION}\"/" "$conflist"
    fi
    echo ""

    # -- Bootstrap node (node 0) ─────────────────────────────────────────────
    # Node 0 starts without a -join flag; it bootstraps as the Raft leader.
    # We pass CMD "run" (the container default) so the entrypoint applies all
    # its defaults.  HTTP_ADV_ADDR / RAFT_ADV_ADDR are overridden via env vars
    # so rqlite advertises the container's short DNS name (not the FQDN from
    # `hostname -f`) — that short name is what Podman's DNS resolves.
    local n0; n0="$(cname 0)"
    local p0; p0="$(host_port 0)"

    echo "  [1/${NODES}] Bootstrap node: $n0  →  http://localhost:${p0}"
    podman run -d \
        --name       "$n0" \
        --network    "$NETWORK" \
        -p           "${p0}:${INTERNAL_HTTP}" \
        -e           "HTTP_ADV_ADDR=${n0}:${INTERNAL_HTTP}" \
        -e           "RAFT_ADV_ADDR=${n0}:${INTERNAL_RAFT}" \
        "$IMAGE"     run

    wait_ready "$n0" "$p0"

    # -- Follower nodes (nodes 1 .. N-1) ─────────────────────────────────────
    # Passing a flag that starts with "-" triggers the entrypoint's merge path:
    #   rqlited <entrypoint-defaults> <user-flags> <data-dir>
    # NOTE: rqlite v9 -join expects the Raft address (port 4002), not HTTP.
    for i in $(seq 1 $(( NODES - 1 ))); do
        local name; name="$(cname "$i")"
        local port; port="$(host_port "$i")"
        local idx=$(( i + 1 ))

        echo "  [${idx}/${NODES}] Follower node:   $name  →  http://localhost:${port}"
        podman run -d \
            --name    "$name" \
            --network "$NETWORK" \
            -p        "${port}:${INTERNAL_HTTP}" \
            -e        "HTTP_ADV_ADDR=${name}:${INTERNAL_HTTP}" \
            -e        "RAFT_ADV_ADDR=${name}:${INTERNAL_RAFT}" \
            "$IMAGE"  -join "${n0}:${INTERNAL_RAFT}"

        wait_ready "$name" "$port"
    done

    # -- Summary ─────────────────────────────────────────────────────────────
    local urls; urls="$(print_urls)"

    echo ""
    echo "Cluster is ready.  ${NODES} node(s) running."
    echo ""
    echo "  Node addresses:"
    for i in $(seq 0 $(( NODES - 1 ))); do
        printf "    %s  →  http://localhost:%s\n" "$(cname "$i")" "$(host_port "$i")"
    done
    echo ""
    echo "  --urls value for run_tests.py:"
    echo "    $urls"
    echo ""
    echo "  Example commands:"
    echo "    python3 run_tests.py fuzz --urls $urls --threads 4 --num-ops 500 --replication-factor $NODES"
    echo "    python3 run_tests.py ycsb --urls $urls --workload a --threads 4 --replication-factor $NODES"
    echo ""
    echo "  To stop the cluster:"
    echo "    ./cluster.sh stop"
}

# ── Dispatch ───────────────────────────────────────────────────────────────────
case "$SUBCMD" in
    start)  start_cluster ;;
    stop)   stop_cluster  ;;
    status) show_status   ;;
    urls)   print_urls    ;;
esac
