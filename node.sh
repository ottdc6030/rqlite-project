#!/usr/bin/env bash
# node.sh — Start rqlite replicas on THIS machine as part of a multi-machine cluster.
#
# Run this script on each participating server.  Exactly one machine must use
# --leader to bootstrap the Raft cluster; every other machine uses --leader-ip
# to join it.  The script uses --network host so rqlited binds directly to the
# machine's real IP — no Podman network or port-mapping needed, and every other
# machine in the cluster can reach each node at MY_IP:PORT.
#
# Usage:
#   ./node.sh --my-ip IP --leader     [--replicas N] [--base-port P]
#   ./node.sh --my-ip IP --leader-ip IP [--leader-port P] [--replicas N] [--base-port P]
#   ./node.sh stop
#   ./node.sh status
#   ./node.sh urls  [--my-ip IP]  [--replicas N] [--base-port P]
#   ./node.sh urls  --all-ips IP,IP,...           [--replicas N] [--base-port P]
#
# Flags:
#   --my-ip IP        This machine's externally reachable IP address (required to start)
#   --leader          This machine bootstraps the cluster (node 0 becomes the Raft leader)
#   --leader-ip IP    IP of the leader machine; nodes on this machine join that cluster
#   --leader-port P   Raft port of the leader's node 0 (default: BASE_PORT + 1 = 4002)
#   --replicas N      Number of rqlite nodes to start on this machine (default: 1)
#   --base-port P     Starting HTTP port for node 0 on this machine (default: 4001)
#   --all-ips IP,...  Used with the 'urls' subcommand to print a combined URL list
#                     for every machine (one IP per machine, replicas ports each)
#
# Port scheme (per node i on this machine):
#   HTTP  BASE_PORT + i * 10
#   Raft  BASE_PORT + i * 10 + 1
#
#   Default (1 replica, base 4001): node 0 → HTTP :4001, Raft :4002
#   3 replicas, base 4001:          node 0 → :4001/:4002
#                                   node 1 → :4011/:4012
#                                   node 2 → :4021/:4022
#
# Typical 3-machine setup (3 replicas each = 9-node cluster):
#
#   Machine A — 10.0.0.1 (leader):
#     ./node.sh --my-ip 10.0.0.1 --leader --replicas 3
#
#   Machine B — 10.0.0.2:
#     ./node.sh --my-ip 10.0.0.2 --leader-ip 10.0.0.1 --replicas 3
#
#   Machine C — 10.0.0.3:
#     ./node.sh --my-ip 10.0.0.3 --leader-ip 10.0.0.1 --replicas 3
#
#   Print combined --urls for all 9 nodes (run on any machine):
#     ./node.sh urls --all-ips 10.0.0.1,10.0.0.2,10.0.0.3 --replicas 3
#
#   Then run tests (replication-factor = replicas per machine = 3):
#     python3 run_tests.py fuzz \
#         --urls 10.0.0.1:4001,10.0.0.1:4011,10.0.0.1:4021,\
#                10.0.0.2:4001,10.0.0.2:4011,10.0.0.2:4021,\
#                10.0.0.3:4001,10.0.0.3:4011,10.0.0.3:4021 \
#         --replication-factor 3 --threads 8

set -euo pipefail

# ── Constants ──────────────────────────────────────────────────────────────────
readonly IMAGE="ghcr.io/rqlite/rqlite:latest"
readonly PREFIX="rqlite-node"
readonly PORT_STEP=10      # gap between successive nodes' HTTP ports
readonly HEALTH_RETRIES=45
readonly HEALTH_INTERVAL=1

# ── Argument parsing ───────────────────────────────────────────────────────────
SUBCMD=""
MY_IP=""
IS_LEADER=false
LEADER_IP=""
LEADER_PORT=""      # resolved after BASE_PORT is known
REPLICAS=1
BASE_PORT=4001
ALL_IPS=""          # used only by 'urls' subcommand

# First token may be a subcommand word; everything else is a named flag.
if [[ $# -gt 0 ]]; then
    case "$1" in
        stop|status|urls|-h|--help)
            SUBCMD="$1"
            shift
            ;;
        *)
            SUBCMD="start"
            ;;
    esac
else
    SUBCMD="start"
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --my-ip)        MY_IP="$2";       shift 2 ;;
        --leader)       IS_LEADER=true;   shift   ;;
        --leader-ip)    LEADER_IP="$2";   shift 2 ;;
        --leader-port)  LEADER_PORT="$2"; shift 2 ;;
        --replicas)     REPLICAS="$2";    shift 2 ;;
        --base-port)    BASE_PORT="$2";   shift 2 ;;
        --all-ips)      ALL_IPS="$2";     shift 2 ;;
        -h|--help)
            sed -n '2,/^set -/{ /^set -/d; s/^# \{0,1\}//; p }' "$0"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Run ./node.sh --help for usage." >&2
            exit 1
            ;;
    esac
done

# Default leader Raft port = node 0's Raft port on the leader machine
LEADER_PORT="${LEADER_PORT:-$(( BASE_PORT + 1 ))}"

# ── Helpers ────────────────────────────────────────────────────────────────────

# Container name for node index i
cname()     { echo "${PREFIX}-${1}"; }

# HTTP port for node i on this machine
http_port() { echo $(( BASE_PORT + $1 * PORT_STEP )); }

# Raft port for node i on this machine (always HTTP+1)
raft_port() { echo $(( BASE_PORT + $1 * PORT_STEP + 1 )); }

# Poll /readyz on localhost until the node is ready or we time out
wait_ready() {
    local name="$1" port="$2"
    printf "    Waiting for %s to be ready" "$name"
    local i
    for i in $(seq 1 "$HEALTH_RETRIES"); do
        if curl -sf "http://localhost:${port}/readyz" &>/dev/null; then
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
stop_nodes() {
    echo "Stopping rqlite nodes on this machine..."
    local removed=0
    local names
    names=$(podman ps -a --format '{{.Names}}' 2>/dev/null \
            | grep "^${PREFIX}-" || true)

    while IFS= read -r name; do
        [[ -z "$name" ]] && continue
        echo "  Removing container: $name"
        podman rm -f "$name" &>/dev/null && (( removed++ )) || true
    done <<< "$names"

    echo "Done. ($removed container(s) removed)"
}

# ── status ─────────────────────────────────────────────────────────────────────
show_status() {
    echo "rqlite nodes on this machine:"
    local found=0
    local names
    names=$(podman ps -a --format '{{.Names}}' 2>/dev/null \
            | grep "^${PREFIX}-" || true)

    while IFS= read -r name; do
        [[ -z "$name" ]] && continue
        local state
        state=$(podman inspect --format '{{.State.Status}}' "$name" 2>/dev/null \
                || echo "unknown")
        printf "  %-22s  state=%s\n" "$name" "$state"
        (( found++ ))
    done <<< "$names"

    [[ $found -eq 0 ]] && echo "  (no rqlite containers found)"
}

# ── urls ──────────────────────────────────────────────────────────────────────
print_urls() {
    local -a ip_list

    if [[ -n "$ALL_IPS" ]]; then
        IFS=',' read -ra ip_list <<< "$ALL_IPS"
    elif [[ -n "$MY_IP" ]]; then
        ip_list=("$MY_IP")
    else
        echo "Error: supply --my-ip IP or --all-ips IP,IP,... with the urls subcommand." >&2
        exit 1
    fi

    local urls=""
    for ip in "${ip_list[@]}"; do
        for i in $(seq 0 $(( REPLICAS - 1 ))); do
            [[ -n "$urls" ]] && urls+=","
            urls+="${ip}:$(http_port "$i")"
        done
    done
    echo "$urls"
}

# ── start ─────────────────────────────────────────────────────────────────────
start_nodes() {
    # -- Validate arguments --------------------------------------------------
    if [[ -z "$MY_IP" ]]; then
        echo "Error: --my-ip IP is required." >&2
        echo "  Tip: use \$(hostname -I | awk '{print \$1}') to auto-detect." >&2
        exit 1
    fi
    if [[ "$IS_LEADER" == false && -z "$LEADER_IP" ]]; then
        echo "Error: specify --leader (this machine bootstraps) or --leader-ip IP (join existing)." >&2
        exit 1
    fi
    if [[ "$IS_LEADER" == true && -n "$LEADER_IP" ]]; then
        echo "Error: --leader and --leader-ip are mutually exclusive." >&2
        exit 1
    fi

    local mode
    if [[ "$IS_LEADER" == true ]]; then
        mode="leader machine — node 0 will bootstrap the cluster"
    else
        mode="follower machine — all nodes join ${LEADER_IP}:${LEADER_PORT}"
    fi

    echo "Starting ${REPLICAS} rqlite replica(s) on ${MY_IP}  (${mode})"
    echo "  Base HTTP port: ${BASE_PORT},  port step: ${PORT_STEP}"
    echo ""

    # -- Remove stale containers from a prior run ----------------------------
    local stale=0
    for i in $(seq 0 $(( REPLICAS - 1 ))); do
        local n; n="$(cname "$i")"
        if podman container exists "$n" 2>/dev/null; then
            echo "  Removing stale container: $n"
            podman rm -f "$n" &>/dev/null
            (( stale++ ))
        fi
    done
    [[ $stale -gt 0 ]] && echo ""

    # -- Start each replica --------------------------------------------------
    for i in $(seq 0 $(( REPLICAS - 1 ))); do
        local name; name="$(cname "$i")"
        local hp;   hp="$(http_port "$i")"
        local rp;   rp="$(raft_port "$i")"
        local idx=$(( i + 1 ))

        # Determine the join address for this specific node:
        #   • Node 0 on the leader machine   → no join (bootstrap)
        #   • Nodes 1..N-1 on leader machine → join node 0 on THIS machine
        #   • Any node on a follower machine → join node 0 on the LEADER machine
        local -a img_args
        if [[ "$IS_LEADER" == true && $i -eq 0 ]]; then
            img_args=("run")                              # bootstrap leader
        elif [[ "$IS_LEADER" == true ]]; then
            img_args=("-join" "${MY_IP}:$(raft_port 0)") # join sibling on same machine
        else
            img_args=("-join" "${LEADER_IP}:${LEADER_PORT}") # join leader machine
        fi

        local label
        [[ "${img_args[0]}" == "run" ]] && label="Bootstrap leader" \
                                        || label="Replica          "

        echo "  [${idx}/${REPLICAS}] ${label}: ${name}"
        echo "           HTTP  ${MY_IP}:${hp}   Raft  ${MY_IP}:${rp}"

        # --network host: the container shares the host's network stack,
        # so rqlited binds directly on the real machine IP.  No port
        # mapping is needed; other machines reach each node at MY_IP:PORT.
        #
        # HTTP_ADDR / RAFT_ADDR   — what rqlited listens on (all interfaces,
        #                           node-specific port)
        # HTTP_ADV_ADDR / RAFT_ADV_ADDR — what rqlite advertises to peers
        #                                  (real host IP + port)
        # NODE_ID                 — unique across the whole cluster
        podman run -d \
            --name    "$name" \
            --network host \
            -e "HTTP_ADDR=0.0.0.0:${hp}" \
            -e "RAFT_ADDR=0.0.0.0:${rp}" \
            -e "HTTP_ADV_ADDR=${MY_IP}:${hp}" \
            -e "RAFT_ADV_ADDR=${MY_IP}:${rp}" \
            -e "NODE_ID=${name}-${MY_IP}" \
            "$IMAGE" "${img_args[@]}"

        wait_ready "$name" "$hp"
    done

    # -- Summary -------------------------------------------------------------
    local machine_urls; machine_urls="$(print_urls)"

    echo ""
    echo "All ${REPLICAS} replica(s) running on ${MY_IP}."
    echo ""
    echo "  Node addresses (this machine):"
    for i in $(seq 0 $(( REPLICAS - 1 ))); do
        printf "    %-22s  HTTP http://%s:%s   Raft %s:%s\n" \
            "$(cname "$i")" \
            "$MY_IP" "$(http_port "$i")" \
            "$MY_IP" "$(raft_port  "$i")"
    done
    echo ""
    echo "  --urls for this machine only:"
    echo "    $machine_urls"
    echo ""
    echo "  To print combined --urls once all machines are up:"
    echo "    ./node.sh urls --all-ips ${MY_IP},MACHINE2_IP,... --replicas ${REPLICAS}"
    echo ""
    echo "  To stop this machine's nodes:"
    echo "    ./node.sh stop"
}

# ── Dispatch ───────────────────────────────────────────────────────────────────
case "$SUBCMD" in
    start)    start_nodes ;;
    stop)     stop_nodes  ;;
    status)   show_status ;;
    urls)     print_urls  ;;
    -h|--help)
        sed -n '2,/^set -/{ /^set -/d; s/^# \{0,1\}//; p }' "$0"
        exit 0
        ;;
    *)
        echo "Unknown subcommand: $SUBCMD" >&2
        exit 1
        ;;
esac
