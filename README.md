# rqlite YCSB Binding & Consistency Fuzzer

Benchmarking and correctness-testing tools for [rqlite](https://rqlite.io), a distributed SQLite database built on Raft consensus.

Two tools are provided:

- **YCSB binding** — integrates rqlite into the [YCSB 0.17.0](https://github.com/brianfrankcooper/YCSB) benchmark suite.
- **Consistency fuzzer** — runs randomised concurrent operations against a live cluster and checks per-key linearizability of the observed history.

---

## Requirements

| Requirement | Version |
|---|---|
| Java (JDK) | 11 or later |
| Maven (`mvn`) | 3.6 or later |
| Python | 3.6 or later |
| `wget` | any recent version |

Maven must be on `PATH`. YCSB 0.17.0 is downloaded automatically by `build.sh` if not already present.

---

## Building

Run the build script from the project root:

```bash
./build.sh
```

This will:
1. Download and extract YCSB 0.17.0 (if not already present), and register the rqlite binding in its launcher.
2. Compile and install `rqlite-binding` to the local Maven repository (`~/.m2`), and copy the fat JAR into YCSB's classpath directory.
3. Compile `rqlite-fuzzer` and produce a self-contained executable JAR.

**Outputs after a successful build:**

| File | Purpose |
|---|---|
| `rqlite-fuzzer/target/rqlite-fuzzer.jar` | Consistency fuzzer (self-contained) |
| `ycsb-0.17.0/rqlite-binding/lib/rqlite-binding-for-ycsb.jar` | YCSB binding JAR |

You can also build individual components:

```bash
./build.sh binding   # compile and install rqlite-binding only. If you don't have YCSB in the folder.
./build.sh fuzzer    # compile rqlite-fuzzer only (binding must already be installed)
```

---

## Starting a local cluster

`cluster.sh` launches one or more rqlite nodes as Podman containers on your local machine and wires them into a Raft cluster automatically. It requires Podman and the `ghcr.io/rqlite/rqlite:latest` image.

### Basic usage

```bash
./cluster.sh                        # 3-node cluster (default)
./cluster.sh 5                      # 5-node cluster
./cluster.sh 3 5001                 # 3-node cluster, base port 5001
./cluster.sh 3 4001 1.0.0           # 3-node cluster, override CNI version (default is 0.4.0, which is what the machines want)
```

Node *i* is mapped to host port `BASE_HTTP_PORT + i × 10`:

| Nodes | Ports |
|---|---|
| 3 (default) | `:4001`, `:4011`, `:4021` |
| 5 | `:4001`, `:4011`, `:4021`, `:4031`, `:4041` |

### Other subcommands

```bash
./cluster.sh stop                   # remove all cluster containers + Podman network
./cluster.sh status                 # show the state of each container
./cluster.sh urls                   # print the --urls string for run_tests.py
./cluster.sh urls 5 5001            # URLs for a hypothetical 5-node cluster at :5001
```

### CNI version override

The third positional argument sets the CNI version written into the Podman network conflist (`~/.config/cni/net.d/rqlite-net.conflist`). The default is `0.4.0` and requires no special handling. Specifying any other version causes the script to:

1. Run `podman system reset --force` (clears all containers, images cached in storage, and network state).
2. Re-create the `rqlite-net` network.
3. Patch `"cniVersion"` in the generated conflist before starting any nodes.

```bash
./cluster.sh 3 4001 0.4.0
```

> **Warning:** `podman system reset --force` is destructive — it removes all containers and resets Podman storage. Only use the CNI override when you specifically need to test a different CNI version.

### After the cluster is up

The script prints the `--urls` string to paste directly into `run_tests.py`:

```
Cluster is ready.  3 node(s) running.

  --urls value for run_tests.py:
    localhost:4001,localhost:4011,localhost:4021

  Example commands:
    python3 run_tests.py fuzz --urls localhost:4001,localhost:4011,localhost:4021 --threads 4 --num-ops 500 --replication-factor 3
    python3 run_tests.py ycsb --urls localhost:4001,localhost:4011,localhost:4021 --workload a --threads 4 --replication-factor 3
```

You can also verify cluster membership directly:

```bash
curl -s http://localhost:4001/nodes?ver=2 | python3 -m json.tool
```

---

## Multi-cluster layout

Both tools support running against multiple independent rqlite clusters in a single test. Clusters are defined by listing **all** node URLs together and specifying a **replication factor** that controls how the list is sliced.

With `--urls` containing N URLs and `--replication-factor R`:
- Cluster 0 → URLs 0 through R−1
- Cluster 1 → URLs R through 2R−1
- …

N must be divisible by R.

Every key is deterministically routed to a cluster via `abs(key.hashCode()) % numClusters`, so the same key always lands on the same cluster.

**Example:** 12 nodes, replication factor 3 → 4 clusters of 3 nodes each:

```
--urls h0:4001,h1:4001,h2:4001,h3:4001,h4:4001,h5:4001,h6:4001,h7:4001,h8:4001,h9:4001,h10:4001,h11:4001
--replication-factor 3
```

For a single cluster, omit `--replication-factor` (defaults to 1) and list as many or as few nodes as desired.

---

## Running tests

All testing is done through `run_tests.py`. Pass `HOST:PORT` or `IP:PORT` addresses — `http://` is prepended automatically.

```
python3 run_tests.py {fuzz|ycsb} --urls HOST:PORT[,...] [options]
```

### Fuzz testing

The fuzzer spawns concurrent threads that issue random reads, writes, scans, and deletes, records the full operation history with timestamps, and then checks that every read result is linearizable with the writes observed around it.

```bash
python3 run_tests.py fuzz \
    --urls 10.0.0.1:4001,10.0.0.2:4001,10.0.0.3:4001 \
    --replication-factor 3 \
    --threads 8 \
    --num-keys 50 \
    --num-ops 5000 \
    --consistency-level weak
```

Exits with code `0` (PASSED) or `1` (FAILED). Violations are printed with the conflicting operation records.

**Fuzz options:**

| Flag | Description |
|---|---|
| `--threads N` | Concurrent client threads (default: 4) |
| `--num-keys N` | Keys in each thread's pool (default: 20) |
| `--num-ops N` | Operations per thread (default: 2000) |
| `--consistency-level LEVEL` | `none` \| `weak` \| `linearizable` \| `strong` (default: `weak`) |
| `--replication-factor N` | Nodes per cluster (default: 1) |
| `--conflict` | Share one key pool across all threads (maximises contention) |
| `--table NAME` | SQL table name to use (default: `fuzz_test`) |
| `--seed N` | Base RNG seed; thread i uses seed+i (default: random) |
| `--follow-leader` | Discover and route writes to the Raft leader |
| `--timeout SECS` | HTTP request timeout in seconds (default: 30) |
| `--connect-timeout SECS` | HTTP connect timeout in seconds (default: 10) |

**Tip:** To deliberately provoke stale-read violations, combine `--consistency-level none` with `--conflict`:

```bash
python3 run_tests.py fuzz \
    --urls 10.0.0.1:4001 \
    --threads 8 \
    --num-keys 20 \
    --num-ops 5000 \
    --consistency-level none \
    --conflict
```

---

### YCSB benchmarking

YCSB runs in two phases: **load** (insert the initial dataset) followed by **run** (execute the benchmark workload). Both phases are run automatically in sequence.

```bash
python3 run_tests.py ycsb \
    --urls 10.0.0.1:4001,10.0.0.2:4001,10.0.0.3:4001 \
    --replication-factor 3 \
    --workload a \
    --threads 8
```

The `--workload` flag accepts:
- A single letter **a–f** (the six standard YCSB workloads)
- A bare name like `workloada`
- An absolute or relative path to a custom workload file

**Standard YCSB workloads:**

| Letter | Mix | Description |
|---|---|---|
| `a` | 50% read / 50% update | Balanced update-heavy |
| `b` | 95% read / 5% update | Read-mostly |
| `c` | 100% read | Read-only |
| `d` | 95% read / 5% insert | Read recent (latest distribution) |
| `e` | 95% scan / 5% insert | Short range scans |
| `f` | 50% read / 50% read-modify-write | Read-modify-write |

**YCSB options:**

| Flag | Description |
|---|---|
| `--workload LETTER\|FILE` | **Required.** Workload letter (a–f), name, or file path |
| `--threads N` | Concurrent client threads |
| `--consistency-level LEVEL` | `none` \| `weak` \| `linearizable` \| `strong` |
| `--replication-factor N` | Nodes per cluster (default: 1) |
| `--fieldcount N` | Number of value fields per record (default: 10) |
| `--fieldnames NAMES` | Explicit comma-separated column names (overrides `--fieldcount`) |
| `--insert-replace` | Use `INSERT OR REPLACE` for idempotent load re-runs |
| `--write-transaction` | Wrap multi-statement writes in a transaction |
| `--follow-leader` | Discover and route writes to the Raft leader |
| `--timeout SECS` | HTTP request timeout in seconds |
| `--connect-timeout SECS` | HTTP connect timeout in seconds |

---

## Project structure

```
final_project/
├── build.sh                          # One-shot build script
├── cluster.sh                        # Start / stop a local Podman cluster
├── run_tests.py                      # Python test runner
├── rqlite-binding/                   # YCSB binding (Maven project)
│   ├── pom.xml
│   └── src/main/java/site/ycsb/db/
│       ├── RqliteClient.java         # YCSB DB subclass
│       └── RqliteHttpHelper.java     # HTTP layer (execute/query/leader discovery)
├── rqlite-fuzzer/                    # Consistency fuzzer (Maven project)
│   ├── pom.xml
│   └── src/main/java/edu/wisc/cs739/fuzzer/
│       ├── FuzzerMain.java           # Entry point and orchestration
│       ├── FuzzerConfig.java         # CLI argument parsing
│       ├── FuzzerThread.java         # Worker thread issuing random ops
│       ├── OperationRecord.java      # Immutable record of one completed op
│       └── ConsistencyChecker.java   # Post-run linearizability checker
└── ycsb-0.17.0/                      # YCSB distribution (auto-downloaded)
    ├── bin/ycsb                      # Launcher (patched to include rqlite)
    └── rqlite-binding/lib/           # Binding JAR installed by build.sh
```
