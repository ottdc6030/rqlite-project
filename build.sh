#!/usr/bin/env bash
# Build script for rqlite-binding and rqlite-fuzzer.
#
# Usage:
#   ./build.sh            -- builds both projects
#   ./build.sh binding    -- builds only rqlite-binding (install to local Maven repo)
#   ./build.sh fuzzer     -- builds only rqlite-fuzzer  (assumes binding already installed)
#
# Output:
#   rqlite-fuzzer/target/rqlite-fuzzer-1.0.0-SNAPSHOT.jar   -- fat JAR, ready to run
#   ycsb-0.17.0/rqlite-binding/lib/rqlite-binding-for-ycsb.jar -- updated YCSB binding

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINDING_DIR="$SCRIPT_DIR/rqlite-binding"
FUZZER_DIR="$SCRIPT_DIR/rqlite-fuzzer"
YCSB_LIB="$SCRIPT_DIR/ycsb-0.17.0/rqlite-binding/lib"
YCSB_BIN="$SCRIPT_DIR/ycsb-0.17.0/bin/ycsb"

if command -v mvn &>/dev/null; then
    echo "Maven found"
else
    echo "Error: cannot find mvn. Install Maven or add it to PATH." >&2
    exit 1
fi


# ---------------------------------------------------------------------------
# Download YCSB 0.17.0 if not already present
# ---------------------------------------------------------------------------
YCSB_DIR="$SCRIPT_DIR/ycsb-0.17.0"
if [[ ! -d "$YCSB_DIR" ]]; then
    echo "=== Downloading YCSB 0.17.0 ==="
    YCSB_TAR="$SCRIPT_DIR/ycsb-0.17.0.tar.gz"
    wget -q --show-progress -O "$YCSB_TAR" \
        "https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz"
    tar -xzf "$YCSB_TAR" -C "$SCRIPT_DIR"
    rm "$YCSB_TAR"
    echo "    YCSB extracted to $YCSB_DIR"
    # Register the rqlite binding in YCSB's launcher script
    sed -i 's/"redis".*: "site.ycsb.db.RedisClient",/"redis"        : "site.ycsb.db.RedisClient",\n    "rqlite"       : "site.ycsb.db.RqliteClient",/' "$YCSB_BIN"
    echo "    rqlite registered in $YCSB_BIN"
    # Register the rqlite binding in bindings.properties (line 28, above accumulo)
    BINDINGS_FILE="$YCSB_DIR/bin/bindings.properties"
    sed -i '28s/^/rqlite:site.ycsb.db.RqliteClient\n/' "$BINDINGS_FILE"
    echo "    rqlite registered in $BINDINGS_FILE"
fi

# ---------------------------------------------------------------------------
build_binding() {
    echo "=== [1/2] Building rqlite-binding ==="
    cd "$BINDING_DIR"
    # 'install' compiles, tests (skipped), packages, AND installs to ~/.m2 local repo.
    # The primary installed artifact is the thin JAR; the fat JAR is attached with
    # the 'for-ycsb' classifier for use with YCSB's lib/ directory.
    mvn install -DskipTests -q
    echo "    rqlite-binding installed to local Maven repo"

    # Keep YCSB's lib/ up to date with the latest fat JAR
    mkdir -p "$YCSB_LIB"
    FAT_JAR="$(ls "$BINDING_DIR/target/rqlite-binding-for-ycsb.jar" 2>/dev/null | head -1)"
    if [[ -n "$FAT_JAR" ]]; then
        cp "$FAT_JAR" "$YCSB_LIB/rqlite-binding-for-ycsb.jar"
        echo "    YCSB binding JAR: $YCSB_LIB/rqlite-binding-for-ycsb.jar"
    else
        echo "    WARNING: could not find *-for-ycsb.jar in $BINDING_DIR/target/" >&2
    fi
}

build_fuzzer() {
    echo "=== [2/2] Building rqlite-fuzzer ==="
    cd "$FUZZER_DIR"
    mvn package -DskipTests -q
    FUZZER_JAR="$FUZZER_DIR/target/rqlite-fuzzer.jar"
    echo "    Fuzzer JAR: $FUZZER_JAR"
}

# ---------------------------------------------------------------------------
TARGET="${1:-all}"

case "$TARGET" in
    binding)
        build_binding
        ;;
    fuzzer)
        build_fuzzer
        ;;
    all|*)
        build_binding
        build_fuzzer
        ;;
esac


echo "Build complete."