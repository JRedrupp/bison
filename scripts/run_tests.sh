#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TESTS_DIR="$REPO_ROOT/tests"
CACHE_DIR="$REPO_ROOT/.bison-cache"
PKG_FILE="$CACHE_DIR/bison.mojopkg"
TMP_DIR="$(mktemp -d)"
PKG_TMP="$TMP_DIR/bison.mojopkg"
trap 'rm -rf "$TMP_DIR"' EXIT

mkdir -p "$CACHE_DIR"

# Rebuild the bison package if it is missing or any source file is newer.
needs_package_rebuild() {
    [ ! -f "$PKG_FILE" ] && return 0
    find "$REPO_ROOT/bison" -name "*.mojo" -newer "$PKG_FILE" -print -quit | grep -q . && return 0
    return 1
}

if needs_package_rebuild; then
    echo "Packaging bison/ -> $PKG_FILE ..."
    # Write to a temp path so an interrupted build never leaves a corrupt cache file.
    mojo package "$REPO_ROOT/bison" -o "$PKG_TMP"
    mv "$PKG_TMP" "$PKG_FILE"
else
    echo "Package up to date: bison.mojopkg"
fi

pids=()
files=()

# Leave at least one core free so the machine stays responsive.
MAX_JOBS=$(( $(nproc) - 1 ))
[ "$MAX_JOBS" -lt 1 ] && MAX_JOBS=1
running=0

for f in "$TESTS_DIR"/test_*.mojo; do
    result_file="$TMP_DIR/$(basename "$f").result"
    echo "Running $f ..."
    (
        if timeout 1800 mojo run -I "$CACHE_DIR" -I "$REPO_ROOT" "$f"; then
            echo "pass" > "$result_file"
        else
            echo "fail" > "$result_file"
        fi
    ) &
    pids+=($!)
    files+=("$f")
    running=$(( running + 1 ))

    # Once we have MAX_JOBS in flight, wait for the oldest to finish
    # before spawning the next one.
    if [ "$running" -ge "$MAX_JOBS" ]; then
        wait "${pids[$(( ${#pids[@]} - running ))]}" || true
        running=$(( running - 1 ))
    fi
done

# Wait for all test processes to finish
for pid in "${pids[@]}"; do
    wait "$pid" || true
done

PASS=0
FAIL=0
ERRORS=()

for i in "${!files[@]}"; do
    f="${files[$i]}"
    result_file="$TMP_DIR/$(basename "$f").result"
    result="$(cat "$result_file" 2>/dev/null || echo fail)"
    if [ "$result" = "pass" ]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        ERRORS+=("$f")
    fi
done

echo ""
echo "Results: $PASS passed, $FAIL failed"

if [ ${#ERRORS[@]} -gt 0 ]; then
    echo "Failed:"
    for e in "${ERRORS[@]}"; do
        echo "  $e"
    done
    exit 1
fi

echo "$PASS" > "$REPO_ROOT/.test-pass-count"
