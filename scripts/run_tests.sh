#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TESTS_DIR="$REPO_ROOT/tests"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

pids=()
files=()

for f in "$TESTS_DIR"/test_*.mojo; do
    result_file="$TMP_DIR/$(basename "$f").result"
    echo "Running $f ..."
    (
        if mojo run -I "$REPO_ROOT" "$f"; then
            echo "pass" > "$result_file"
        else
            echo "fail" > "$result_file"
        fi
    ) &
    pids+=($!)
    files+=("$f")
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
