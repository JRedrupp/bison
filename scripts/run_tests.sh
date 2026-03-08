#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TESTS_DIR="$REPO_ROOT/tests"
PASS=0
FAIL=0
ERRORS=()

for f in "$TESTS_DIR"/test_*.mojo; do
    echo "Running $f ..."
    if mojo run -I "$REPO_ROOT" "$f"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        ERRORS+=("$f")
    fi
done

echo ""
echo "Results: $PASS passed, $FAIL failed"

# Write pass count to a file so update_compat.py can read it in CI
echo "$PASS" > "$REPO_ROOT/.test-pass-count"

if [ ${#ERRORS[@]} -gt 0 ]; then
    echo "Failed:"
    for e in "${ERRORS[@]}"; do
        echo "  $e"
    done
    exit 1
fi
