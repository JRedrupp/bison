#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TESTS_DIR="$REPO_ROOT/tests"

echo "Running all tests..."
mojo run -I "$REPO_ROOT" -I "$TESTS_DIR" "$TESTS_DIR/run_all_tests.mojo"
