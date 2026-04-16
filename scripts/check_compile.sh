#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# check_compile.sh — verify that all Mojo benchmark entry points compile.
#
# Catches import errors, type errors, and syntax issues in benchmark files
# that `mojo package bison/ --Werror` (the pre-commit check) does not cover,
# since those files live outside the bison package.
#
# Test files are intentionally excluded: they are already compiled and run by
# the test runner (run_tests.sh), so a separate compile check would be
# redundant.
#
# Uses `mojo build` to compile each file.  `-Xlinker -lm` is passed so that
# files using math functions (log10, sqrt, etc.) link correctly.  Remaining
# linker-only failures (other missing system libraries) are still reported as
# warnings rather than errors, since those are environment-specific and not
# source-level bugs.
#
# Files are compiled in parallel for speed.
#
# Usage:
#   pixi run check-compile
# ---------------------------------------------------------------------------

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CACHE_DIR="$REPO_ROOT/.bison-cache"
PKG_FILE="$CACHE_DIR/bison.mojopkg"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

mkdir -p "$CACHE_DIR"

# Build bison.mojopkg if needed.
if [ ! -f "$PKG_FILE" ] || \
   find "$REPO_ROOT/bison" -name "*.mojo" -newer "$PKG_FILE" -print -quit | grep -q .; then
    echo "Packaging bison/ -> $PKG_FILE ..."
    TMP_PKG="$TMP_DIR/bison.mojopkg"
    mojo package "$REPO_ROOT/bison" -o "$TMP_PKG"
    mv "$TMP_PKG" "$PKG_FILE"
else
    echo "Package up to date: bison.mojopkg"
fi

# Collect entry-point files: benchmarks only.
# Tests are validated by the test runner (run_tests.sh).
#
# bench_core.mojo and bench_profile.mojo are temporarily excluded: they
# both call DataFrame.query() from a monolithic def main(), which hits a
# Mojo 0.26.3 nightly compile-time deadlock (all compiler threads wait
# forever on sem_wait inside libAsyncRTMojoBindings.so).  The same call
# distributed across many small test functions — as in tests/test_expr.mojo —
# compiles in ~30-45s, so the regular test suite is unaffected.  See
# repro_hang.mojo / repro_hang.patch at the repo root for the root-cause
# analysis (typed AnyArray.as_*() monomorphisation in the query evaluator's
# transitive call graph).  Re-enable when the compiler regression lands
# upstream.
SKIP_FILES=(bench_core.mojo bench_profile.mojo)
FILES=()
for f in "$REPO_ROOT"/benchmarks/bench_*.mojo; do
    [ -f "$f" ] || continue
    skip=0
    for s in "${SKIP_FILES[@]}"; do
        if [ "$(basename "$f")" = "$s" ]; then
            skip=1
            break
        fi
    done
    if [ "$skip" -eq 0 ]; then
        FILES+=("$f")
    else
        echo "  SKIP $(basename "$f")  (Mojo 0.26.3 nightly compile-deadlock — see repro_hang.mojo)"
    fi
done

echo "Compile-checking ${#FILES[@]} files ..."
echo ""

BIN_DIR="$TMP_DIR/bin"
RESULT_DIR="$TMP_DIR/results"
LOG_DIR="$TMP_DIR/logs"
mkdir -p "$BIN_DIR" "$RESULT_DIR" "$LOG_DIR"

# Compile in parallel.
MAX_JOBS=$(( $(nproc) - 1 ))
[ "$MAX_JOBS" -lt 1 ] && MAX_JOBS=1

pids=()
running=0

for f in "${FILES[@]}"; do
    name="$(basename "$f" .mojo)"
    result_file="$RESULT_DIR/$name"
    log_file="$LOG_DIR/$name.log"
    (
        if mojo build -I "$CACHE_DIR" -I "$REPO_ROOT" -Xlinker -lm "$f" -o "$BIN_DIR/$name" >"$log_file" 2>&1; then
            echo "pass" > "$result_file"
        elif grep -q "failed to link executable" "$log_file"; then
            # Linker-only failure (e.g. missing libm on some systems).
            # The source compiled — the link step failed due to the environment.
            echo "link" > "$result_file"
        else
            echo "fail" > "$result_file"
        fi
    ) &
    pids+=($!)
    running=$(( running + 1 ))

    if [ "$running" -ge "$MAX_JOBS" ]; then
        wait "${pids[$(( ${#pids[@]} - running ))]}" || true
        running=$(( running - 1 ))
    fi
done

# Wait for all compile jobs to finish.
for pid in "${pids[@]}"; do
    wait "$pid" || true
done

# Collect results.
PASS=0
FAIL=0
LINK=0
ERRORS=()

for f in "${FILES[@]}"; do
    name="$(basename "$f" .mojo)"
    result_file="$RESULT_DIR/$name"
    result="$(cat "$result_file" 2>/dev/null || echo fail)"
    if [ "$result" = "pass" ]; then
        echo "  OK   $(basename "$f")"
        PASS=$((PASS + 1))
    elif [ "$result" = "link" ]; then
        echo "  WARN $(basename "$f")  (link-only failure)"
        LINK=$((LINK + 1))
    else
        echo "  FAIL $(basename "$f")"
        FAIL=$((FAIL + 1))
        ERRORS+=("$f")
    fi
done

echo ""
echo "Results: $PASS passed, $FAIL failed, $LINK link-only warnings"

if [ ${#ERRORS[@]} -gt 0 ]; then
    echo ""
    echo "Failed to compile:"
    for e in "${ERRORS[@]}"; do
        echo "  $e"
    done
    echo ""
    echo "Re-run individually for detailed error output:"
    echo "  mojo build -I .bison-cache -I . -Xlinker -lm <file>"
    exit 1
fi
