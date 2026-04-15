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

BIN_DIR="$TMP_DIR/bin"
COMPILE_RESULT_DIR="$TMP_DIR/compile_results"
LOG_DIR="$TMP_DIR/logs"
mkdir -p "$BIN_DIR" "$COMPILE_RESULT_DIR" "$LOG_DIR"

# Leave at least one core free so the machine stays responsive.
MAX_JOBS=$(( $(nproc) - 1 ))
[ "$MAX_JOBS" -lt 1 ] && MAX_JOBS=1

# ── Phase 1: Compile ────────────────────────────────────────────────────
echo "Compiling tests ..."
echo ""

files=()
pids=()
running=0

for f in "$TESTS_DIR"/test_*.mojo; do
    name="$(basename "$f" .mojo)"
    compile_result="$COMPILE_RESULT_DIR/$name"
    log_file="$LOG_DIR/$name.log"
    files+=("$f")
    echo "  Compiling $(basename "$f") ..."
    (
        if mojo build -I "$CACHE_DIR" -I "$REPO_ROOT" -Xlinker -lm "$f" -o "$BIN_DIR/$name" >"$log_file" 2>&1; then
            echo "pass" > "$compile_result"
        else
            echo "fail" > "$compile_result"
        fi
    ) &
    pids+=($!)
    running=$(( running + 1 ))

    if [ "$running" -ge "$MAX_JOBS" ]; then
        wait "${pids[$(( ${#pids[@]} - running ))]}" || true
        running=$(( running - 1 ))
    fi
done

for pid in "${pids[@]}"; do
    wait "$pid" || true
done

# Report compile results and abort early on compile failures.
COMPILE_PASS=0
COMPILE_FAIL=0
COMPILE_ERRORS=()

for f in "${files[@]}"; do
    name="$(basename "$f" .mojo)"
    compile_result="$COMPILE_RESULT_DIR/$name"
    result="$(cat "$compile_result" 2>/dev/null || echo fail)"
    if [ "$result" = "pass" ]; then
        echo "  OK   $(basename "$f")"
        COMPILE_PASS=$((COMPILE_PASS + 1))
    else
        echo "  FAIL $(basename "$f")"
        COMPILE_FAIL=$((COMPILE_FAIL + 1))
        COMPILE_ERRORS+=("$f")
    fi
done

echo ""
echo "Compile: $COMPILE_PASS passed, $COMPILE_FAIL failed"

if [ ${#COMPILE_ERRORS[@]} -gt 0 ]; then
    echo ""
    echo "Failed to compile:"
    for e in "${COMPILE_ERRORS[@]}"; do
        echo "  $e"
        log_file="$LOG_DIR/$(basename "$e" .mojo).log"
        if [ -f "$log_file" ]; then
            sed 's/^/    /' "$log_file"
        fi
    done
    exit 1
fi

# ── Phase 2: Run ────────────────────────────────────────────────────────
echo ""
echo "Running tests ..."
echo ""

pids=()
running=0

for f in "${files[@]}"; do
    name="$(basename "$f" .mojo)"
    result_file="$TMP_DIR/$name.result"
    echo "  Running $name ..."
    (
        if timeout 1800 "$BIN_DIR/$name"; then
            echo "pass" > "$result_file"
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

for pid in "${pids[@]}"; do
    wait "$pid" || true
done

PASS=0
FAIL=0
ERRORS=()

for i in "${!files[@]}"; do
    f="${files[$i]}"
    name="$(basename "$f" .mojo)"
    result_file="$TMP_DIR/$name.result"
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
