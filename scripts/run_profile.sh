#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# run_profile.sh — build bison with debug symbols and profile under
# samply (default) or callgrind.
#
# Usage:
#   pixi run profile                     # samply, all operations
#   pixi run profile sort                # samply, just sort_values
#   pixi run profile merge --callgrind   # callgrind for merge
#   pixi run profile --help
# ---------------------------------------------------------------------------

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CACHE_DIR="$REPO_ROOT/.bison-cache"
PKG_FILE="$CACHE_DIR/bison.mojopkg"
PROFILE_DIR="$REPO_ROOT/profile_results"
BENCH_SRC="$REPO_ROOT/benchmarks/bench_profile.mojo"
BIN_OUT="/tmp/bison_profile_$$"

trap 'rm -f "$BIN_OUT"' EXIT

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
OP="all"
TOOL="samply"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPERATION] [--samply | --callgrind]

Profile bison benchmark operations using external profiling tools.

Operations:
  all       Profile all operations (default)
  sort      DataFrame.sort_values (single key)
  groupby   DataFrame.groupby().sum()
  merge     DataFrame.merge (inner join)
  query     DataFrame.query (compound expression)
  csv       CSV round-trip (to_csv + read_csv)

Tools:
  --samply      Use samply sampling profiler (default)
                Produces interactive flamegraphs via Firefox Profiler.
                Install: cargo install samply
  --callgrind   Use valgrind's callgrind for instruction-level profiling
                Note: may fail if Mojo uses unsupported instructions (AVX-512)

Examples:
  pixi run profile                   # all ops, samply
  pixi run profile sort              # just sort_values
  pixi run profile merge --callgrind # merge with callgrind

Output is written to profile_results/ in the repo root.
EOF
    exit 0
}

for arg in "$@"; do
    case "$arg" in
        --help|-h) usage ;;
        --samply) TOOL="samply" ;;
        --callgrind) TOOL="callgrind" ;;
        sort|groupby|merge|query|csv|all) OP="$arg" ;;
        *) echo "Unknown argument: $arg" >&2; usage ;;
    esac
done

# ---------------------------------------------------------------------------
# Validate tool availability
# ---------------------------------------------------------------------------
if [ "$TOOL" = "samply" ]; then
    if ! command -v samply &>/dev/null; then
        echo "Error: samply not found." >&2
        echo "Install with: cargo install samply" >&2
        echo "" >&2
        echo "Alternatively, use --callgrind (may not work with AVX-512)." >&2
        exit 1
    fi
    # samply needs perf_event_paranoid <= 1
    PARANOID="$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 0)"
    if [ "$PARANOID" -gt 1 ]; then
        echo "samply requires perf_event_paranoid <= 1 (currently $PARANOID)." >&2
        echo "Run: echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid" >&2
        exit 1
    fi
elif [ "$TOOL" = "callgrind" ]; then
    if ! command -v valgrind &>/dev/null; then
        echo "Error: valgrind not found. Install it or use --samply." >&2
        exit 1
    fi
    echo "WARNING: callgrind may crash on Mojo binaries that use AVX-512."
    echo "         Use --samply (the default) if you encounter issues."
    echo ""
fi

# ---------------------------------------------------------------------------
# Build bison.mojopkg (reuses cache from run_benchmarks.sh)
# ---------------------------------------------------------------------------
mkdir -p "$CACHE_DIR" "$PROFILE_DIR"

needs_package_rebuild() {
    [ ! -f "$PKG_FILE" ] && return 0
    find "$REPO_ROOT/bison" -name "*.mojo" -newer "$PKG_FILE" -print -quit | grep -q . && return 0
    return 1
}

if needs_package_rebuild; then
    echo "Packaging bison/ -> $PKG_FILE ..."
    TMP_PKG="$(mktemp -d)/bison.mojopkg"
    mojo package "$REPO_ROOT/bison" -o "$TMP_PKG"
    mv "$TMP_PKG" "$PKG_FILE"
else
    echo "Package up to date: bison.mojopkg"
fi

# ---------------------------------------------------------------------------
# Compile bench_profile.mojo with debug symbols
#
# -g                       Full debug info (function names + line numbers)
# --debug-info-language C  Makes symbols readable by samply/callgrind/perf
# ---------------------------------------------------------------------------
echo "Compiling bench_profile.mojo with debug symbols ..."
mojo build \
    -I "$CACHE_DIR" \
    -I "$REPO_ROOT" \
    "$BENCH_SRC" \
    -g --debug-info-language C \
    -o "$BIN_OUT"

echo "Binary: $BIN_OUT"
echo ""

# ---------------------------------------------------------------------------
# Run under profiler
# ---------------------------------------------------------------------------
if [ "$TOOL" = "samply" ]; then
    OUTFILE="$PROFILE_DIR/${OP}.samply.json"
    echo "Running under samply (op=$OP) ..."
    echo "  Output: $OUTFILE"
    echo ""

    BISON_PROFILE_OP="$OP" samply record \
        --save-only \
        --output "$OUTFILE" \
        "$BIN_OUT"

    echo ""
    echo "Profile saved: $OUTFILE"
    echo ""
    echo "To view the interactive flamegraph:"
    echo "  samply load $OUTFILE"

elif [ "$TOOL" = "callgrind" ]; then
    OUTFILE="$PROFILE_DIR/callgrind.out.${OP}"
    echo "Running under callgrind (op=$OP) ..."
    echo "  Output: $OUTFILE"
    echo ""

    BISON_PROFILE_OP="$OP" valgrind \
        --tool=callgrind \
        --callgrind-out-file="$OUTFILE" \
        "$BIN_OUT" 2>&1

    echo ""
    echo "=== Callgrind Summary ==="
    echo ""
    callgrind_annotate --auto=yes --inclusive=yes "$OUTFILE" \
        | head -80
    echo ""
    echo "Full output: $OUTFILE"
    echo "View details: callgrind_annotate --auto=yes $OUTFILE | less"
    echo "GUI viewer:   kcachegrind $OUTFILE  (if installed)"
fi

echo ""
echo "Profile complete."
