#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCH_DIR="$REPO_ROOT/benchmarks"
RESULTS_DIR="$REPO_ROOT/results"
CACHE_DIR="$REPO_ROOT/.bison-cache"
PKG_FILE="$CACHE_DIR/bison.mojopkg"
TMP_DIR="$(mktemp -d)"
PKG_TMP="$TMP_DIR/bison.mojopkg"
trap 'rm -rf "$TMP_DIR"' EXIT

mkdir -p "$CACHE_DIR" "$RESULTS_DIR"

# Rebuild the bison package if it is missing or any source file is newer.
needs_package_rebuild() {
    [ ! -f "$PKG_FILE" ] && return 0
    find "$REPO_ROOT/bison" -name "*.mojo" -newer "$PKG_FILE" -print -quit | grep -q . && return 0
    return 1
}

if needs_package_rebuild; then
    echo "Packaging bison/ -> $PKG_FILE ..."
    mojo package "$REPO_ROOT/bison" -o "$PKG_TMP"
    mv "$PKG_TMP" "$PKG_FILE"
else
    echo "Package up to date: bison.mojopkg"
fi

# Collect git metadata for the result envelope.
COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo "unknown")"
TIMESTAMP="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
REF="$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")"

BLOB_DIR="$TMP_DIR/blobs"
mkdir -p "$BLOB_DIR"
WARN_COUNT=0

for f in "$BENCH_DIR"/bench_*.mojo; do
    name="$(basename "$f" .mojo)"
    echo "Running $name ..."
    blob_file="$BLOB_DIR/$name.json"
    if mojo run -I "$CACHE_DIR" -I "$REPO_ROOT" "$f" > "$blob_file"; then
        echo "  OK"
    else
        echo "  WARNING: $name exited with non-zero status — skipping"
        rm -f "$blob_file"
        WARN_COUNT=$((WARN_COUNT + 1))
    fi
done

# Merge all per-file JSON blobs into a single envelope.
OUT_FILE="$RESULTS_DIR/${COMMIT}.json"
python3 - "$BLOB_DIR" "$COMMIT" "$TIMESTAMP" "$REF" "$OUT_FILE" <<'PYEOF'
import json, sys, os, glob

blob_dir, commit, timestamp, ref, out_file = sys.argv[1:]

all_results = []
for path in sorted(glob.glob(os.path.join(blob_dir, "*.json"))):
    with open(path) as f:
        try:
            data = json.load(f)
            all_results.extend(data.get("results", []))
        except json.JSONDecodeError:
            print(f"WARNING: could not parse {path}", file=sys.stderr)

envelope = {
    "commit": commit,
    "timestamp": timestamp,
    "ref": ref,
    "results": all_results,
}

with open(out_file, "w") as f:
    json.dump(envelope, f, indent=2)
    f.write("\n")

print(f"Written {out_file}")
PYEOF

cp "$OUT_FILE" "$RESULTS_DIR/latest.json"
echo "Written $RESULTS_DIR/latest.json"

if [ "$WARN_COUNT" -gt 0 ]; then
    echo "$WARN_COUNT benchmark(s) failed — see warnings above"
fi

echo "Benchmark run complete."
exit 0
