#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# build_marrow.sh — package the vendored marrow submodule into a .mojoc.
#
# `mojo precompile` (formerly `mojo package`, now deprecated as of Mojo 1.0)
# rejects the entire compile with "'main()' is not supported within
# packages" if ANY .mojo file anywhere in the given directory tree contains
# `def main()` — even if that file would never be imported. This is new,
# stricter compiler behavior (not present in bison's previous toolchain
# pin) and is not a defect in marrow's source.
#
# marrow's tests/, kernels/tests/, and expr/tests/ directories all contain
# `def main()` by design (marrow's own TestSuite.run/BenchSuite.run
# convention — they're meant to be compiled individually by marrow's own
# test harness, never packaged as a whole). Since `mojo precompile` has no
# exclude flag, we rsync a filtered mirror of the submodule (test
# directories stripped) into .bison-cache/ and package that instead.
#
# See docs/mojo-patterns.md for more detail on this workaround.
#
# Usage:
#   pixi run build-marrow
# ---------------------------------------------------------------------------

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="$REPO_ROOT/vendor/marrow/marrow"
CACHE_DIR="$REPO_ROOT/.bison-cache/marrow-lib"
MIRROR_DIR="$CACHE_DIR/marrow"
OUT_FILE="$REPO_ROOT/.pixi/envs/default/lib/mojo/marrow.mojoc"

mkdir -p "$MIRROR_DIR"

# Mirror the submodule, excluding test directories (they contain def main()
# entry points, which the compiler now rejects anywhere in a packaged tree).
# `--exclude 'tests/'` matches the directory name "tests" anywhere in the
# tree, so it catches tests/, kernels/tests/, and expr/tests/ in one flag.
# --delete keeps the mirror in sync if files are removed upstream.
rsync -a --delete --exclude 'tests/' "$SRC_DIR/" "$MIRROR_DIR/"

mkdir -p "$(dirname "$OUT_FILE")"
mojo precompile "$MIRROR_DIR" -o "$OUT_FILE"
