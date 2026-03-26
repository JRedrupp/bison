#!/usr/bin/env bash
# Run mojo format on the files passed by pre-commit.
# Pre-commit detects any modifications and fails the hook if files changed.
set -euo pipefail

PIXI_BIN="${HOME}/.pixi/bin/pixi"

if command -v pixi &>/dev/null; then
    pixi run -e default mojo format "$@"
elif [ -x "${PIXI_BIN}" ]; then
    "${PIXI_BIN}" run -e default mojo format "$@"
else
    echo "error: neither 'pixi' nor '${PIXI_BIN}' found" >&2
    exit 1
fi
