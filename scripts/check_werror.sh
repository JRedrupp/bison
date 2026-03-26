#!/usr/bin/env bash
# Run mojo package --Werror to ensure no warnings exist in the bison package.
# Tries pixi first, then falls back to locating mojo directly.
set -euo pipefail

PIXI_BIN="${HOME}/.pixi/bin/pixi"

if command -v pixi &>/dev/null; then
    pixi run check
elif [ -x "${PIXI_BIN}" ]; then
    "${PIXI_BIN}" run check
else
    echo "error: neither 'pixi' nor '${PIXI_BIN}' found" >&2
    exit 1
fi
