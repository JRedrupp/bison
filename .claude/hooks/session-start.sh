#!/bin/bash
set -euo pipefail

# Only run in Claude Code remote (web) environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR"

# Install pixi if not already present
if ! command -v pixi &>/dev/null; then
  echo "Installing pixi..."
  curl -fsSL https://pixi.sh/install.sh | sh
  export PATH="$HOME/.pixi/bin:$PATH"
fi

# Ensure pixi is on PATH for all subsequent Bash tool invocations.
# The hook runs in a subprocess, so `export PATH` above does not persist.
# Appending to ~/.bashrc makes it available in every new shell.
if ! grep -q '.pixi/bin' "$HOME/.bashrc" 2>/dev/null; then
  echo 'export PATH="$HOME/.pixi/bin:$PATH"' >> "$HOME/.bashrc"
fi

# Initialise git submodules (e.g. vendor/marrow) if not already checked out
echo "Initialising submodules..."
git submodule update --init --recursive

# Install perf for profiling (kernel-version-independent binary)
if ! command -v perf &>/dev/null; then
  echo "Installing perf..."
  apt-get update -qq && apt-get install -y -qq linux-tools-common linux-tools-generic >/dev/null 2>&1
  # The wrapper script fails on mismatched kernels; symlink the real binary.
  PERF_BIN=$(find /usr/lib/linux-tools -name perf -type f 2>/dev/null | head -1)
  if [ -n "$PERF_BIN" ]; then
    ln -sf "$PERF_BIN" /usr/local/bin/perf
  fi
fi

# Install all project dependencies (MAX + pandas + numpy)
echo "Running pixi install..."
pixi install

# Install pre-commit and its git hooks (mirrors CI setup)
echo "Installing pre-commit..."
pip install --quiet pre-commit
pre-commit install

# Generate _version.mojo from pixi.toml (required before tests or imports)
echo "Generating version file..."
pixi run gen-version

# Build marrow package (required by tests and Arrow integration)
echo "Building marrow..."
pixi run build-marrow

echo "Session start complete."
