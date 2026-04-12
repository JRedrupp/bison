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

# Install all project dependencies (MAX + pandas + numpy + marrow)
echo "Running pixi install..."
pixi install

# Install pre-commit and its git hooks (mirrors CI setup)
echo "Installing pre-commit..."
pip install --quiet pre-commit
pre-commit install

# Generate _version.mojo from pixi.toml (required before tests or imports)
echo "Generating version file..."
pixi run gen-version

echo "Session start complete."
