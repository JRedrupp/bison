"""Generate the performance dashboard data file.

Reads every ``results/*.json`` file produced by ``scripts/run_benchmarks.sh``,
merges new runs into the rolling history kept in ``docs/data.json``, and
writes the updated history back.

Usage::

    python scripts/generate_report.py

Paths are resolved relative to the repository root (the parent directory of
this script).  The script is idempotent: running it twice produces the same
``docs/data.json``.
"""

from __future__ import annotations

import json
import pathlib
import sys

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_RUNS = 200

REPO_ROOT = pathlib.Path(__file__).parent.parent
RESULTS_DIR = REPO_ROOT / "results"
DOCS_DIR = REPO_ROOT / "docs"
DATA_FILE = DOCS_DIR / "data.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_history() -> dict:
    """Return the existing history, or an empty skeleton if absent."""
    if DATA_FILE.exists():
        with DATA_FILE.open() as f:
            return json.load(f)
    return {"runs": [], "operations": []}


def _result_list_to_dict(results: list[dict]) -> dict[str, dict]:
    """Convert the flat ``results`` list from a run file to a name-keyed dict.

    Skipped entries (``"skipped": true``) are stored with null values so the
    dashboard can surface coverage gaps.
    """
    out: dict[str, dict] = {}
    for entry in results:
        name = entry["name"]
        if entry.get("skipped"):
            out[name] = {"bison_ms": None, "pandas_ms": None, "ratio": None}
        else:
            out[name] = {
                "bison_ms": entry.get("bison_ms"),
                "pandas_ms": entry.get("pandas_ms"),
                "ratio": entry.get("ratio"),
            }
    return out


def _load_result_files() -> list[dict]:
    """Read every ``results/*.json`` (excluding ``latest.json``) and return
    a list of normalised run dicts ready for merging into history."""
    runs: list[dict] = []
    if not RESULTS_DIR.exists():
        return runs
    for path in sorted(RESULTS_DIR.glob("*.json")):
        if path.name == "latest.json":
            continue
        try:
            with path.open() as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            print(f"WARNING: could not read {path}: {exc}", file=sys.stderr)
            continue
        commit = data.get("commit", "unknown")
        runs.append(
            {
                "commit": commit,
                "short_commit": commit[:7],
                "timestamp": data.get("timestamp", ""),
                "ref": data.get("ref", ""),
                "results": _result_list_to_dict(data.get("results", [])),
            }
        )
    return runs


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    history = _load_history()
    existing_commits: set[str] = {r["commit"] for r in history["runs"]}

    new_runs = _load_result_files()
    added = 0
    for run in new_runs:
        if run["commit"] not in existing_commits:
            history["runs"].append(run)
            existing_commits.add(run["commit"])
            added += 1

    # Sort by timestamp descending, keep the most recent MAX_RUNS.
    history["runs"].sort(key=lambda r: r.get("timestamp", ""), reverse=True)
    history["runs"] = history["runs"][:MAX_RUNS]

    # Rebuild the union of all operation names, preserving insertion order.
    seen: dict[str, None] = {}
    for run in history["runs"]:
        for name in run.get("results", {}):
            seen[name] = None
    history["operations"] = list(seen)

    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    with DATA_FILE.open("w") as f:
        json.dump(history, f, indent=2)
        f.write("\n")

    total = len(history["runs"])
    print(f"generate_report: added {added} new run(s), {total} total in history.")
    print(f"Written {DATA_FILE}")


if __name__ == "__main__":
    main()
