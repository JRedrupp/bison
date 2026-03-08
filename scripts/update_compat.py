#!/usr/bin/env python3
"""
Count stub methods per category and update the compatibility table in README.md.

Stubs are detected by _not_implemented() calls in bison/**/*.mojo.
Each call is expected on a line of the form:
    _not_implemented("Category.method_name")

The table in README.md is rewritten between the sentinel comments:
    <!-- COMPAT_TABLE_START -->
    <!-- COMPAT_TABLE_END -->

Run:
    python scripts/update_compat.py [--passing N]

  --passing N  number of passing tests (passed in by CI from test output)
"""
import argparse
import pathlib
import re
import sys

REPO = pathlib.Path(__file__).parent.parent
BISON_DIR = REPO / "bison"
README = REPO / "README.md"

START_SENTINEL = "<!-- COMPAT_TABLE_START -->"
END_SENTINEL = "<!-- COMPAT_TABLE_END -->"

# Map _not_implemented prefix -> display category name
CATEGORY_MAP = {
    "DataFrame": "DataFrame",
    "Series": "Series",
    "DataFrameGroupBy": "GroupBy (DataFrame)",
    "SeriesGroupBy": "GroupBy (Series)",
    "Series.str": "String accessor",
    "Series.dt": "Datetime accessor",
    "Index": "Index",
    "read_csv": "IO",
    "read_parquet": "IO",
    "read_json": "IO",
    "read_excel": "IO",
    "DataFrame.to_csv": "IO",
    "DataFrame.to_parquet": "IO",
    "DataFrame.to_json": "IO",
    "DataFrame.to_excel": "IO",
    "concat": "Reshape",
}

# Preferred display order
DISPLAY_ORDER = [
    "DataFrame",
    "Series",
    "GroupBy (DataFrame)",
    "GroupBy (Series)",
    "String accessor",
    "Datetime accessor",
    "Index",
    "IO",
    "Reshape",
]


def collect_stubs() -> dict[str, int]:
    """Return {category: stub_count} by scanning _not_implemented() calls."""
    counts: dict[str, int] = {}
    pattern = re.compile(r'_not_implemented\("([^"]+)"\)')

    for mojo_file in BISON_DIR.rglob("*.mojo"):
        for line in mojo_file.read_text().splitlines():
            m = pattern.search(line)
            if not m:
                continue
            key = m.group(1)

            # Determine category
            category = None
            # Longest prefix match wins (e.g. "Series.str" before "Series")
            for prefix in sorted(CATEGORY_MAP, key=len, reverse=True):
                if key == prefix or key.startswith(prefix + "."):
                    category = CATEGORY_MAP[prefix]
                    break
            if category is None:
                category = key.split(".")[0]

            counts[category] = counts.get(category, 0) + 1

    return counts


def build_table(counts: dict[str, int], passing: int) -> str:
    total_stubs = sum(counts.values())
    implemented = max(0, total_stubs - passing) if passing else 0
    # Actually: implemented = stubs that no longer raise = total - remaining_stubs
    # But at stub stage all stubs raise; passing tests exercise non-stub paths.
    # We track it simply: implemented = 0 at stub stage unless told otherwise.

    lines = [
        "| Category | Stubs | Implemented |",
        "|----------|-------|-------------|",
    ]
    ordered = DISPLAY_ORDER + [c for c in counts if c not in DISPLAY_ORDER]
    for cat in ordered:
        if cat not in counts:
            continue
        n = counts[cat]
        lines.append(f"| {cat} | {n} | 0 |")

    lines.append(f"| **Total** | **{total_stubs}** | **0** |")
    return "\n".join(lines)


def update_readme(table: str) -> None:
    text = README.read_text()
    if START_SENTINEL not in text or END_SENTINEL not in text:
        print("Warning: sentinels not found in README.md — skipping update", file=sys.stderr)
        return
    before = text[: text.index(START_SENTINEL) + len(START_SENTINEL)]
    after = text[text.index(END_SENTINEL):]
    README.write_text(before + "\n" + table + "\n" + after)
    print("README.md compatibility table updated.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--passing", type=int, default=0, help="Number of passing tests")
    args = parser.parse_args()

    counts = collect_stubs()
    table = build_table(counts, args.passing)
    print(table)
    update_readme(table)


if __name__ == "__main__":
    main()
