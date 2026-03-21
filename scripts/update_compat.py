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
    python scripts/update_compat.py
"""
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


STRUCT_KEY_PREFIX = {
    "DataFrame": "DataFrame",
    "Series": "Series",
    "DataFrameGroupBy": "DataFrameGroupBy",
    "SeriesGroupBy": "SeriesGroupBy",
    "StringMethods": "Series.str",
    "DatetimeMethods": "Series.dt",
    "Index": "Index",
    "RangeIndex": "Index",
    "LocIndexer": "DataFrame.loc",
    "ILocIndexer": "DataFrame.iloc",
    "AtIndexer": "DataFrame.at",
    "IAtIndexer": "DataFrame.iat",
}


TOP_LEVEL_KEY_MAP = {
    "read_csv": "read_csv",
    "read_parquet": "read_parquet",
    "read_json": "read_json",
    "read_excel": "read_excel",
    # IO writer helper fns in bison/io/*.mojo map to DataFrame IO category.
    "to_csv": "DataFrame.to_csv",
    "to_parquet": "DataFrame.to_parquet",
    "to_json": "DataFrame.to_json",
    "to_excel": "DataFrame.to_excel",
    "concat": "concat",
}


NOT_IMPLEMENTED_PATTERN = re.compile(r'_not_implemented\("([^"]+)"\)')
STRUCT_PATTERN = re.compile(r"^\s*struct\s+([A-Za-z_][A-Za-z0-9_]*)\b")
FN_PATTERN = re.compile(r"^\s*(?:fn|def)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(")


def _category_for_key(key: str) -> str:
    # Longest prefix match wins (e.g. "Series.str" before "Series").
    for prefix in sorted(CATEGORY_MAP, key=len, reverse=True):
        if key == prefix or key.startswith(prefix + "."):
            return CATEGORY_MAP[prefix]
    return key.split(".")[0]


def _function_key(struct_name: str | None, fn_name: str) -> str | None:
    if struct_name is not None and struct_name in STRUCT_KEY_PREFIX:
        return STRUCT_KEY_PREFIX[struct_name] + "." + fn_name
    return TOP_LEVEL_KEY_MAP.get(fn_name)


def collect_stubs() -> dict[str, int]:
    """Return {category: stub_count} by scanning _not_implemented() calls."""
    counts: dict[str, int] = {}

    for mojo_file in BISON_DIR.rglob("*.mojo"):
        for line in mojo_file.read_text().splitlines():
            m = NOT_IMPLEMENTED_PATTERN.search(line)
            if not m:
                continue
            key = m.group(1)
            category = _category_for_key(key)
            counts[category] = counts.get(category, 0) + 1

    return counts


def collect_totals() -> dict[str, int]:
    """Return {category: total_count} for supported API fn declarations."""
    totals: dict[str, int] = {}

    for mojo_file in BISON_DIR.rglob("*.mojo"):
        current_struct: str | None = None
        for line in mojo_file.read_text().splitlines():
            stripped = line.lstrip()

            # Dedented non-empty line exits the current struct scope.
            if current_struct is not None and stripped and line == stripped:
                current_struct = None

            m_struct = STRUCT_PATTERN.match(line)
            if m_struct:
                current_struct = m_struct.group(1)
                continue

            m_fn = FN_PATTERN.match(line)
            if not m_fn:
                continue

            fn_name = m_fn.group(1)
            key = _function_key(current_struct, fn_name)
            if key is None:
                continue

            category = _category_for_key(key)
            totals[category] = totals.get(category, 0) + 1

    return totals


def build_table(stubs: dict[str, int], totals: dict[str, int]) -> str:
    implemented_counts: dict[str, int] = {}

    categories = set(stubs) | set(totals)
    ordered = DISPLAY_ORDER + [c for c in categories if c not in DISPLAY_ORDER]
    for cat in ordered:
        total = totals.get(cat, 0)
        remaining_stubs = stubs.get(cat, 0)

        # Guard against mismatches when category mapping evolves.
        if total < remaining_stubs:
            total = remaining_stubs

        implemented_counts[cat] = total - remaining_stubs

    total_stubs = sum(stubs.values())
    total_implemented = sum(implemented_counts.values())

    lines = [
        "| Category | Stubs | Implemented |",
        "|----------|-------|-------------|",
    ]
    for cat in ordered:
        if cat not in categories:
            continue
        lines.append(f"| {cat} | {stubs.get(cat, 0)} | {implemented_counts.get(cat, 0)} |")

    lines.append(f"| **Total** | **{total_stubs}** | **{total_implemented}** |")
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
    stubs = collect_stubs()
    totals = collect_totals()
    table = build_table(stubs, totals)
    print(table)
    update_readme(table)


if __name__ == "__main__":
    main()
