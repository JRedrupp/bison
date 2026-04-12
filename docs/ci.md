# CI/CD and code quality

## GitHub Actions workflows

| Workflow | Trigger | What it does |
|----------|---------|--------------|
| `ci.yml` | push / PR | Matrix: `locked` (pixi.lock) + `latest` (newest MAX >= 26.1); lint, test, update compat table on main push |
| `nightly.yml` | daily + manual | Switches to nightly MAX channel; drops pyarrow; runs tests |
| `release.yml` | `v*.*.*` tag | Tests + creates GitHub Release |
| `benchmarks.yml` | push | Runs bench suite; updates dashboard |
| `renovate.yml` | schedule | Automated dependency bumps |

## Pre-commit hooks

Enforced by `.pre-commit-config.yaml`:

- Trailing whitespace / end-of-file fixer
- YAML, TOML, merge-conflict checks
- No bare `raise Error("not yet implemented")` — must use `_not_implemented()`
- `mojo format` auto-formatting
- `mojo package --Werror` — zero warnings policy

Run all hooks manually:

```bash
pixi run lint
```

## Zero warnings policy

The project enforces zero compiler warnings. `pixi run check` runs
`mojo package bison/ --Werror` and must pass before any PR can merge. CI runs
this automatically on every push and pull request.

## Pixi tasks

All development tasks run through Pixi:

```bash
pixi run gen-version    # write bison/_version.mojo from pixi.toml
pixi run test           # regenerates version then runs all tests
pixi run fmt            # mojo format bison/
pixi run check          # mojo package bison/ --Werror (no warnings allowed)
pixi run check-compile  # compile-check all test and benchmark entry points
pixi run lint           # pre-commit run --all-files
pixi run bench          # run benchmarks (depends on gen-version)
pixi run profile        # profile operations with samply (see profiling.md)
pixi run gen-report     # merge benchmark results into docs/data.json
```

## Benchmarks

`benchmarks/bench_core.mojo` measures bison vs pandas across aggregation,
groupby, indexing, and I/O operations. `benchmarks/_bench_utils.mojo` provides
`time_fn()` (wraps Python timeit) and `BenchResult` (outputs JSON with
ratio = bison_ms / pandas_ms).

Iteration counts: `FAST_ITERS=100`, `MED_ITERS=20`, `SLOW_ITERS=3`,
`IO_ITERS=5`. Results are stored in `results/<commit>.json`; the latest is
symlinked as `results/latest.json`. `scripts/generate_report.py` merges history
into `docs/data.json` (capped at 200 runs).

For profiling details, see [profiling.md](profiling.md).

## Releasing

1. Bump the version in `pixi.toml` (single source of truth).
2. Update `CHANGELOG.md`.
3. Commit, tag `vX.Y.Z`, push tags.
4. The `release.yml` workflow creates the GitHub Release automatically.
