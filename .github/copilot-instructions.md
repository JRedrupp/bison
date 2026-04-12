# Copilot instructions for bison

## Project overview

bison is a Mojo DataFrame library with a pandas-compatible API. The goal is
that `import bison as bs` is a drop-in replacement for `import pandas as pd`.

For detailed documentation, see the following references:

- [CONTRIBUTING.md](../CONTRIBUTING.md) — setup, stub pattern, code style
- [docs/architecture.md](../docs/architecture.md) — column storage, type predicates, marrow integration
- [docs/testing.md](../docs/testing.md) — test files, helpers, caching
- [docs/mojo-patterns.md](../docs/mojo-patterns.md) — Mojo-specific patterns and pitfalls
- [docs/ci.md](../docs/ci.md) — CI workflows, pre-commit hooks, benchmarks
- [docs/api-reference.md](../docs/api-reference.md) — full method listing

## Quick reference

- **Language**: Mojo (`.mojo` files). Python only in `scripts/`.
- **Environment**: Pixi. Never use Magic (deprecated).
- **Tasks**: `pixi run test`, `pixi run fmt`, `pixi run check`, `pixi run lint`
- **Version source of truth**: `[project] version` in `pixi.toml`. Never edit `bison/_version.mojo`.
- **Stub pattern**: Use `_not_implemented("TypeName.method_name")`, never bare `raise Error(...)`.
- **`def` not `fn`**: The `fn` keyword is deprecated on nightly Mojo.

## Constraints

- `.CLAUDE` and `.claude/` are gitignored — never reference or commit them.
- `SESSION.md` is gitignored — never commit it.
- `README.md` — plain prose, no emojis, no AI-sounding language.
- GitHub issues #1-33 track each stub category. Reference the relevant issue in PR descriptions.
