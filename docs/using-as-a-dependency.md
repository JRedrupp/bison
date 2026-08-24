# Using bison as a dependency

bison is not published to a conda/pixi package channel yet, and won't be
until the API surface and the vendoring story around `mojo precompile` are
more settled — see [Status](#status-and-caveats) below. Until then, the
supported way to consume bison from another Mojo project is to vendor it as
a git submodule and compile it locally against your own pinned Mojo/MAX
version, the same way bison itself consumes
[marrow](https://github.com/JRedrupp/marrow).

This is not a workaround; it's the normal pattern for Mojo libraries today.
A precompiled Mojo package (`.mojoc`) is tied to the exact compiler version
that produced it — Mojo's own docs describe it as "not intended as a
distributable format" for that reason — so cross-project distribution
generally means shipping source and building it as part of your own
project's build, not shipping a prebuilt binary.

## 1. Add bison as a git submodule

```bash
git submodule add https://github.com/JRedrupp/bison.git vendor/bison
git submodule update --init --recursive
```

The `--recursive` update also pulls in bison's own vendored `marrow`
submodule at `vendor/bison/vendor/marrow`, since bison depends on it.

## 2. Match bison's pinned Mojo/MAX version

Check `vendor/bison/pixi.toml` for the exact `max`/`mblack` pin bison was
built and tested against:

```bash
grep -A2 '\[dependencies\]' vendor/bison/pixi.toml
```

Your own `pixi.toml` needs to resolve to the same `max`/`mblack` version (or
a newer one you've re-verified bison against yourself). A mismatch doesn't
fail quietly — it surfaces as a hard error at import time when the
precompiled files don't match your compiler's version. bison is still
`0.x-alpha` and this pin moves with every release, so re-check it whenever
you bump the submodule (see [Keeping the vendored copy in
sync](#keeping-the-vendored-copy-in-sync)).

## 3. Precompile marrow

marrow's `tests/`, `kernels/tests/`, and `expr/tests/` directories contain
`def main()` entry points by design, which `mojo precompile` rejects
anywhere in a package tree. bison works around this with a filtered rsync
mirror before precompiling; do the same for your own build:

```bash
#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="$REPO_ROOT/vendor/bison/vendor/marrow/marrow"
MIRROR_DIR="$REPO_ROOT/.build-cache/marrow"
OUT_DIR="$REPO_ROOT/.pixi/envs/default/lib/mojo"

mkdir -p "$MIRROR_DIR" "$OUT_DIR"
rsync -a --delete --exclude 'tests/' "$SRC_DIR/" "$MIRROR_DIR/"
mojo precompile "$MIRROR_DIR" -o "$OUT_DIR/marrow.mojoc"
```

See [Mojo patterns — `mojo precompile` rejects any `def main()` file
anywhere in the tree](mojo-patterns.md) for more detail on why this is
necessary. bison's own `bison/` package has no `def main()` files in it, so
this step is only needed for marrow.

## 4. Import bison

Two options, depending on whether you want faster incremental rebuilds:

**Source import (simplest, no precompile step).** Add the bison checkout to
your import search path and import it directly:

```bash
mojo build -I vendor/bison your_program.mojo
```

```mojo
import bison as bs
```

Mojo compiles `bison/` from source as part of your build every time. No
extra tooling, but every build recompiles bison's ~13,500 lines along with
your own code.

**Precompiled package (faster rebuilds).** Precompile `bison/` into a
`.mojoc` the same way bison's own `check` and `test` tasks do, and drop it
into your environment's package search path:

```bash
mojo precompile vendor/bison/bison -o .pixi/envs/default/lib/mojo/bison.mojoc
```

Once `bison.mojoc` is on the search path, `import bison as bs` resolves it
directly with no `-I` flag needed. Rerun this whenever you bump the
vendored submodule.

Either way, `marrow.mojoc` from step 3 must already be on the search path —
bison imports marrow itself.

## Keeping the vendored copy in sync

Pin to bison's tagged releases rather than tracking `main`, since bison is
pre-1.0 and its stub surface and API still shift between releases:

```bash
cd vendor/bison
git fetch --tags
git checkout v0.2.0-alpha
cd ../..
git add vendor/bison
git commit -m "chore: bump vendored bison to v0.2.0-alpha"
```

After bumping, re-check step 2 (the Mojo/MAX pin) and re-run the marrow
precompile step — both can change between bison releases.

## Status and caveats

- bison is `0.2.0-alpha`. A number of pandas methods still raise via
  `_not_implemented()` rather than running natively — check the [API
  reference](api-reference.md) for what's implemented before depending on a
  specific method.
- There is no stability guarantee on the Mojo/MAX version bison targets
  between releases; it tracks current Mojo fairly closely (see
  [CHANGELOG](../CHANGELOG.md)).
- If you hit `def main()`/precompile issues that aren't covered here, or
  version-mismatch errors at import time, check
  [`docs/mojo-patterns.md`](mojo-patterns.md) first — most toolchain-level
  gotchas bison itself has hit are documented there.
- A published conda/pixi channel is the natural next step once the API and
  the precompile-versioning story stabilize further, but isn't in place
  yet. If that would unblock your use case, open an issue.
