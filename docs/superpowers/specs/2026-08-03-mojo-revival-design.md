# Mojo toolchain revival — design spec

**Date:** 2026-08-03

## Problem

bison is pinned to `max==26.3.0.dev2026041520` (mid-April 2026). The last
commit to `main` was 2026-05-14; the project has been dormant for ~3.5
months. Renovate's open PR #761 (bump `max` to 26.4.0) fails CI on every
leg. The project isn't actually broken on its current pin — `pixi run check`
and `pixi run test` both pass cleanly today — but it can't move forward.

### Root cause

All of PR #761's CI failures trace to compile errors 100% inside
`vendor/marrow/` (the vendored Arrow-interop git submodule,
`kszucs/marrow`), not bison's own code:

- `kernels/aggregate.mojo`'s `algo_min`/`algo_max`/`algo_sum`/`algo_product`
  calls pass a `reduce_dim` keyword argument that the stdlib's
  `algorithm.reduction.min/max/sum/product` no longer accepts.
- `views.mojo`'s `BufferView`/`BitmapView` no longer satisfy the
  `DevicePassable` trait (their `_to_device_type` signature doesn't match
  the trait's current shape).

A probe build (disposable git worktree, bison @ HEAD, marrow bumped to its
own latest upstream commit `ac4b8a0`, `max` bumped to the actual latest
nightly `26.5.0.dev2026080206`) showed:

- **marrow, even at its own newest commit, does not compile against
  today's nightly** — 204 errors, concentrated in `c_data.mojo`,
  `buffers.mojo`, `views.mojo`, `arrays.mojo`. marrow's own upstream has
  been just as dormant as bison — its last commit is also mid-May 2026.
- **bison's own ~13,500 lines mostly compile.** Isolating marrow out (by
  reusing an old-compiler-built `marrow.mojopkg` just to get past the
  import, then invoking `mojo package bison/ --Werror` directly) surfaced
  only ~52 real bison-side errors, all mechanical:
  - `deinit` argument syntax: `deinit(take: T)` style constructors must
    become `__init__(*, deinit move: T)` (13 sites).
  - Redundant trait composition: `Copyable`/`Comparable & Copyable`
    already imply `Movable`; the explicit `& Movable` is now an error, not
    a warning (11 sites).
  - Deprecated positional `__getitem__`: must pass `unsafe_offset=`
    instead (14 sites, all in `column.mojo` ~4437–4468).
  - Stricter origin/lifetime inference on `Variant["value"]` accesses,
    surfacing as "cannot return reference with incompatible origin" (9
    sites, `column.mojo`) plus one cascading copy-constructor-synthesis
    failure on `ColumnIndex`.
  - One `read`→`imm` argument-convention rename (`column.mojo:4592`).

This is a two-front migration with very different sizes: bison's own fixes
are mechanical, ~1 week of work. marrow is the real blocker, and since its
upstream has also stalled, waiting on it has no defined end date.

### A secondary finding: the nightly-drift detector was broken

`.github/workflows/nightly.yml` is supposed to catch exactly this kind of
drift daily. Its "switch to nightly channel" step does:

```sh
sed -i 's|https://conda.modular.com/max"|https://conda.modular.com/max-nightly"|g' pixi.toml
```

`pixi.toml`'s channel is already `https://conda.modular.com/max-nightly`,
so this substitution is a no-op — and even if it weren't, `max` is pinned
with an exact `==` version, so pixi would still solve to the committed
build regardless of channel. The daily nightly job has therefore been
silently re-testing the same pinned build every day instead of probing
what's actually current, which is why 3.5 months of drift went unnoticed.

## Approach

Fork `kszucs/marrow`, fix it ourselves, and update bison to consume the
fork. Chosen over waiting on upstream (no ETA, project also stalled) or
staging through an intermediate mojo checkpoint (unnecessary once we're not
dependent on upstream's pace — we can target latest nightly directly).

Three independently-shippable PRs, in order:

### PR 1 — marrow fork

- Fork `kszucs/marrow` to the user's GitHub account, branch
  `fix/mojo-26.5-nightly` (or matching whatever nightly is current when
  work starts — re-resolve rather than hardcoding today's date-stamp).
- Fix the compile errors needed to get `mojo package vendor/marrow/marrow
  --Werror` clean against latest nightly MAX. Known root causes going in
  (from the probe): the `reduce_dim` kwarg removal in
  `algorithm.reduction`, and the `DevicePassable` trait shape change for
  `BufferView`/`BitmapView`. The full 204-error probe list likely
  collapses to a handful of root causes cascading across files, but the
  true count is only known once fixing starts.
- In bison: update `.gitmodules`' URL to the fork, re-point the submodule
  commit, `git submodule update --init`.
- Verify standalone before touching bison's own pin: `mojo package
  vendor/marrow/marrow --Werror` clean, and marrow's own test suite if
  runnable outside marrow's own pixi env.
- This PR changes only the submodule pointer and `.gitmodules` — bison's
  `pixi.toml` stays on the current MAX pin, so bison itself should still
  build unchanged.

### PR 2 — bison bump

- Bump `pixi.toml`'s `max`/`mblack` to latest nightly (re-resolved at
  implementation time, not hardcoded to today's build).
- Fix the ~52 known mechanical bison-side errors (categories listed under
  Root Cause above), plus whatever else the new pin surfaces once marrow
  actually compiles and the rest of the type-check pipeline can run.
- Regenerate `pixi.lock`.
- `pixi run check`, `pixi run test`, `pixi run check-compile` all green.
- Update `docs/mojo-patterns.md` with the newly-discovered gotchas
  (`deinit`/move syntax, redundant trait composition, `read`→`imm`, the
  stricter origin rules) so they don't get rediscovered.
- Per `CLAUDE.md`'s session-notes rule, log tech debt/refactoring
  observations to `SESSION.md` as encountered during this work — including
  re-checking whether compiler bug #642 (the query/eval generic-dispatch
  deadlock documented in `docs/mojo-patterns.md`) still reproduces on the
  new compiler. If it's fixed, that's a doc update, not part of this PR.

### PR 3 — nightly CI fix

- Fix `.github/workflows/nightly.yml`'s dead `sed` / exact-pin combination
  so the daily job actually solves against whatever's newest that day,
  instead of re-testing the committed pin.
- Add a step that finds-or-creates a tracking issue (e.g. labeled
  `nightly-broken`) and comments with the failure link when the nightly
  job fails, so drift produces a visible, durable signal instead of only a
  red X in the Actions tab that's easy to miss.
- Leave `ci.yml`'s `latest`/`locked` matrix as-is — it isn't the broken
  piece.

## Testing / verification

- PR 1: `mojo package vendor/marrow/marrow --Werror` clean on the fork,
  standalone.
- PR 2: `pixi run check` (zero warnings), `pixi run test` (full suite),
  `pixi run check-compile` all green on the new pin. Spot-check `pixi run
  bench` doesn't crash (not required to pass performance thresholds) since
  window/aggregation kernels touch the same `algorithm.reduction` surface
  that broke in marrow.
- PR 3: trigger the nightly workflow manually (`workflow_dispatch`) to
  confirm it solves a newer build than the committed pin. Temporarily point
  it at a deliberately-incompatible version to confirm the failure path
  files/updates the tracking issue, then revert that test tweak.

## Out of scope

- Pushing marrow/bison further than "compiles clean on today's latest
  nightly" — nightlies roll daily, so there's no fixed target beyond that.
- Upstreaming the marrow fixes as PRs to `kszucs/marrow` — worth doing
  once the fork is stable, but not a blocking part of this work.
- Re-litigating compiler bug #642 unless the new toolchain already fixes
  it incidentally.
