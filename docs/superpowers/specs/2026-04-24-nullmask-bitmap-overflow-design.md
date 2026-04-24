# NullMask bitmap overflow fix — design spec

**Issue:** #756
**Branch:** `fix/756-nullmask-bitmap-overflow`
**Date:** 2026-04-24

## Problem

`NullMask.append` in `column.mojo` (~line 4667) has a capacity bug. When
appending a valid (`False`) entry while `_has_nulls=True`, the bitmap is not
grown if `_length > _capacity`. The `elif self._has_nulls` branch guards
`unsafe_clear` behind `if self._length <= self._capacity`, silently skipping
the write when capacity is exhausted. New bits beyond the old capacity are
left uninitialized and may be read as null by `null_mask_copy()`.

`null_mask_copy()` (~line 5284) rebuilds a fresh `NullMask` by iterating the
Arrow validity bitmap and calling `append_valid` / `append_null`, so it
inherits the bug. The underlying Column's `NullMask` (built via
`NullMask.from_list`) is correct; only the copy used by `_ToPandasVisitor`
is wrong.

Concrete symptom: `Series.rolling(200).min().to_pandas()` on 1000 elements
returns false `NaN` at index ~519 and beyond because the null mask copy
overflows its 64-bit initial allocation.

## Fix

**File:** `bison/column.mojo`, `NullMask.append` (~line 4674)

Replace:

```mojo
elif self._has_nulls:
    if self._length <= self._capacity:
        self._builder.unsafe_clear(self._length - 1)
```

With:

```mojo
elif self._has_nulls:
    self._ensure_capacity(self._length)
    self._builder.unsafe_clear(self._length - 1)
```

The `if self._length <= self._capacity` guard is removed. `_ensure_capacity`
already handles the no-op case (returns early when `min_bits <= _capacity`)
and grows the bitmap when needed — which is exactly what was missing.

The no-nulls fast path (`_has_nulls == False`) is unaffected; no allocation
happens when there are no nulls.

## Tests (TDD — write failing tests first)

### 1. Unit test — `tests/test_missing.mojo`

Directly exercises `NullMask.append` across a capacity boundary:

```mojo
def test_null_mask_append_valid_grows_capacity() raises:
    # One null activates _has_nulls and allocates 64 bits.
    # 300 subsequent valid appends must force capacity growth and
    # write clean 0-bits — not leave uninitialized bits as null.
    var mask = NullMask()
    mask.append_null()               # index 0 — null
    for i in range(300):
        mask.append_valid()          # indices 1–300 — valid
    assert_true(mask.is_null(0))
    for i in range(1, 301):
        assert_true(mask.is_valid(i))
```

This test fails before the fix and passes after.

### 2. Integration test — `tests/test_window.mojo`

Exact repro from the issue:

```mojo
def test_series_rolling_large_window_no_false_nan() raises:
    var pd = Python.import_module("pandas")
    var vals = Python.evaluate("[float(i % 17) for i in range(1000)]")
    var pd_s = pd.Series(vals, name="x")
    var s = Series(pd_s, "x")
    var result = s.rolling(200).min().to_pandas()
    var expected = pd_s.rolling(200).min()
    _assert_series_close(result, expected)
```

This test fails before the fix (false NaN at index ~519) and passes after.

## Scope

- **1 file changed:** `bison/column.mojo` (~2 lines)
- **2 test additions:** `tests/test_missing.mojo`, `tests/test_window.mojo`
- No API changes, no new imports, no behaviour changes outside the buggy branch
