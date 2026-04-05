"""Pandas-oracle conformance matrix for DataFrame.query and DataFrame.eval.

For each in-scope expression, we:
  1. Create the same data in both pandas and bison.
  2. Run the expression through bison (query/eval).
  3. Build the expected result using pandas boolean-index operations (the oracle).
  4. Assert bison's result matches the pandas oracle exactly.

The oracle uses pandas' native vectorised boolean operations (``pd_df["a"] < 3``,
``(mask1) & (mask2)``, etc.) instead of ``DataFrame.query`` / ``DataFrame.eval``
because those methods call Python's evaluator internally, which exceeds the Python
call-stack depth available in the Mojo test runner.

Coverage:
  - scalar comparisons (all six operators: <, <=, >, >=, ==, !=)
  - column-vs-column comparisons
  - logical chains (and, or, not)
  - parentheses precedence
  - null-containing data (float column with None values)
"""

from std.python import Python, PythonObject
from std.testing import assert_true, assert_equal, TestSuite
from bison import DataFrame, Series


# ------------------------------------------------------------------
# query() – scalar comparisons
# ------------------------------------------------------------------


def test_conformance_query_scalar_lt() raises:
    """Bison query('a < 3') row count and values match the pandas boolean-index oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5], 'b': [10, 20, 30, 40, 50]}"))
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a < 3")
    var pd_filtered = pd_df[pd_df["a"] < 3]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


def test_conformance_query_scalar_le() raises:
    """Bison query('a <= 3') matches the pandas boolean-index oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a <= 3")
    var pd_filtered = pd_df[pd_df["a"] <= 3]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


def test_conformance_query_scalar_gt() raises:
    """Bison query('a > 3') matches the pandas boolean-index oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a > 3")
    var pd_filtered = pd_df[pd_df["a"] > 3]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


def test_conformance_query_scalar_ge() raises:
    """Bison query('a >= 3') matches the pandas boolean-index oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a >= 3")
    var pd_filtered = pd_df[pd_df["a"] >= 3]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


def test_conformance_query_scalar_eq() raises:
    """Bison query('a == 3') matches the pandas boolean-index oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a == 3")
    var pd_filtered = pd_df[pd_df["a"] == 3]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


def test_conformance_query_scalar_ne() raises:
    """Bison query('a != 3') matches the pandas boolean-index oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a != 3")
    var pd_filtered = pd_df[pd_df["a"] != 3]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


def test_conformance_query_float_scalar() raises:
    """Bison query('y >= 2.5') on a float column matches the pandas boolean-index oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'y': [1.0, 2.5, 3.0, 4.5, 5.0]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("y >= 2.5")
    var pd_filtered = pd_df[pd_df["y"] >= 2.5]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["y"].tolist()
    var bs_y_series = bs_result["y"]
    ref bs_y = bs_y_series._col._data[List[Float64]]
    for i in range(expected_n):
        assert_true(abs(bs_y[i] - atof(String(pd_vals[i]))) < 1e-9)


def test_conformance_query_string_eq() raises:
    """Bison query(\"name == 'alice'\") on a string column matches the pandas boolean-index oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate(
            "{'name': ['alice', 'bob', 'alice', 'carol', 'bob'], 'val': [1, 2, 3, 4, 5]}"
        )
    )
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("name == 'alice'")
    var pd_filtered = pd_df[pd_df["name"] == "alice"]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["val"].tolist()
    var bs_val_series = bs_result["val"]
    ref bs_vals = bs_val_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_vals[i]), Int(py=pd_vals[i]))


def test_conformance_query_string_ne() raises:
    """Bison query(\"name != 'bob'\") on a string column matches the pandas boolean-index oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate(
            "{'name': ['alice', 'bob', 'alice', 'carol', 'bob'], 'val': [1, 2, 3, 4, 5]}"
        )
    )
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("name != 'bob'")
    var pd_filtered = pd_df[pd_df["name"] != "bob"]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["val"].tolist()
    var bs_val_series = bs_result["val"]
    ref bs_vals = bs_val_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_vals[i]), Int(py=pd_vals[i]))


# ------------------------------------------------------------------
# query() – column-vs-column comparisons
# ------------------------------------------------------------------


def test_conformance_query_col_vs_col_lt() raises:
    """Bison query('a < b') matches the pandas boolean-index oracle for column-vs-column <."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1, 5, 3, 2, 4], 'b': [2, 4, 3, 5, 1]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a < b")
    var pd_filtered = pd_df[pd_df["a"] < pd_df["b"]]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


def test_conformance_query_col_vs_col_eq() raises:
    """Bison query('a == b') matches the pandas boolean-index oracle for column-vs-column ==."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1, 3, 3, 2, 5], 'b': [2, 3, 1, 2, 4]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a == b")
    var pd_filtered = pd_df[pd_df["a"] == pd_df["b"]]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


def test_conformance_query_col_vs_col_ne() raises:
    """Bison query('a != b') matches the pandas boolean-index oracle for column-vs-column !=."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1, 3, 3, 2, 5], 'b': [2, 3, 1, 2, 4]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a != b")
    var pd_filtered = pd_df[pd_df["a"] != pd_df["b"]]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


# ------------------------------------------------------------------
# query() – logical chains
# ------------------------------------------------------------------


def test_conformance_query_and() raises:
    """Bison query('a > 1 and b < 4') matches the pandas & oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1, 2, 3, 4], 'b': [10, 3, 5, 2]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a > 1 and b < 4")
    var pd_filtered = pd_df[(pd_df["a"] > 1) & (pd_df["b"] < 4)]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


def test_conformance_query_or() raises:
    """Bison query('a < 2 or a > 3') matches the pandas | oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a < 2 or a > 3")
    var pd_filtered = pd_df[(pd_df["a"] < 2) | (pd_df["a"] > 3)]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


def test_conformance_query_not() raises:
    """Bison query('not a > 3') matches the pandas ~ oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("not a > 3")
    var pd_filtered = pd_df[~(pd_df["a"] > 3)]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


def test_conformance_query_chained_and() raises:
    """Bison query('a > 0 and b > 0') across two columns matches the pandas & oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1, 2, 3], 'b': [4, 0, 5]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a > 0 and b > 0")
    var pd_filtered = pd_df[(pd_df["a"] > 0) & (pd_df["b"] > 0)]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


# ------------------------------------------------------------------
# query() – parentheses precedence
# ------------------------------------------------------------------


def test_conformance_query_parens_override_precedence() raises:
    """Bison query('a > 1 and (b > 5 or a > 2)') matches the grouped pandas oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1, 2, 3], 'b': [10, 1, 10]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a > 1 and (b > 5 or a > 2)")
    var pd_filtered = pd_df[(pd_df["a"] > 1) & ((pd_df["b"] > 5) | (pd_df["a"] > 2))]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


def test_conformance_query_not_parens() raises:
    """Bison query('not (a == 1 or b == 2)') matches the ~(|) pandas oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1, 2, 3, 4], 'b': [5, 2, 3, 4]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("not (a == 1 or b == 2)")
    var pd_filtered = pd_df[~((pd_df["a"] == 1) | (pd_df["b"] == 2))]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Int64]]
    for i in range(expected_n):
        assert_equal(Int(bs_a[i]), Int(py=pd_vals[i]))


# ------------------------------------------------------------------
# query() – null-containing data
# ------------------------------------------------------------------


def test_conformance_query_nulls_excluded() raises:
    """Null rows in bison query('a > 1') are excluded, matching the pandas oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, None, 3.0, None, 5.0]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a > 1")
    var pd_filtered = pd_df[pd_df["a"] > 1]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Float64]]
    for i in range(expected_n):
        assert_true(abs(bs_a[i] - atof(String(pd_vals[i]))) < 1e-9)


def test_conformance_query_nulls_and() raises:
    """Null rows in bison query('a > 1 and a < 5') are excluded, matching the pandas & oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, None, 3.0, None, 6.0]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a > 1 and a < 5")
    var pd_filtered = pd_df[(pd_df["a"] > 1) & (pd_df["a"] < 5)]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Float64]]
    for i in range(expected_n):
        assert_true(abs(bs_a[i] - atof(String(pd_vals[i]))) < 1e-9)


def test_conformance_query_nulls_or() raises:
    """Null rows in bison query('a < 2 or a > 5') are excluded, matching the pandas | oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, None, 6.0, None, 3.0]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_result = bs_df.query("a < 2 or a > 5")
    var pd_filtered = pd_df[(pd_df["a"] < 2) | (pd_df["a"] > 5)]
    var expected_n = Int(py=pd_filtered.__len__())

    assert_equal(bs_result.shape()[0], expected_n)
    var pd_vals = pd_filtered["a"].tolist()
    var bs_a_series = bs_result["a"]
    ref bs_a = bs_a_series._col._data[List[Float64]]
    for i in range(expected_n):
        assert_true(abs(bs_a[i] - atof(String(pd_vals[i]))) < 1e-9)


# ------------------------------------------------------------------
# eval() – scalar comparisons
# ------------------------------------------------------------------


def test_conformance_eval_scalar_lt() raises:
    """Bison eval('a < 3') boolean mask matches the pandas (a < 3) oracle element-by-element."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("a < 3")
    var pd_bools = (pd_df["a"] < 3).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 5)
    for i in range(5):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


def test_conformance_eval_scalar_le() raises:
    """Bison eval('a <= 3') boolean mask matches the pandas (a <= 3) oracle element-by-element."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("a <= 3")
    var pd_bools = (pd_df["a"] <= 3).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 5)
    for i in range(5):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


def test_conformance_eval_scalar_gt() raises:
    """Bison eval('a > 3') boolean mask matches the pandas (a > 3) oracle element-by-element."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("a > 3")
    var pd_bools = (pd_df["a"] > 3).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 5)
    for i in range(5):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


def test_conformance_eval_scalar_ge() raises:
    """Bison eval('a >= 3') boolean mask matches the pandas (a >= 3) oracle element-by-element."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("a >= 3")
    var pd_bools = (pd_df["a"] >= 3).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 5)
    for i in range(5):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


def test_conformance_eval_scalar_eq() raises:
    """Bison eval('a == 3') boolean mask matches the pandas (a == 3) oracle element-by-element."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("a == 3")
    var pd_bools = (pd_df["a"] == 3).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 5)
    for i in range(5):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


def test_conformance_eval_scalar_ne() raises:
    """Bison eval('a != 3') boolean mask matches the pandas (a != 3) oracle element-by-element."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("a != 3")
    var pd_bools = (pd_df["a"] != 3).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 5)
    for i in range(5):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


def test_conformance_eval_float_scalar() raises:
    """Bison eval('y >= 2.5') on a float column matches the pandas (y >= 2.5) oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'y': [1.0, 2.5, 3.0, 4.5, 5.0]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("y >= 2.5")
    var pd_bools = (pd_df["y"] >= 2.5).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 5)
    for i in range(5):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


# ------------------------------------------------------------------
# eval() – column-vs-column comparisons
# ------------------------------------------------------------------


def test_conformance_eval_col_vs_col_lt() raises:
    """Bison eval('a < b') boolean mask matches the pandas (a < b) oracle element-by-element."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1, 5, 3, 2, 4], 'b': [2, 4, 3, 5, 1]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("a < b")
    var pd_bools = (pd_df["a"] < pd_df["b"]).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 5)
    for i in range(5):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


def test_conformance_eval_col_vs_col_eq() raises:
    """Bison eval('a == b') boolean mask matches the pandas (a == b) oracle element-by-element."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1, 3, 3, 2, 5], 'b': [2, 3, 1, 2, 4]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("a == b")
    var pd_bools = (pd_df["a"] == pd_df["b"]).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 5)
    for i in range(5):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


# ------------------------------------------------------------------
# eval() – logical chains
# ------------------------------------------------------------------


def test_conformance_eval_and() raises:
    """Bison eval('a > 1 and b < 4') boolean mask matches the pandas & oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1, 2, 3, 4], 'b': [10, 3, 5, 2]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("a > 1 and b < 4")
    var pd_bools = ((pd_df["a"] > 1) & (pd_df["b"] < 4)).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 4)
    for i in range(4):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


def test_conformance_eval_or() raises:
    """Bison eval('a < 2 or a > 3') boolean mask matches the pandas | oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("a < 2 or a > 3")
    var pd_bools = ((pd_df["a"] < 2) | (pd_df["a"] > 3)).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 5)
    for i in range(5):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


def test_conformance_eval_not() raises:
    """Bison eval('not a > 3') boolean mask matches the pandas ~ oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4, 5]}"))
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("not a > 3")
    var pd_bools = (~(pd_df["a"] > 3)).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 5)
    for i in range(5):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


def test_conformance_eval_chained_and() raises:
    """Bison eval('a > 0 and b > 0') boolean mask matches the pandas & oracle across two columns."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1, 2, 3], 'b': [4, 0, 5]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("a > 0 and b > 0")
    var pd_bools = ((pd_df["a"] > 0) & (pd_df["b"] > 0)).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 3)
    for i in range(3):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


# ------------------------------------------------------------------
# eval() – parentheses precedence
# ------------------------------------------------------------------


def test_conformance_eval_parens() raises:
    """Bison eval('a > 1 and (b > 5 or a > 2)') boolean mask matches the grouped pandas oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1, 2, 3], 'b': [10, 1, 10]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("a > 1 and (b > 5 or a > 2)")
    var pd_bools = ((pd_df["a"] > 1) & ((pd_df["b"] > 5) | (pd_df["a"] > 2))).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 3)
    for i in range(3):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


def test_conformance_eval_not_parens() raises:
    """Bison eval('not (a == 1 or b == 2)') boolean mask matches the ~(|) pandas oracle."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1, 2, 3, 4], 'b': [5, 2, 3, 4]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("not (a == 1 or b == 2)")
    var pd_bools = (~((pd_df["a"] == 1) | (pd_df["b"] == 2))).tolist()

    ref bs_d = bs_mask._col._data[List[Bool]]
    assert_equal(len(bs_d), 4)
    for i in range(4):
        assert_equal(bs_d[i], Bool(py=pd_bools[i]))


# ------------------------------------------------------------------
# eval() – null-containing data
# ------------------------------------------------------------------


def test_conformance_eval_nulls_simple() raises:
    """Bison eval('a > 1') null rows carry a null flag; query correctly excludes them.

    Null semantics: bison uses Kleene-three-valued logic, storing True/False data
    alongside a null mask.  For query(), null rows are excluded (treated as False),
    which matches pandas boolean-index behaviour where NaN propagates to False.
    """
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'a': [1.0, None, 3.0, None, 5.0]}")
    )
    var bs_df = DataFrame(pd_df)

    var bs_mask = bs_df.eval("a > 1")

    # Null positions must carry the null flag.
    assert_true(len(bs_mask._col._null_mask) > 0)
    assert_true(bs_mask._col._null_mask[1])
    assert_true(bs_mask._col._null_mask[3])

    # Non-null positions must agree with pandas boolean operations.
    var pd_bools = (pd_df["a"] > 1)
    assert_true(not bs_mask._col._null_mask[0])
    assert_true(not bs_mask._col._data[List[Bool]][0])
    assert_true(not bs_mask._col._null_mask[2])
    assert_true(bs_mask._col._data[List[Bool]][2])
    assert_true(not bs_mask._col._null_mask[4])
    assert_true(bs_mask._col._data[List[Bool]][4])

    # query() must exclude null rows; count must match pandas oracle.
    var bs_result = bs_df.query("a > 1")
    var pd_filtered = pd_df[pd_df["a"] > 1]
    var expected_n = Int(py=pd_filtered.__len__())
    assert_equal(bs_result.shape()[0], expected_n)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
