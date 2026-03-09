"""Tests for from_pandas / to_pandas interop (these work at stub stage)."""
from python import Python, PythonObject
from testing import assert_equal, assert_true, TestSuite
from bison import DataFrame, Series, Column


def test_df_from_pandas_preserves_shape():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]}"))
    var df = DataFrame.from_pandas(pd_df)
    assert_equal(df.shape()[0], 3)
    assert_equal(df.shape()[1], 3)


def test_df_to_pandas_identity():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'x': [10, 20]}"))
    var df = DataFrame.from_pandas(pd_df)
    var back = df.to_pandas()
    var testing = Python.import_module("pandas.testing")
    testing.assert_frame_equal(pd_df, back)


def test_series_from_pandas_preserves_name():
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1, 2, 3]"), name="score")
    var s = Series.from_pandas(pd_s)
    assert_equal(s.name, "score")
    assert_equal(s.__len__(), 3)


def test_series_to_pandas_identity():
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[5, 6, 7]"), name="v")
    var s = Series.from_pandas(pd_s)
    var back = s.to_pandas()
    var testing = Python.import_module("pandas.testing")
    testing.assert_series_equal(pd_s, back)


def test_df_columns_match():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'alpha': [1], 'beta': [2], 'gamma': [3]}"))
    var df = DataFrame.from_pandas(pd_df)
    var cols = df.columns()
    assert_equal(cols[0], "alpha")
    assert_equal(cols[1], "beta")
    assert_equal(cols[2], "gamma")


def test_quickstart_example():
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))

    var df = DataFrame.from_pandas(pd_df)
    assert_equal(df.shape()[0], 3)
    assert_equal(df.shape()[1], 2)

    var cols = df.columns()
    assert_equal(cols[0], "a")
    assert_equal(cols[1], "b")

    var original = df.to_pandas()
    var testing = Python.import_module("pandas.testing")
    testing.assert_frame_equal(pd_df, original)


def test_column_typed_storage():
    """Verify from_pandas routes values into the correct Variant arm."""
    var pd = Python.import_module("pandas")

    # int64 column -> List[Int64] arm
    var s_int = pd.Series(Python.evaluate("[1, 2, 3]"), dtype="int64", name="i")
    var col_int = Column.from_pandas(s_int, "i")
    assert_true(col_int._data.isa[List[Int64]](), "int column should use List[Int64]")
    assert_equal(col_int.__len__(), 3)

    # float64 column -> List[Float64] arm
    var s_float = pd.Series(Python.evaluate("[1.1, 2.2]"), dtype="float64", name="f")
    var col_float = Column.from_pandas(s_float, "f")
    assert_true(col_float._data.isa[List[Float64]](), "float column should use List[Float64]")
    assert_equal(col_float.__len__(), 2)

    # bool column -> List[Bool] arm
    var s_bool = pd.Series(Python.evaluate("[True, False]"), dtype="bool", name="b")
    var col_bool = Column.from_pandas(s_bool, "b")
    assert_true(col_bool._data.isa[List[Bool]](), "bool column should use List[Bool]")
    assert_equal(col_bool.__len__(), 2)

    # object column -> List[PythonObject] arm
    var s_obj = pd.Series(Python.evaluate("['x', 'y']"), dtype="object", name="o")
    var col_obj = Column.from_pandas(s_obj, "o")
    assert_true(col_obj._data.isa[List[PythonObject]](), "object column should use List[PythonObject]")
    assert_equal(col_obj.__len__(), 2)


def test_series_index_roundtrip():
    """Custom string index must survive from_pandas → to_pandas."""
    var pd = Python.import_module("pandas")
    var testing = Python.import_module("pandas.testing")
    var pd_s = pd.Series(
        Python.evaluate("[1, 2, 3]"),
        index=Python.evaluate("['a', 'b', 'c']"),
        name="x",
    )
    var s = Series.from_pandas(pd_s)
    var back = s.to_pandas()
    testing.assert_series_equal(pd_s, back)


def test_df_index_roundtrip():
    """Custom string index on a DataFrame must survive from_pandas → to_pandas."""
    var pd = Python.import_module("pandas")
    var testing = Python.import_module("pandas.testing")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'v': [10, 20]}"),
        index=Python.evaluate("['r0', 'r1']"),
    )
    var df = DataFrame.from_pandas(pd_df)
    var back = df.to_pandas()
    testing.assert_frame_equal(pd_df, back)


def test_float64_bitcast_roundtrip():
    """Float64 values must survive from_pandas with exact bit-for-bit fidelity.

    Uses Python struct to verify that the bits stored in the Column exactly
    match the original Python float IEEE-754 representation, catching any
    precision loss introduced by the String round-trip.
    """
    var pd = Python.import_module("pandas")
    var struct_mod = Python.import_module("struct")
    # Build a Series with values that cover the full float64 range:
    # smallest subnormal, a near-max value, negative, zero, and a repeating decimal.
    var py_values = Python.evaluate(
        "[5e-324, 1.7976931348623157e+308, -1.7976931348623157e+308, 0.0, 1.0/3.0]"
    )
    var pd_s = pd.Series(py_values, dtype="float64", name="f")
    var col = Column.from_pandas(pd_s, "f")
    var data = col._float64_data().copy()  # internal access needed for bit-level verification
    for i in range(5):
        var py_val = py_values[i]
        var packed_expected = struct_mod.unpack("q", struct_mod.pack("d", py_val))
        var expected_bits = Int64(Int(py=packed_expected[0]))
        var packed_got = struct_mod.unpack("q", struct_mod.pack("d", data[i]))
        var got_bits = Int64(Int(py=packed_got[0]))
        assert_equal(got_bits, expected_bits)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
