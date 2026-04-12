"""Tests for from_pandas / to_pandas interop (these work at stub stage)."""
from std.python import Python, PythonObject
from std.testing import assert_equal, assert_true, assert_false, TestSuite
from bison import DataFrame, Series, Column, NullMask, int64, float64, object_, string_
from _helpers import assert_frame_equal, assert_series_equal


def test_df_from_pandas_preserves_shape() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]}"))
    var df = DataFrame.from_pandas(pd_df)
    assert_equal(df.shape()[0], 3)
    assert_equal(df.shape()[1], 3)


def test_df_to_pandas_identity() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'x': [10, 20]}"))
    var df = DataFrame.from_pandas(pd_df)
    var back = df.to_pandas()
    assert_frame_equal(pd_df, back)


def test_series_from_pandas_preserves_name() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[1, 2, 3]"), name="score")
    var s = Series.from_pandas(pd_s)
    assert_equal(s.name.value(), "score")
    assert_equal(s.__len__(), 3)


def test_series_to_pandas_identity() raises:
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(Python.evaluate("[5, 6, 7]"), name="v")
    var s = Series.from_pandas(pd_s)
    var back = s.to_pandas()
    assert_series_equal(pd_s, back)


def test_df_columns_match() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'alpha': [1], 'beta': [2], 'gamma': [3]}"))
    var df = DataFrame.from_pandas(pd_df)
    var cols = df.columns()
    assert_equal(cols[0], "alpha")
    assert_equal(cols[1], "beta")
    assert_equal(cols[2], "gamma")


def test_quickstart_example() raises:
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 5, 6]}"))

    var df = DataFrame.from_pandas(pd_df)
    assert_equal(df.shape()[0], 3)
    assert_equal(df.shape()[1], 2)

    var cols = df.columns()
    assert_equal(cols[0], "a")
    assert_equal(cols[1], "b")

    var original = df.to_pandas()
    assert_frame_equal(pd_df, original)


def test_column_typed_storage() raises:
    """Verify from_pandas routes values into the correct Variant arm."""
    var pd = Python.import_module("pandas")

    # int64 column -> List[Int64] arm
    var s_int = pd.Series(Python.evaluate("[1, 2, 3]"), dtype="int64", name="i")
    var col_int = Column.from_pandas(s_int, "i")
    assert_true(col_int.is_int(), "int column should use List[Int64]")
    assert_equal(col_int.__len__(), 3)

    # float64 column -> List[Float64] arm
    var s_float = pd.Series(Python.evaluate("[1.1, 2.2]"), dtype="float64", name="f")
    var col_float = Column.from_pandas(s_float, "f")
    assert_true(col_float.is_float(), "float column should use List[Float64]")
    assert_equal(col_float.__len__(), 2)

    # bool column -> List[Bool] arm
    var s_bool = pd.Series(Python.evaluate("[True, False]"), dtype="bool", name="b")
    var col_bool = Column.from_pandas(s_bool, "b")
    assert_true(col_bool.is_bool(), "bool column should use List[Bool]")
    assert_equal(col_bool.__len__(), 2)

    # pure-string object column -> List[String] arm (promoted)
    var s_obj = pd.Series(Python.evaluate("['x', 'y']"), dtype="object", name="o")
    var col_obj = Column.from_pandas(s_obj, "o")
    assert_true(col_obj.is_string(), "pure-string object column should be promoted to List[String]")
    assert_equal(col_obj.__len__(), 2)
    # #644: promoted string columns carry string_ dtype (not object_).
    assert_equal(col_obj.dtype.name, "string")


def test_series_index_roundtrip() raises:
    """Custom string index must survive from_pandas → to_pandas."""
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(
        Python.evaluate("[1, 2, 3]"),
        index=Python.evaluate("['a', 'b', 'c']"),
        name="x",
    )
    var s = Series.from_pandas(pd_s)
    var back = s.to_pandas()
    assert_series_equal(pd_s, back)


def test_df_index_roundtrip() raises:
    """Custom string index on a DataFrame must survive from_pandas → to_pandas."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'v': [10, 20]}"),
        index=Python.evaluate("['r0', 'r1']"),
    )
    var df = DataFrame.from_pandas(pd_df)
    var back = df.to_pandas()
    assert_frame_equal(pd_df, back)


def test_float64_bitcast_roundtrip() raises:
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


def test_int_column_with_nulls_to_pandas() raises:
    """Integer Column with null entries must round-trip through to_pandas() without raising.

    When a Column has integer dtype and any null mask entries are True,
    to_pandas() must use the pandas nullable integer dtype (e.g. "Int64")
    so that None values are accepted.  The resulting pandas Series must
    expose pd.isna() == True at the null positions and the correct integer
    values elsewhere.
    """
    var pd = Python.import_module("pandas")
    # Build an int64 Series with a NaN in position 1 (pandas uses Int64 for nullable int).
    var pd_s = pd.Series(
        Python.evaluate("[1, None, 3]"),
        dtype="Int64",
        name="with_nulls",
    )
    var col = Column.from_pandas(pd_s, "with_nulls")
    # The column must have a null mask with True at index 1.
    assert_true(col.is_null(1), "null mask should be True at index 1")
    # Round-trip back to pandas must not raise.
    var back = col.to_pandas()
    # Non-null values must be preserved.
    assert_equal(Int(py=back[0]), 1)
    assert_equal(Int(py=back[2]), 3)
    # Null position must be NA in the result.
    assert_true(Bool(py=pd.isna(back[1])), "position 1 should be NA after round-trip")


def test_int_column_direct_null_mask_to_pandas() raises:
    """Column constructed directly with an int64 arm and a null mask must not raise on to_pandas().

    This covers the code path where a caller builds a Column manually
    (e.g. from_dict or direct Column(...)) and sets a null mask instead of
    going through from_records.
    """
    var pd = Python.import_module("pandas")
    var data = List[Int64]()
    data.append(10)
    data.append(0)
    data.append(30)
    var col = Column("x", data^, int64)
    var mask = NullMask()
    mask.append_valid()
    mask.append_null()
    mask.append_valid()
    col.set_null_mask(mask^)
    # Must not raise even though dtype is int64 and mask has True entries.
    var back = col.to_pandas()
    assert_equal(Int(py=back[0]), 10)
    assert_equal(Int(py=back[2]), 30)
    assert_true(Bool(py=pd.isna(back[1])), "position 1 should be NA")


def test_obj_column_with_null_mask_to_pandas() raises:
    """List[PythonObject] Column with a null mask must emit NaN at masked positions.

    Regression test for issue #344: _ToPandasVisitor.on_obj previously ignored
    the null_mask and appended raw data[i] unconditionally, so a non-None value
    stored at a masked position would reach to_pandas() instead of NaN.
    """
    var pd = Python.import_module("pandas")
    # Build a List[PythonObject] column manually with a non-None value at
    # position 1, but mark position 1 as null in the mask.
    var raw = List[PythonObject]()
    raw.append(Python.evaluate("'apple'"))
    raw.append(Python.evaluate("'should-be-null'"))  # stored value, must be masked
    raw.append(Python.evaluate("'cherry'"))
    var col = Column("fruit", raw^, object_)
    var mask = NullMask()
    mask.append_valid()
    mask.append_null()
    mask.append_valid()
    col.set_null_mask(mask^)
    var back = col.to_pandas()
    assert_equal(String(py=back[0]), "apple")
    assert_true(Bool(py=pd.isna(back[1])), "masked position must be NaN, not the stored value")
    assert_equal(String(py=back[2]), "cherry")


def test_string_promotion_from_pandas() raises:
    """Pure-string object columns from pandas should be stored as List[String]."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'name': ['alice', 'bob', 'carol'], 'val': [1, 2, 3]}")
    )
    var df = DataFrame.from_pandas(pd_df)
    # 'name' column should be promoted to List[String]
    ref name_col = df._cols[0]
    assert_true(
        name_col.is_string(),
        "pure-string column should be promoted to List[String]",
    )
    # #644: promoted string columns now carry string_ dtype.
    assert_equal(name_col.dtype.name, "string")
    assert_true(name_col.is_string())
    assert_false(name_col.is_object())
    # 'val' column should remain List[Int64]
    ref val_col = df._cols[1]
    assert_true(
        val_col.is_int(),
        "int column should remain List[Int64]",
    )


def test_mixed_object_column_not_promoted() raises:
    """Object columns with mixed types must stay as List[PythonObject]."""
    var pd = Python.import_module("pandas")
    var s = pd.Series(Python.evaluate("['hello', 42, 3.14]"), dtype="object", name="mix")
    var col = Column.from_pandas(s, "mix")
    assert_true(
        col.is_object(),
        "mixed-type object column must remain List[PythonObject]",
    )


def test_string_promotion_with_nulls() raises:
    """String object columns with NaN/None should still promote, with null mask."""
    var pd = Python.import_module("pandas")
    var np = Python.import_module("numpy")
    var s = pd.Series(
        Python.evaluate("['a', None, 'c']"), dtype="object", name="s"
    )
    var col = Column.from_pandas(s, "s")
    assert_true(
        col.is_string(),
        "string column with nulls should promote to List[String]",
    )
    # #644: promoted string column carries string_ dtype.
    assert_equal(col.dtype.name, "string")
    assert_equal(col.__len__(), 3)
    # Null mask: position 1 should be null
    assert_true(col.is_valid(0), "position 0 should not be null")
    assert_true(col.is_null(1), "position 1 should be null")
    assert_true(col.is_valid(2), "position 2 should not be null")


def test_string_promotion_roundtrip() raises:
    """Promoted string columns must round-trip through to_pandas correctly."""
    var pd = Python.import_module("pandas")
    var pd_df = pd.DataFrame(
        Python.evaluate("{'city': ['NYC', 'LA', 'SF'], 'pop': [8.3, 3.9, 0.87]}")
    )
    var df = DataFrame.from_pandas(pd_df)
    # Verify promotion happened
    assert_true(
        df._cols[0].is_string(),
        "city column should be promoted to List[String]",
    )
    # #644: promoted string column carries string_ dtype.
    assert_equal(df._cols[0].dtype.name, "string")
    # Round-trip back to pandas — verify values match
    var back = df.to_pandas()
    assert_equal(Int(py=back.shape[0]), 3)
    assert_equal(Int(py=back.shape[1]), 2)
    assert_equal(String(py=back["city"][0]), "NYC")
    assert_equal(String(py=back["city"][1]), "LA")
    assert_equal(String(py=back["city"][2]), "SF")
    # Numeric column must also round-trip
    assert_frame_equal(pd_df[["pop"]], back[["pop"]])


def test_string_promotion_with_nulls_roundtrip() raises:
    """String columns with nulls must round-trip through to_pandas correctly."""
    var pd = Python.import_module("pandas")
    var pd_s = pd.Series(
        Python.evaluate("['x', None, 'z']"), dtype="object", name="s"
    )
    var s = Series.from_pandas(pd_s)
    assert_true(
        s._col.is_string(),
        "string series with nulls should promote to List[String]",
    )
    var back = s.to_pandas()
    assert_equal(String(py=back[0]), "x")
    assert_true(Bool(py=pd.isna(back[1])), "null should round-trip as NaN")
    assert_equal(String(py=back[2]), "z")


def test_empty_object_column_promoted() raises:
    """An empty object column should promote to List[String] (vacuously all-string)."""
    var pd = Python.import_module("pandas")
    var s = pd.Series(Python.evaluate("[]"), dtype="object", name="e")
    var col = Column.from_pandas(s, "e")
    assert_true(
        col.is_string(),
        "empty object column should promote to List[String]",
    )
    # #644: empty promoted string column carries string_ dtype.
    assert_equal(col.dtype.name, "string")
    assert_equal(col.__len__(), 0)


def test_string_dtype_distinct_from_object() raises:
    """A List[String] column carries string_ dtype, distinct from object_ (#644)."""
    var data = List[String]()
    data.append("a")
    data.append("b")
    # Note: the dtype arg is intentionally ``object_`` to verify that the
    # List[String] constructor force-overrides it to ``string_``.
    var col = Column("x", data^, object_)
    assert_equal(col.dtype.name, "string")
    assert_true(col.is_string())
    assert_false(col.is_object())
    assert_true(col.dtype == string_)
    assert_false(col.dtype == object_)


def test_object_dtype_distinct_from_string() raises:
    """A List[PythonObject] column carries object_ dtype, distinct from string_ (#644).
    """
    var raw = List[PythonObject]()
    raw.append(Python.evaluate("'a'"))
    raw.append(Python.evaluate("42"))
    var col = Column("x", raw^, object_)
    assert_equal(col.dtype.name, "object")
    assert_true(col.is_object())
    assert_false(col.is_string())


def test_string_dtype_round_trip_pandas() raises:
    """A string_ column round-trips through to_pandas → from_pandas (#644)."""
    var data = List[String]()
    data.append("x")
    data.append("y")
    data.append("z")
    var col = Column("s", data^, string_)
    assert_equal(col.dtype.name, "string")
    var ser = col.to_pandas()
    # to_pandas maps string_ → pandas "object" for round-trip stability.
    assert_equal(String(ser.dtype), "object")
    var col2 = Column.from_pandas(ser, "s")
    assert_equal(col2.dtype.name, "string")
    assert_true(col2.is_string())


def test_dataframe_dtypes_renders_string() raises:
    """DataFrame.dtypes renders string columns as 'string' (#644)."""
    var d_str = List[String]()
    d_str.append("a")
    d_str.append("b")
    var d_int = List[Int64]()
    d_int.append(Int64(1))
    d_int.append(Int64(2))
    var cols = List[Column]()
    cols.append(Column("s", d_str^, string_))
    cols.append(Column("i", d_int^, int64))
    var df = DataFrame(cols^)
    var dts = df.dtypes()
    var back = dts.to_pandas()
    # back is a pandas Series indexed by column name.
    assert_equal(String(py=back["s"]), "string")
    assert_equal(String(py=back["i"]), "int64")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
