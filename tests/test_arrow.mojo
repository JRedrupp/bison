"""Tests for Arrow ↔ bison conversion (bison/arrow.mojo)."""
from std.testing import assert_equal, assert_true, assert_false
from bison import (
    Column,
    DataFrame,
    NullMask,
    int64,
    float64,
    bool_,
    object_,
    string_,
    column_to_marrow_array,
    marrow_array_to_column,
    dataframe_to_record_batch,
    record_batch_to_dataframe,
)


def test_int64_round_trip_no_nulls() raises:
    """Int64 column survives Arrow round-trip with values and no nulls."""
    var data = List[Int64]()
    data.append(1)
    data.append(2)
    data.append(3)
    var col = Column("a", data^, int64)
    var arr = column_to_marrow_array(col)
    var col2 = marrow_array_to_column(arr^, "a")
    assert_equal(len(col2), 3)
    assert_equal(col2._int64_list()[0], Int64(1))
    assert_equal(col2._int64_list()[1], Int64(2))
    assert_equal(col2._int64_list()[2], Int64(3))
    assert_false(col2.has_nulls())


def test_float64_round_trip_no_nulls() raises:
    """Float64 column survives Arrow round-trip without data loss."""
    var data = List[Float64]()
    data.append(4.0)
    data.append(5.0)
    data.append(6.0)
    var col = Column("b", data^, float64)
    var arr = column_to_marrow_array(col)
    var col2 = marrow_array_to_column(arr^, "b")
    assert_equal(len(col2), 3)
    assert_equal(col2._float64_list()[0], Float64(4.0))
    assert_equal(col2._float64_list()[2], Float64(6.0))
    assert_false(col2.has_nulls())


def test_bool_round_trip_no_nulls() raises:
    """Bool column survives Arrow round-trip."""
    var data = List[Bool]()
    data.append(True)
    data.append(False)
    data.append(True)
    var col = Column("c", data^, bool_)
    var arr = column_to_marrow_array(col)
    var col2 = marrow_array_to_column(arr^, "c")
    assert_equal(len(col2), 3)
    assert_true(col2._bool_list()[0])
    assert_false(col2._bool_list()[1])
    assert_true(col2._bool_list()[2])
    assert_false(col2.has_nulls())


def test_string_round_trip_no_nulls() raises:
    """String column survives Arrow round-trip (uses string_ dtype, #644)."""
    var data = List[String]()
    data.append("x")
    data.append("y")
    data.append("z")
    var col = Column("d", data^, string_)
    var arr = column_to_marrow_array(col)
    var col2 = marrow_array_to_column(arr^, "d")
    assert_equal(len(col2), 3)
    assert_equal(col2._str_list()[0], "x")
    assert_equal(col2._str_list()[1], "y")
    assert_equal(col2._str_list()[2], "z")
    assert_false(col2.has_nulls())


def test_null_mask_preserved_int64() raises:
    """Int64 null mask is preserved through the Arrow round-trip."""
    var data = List[Int64]()
    data.append(0)
    data.append(42)
    data.append(0)
    var col = Column("x", data^, int64)
    var mask = NullMask()
    mask.append(True)
    mask.append(False)
    mask.append(True)
    col.set_null_mask(mask^)
    var arr = column_to_marrow_array(col)
    var col2 = marrow_array_to_column(arr^, "x")
    assert_true(col2.has_nulls())
    assert_true(col2.is_null(0))
    assert_false(col2.is_null(1))
    assert_true(col2.is_null(2))
    assert_equal(col2._int64_list()[1], Int64(42))


def test_null_mask_preserved_string() raises:
    """String null mask is preserved through the Arrow round-trip."""
    var data = List[String]()
    data.append("hello")
    data.append("")
    data.append("world")
    var col = Column("s", data^, string_)
    var mask = NullMask()
    mask.append(False)
    mask.append(True)
    mask.append(False)
    col.set_null_mask(mask^)
    var arr = column_to_marrow_array(col)
    var col2 = marrow_array_to_column(arr^, "s")
    assert_true(col2.has_nulls())
    assert_false(col2.is_null(0))
    assert_true(col2.is_null(1))
    assert_false(col2.is_null(2))
    assert_equal(col2._str_list()[0], "hello")
    assert_equal(col2._str_list()[2], "world")


def test_dataframe_round_trip() raises:
    """DataFrame→RecordBatch→DataFrame round-trip preserves all columns."""
    var d1 = List[Int64]()
    d1.append(1)
    d1.append(2)
    d1.append(3)
    var d2 = List[Float64]()
    d2.append(4.0)
    d2.append(5.0)
    d2.append(6.0)
    var d3 = List[String]()
    d3.append("x")
    d3.append("y")
    d3.append("z")

    var cols = List[Column]()
    cols.append(Column("a", d1^, int64))
    cols.append(Column("b", d2^, float64))
    cols.append(Column("c", d3^, string_))
    var df = DataFrame(cols^)

    var rb = dataframe_to_record_batch(df)
    var df2 = record_batch_to_dataframe(rb^)

    var shape = df2.shape()
    assert_equal(shape[0], 3)
    assert_equal(shape[1], 3)

    var colnames = df2.columns()
    assert_equal(colnames[0], "a")
    assert_equal(colnames[1], "b")
    assert_equal(colnames[2], "c")

    assert_equal(df2._cols[0]._int64_list()[0], Int64(1))
    assert_equal(df2._cols[1]._float64_list()[1], Float64(5.0))
    assert_equal(df2._cols[2]._str_list()[2], "z")


def test_dataframe_round_trip_with_nulls() raises:
    """DataFrame round-trip preserves null masks."""
    var d_a = List[Int64]()
    d_a.append(10)
    d_a.append(0)
    d_a.append(30)
    var col_a = Column("a", d_a^, int64)
    var mask_a = NullMask()
    mask_a.append(False)
    mask_a.append(True)
    mask_a.append(False)
    col_a.set_null_mask(mask_a^)

    var d_b = List[String]()
    d_b.append("hi")
    d_b.append("bye")
    d_b.append("")
    var col_b = Column("b", d_b^, string_)
    var mask_b = NullMask()
    mask_b.append(False)
    mask_b.append(False)
    mask_b.append(True)
    col_b.set_null_mask(mask_b^)

    var cols = List[Column]()
    cols.append(col_a^)
    cols.append(col_b^)
    var df = DataFrame(cols^)

    var rb = dataframe_to_record_batch(df)
    var df2 = record_batch_to_dataframe(rb^)

    assert_true(df2._cols[0].has_nulls())
    assert_false(df2._cols[0].is_null(0))
    assert_true(df2._cols[0].is_null(1))
    assert_false(df2._cols[0].is_null(2))

    assert_true(df2._cols[1].has_nulls())
    assert_false(df2._cols[1].is_null(0))
    assert_false(df2._cols[1].is_null(1))
    assert_true(df2._cols[1].is_null(2))


def test_python_object_column_raises() raises:
    """PythonObject columns raise a clear error with 'PythonObject' in the message."""
    var col = Column()  # empty column — defaults to List[PythonObject] / object_
    var raised = False
    try:
        _ = column_to_marrow_array(col)
    except e:
        raised = True
        assert_true("PythonObject" in String(e))
    assert_true(raised)


def test_storage_active_int64_no_nulls() raises:
    """Phase 6c: int64 Column constructed via typed-list ctor populates
    the AnyArray arm of _storage.
    """
    var data = List[Int64]()
    data.append(1)
    data.append(2)
    data.append(3)
    var col = Column("a", data^, int64)
    assert_equal(len(col), 3)


def test_storage_active_float64_no_nulls() raises:
    """Phase 6c: float64 Column constructed via typed-list ctor populates
    the AnyArray arm of _storage.
    """
    var data = List[Float64]()
    data.append(1.5)
    data.append(2.5)
    data.append(3.5)
    var col = Column("b", data^, float64)
    assert_equal(len(col), 3)


def test_storage_active_bool_no_nulls() raises:
    """Phase 6c: bool Column constructed via typed-list ctor populates
    the AnyArray arm of _storage.
    """
    var data = List[Bool]()
    data.append(True)
    data.append(False)
    data.append(True)
    var col = Column("c", data^, bool_)
    assert_equal(len(col), 3)


def test_fast_path_anyarray_backed_int64() raises:
    """column_to_marrow_array returns correct values for an AnyArray-backed int64 column."""
    var data = List[Int64]()
    data.append(7)
    data.append(8)
    data.append(9)
    var col = Column("a", data^, int64)
    # marrow_array_to_column sets _storage = ColumnStorage(arr.copy()), guaranteeing AnyArray backing.
    var col2 = marrow_array_to_column(column_to_marrow_array(col), "a")
    # Second conversion should hit the fast path.
    var col3 = marrow_array_to_column(column_to_marrow_array(col2), "a")
    assert_equal(len(col3), 3)
    assert_equal(col3._int64_list()[0], Int64(7))
    assert_equal(col3._int64_list()[1], Int64(8))
    assert_equal(col3._int64_list()[2], Int64(9))
    assert_false(col3.has_nulls())


def test_fast_path_anyarray_backed_with_nulls() raises:
    """Nulls survive column_to_marrow_array fast path for AnyArray-backed string column."""
    var data = List[String]()
    data.append("a")
    data.append("")
    data.append("c")
    var col = Column("s", data^, string_)
    var mask = NullMask()
    mask.append(False)
    mask.append(True)
    mask.append(False)
    col.set_null_mask(mask^)
    var col2 = marrow_array_to_column(column_to_marrow_array(col), "s")
    var col3 = marrow_array_to_column(column_to_marrow_array(col2), "s")
    assert_true(col3.has_nulls())
    assert_false(col3.is_null(0))
    assert_true(col3.is_null(1))
    assert_false(col3.is_null(2))
    assert_equal(col3._str_list()[0], "a")
    assert_equal(col3._str_list()[2], "c")


def test_null_mask_copy_no_nulls_returns_empty() raises:
    """AnyArray-backed column with no nulls: null_mask_copy returns an empty NullMask.

    The sort kernel uses `len(null_mask) > 0` to decide whether to enter the
    null-check branch on every comparison.  For a no-null column the mask must
    be empty so the kernel uses the fast path with no null overhead.
    """
    var data = List[Float64]()
    data.append(3.0)
    data.append(1.0)
    data.append(2.0)
    var col = Column("a", data^, float64)
    var col2 = marrow_array_to_column(column_to_marrow_array(col), "a")
    var mask = col2.null_mask_copy()
    assert_equal(len(mask), 0)


def main() raises:
    test_int64_round_trip_no_nulls()
    test_float64_round_trip_no_nulls()
    test_bool_round_trip_no_nulls()
    test_string_round_trip_no_nulls()
    test_null_mask_preserved_int64()
    test_null_mask_preserved_string()
    test_dataframe_round_trip()
    test_dataframe_round_trip_with_nulls()
    test_python_object_column_raises()
    test_storage_active_int64_no_nulls()
    test_storage_active_float64_no_nulls()
    test_storage_active_bool_no_nulls()
    test_fast_path_anyarray_backed_int64()
    test_fast_path_anyarray_backed_with_nulls()
    test_null_mask_copy_no_nulls_returns_empty()
    print("test_arrow: all tests passed")
