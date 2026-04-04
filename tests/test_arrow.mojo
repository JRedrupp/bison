"""Tests for Arrow ↔ bison conversion (bison/arrow.mojo)."""
from std.testing import assert_equal, assert_true, assert_false
from bison import (
    Column,
    ColumnData,
    DataFrame,
    int64,
    float64,
    bool_,
    object_,
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
    var col = Column("a", ColumnData(data^), int64)
    var arr = column_to_marrow_array(col)
    var col2 = marrow_array_to_column(arr^, "a")
    assert_equal(len(col2), 3)
    assert_equal(col2._data[List[Int64]][0], Int64(1))
    assert_equal(col2._data[List[Int64]][1], Int64(2))
    assert_equal(col2._data[List[Int64]][2], Int64(3))
    assert_equal(len(col2._null_mask), 0)


def test_float64_round_trip_no_nulls() raises:
    """Float64 column survives Arrow round-trip without data loss."""
    var data = List[Float64]()
    data.append(4.0)
    data.append(5.0)
    data.append(6.0)
    var col = Column("b", ColumnData(data^), float64)
    var arr = column_to_marrow_array(col)
    var col2 = marrow_array_to_column(arr^, "b")
    assert_equal(len(col2), 3)
    assert_equal(col2._data[List[Float64]][0], Float64(4.0))
    assert_equal(col2._data[List[Float64]][2], Float64(6.0))
    assert_equal(len(col2._null_mask), 0)


def test_bool_round_trip_no_nulls() raises:
    """Bool column survives Arrow round-trip."""
    var data = List[Bool]()
    data.append(True)
    data.append(False)
    data.append(True)
    var col = Column("c", ColumnData(data^), bool_)
    var arr = column_to_marrow_array(col)
    var col2 = marrow_array_to_column(arr^, "c")
    assert_equal(len(col2), 3)
    assert_true(col2._data[List[Bool]][0])
    assert_false(col2._data[List[Bool]][1])
    assert_true(col2._data[List[Bool]][2])
    assert_equal(len(col2._null_mask), 0)


def test_string_round_trip_no_nulls() raises:
    """String column survives Arrow round-trip (uses object_ dtype)."""
    var data = List[String]()
    data.append("x")
    data.append("y")
    data.append("z")
    var col = Column("d", ColumnData(data^), object_)
    var arr = column_to_marrow_array(col)
    var col2 = marrow_array_to_column(arr^, "d")
    assert_equal(len(col2), 3)
    assert_equal(col2._data[List[String]][0], "x")
    assert_equal(col2._data[List[String]][1], "y")
    assert_equal(col2._data[List[String]][2], "z")
    assert_equal(len(col2._null_mask), 0)


def test_null_mask_preserved_int64() raises:
    """Int64 null mask is preserved through the Arrow round-trip."""
    var data = List[Int64]()
    data.append(0)
    data.append(42)
    data.append(0)
    var col = Column("x", ColumnData(data^), int64)
    col._null_mask = List[Bool]()
    col._null_mask.append(True)
    col._null_mask.append(False)
    col._null_mask.append(True)
    var arr = column_to_marrow_array(col)
    var col2 = marrow_array_to_column(arr^, "x")
    assert_equal(len(col2._null_mask), 3)
    assert_true(col2._null_mask[0])
    assert_false(col2._null_mask[1])
    assert_true(col2._null_mask[2])
    assert_equal(col2._data[List[Int64]][1], Int64(42))


def test_null_mask_preserved_string() raises:
    """String null mask is preserved through the Arrow round-trip."""
    var data = List[String]()
    data.append("hello")
    data.append("")
    data.append("world")
    var col = Column("s", ColumnData(data^), object_)
    col._null_mask = List[Bool]()
    col._null_mask.append(False)
    col._null_mask.append(True)
    col._null_mask.append(False)
    var arr = column_to_marrow_array(col)
    var col2 = marrow_array_to_column(arr^, "s")
    assert_equal(len(col2._null_mask), 3)
    assert_false(col2._null_mask[0])
    assert_true(col2._null_mask[1])
    assert_false(col2._null_mask[2])
    assert_equal(col2._data[List[String]][0], "hello")
    assert_equal(col2._data[List[String]][2], "world")


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
    cols.append(Column("a", ColumnData(d1^), int64))
    cols.append(Column("b", ColumnData(d2^), float64))
    cols.append(Column("c", ColumnData(d3^), object_))
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

    assert_equal(df2._cols[0]._data[List[Int64]][0], Int64(1))
    assert_equal(df2._cols[1]._data[List[Float64]][1], Float64(5.0))
    assert_equal(df2._cols[2]._data[List[String]][2], "z")


def test_dataframe_round_trip_with_nulls() raises:
    """DataFrame round-trip preserves null masks."""
    var d_a = List[Int64]()
    d_a.append(10)
    d_a.append(0)
    d_a.append(30)
    var col_a = Column("a", ColumnData(d_a^), int64)
    col_a._null_mask = List[Bool]()
    col_a._null_mask.append(False)
    col_a._null_mask.append(True)
    col_a._null_mask.append(False)

    var d_b = List[String]()
    d_b.append("hi")
    d_b.append("bye")
    d_b.append("")
    var col_b = Column("b", ColumnData(d_b^), object_)
    col_b._null_mask = List[Bool]()
    col_b._null_mask.append(False)
    col_b._null_mask.append(False)
    col_b._null_mask.append(True)

    var cols = List[Column]()
    cols.append(col_a^)
    cols.append(col_b^)
    var df = DataFrame(cols^)

    var rb = dataframe_to_record_batch(df)
    var df2 = record_batch_to_dataframe(rb^)

    assert_equal(len(df2._cols[0]._null_mask), 3)
    assert_false(df2._cols[0]._null_mask[0])
    assert_true(df2._cols[0]._null_mask[1])
    assert_false(df2._cols[0]._null_mask[2])

    assert_equal(len(df2._cols[1]._null_mask), 3)
    assert_false(df2._cols[1]._null_mask[0])
    assert_false(df2._cols[1]._null_mask[1])
    assert_true(df2._cols[1]._null_mask[2])


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
    print("test_arrow: all tests passed")
