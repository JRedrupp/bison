from std.python import Python, PythonObject


fn assert_frame_equal(left: PythonObject, right: PythonObject) raises:
    """Assert two pandas DataFrames are equal (delegates to pandas.testing)."""
    var testing = Python.import_module("pandas.testing")
    testing.assert_frame_equal(left, right)


fn assert_series_equal(left: PythonObject, right: PythonObject) raises:
    """Assert two pandas Series are equal (delegates to pandas.testing)."""
    var testing = Python.import_module("pandas.testing")
    testing.assert_series_equal(left, right)


fn make_simple_df() raises -> PythonObject:
    """Return a small pandas DataFrame for use in tests."""
    var pd = Python.import_module("pandas")
    return pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4.0, 5.0, 6.0], 'c': ['x', 'y', 'z']}"))
