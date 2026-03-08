"""Tests that aggregation stubs raise 'not implemented'."""
from python import Python
from testing import assert_true
from bison import DataFrame, Series


def _raises(name: String, raised: Bool) raises:
    if not raised:
        raise Error(name + " should have raised")


def test_df_sum_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame({"a": [1, 2, 3]}))
    var raised = False
    try:
        _ = df.sum()
    except:
        raised = True
    _raises("DataFrame.sum", raised)


def test_df_mean_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame({"a": [1.0, 2.0]}))
    var raised = False
    try:
        _ = df.mean()
    except:
        raised = True
    _raises("DataFrame.mean", raised)


def test_df_describe_stub() raises:
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame({"a": [1, 2, 3]}))
    var raised = False
    try:
        _ = df.describe()
    except:
        raised = True
    _raises("DataFrame.describe", raised)


def test_series_mean_stub() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series([1.0, 2.0, 3.0]))
    var raised = False
    try:
        _ = s.mean()
    except:
        raised = True
    _raises("Series.mean", raised)


def test_series_value_counts_stub() raises:
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(["a", "b", "a"]))
    var raised = False
    try:
        _ = s.value_counts()
    except:
        raised = True
    _raises("Series.value_counts", raised)


def main() raises:
    test_df_sum_stub()
    test_df_mean_stub()
    test_df_describe_stub()
    test_series_mean_stub()
    test_series_value_counts_stub()
    print("test_aggregation: all tests passed")
