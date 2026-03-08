"""Tests that aggregation stubs raise 'not implemented'."""
from python import Python
from testing import assert_true
from bison import DataFrame, Series


def _raises(name: String, raised: Bool):
    if not raised:
        raise Error(name + " should have raised")


def test_df_sum_stub():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.sum()
    except:
        raised = True
    _raises("DataFrame.sum", raised)


def test_df_mean_stub():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0]}")))
    var raised = False
    try:
        _ = df.mean()
    except:
        raised = True
    _raises("DataFrame.mean", raised)


def test_df_describe_stub():
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = df.describe()
    except:
        raised = True
    _raises("DataFrame.describe", raised)


def test_series_mean_stub():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("[1.0, 2.0, 3.0]")))
    var raised = False
    try:
        _ = s.mean()
    except:
        raised = True
    _raises("Series.mean", raised)


def test_series_value_counts_stub():
    var pd = Python.import_module("pandas")
    var s = Series(pd.Series(Python.evaluate("['a', 'b', 'a']")))
    var raised = False
    try:
        _ = s.value_counts()
    except:
        raised = True
    _raises("Series.value_counts", raised)


def main():
    test_df_sum_stub()
    test_df_mean_stub()
    test_df_describe_stub()
    test_series_mean_stub()
    test_series_value_counts_stub()
    print("test_aggregation: all tests passed")
