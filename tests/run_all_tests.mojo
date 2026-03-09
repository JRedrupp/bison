"""Combined test runner — invokes every test suite in a single binary call.

Run with:
    mojo run -I <repo_root> -I <repo_root>/tests tests/run_all_tests.mojo

This file is the entry-point used by scripts/run_tests.sh so that the Mojo
binary is started exactly once instead of once per test file.
"""

import test_accessors
import test_aggregation
import test_combining
import test_dataframe
import test_groupby
import test_interop
import test_io
import test_missing
import test_reshaping
import test_series


fn run_accessors_tests() raises:
    test_accessors.test_str_upper_stub()
    test_accessors.test_str_contains_stub()
    test_accessors.test_dt_year_stub()
    test_accessors.test_dt_month_stub()
    print("test_accessors: all tests passed")


fn run_aggregation_tests() raises:
    test_aggregation.test_series_sum_int()
    test_aggregation.test_series_sum_float()
    test_aggregation.test_series_sum_skipna_flag()
    test_aggregation.test_series_sum_skipna_true_nan()
    test_aggregation.test_series_sum_skipna_false_nan()
    test_aggregation.test_df_sum()
    test_aggregation.test_series_count_no_nulls()
    test_aggregation.test_series_count_with_nulls()
    test_aggregation.test_df_count()
    test_aggregation.test_series_mean_int()
    test_aggregation.test_series_mean_float()
    test_aggregation.test_series_mean_skipna_true()
    test_aggregation.test_series_mean_skipna_false_nan()
    test_aggregation.test_df_mean()
    test_aggregation.test_series_min_int()
    test_aggregation.test_series_max_int()
    test_aggregation.test_series_min_skipna()
    test_aggregation.test_df_min()
    test_aggregation.test_df_max()
    test_aggregation.test_series_std()
    test_aggregation.test_series_var()
    test_aggregation.test_series_std_single_element()
    test_aggregation.test_df_std()
    test_aggregation.test_series_nunique_int()
    test_aggregation.test_series_nunique_with_nulls()
    test_aggregation.test_df_nunique()
    test_aggregation.test_series_median_odd()
    test_aggregation.test_series_median_even()
    test_aggregation.test_series_median_skipna_false()
    test_aggregation.test_series_quantile_25()
    test_aggregation.test_series_quantile_75()
    test_aggregation.test_df_median()
    test_aggregation.test_df_describe_stub()
    test_aggregation.test_series_value_counts()
    print("test_aggregation: all tests passed")


fn run_combining_tests() raises:
    test_combining.test_merge_stub()
    test_combining.test_join_stub()
    test_combining.test_append_stub()
    print("test_combining: all tests passed")


fn run_dataframe_tests() raises:
    test_dataframe.test_shape_from_pandas()
    test_dataframe.test_len()
    test_dataframe.test_empty_false()
    test_dataframe.test_empty_true()
    test_dataframe.test_columns()
    test_dataframe.test_ndim()
    test_dataframe.test_size()
    test_dataframe.test_contains()
    test_dataframe.test_to_pandas_roundtrip()
    test_dataframe.test_from_dict()
    print("test_dataframe: all tests passed")


fn run_groupby_tests() raises:
    test_groupby.test_groupby_stub()
    test_groupby.test_groupby_sum_stub()
    print("test_groupby: all tests passed")


fn run_interop_tests() raises:
    test_interop.test_df_from_pandas_preserves_shape()
    test_interop.test_df_to_pandas_identity()
    test_interop.test_series_from_pandas_preserves_name()
    test_interop.test_series_to_pandas_identity()
    test_interop.test_df_columns_match()
    test_interop.test_quickstart_example()
    test_interop.test_column_typed_storage()
    test_interop.test_series_index_roundtrip()
    test_interop.test_df_index_roundtrip()
    test_interop.test_float64_bitcast_roundtrip()
    print("test_interop: all tests passed")


fn run_io_tests() raises:
    test_io.test_read_csv_stub()
    test_io.test_read_parquet_stub()
    test_io.test_read_json_stub()
    test_io.test_read_excel_stub()
    test_io.test_to_csv_stub()
    print("test_io: all tests passed")


fn run_missing_tests() raises:
    test_missing.test_df_isna_stub()
    test_missing.test_df_fillna_stub()
    test_missing.test_df_dropna_stub()
    test_missing.test_df_ffill_stub()
    test_missing.test_series_fillna_stub()
    print("test_missing: all tests passed")


fn run_reshaping_tests() raises:
    test_reshaping.test_sort_values_stub()
    test_reshaping.test_pivot_stub()
    test_reshaping.test_melt_stub()
    test_reshaping.test_transpose_stub()
    test_reshaping.test_drop_duplicates_stub()
    test_reshaping.test_series_sort_values_stub()
    print("test_reshaping: all tests passed")


fn run_series_tests() raises:
    test_series.test_from_pandas()
    test_series.test_size()
    test_series.test_empty_false()
    test_series.test_empty_true()
    test_series.test_shape()
    test_series.test_to_pandas_roundtrip()
    test_series.test_sum()
    test_series.test_mean()
    test_series.test_median()
    test_series.test_min()
    test_series.test_max()
    test_series.test_std()
    test_series.test_var()
    test_series.test_count()
    test_series.test_nunique()
    test_series.test_quantile()
    test_series.test_describe()
    test_series.test_value_counts()
    test_series.test_stub_raises_head()
    print("test_series: all tests passed")


def main():
    run_accessors_tests()
    run_aggregation_tests()
    run_combining_tests()
    run_dataframe_tests()
    run_groupby_tests()
    run_interop_tests()
    run_io_tests()
    run_missing_tests()
    run_reshaping_tests()
    run_series_tests()
    print("")
    print("All test suites passed.")
