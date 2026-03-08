from python import Python, PythonObject
from ._errors import _not_implemented
from .series import Series
from .groupby import DataFrameGroupBy


struct DataFrame:
    """A two-dimensional labeled data structure, mirroring the pandas DataFrame API."""

    var _pd_df: PythonObject   # backing pandas DataFrame — stub stage only
    var _columns: List[String]
    var _nrows: Int
    var _ncols: Int

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    fn __init__(out self, pd_df: PythonObject):
        self._pd_df = pd_df
        self._columns = List[String]()
        try:
            var cols = pd_df.columns.tolist()
            var n = int(cols.__len__())
            for i in range(n):
                self._columns.append(str(cols[i]))
            self._nrows = int(pd_df.__len__())
            self._ncols = n
        except:
            self._nrows = 0
            self._ncols = 0

    @staticmethod
    fn from_pandas(pd_df: PythonObject) raises -> DataFrame:
        return DataFrame(pd_df)

    fn to_pandas(self) -> PythonObject:
        return self._pd_df

    @staticmethod
    fn from_dict(data: PythonObject) raises -> DataFrame:
        """Create DataFrame from a dict-like PythonObject."""
        var pd = Python.import_module("pandas")
        return DataFrame(pd.DataFrame(data))

    @staticmethod
    fn from_records(records: PythonObject, columns: PythonObject = PythonObject(None)) raises -> DataFrame:
        """Create DataFrame from a list of dicts/tuples."""
        var pd = Python.import_module("pandas")
        return DataFrame(pd.DataFrame.from_records(records, columns=columns))

    # ------------------------------------------------------------------
    # Attributes
    # ------------------------------------------------------------------

    fn shape(self) -> Tuple[Int, Int]:
        return (self._nrows, self._ncols)

    fn size(self) -> Int:
        return self._nrows * self._ncols

    fn empty(self) -> Bool:
        return self._nrows == 0 or self._ncols == 0

    fn columns(self) -> List[String]:
        return self._columns

    fn ndim(self) -> Int:
        return 2

    fn dtypes(self) raises -> PythonObject:
        _not_implemented("DataFrame.dtypes")
        return PythonObject(None)

    fn info(self) raises:
        _not_implemented("DataFrame.info")

    fn memory_usage(self, deep: Bool = False) raises -> PythonObject:
        _not_implemented("DataFrame.memory_usage")
        return PythonObject(None)

    # ------------------------------------------------------------------
    # Selection / indexing
    # ------------------------------------------------------------------

    fn __getitem__(self, key: String) raises -> Series:
        _not_implemented("DataFrame.__getitem__")
        return Series(PythonObject(None))

    fn __setitem__(inout self, key: String, value: PythonObject) raises:
        _not_implemented("DataFrame.__setitem__")

    fn get(self, key: String, default: PythonObject = PythonObject(None)) raises -> PythonObject:
        _not_implemented("DataFrame.get")
        return default

    fn head(self, n: Int = 5) raises -> DataFrame:
        _not_implemented("DataFrame.head")
        return self

    fn tail(self, n: Int = 5) raises -> DataFrame:
        _not_implemented("DataFrame.tail")
        return self

    fn sample(self, n: Int = 1, frac: PythonObject = PythonObject(None), random_state: PythonObject = PythonObject(None)) raises -> DataFrame:
        _not_implemented("DataFrame.sample")
        return self

    fn filter(self, items: PythonObject = PythonObject(None), like: String = "", regex: String = "", axis: Int = 1) raises -> DataFrame:
        _not_implemented("DataFrame.filter")
        return self

    fn select_dtypes(self, include: PythonObject = PythonObject(None), exclude: PythonObject = PythonObject(None)) raises -> DataFrame:
        _not_implemented("DataFrame.select_dtypes")
        return self

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    fn sum(self, axis: Int = 0, skipna: Bool = True) raises -> PythonObject:
        _not_implemented("DataFrame.sum")
        return PythonObject(None)

    fn mean(self, axis: Int = 0, skipna: Bool = True) raises -> PythonObject:
        _not_implemented("DataFrame.mean")
        return PythonObject(None)

    fn median(self, axis: Int = 0, skipna: Bool = True) raises -> PythonObject:
        _not_implemented("DataFrame.median")
        return PythonObject(None)

    fn min(self, axis: Int = 0, skipna: Bool = True) raises -> PythonObject:
        _not_implemented("DataFrame.min")
        return PythonObject(None)

    fn max(self, axis: Int = 0, skipna: Bool = True) raises -> PythonObject:
        _not_implemented("DataFrame.max")
        return PythonObject(None)

    fn std(self, axis: Int = 0, ddof: Int = 1, skipna: Bool = True) raises -> PythonObject:
        _not_implemented("DataFrame.std")
        return PythonObject(None)

    fn var(self, axis: Int = 0, ddof: Int = 1, skipna: Bool = True) raises -> PythonObject:
        _not_implemented("DataFrame.var")
        return PythonObject(None)

    fn count(self, axis: Int = 0) raises -> PythonObject:
        _not_implemented("DataFrame.count")
        return PythonObject(None)

    fn nunique(self, axis: Int = 0) raises -> PythonObject:
        _not_implemented("DataFrame.nunique")
        return PythonObject(None)

    fn describe(self, include: PythonObject = PythonObject(None), exclude: PythonObject = PythonObject(None)) raises -> DataFrame:
        _not_implemented("DataFrame.describe")
        return self

    fn quantile(self, q: Float64 = 0.5, axis: Int = 0) raises -> PythonObject:
        _not_implemented("DataFrame.quantile")
        return PythonObject(None)

    fn abs(self) raises -> DataFrame:
        _not_implemented("DataFrame.abs")
        return self

    fn cumsum(self, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.cumsum")
        return self

    fn cumprod(self, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.cumprod")
        return self

    fn cummin(self, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.cummin")
        return self

    fn cummax(self, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.cummax")
        return self

    fn agg(self, func: PythonObject, axis: Int = 0) raises -> PythonObject:
        _not_implemented("DataFrame.agg")
        return PythonObject(None)

    fn aggregate(self, func: PythonObject, axis: Int = 0) raises -> PythonObject:
        _not_implemented("DataFrame.aggregate")
        return PythonObject(None)

    fn apply(self, func: PythonObject, axis: Int = 0) raises -> PythonObject:
        _not_implemented("DataFrame.apply")
        return PythonObject(None)

    fn applymap(self, func: PythonObject) raises -> DataFrame:
        _not_implemented("DataFrame.applymap")
        return self

    fn transform(self, func: PythonObject, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.transform")
        return self

    fn eval(self, expr: String) raises -> PythonObject:
        _not_implemented("DataFrame.eval")
        return PythonObject(None)

    fn query(self, expr: String) raises -> DataFrame:
        _not_implemented("DataFrame.query")
        return self

    fn pipe(self, func: PythonObject) raises -> PythonObject:
        _not_implemented("DataFrame.pipe")
        return PythonObject(None)

    # ------------------------------------------------------------------
    # Missing data
    # ------------------------------------------------------------------

    fn isna(self) raises -> DataFrame:
        _not_implemented("DataFrame.isna")
        return self

    fn isnull(self) raises -> DataFrame:
        _not_implemented("DataFrame.isnull")
        return self

    fn notna(self) raises -> DataFrame:
        _not_implemented("DataFrame.notna")
        return self

    fn notnull(self) raises -> DataFrame:
        _not_implemented("DataFrame.notnull")
        return self

    fn fillna(self, value: PythonObject, method: String = "", axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.fillna")
        return self

    fn dropna(self, axis: Int = 0, how: String = "any", thresh: PythonObject = PythonObject(None), subset: PythonObject = PythonObject(None)) raises -> DataFrame:
        _not_implemented("DataFrame.dropna")
        return self

    fn ffill(self, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.ffill")
        return self

    fn bfill(self, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.bfill")
        return self

    fn interpolate(self, method: String = "linear", axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.interpolate")
        return self

    # ------------------------------------------------------------------
    # Reshaping / sorting
    # ------------------------------------------------------------------

    fn sort_values(self, by: PythonObject, ascending: Bool = True, na_position: String = "last") raises -> DataFrame:
        _not_implemented("DataFrame.sort_values")
        return self

    fn sort_index(self, axis: Int = 0, ascending: Bool = True) raises -> DataFrame:
        _not_implemented("DataFrame.sort_index")
        return self

    fn reset_index(self, drop: Bool = False) raises -> DataFrame:
        _not_implemented("DataFrame.reset_index")
        return self

    fn set_index(self, keys: PythonObject, drop: Bool = True) raises -> DataFrame:
        _not_implemented("DataFrame.set_index")
        return self

    fn rename(self, columns: PythonObject = PythonObject(None), index: PythonObject = PythonObject(None)) raises -> DataFrame:
        _not_implemented("DataFrame.rename")
        return self

    fn rename_axis(self, mapper: PythonObject = PythonObject(None), axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.rename_axis")
        return self

    fn reindex(self, labels: PythonObject = PythonObject(None), axis: Int = 0, fill_value: PythonObject = PythonObject(None)) raises -> DataFrame:
        _not_implemented("DataFrame.reindex")
        return self

    fn drop(self, labels: PythonObject = PythonObject(None), axis: Int = 0, columns: PythonObject = PythonObject(None)) raises -> DataFrame:
        _not_implemented("DataFrame.drop")
        return self

    fn drop_duplicates(self, subset: PythonObject = PythonObject(None), keep: String = "first") raises -> DataFrame:
        _not_implemented("DataFrame.drop_duplicates")
        return self

    fn duplicated(self, subset: PythonObject = PythonObject(None), keep: String = "first") raises -> PythonObject:
        _not_implemented("DataFrame.duplicated")
        return PythonObject(None)

    fn pivot(self, index: String = "", columns: String = "", values: String = "") raises -> DataFrame:
        _not_implemented("DataFrame.pivot")
        return self

    fn pivot_table(self, values: PythonObject = PythonObject(None), index: PythonObject = PythonObject(None), columns: PythonObject = PythonObject(None), aggfunc: String = "mean") raises -> DataFrame:
        _not_implemented("DataFrame.pivot_table")
        return self

    fn melt(self, id_vars: PythonObject = PythonObject(None), value_vars: PythonObject = PythonObject(None), var_name: String = "variable", value_name: String = "value") raises -> DataFrame:
        _not_implemented("DataFrame.melt")
        return self

    fn stack(self, level: Int = -1) raises -> PythonObject:
        _not_implemented("DataFrame.stack")
        return PythonObject(None)

    fn unstack(self, level: Int = -1) raises -> PythonObject:
        _not_implemented("DataFrame.unstack")
        return PythonObject(None)

    fn transpose(self) raises -> DataFrame:
        _not_implemented("DataFrame.transpose")
        return self

    fn T(self) raises -> DataFrame:
        _not_implemented("DataFrame.T")
        return self

    fn swaplevel(self, i: Int = -2, j: Int = -1, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.swaplevel")
        return self

    fn explode(self, column: String) raises -> DataFrame:
        _not_implemented("DataFrame.explode")
        return self

    fn clip(self, lower: PythonObject = PythonObject(None), upper: PythonObject = PythonObject(None)) raises -> DataFrame:
        _not_implemented("DataFrame.clip")
        return self

    fn round(self, decimals: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.round")
        return self

    fn astype(self, dtype: PythonObject) raises -> DataFrame:
        _not_implemented("DataFrame.astype")
        return self

    fn copy(self, deep: Bool = True) raises -> DataFrame:
        _not_implemented("DataFrame.copy")
        return self

    fn assign(self, **kwargs: PythonObject) raises -> DataFrame:
        _not_implemented("DataFrame.assign")
        return self

    fn insert(inout self, loc: Int, column: String, value: PythonObject) raises:
        _not_implemented("DataFrame.insert")

    fn pop(inout self, item: String) raises -> Series:
        _not_implemented("DataFrame.pop")
        return Series(PythonObject(None))

    fn where(self, cond: PythonObject, other: PythonObject = PythonObject(None)) raises -> DataFrame:
        _not_implemented("DataFrame.where")
        return self

    fn mask(self, cond: PythonObject, other: PythonObject = PythonObject(None)) raises -> DataFrame:
        _not_implemented("DataFrame.mask")
        return self

    fn isin(self, values: PythonObject) raises -> DataFrame:
        _not_implemented("DataFrame.isin")
        return self

    fn combine_first(self, other: DataFrame) raises -> DataFrame:
        _not_implemented("DataFrame.combine_first")
        return self

    fn update(inout self, other: DataFrame) raises:
        _not_implemented("DataFrame.update")

    # ------------------------------------------------------------------
    # Combining
    # ------------------------------------------------------------------

    fn merge(
        self,
        right: DataFrame,
        how: String = "inner",
        on: PythonObject = PythonObject(None),
        left_on: PythonObject = PythonObject(None),
        right_on: PythonObject = PythonObject(None),
        left_index: Bool = False,
        right_index: Bool = False,
        suffixes: PythonObject = PythonObject(None),
    ) raises -> DataFrame:
        _not_implemented("DataFrame.merge")
        return self

    fn join(
        self,
        other: DataFrame,
        on: PythonObject = PythonObject(None),
        how: String = "left",
        lsuffix: String = "",
        rsuffix: String = "",
        sort: Bool = False,
    ) raises -> DataFrame:
        _not_implemented("DataFrame.join")
        return self

    fn append(self, other: DataFrame, ignore_index: Bool = False) raises -> DataFrame:
        _not_implemented("DataFrame.append")
        return self

    # ------------------------------------------------------------------
    # GroupBy
    # ------------------------------------------------------------------

    fn groupby(
        self,
        by: PythonObject,
        axis: Int = 0,
        as_index: Bool = True,
        sort: Bool = True,
        dropna: Bool = True,
    ) raises -> DataFrameGroupBy:
        _not_implemented("DataFrame.groupby")
        return DataFrameGroupBy(PythonObject(None))

    fn resample(self, rule: String, axis: Int = 0) raises -> PythonObject:
        _not_implemented("DataFrame.resample")
        return PythonObject(None)

    fn rolling(self, window: Int, min_periods: PythonObject = PythonObject(None)) raises -> PythonObject:
        _not_implemented("DataFrame.rolling")
        return PythonObject(None)

    fn expanding(self, min_periods: Int = 1) raises -> PythonObject:
        _not_implemented("DataFrame.expanding")
        return PythonObject(None)

    fn ewm(self, com: PythonObject = PythonObject(None), span: PythonObject = PythonObject(None)) raises -> PythonObject:
        _not_implemented("DataFrame.ewm")
        return PythonObject(None)

    # ------------------------------------------------------------------
    # IO
    # ------------------------------------------------------------------

    fn to_csv(self, path_or_buf: String = "", sep: String = ",", index: Bool = True) raises -> PythonObject:
        _not_implemented("DataFrame.to_csv")
        return PythonObject(None)

    fn to_parquet(self, path: String, engine: String = "auto", compression: String = "snappy") raises:
        _not_implemented("DataFrame.to_parquet")

    fn to_json(self, path_or_buf: String = "", orient: String = "") raises -> PythonObject:
        _not_implemented("DataFrame.to_json")
        return PythonObject(None)

    fn to_excel(self, excel_writer: String, sheet_name: String = "Sheet1", index: Bool = True) raises:
        _not_implemented("DataFrame.to_excel")

    fn to_dict(self, orient: String = "dict") raises -> PythonObject:
        _not_implemented("DataFrame.to_dict")
        return PythonObject(None)

    fn to_records(self, index: Bool = True) raises -> PythonObject:
        _not_implemented("DataFrame.to_records")
        return PythonObject(None)

    fn to_numpy(self) raises -> PythonObject:
        _not_implemented("DataFrame.to_numpy")
        return PythonObject(None)

    fn to_string(self) raises -> String:
        _not_implemented("DataFrame.to_string")
        return ""

    fn to_html(self) raises -> String:
        _not_implemented("DataFrame.to_html")
        return ""

    fn to_markdown(self) raises -> String:
        _not_implemented("DataFrame.to_markdown")
        return ""

    # ------------------------------------------------------------------
    # Repr / iteration
    # ------------------------------------------------------------------

    fn __repr__(self) raises -> String:
        return str(self._pd_df)

    fn __len__(self) -> Int:
        return self._nrows

    fn __contains__(self, key: String) -> Bool:
        for col in self._columns:
            if col[] == key:
                return True
        return False

    fn items(self) raises -> PythonObject:
        _not_implemented("DataFrame.items")
        return PythonObject(None)

    fn iterrows(self) raises -> PythonObject:
        _not_implemented("DataFrame.iterrows")
        return PythonObject(None)

    fn itertuples(self, index: Bool = True, name: String = "Pandas") raises -> PythonObject:
        _not_implemented("DataFrame.itertuples")
        return PythonObject(None)
