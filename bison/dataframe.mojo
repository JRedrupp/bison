from python import Python, PythonObject
from collections import Optional
from ._errors import _not_implemented
from .series import Series
from .groupby import DataFrameGroupBy


struct DataFrame(Copyable, Movable):
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
            var n = cols.__len__()
            for i in range(n):
                self._columns.append(String(cols[i]))
            self._nrows = pd_df.__len__()
            self._ncols = n
        except:
            self._nrows = 0
            self._ncols = 0

    fn __copyinit__(out self, existing: Self):
        self._pd_df = existing._pd_df
        self._columns = existing._columns.copy()
        self._nrows = existing._nrows
        self._ncols = existing._ncols

    fn __moveinit__(out self, deinit existing: Self):
        self._pd_df = existing._pd_df^
        self._columns = existing._columns^
        self._nrows = existing._nrows
        self._ncols = existing._ncols

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
    fn from_records(records: PythonObject, columns: Optional[PythonObject] = None) raises -> DataFrame:
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
        return self._columns.copy()

    fn ndim(self) -> Int:
        return 2

    fn dtypes(self) raises -> Series:
        _not_implemented("DataFrame.dtypes")
        return Series(PythonObject(None))

    fn info(self) raises:
        _not_implemented("DataFrame.info")

    fn memory_usage(self, deep: Bool = False) raises -> Series:
        _not_implemented("DataFrame.memory_usage")
        return Series(PythonObject(None))

    # ------------------------------------------------------------------
    # Selection / indexing
    # ------------------------------------------------------------------

    fn __getitem__(self, key: String) raises -> Series:
        _not_implemented("DataFrame.__getitem__")
        return Series(PythonObject(None))

    fn __setitem__(inout self, key: String, value: PythonObject) raises:
        _not_implemented("DataFrame.__setitem__")

    fn get(self, key: String, default: Optional[PythonObject] = None) raises -> PythonObject:
        _not_implemented("DataFrame.get")
        return default

    fn head(self, n: Int = 5) raises -> DataFrame:
        _not_implemented("DataFrame.head")
        return DataFrame(self._pd_df)

    fn tail(self, n: Int = 5) raises -> DataFrame:
        _not_implemented("DataFrame.tail")
        return DataFrame(self._pd_df)

    fn sample(self, n: Int = 1, frac: Optional[PythonObject] = None, random_state: Optional[PythonObject] = None) raises -> DataFrame:
        _not_implemented("DataFrame.sample")
        return DataFrame(self._pd_df)

    fn filter(self, items: Optional[PythonObject] = None, like: String = "", regex: String = "", axis: Int = 1) raises -> DataFrame:
        _not_implemented("DataFrame.filter")
        return DataFrame(self._pd_df)

    fn select_dtypes(self, include: Optional[PythonObject] = None, exclude: Optional[PythonObject] = None) raises -> DataFrame:
        _not_implemented("DataFrame.select_dtypes")
        return DataFrame(self._pd_df)

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    fn sum(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        _not_implemented("DataFrame.sum")
        return Series(PythonObject(None))

    fn mean(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        _not_implemented("DataFrame.mean")
        return Series(PythonObject(None))

    fn median(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        _not_implemented("DataFrame.median")
        return Series(PythonObject(None))

    fn min(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        _not_implemented("DataFrame.min")
        return Series(PythonObject(None))

    fn max(self, axis: Int = 0, skipna: Bool = True) raises -> Series:
        _not_implemented("DataFrame.max")
        return Series(PythonObject(None))

    fn std(self, axis: Int = 0, ddof: Int = 1, skipna: Bool = True) raises -> Series:
        _not_implemented("DataFrame.std")
        return Series(PythonObject(None))

    fn var(self, axis: Int = 0, ddof: Int = 1, skipna: Bool = True) raises -> Series:
        _not_implemented("DataFrame.var")
        return Series(PythonObject(None))

    fn count(self, axis: Int = 0) raises -> Series:
        _not_implemented("DataFrame.count")
        return Series(PythonObject(None))

    fn nunique(self, axis: Int = 0) raises -> Series:
        _not_implemented("DataFrame.nunique")
        return Series(PythonObject(None))

    fn describe(self, include: Optional[PythonObject] = None, exclude: Optional[PythonObject] = None) raises -> DataFrame:
        _not_implemented("DataFrame.describe")
        return DataFrame(self._pd_df)

    fn quantile(self, q: Float64 = 0.5, axis: Int = 0) raises -> Series:
        _not_implemented("DataFrame.quantile")
        return Series(PythonObject(None))

    fn abs(self) raises -> DataFrame:
        _not_implemented("DataFrame.abs")
        return DataFrame(self._pd_df)

    fn cumsum(self, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.cumsum")
        return DataFrame(self._pd_df)

    fn cumprod(self, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.cumprod")
        return DataFrame(self._pd_df)

    fn cummin(self, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.cummin")
        return DataFrame(self._pd_df)

    fn cummax(self, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.cummax")
        return DataFrame(self._pd_df)

    fn agg(self, func: PythonObject, axis: Int = 0) raises -> Series:
        _not_implemented("DataFrame.agg")
        return Series(PythonObject(None))

    fn aggregate(self, func: PythonObject, axis: Int = 0) raises -> Series:
        _not_implemented("DataFrame.aggregate")
        return Series(PythonObject(None))

    fn apply(self, func: PythonObject, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.apply")
        return DataFrame(self._pd_df)

    fn applymap(self, func: PythonObject) raises -> DataFrame:
        _not_implemented("DataFrame.applymap")
        return DataFrame(self._pd_df)

    fn transform(self, func: PythonObject, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.transform")
        return DataFrame(self._pd_df)

    fn eval(self, expr: String) raises -> Series:
        _not_implemented("DataFrame.eval")
        return Series(PythonObject(None))

    fn query(self, expr: String) raises -> DataFrame:
        _not_implemented("DataFrame.query")
        return DataFrame(self._pd_df)

    fn pipe(self, func: PythonObject) raises -> DataFrame:
        _not_implemented("DataFrame.pipe")
        return DataFrame(self._pd_df)

    # ------------------------------------------------------------------
    # Missing data
    # ------------------------------------------------------------------

    fn isna(self) raises -> DataFrame:
        _not_implemented("DataFrame.isna")
        return DataFrame(self._pd_df)

    fn isnull(self) raises -> DataFrame:
        _not_implemented("DataFrame.isnull")
        return DataFrame(self._pd_df)

    fn notna(self) raises -> DataFrame:
        _not_implemented("DataFrame.notna")
        return DataFrame(self._pd_df)

    fn notnull(self) raises -> DataFrame:
        _not_implemented("DataFrame.notnull")
        return DataFrame(self._pd_df)

    fn fillna(self, value: PythonObject, method: String = "", axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.fillna")
        return DataFrame(self._pd_df)

    fn dropna(self, axis: Int = 0, how: String = "any", thresh: Optional[PythonObject] = None, subset: Optional[PythonObject] = None) raises -> DataFrame:
        _not_implemented("DataFrame.dropna")
        return DataFrame(self._pd_df)

    fn ffill(self, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.ffill")
        return DataFrame(self._pd_df)

    fn bfill(self, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.bfill")
        return DataFrame(self._pd_df)

    fn interpolate(self, method: String = "linear", axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.interpolate")
        return DataFrame(self._pd_df)

    # ------------------------------------------------------------------
    # Reshaping / sorting
    # ------------------------------------------------------------------

    fn sort_values(self, by: PythonObject, ascending: Bool = True, na_position: String = "last") raises -> DataFrame:
        _not_implemented("DataFrame.sort_values")
        return DataFrame(self._pd_df)

    fn sort_index(self, axis: Int = 0, ascending: Bool = True) raises -> DataFrame:
        _not_implemented("DataFrame.sort_index")
        return DataFrame(self._pd_df)

    fn reset_index(self, drop: Bool = False) raises -> DataFrame:
        _not_implemented("DataFrame.reset_index")
        return DataFrame(self._pd_df)

    fn set_index(self, keys: PythonObject, drop: Bool = True) raises -> DataFrame:
        _not_implemented("DataFrame.set_index")
        return DataFrame(self._pd_df)

    fn rename(self, columns: Optional[PythonObject] = None, index: Optional[PythonObject] = None) raises -> DataFrame:
        _not_implemented("DataFrame.rename")
        return DataFrame(self._pd_df)

    fn rename_axis(self, mapper: Optional[PythonObject] = None, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.rename_axis")
        return DataFrame(self._pd_df)

    fn reindex(self, labels: Optional[PythonObject] = None, axis: Int = 0, fill_value: Optional[PythonObject] = None) raises -> DataFrame:
        _not_implemented("DataFrame.reindex")
        return DataFrame(self._pd_df)

    fn drop(self, labels: Optional[PythonObject] = None, axis: Int = 0, columns: Optional[PythonObject] = None) raises -> DataFrame:
        _not_implemented("DataFrame.drop")
        return DataFrame(self._pd_df)

    fn drop_duplicates(self, subset: Optional[PythonObject] = None, keep: String = "first") raises -> DataFrame:
        _not_implemented("DataFrame.drop_duplicates")
        return DataFrame(self._pd_df)

    fn duplicated(self, subset: Optional[PythonObject] = None, keep: String = "first") raises -> Series:
        _not_implemented("DataFrame.duplicated")
        return Series(PythonObject(None))

    fn pivot(self, index: String = "", columns: String = "", values: String = "") raises -> DataFrame:
        _not_implemented("DataFrame.pivot")
        return DataFrame(self._pd_df)

    fn pivot_table(self, values: Optional[PythonObject] = None, index: Optional[PythonObject] = None, columns: Optional[PythonObject] = None, aggfunc: String = "mean") raises -> DataFrame:
        _not_implemented("DataFrame.pivot_table")
        return DataFrame(self._pd_df)

    fn melt(self, id_vars: Optional[PythonObject] = None, value_vars: Optional[PythonObject] = None, var_name: String = "variable", value_name: String = "value") raises -> DataFrame:
        _not_implemented("DataFrame.melt")
        return DataFrame(self._pd_df)

    fn stack(self, level: Int = -1) raises -> Series:
        _not_implemented("DataFrame.stack")
        return Series(PythonObject(None))

    fn unstack(self, level: Int = -1) raises -> DataFrame:
        _not_implemented("DataFrame.unstack")
        return DataFrame(self._pd_df)

    fn transpose(self) raises -> DataFrame:
        _not_implemented("DataFrame.transpose")
        return DataFrame(self._pd_df)

    fn T(self) raises -> DataFrame:
        _not_implemented("DataFrame.T")
        return DataFrame(self._pd_df)

    fn swaplevel(self, i: Int = -2, j: Int = -1, axis: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.swaplevel")
        return DataFrame(self._pd_df)

    fn explode(self, column: String) raises -> DataFrame:
        _not_implemented("DataFrame.explode")
        return DataFrame(self._pd_df)

    fn clip(self, lower: Optional[PythonObject] = None, upper: Optional[PythonObject] = None) raises -> DataFrame:
        _not_implemented("DataFrame.clip")
        return DataFrame(self._pd_df)

    fn round(self, decimals: Int = 0) raises -> DataFrame:
        _not_implemented("DataFrame.round")
        return DataFrame(self._pd_df)

    fn astype(self, dtype: PythonObject) raises -> DataFrame:
        _not_implemented("DataFrame.astype")
        return DataFrame(self._pd_df)

    fn copy(self, deep: Bool = True) raises -> DataFrame:
        _not_implemented("DataFrame.copy")
        return DataFrame(self._pd_df)

    fn assign(self, **kwargs: PythonObject) raises -> DataFrame:
        _not_implemented("DataFrame.assign")
        return DataFrame(self._pd_df)

    fn insert(inout self, loc: Int, column: String, value: PythonObject) raises:
        _not_implemented("DataFrame.insert")

    fn pop(inout self, item: String) raises -> Series:
        _not_implemented("DataFrame.pop")
        return Series(PythonObject(None))

    fn where(self, cond: PythonObject, other: Optional[PythonObject] = None) raises -> DataFrame:
        _not_implemented("DataFrame.where")
        return DataFrame(self._pd_df)

    fn mask(self, cond: PythonObject, other: Optional[PythonObject] = None) raises -> DataFrame:
        _not_implemented("DataFrame.mask")
        return DataFrame(self._pd_df)

    fn isin(self, values: PythonObject) raises -> DataFrame:
        _not_implemented("DataFrame.isin")
        return DataFrame(self._pd_df)

    fn combine_first(self, other: DataFrame) raises -> DataFrame:
        _not_implemented("DataFrame.combine_first")
        return DataFrame(self._pd_df)

    fn update(inout self, other: DataFrame) raises:
        _not_implemented("DataFrame.update")

    # ------------------------------------------------------------------
    # Combining
    # ------------------------------------------------------------------

    fn merge(
        self,
        right: DataFrame,
        how: String = "inner",
        on: Optional[PythonObject] = None,
        left_on: Optional[PythonObject] = None,
        right_on: Optional[PythonObject] = None,
        left_index: Bool = False,
        right_index: Bool = False,
        suffixes: Optional[PythonObject] = None,
    ) raises -> DataFrame:
        _not_implemented("DataFrame.merge")
        return DataFrame(self._pd_df)

    fn join(
        self,
        other: DataFrame,
        on: Optional[PythonObject] = None,
        how: String = "left",
        lsuffix: String = "",
        rsuffix: String = "",
        sort: Bool = False,
    ) raises -> DataFrame:
        _not_implemented("DataFrame.join")
        return DataFrame(self._pd_df)

    fn append(self, other: DataFrame, ignore_index: Bool = False) raises -> DataFrame:
        _not_implemented("DataFrame.append")
        return DataFrame(self._pd_df)

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

    fn rolling(self, window: Int, min_periods: Optional[PythonObject] = None) raises -> PythonObject:
        _not_implemented("DataFrame.rolling")
        return PythonObject(None)

    fn expanding(self, min_periods: Int = 1) raises -> PythonObject:
        _not_implemented("DataFrame.expanding")
        return PythonObject(None)

    fn ewm(self, com: Optional[PythonObject] = None, span: Optional[PythonObject] = None) raises -> PythonObject:
        _not_implemented("DataFrame.ewm")
        return PythonObject(None)

    # ------------------------------------------------------------------
    # IO
    # ------------------------------------------------------------------

    fn to_csv(self, path_or_buf: String = "", sep: String = ",", index: Bool = True) raises -> String:
        _not_implemented("DataFrame.to_csv")
        return String("")

    fn to_parquet(self, path: String, engine: String = "auto", compression: String = "snappy") raises:
        _not_implemented("DataFrame.to_parquet")

    fn to_json(self, path_or_buf: String = "", orient: String = "") raises -> String:
        _not_implemented("DataFrame.to_json")
        return String("")

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
        return String(self._pd_df)

    fn __len__(self) -> Int:
        return self._nrows

    fn __contains__(self, key: String) -> Bool:
        for col in self._columns:
            if col == key:
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
