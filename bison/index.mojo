from std.collections import Set
from std.python import PythonObject
from std.builtin.sort import sort
from std.utils import Variant


struct Index(Copyable, Movable):
    var _data: List[String]
    var name: String

    def __init__(out self, var data: List[String], name: String = ""):
        self._data = data^
        self.name = name

    def __init__(out self, *, copy: Self):
        self._data = copy._data.copy()
        self.name = copy.name

    def __init__(out self, *, deinit take: Self):
        self._data = take._data^
        self.name = take.name^

    def __len__(self) -> Int:
        return len(self._data)

    def __getitem__(self, i: Int) -> String:
        return self._data[i]

    def tolist(self) -> List[String]:
        return self._data.copy()

    def __repr__(self) -> String:
        var s = String("Index([")
        for i in range(len(self._data)):
            if i > 0:
                s += ", "
            s += "'" + self._data[i] + "'"
        s += "]"
        if self.name:
            s += ", name='" + self.name + "'"
        s += ")"
        return s

    def get_loc(self, key: String) raises -> Int:
        for i in range(len(self._data)):
            if self._data[i] == key:
                return i
        raise Error("KeyError: '" + key + "'")

    def unique(self) raises -> Index:
        var seen = Set[String]()
        var result = List[String]()
        for i in range(len(self._data)):
            if self._data[i] not in seen:
                seen.add(self._data[i])
                result.append(self._data[i])
        return Index(result^, self.name)

    def sort_values(self, ascending: Bool = True) raises -> Index:
        var result = self._data.copy()
        sort(result)
        if not ascending:
            var n = len(result)
            for i in range(n // 2):
                var tmp = result[i]
                result[i] = result[n - 1 - i]
                result[n - 1 - i] = tmp
        return Index(result^, self.name)


struct RangeIndex:
    var start: Int
    var stop: Int
    var step: Int
    var name: String

    def __init__(
        out self, stop: Int, start: Int = 0, step: Int = 1, name: String = ""
    ):
        self.start = start
        self.stop = stop
        self.step = step
        self.name = name

    def __len__(self) -> Int:
        if self.step > 0:
            return max(0, (self.stop - self.start + self.step - 1) // self.step)
        return 0

    def __getitem__(self, i: Int) -> Int:
        return self.start + i * self.step

    def __repr__(self) -> String:
        return (
            "RangeIndex(start="
            + String(self.start)
            + ", stop="
            + String(self.stop)
            + ", step="
            + String(self.step)
            + ")"
        )


# ColumnIndex is the native row-index storage type for Column.
#
# Three active arms:
#   Index (List[String])  — string labels (most common case)
#   List[Int64]           — integer labels
#   List[PythonObject]    — fallback for DatetimeIndex, MultiIndex, etc.
#                           An *empty* List[PythonObject] means "no explicit
#                           index" (i.e. a default RangeIndex).
comptime ColumnIndex = Variant[Index, List[Int64], List[PythonObject]]
