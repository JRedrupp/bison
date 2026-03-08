from ._errors import _not_implemented


struct Index(Copyable, Movable):
    var _data: List[String]
    var name: String

    fn __init__(out self, data: List[String], name: String = ""):
        self._data = data
        self.name = name

    fn __copyinit__(out self, existing: Self):
        self._data = existing._data.copy()
        self.name = existing.name

    fn __moveinit__(out self, deinit existing: Self):
        self._data = existing._data^
        self.name = existing.name^

    fn __len__(self) -> Int:
        return len(self._data)

    fn __getitem__(self, i: Int) -> String:
        return self._data[i]

    fn tolist(self) -> List[String]:
        return self._data

    fn __repr__(self) -> String:
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

    fn get_loc(self, key: String) raises -> Int:
        _not_implemented("Index.get_loc")
        return -1

    fn unique(self) raises -> Index:
        _not_implemented("Index.unique")
        return Index(self._data.copy(), self.name)

    fn sort_values(self, ascending: Bool = True) raises -> Index:
        _not_implemented("Index.sort_values")
        return Index(self._data.copy(), self.name)


struct RangeIndex:
    var start: Int
    var stop: Int
    var step: Int
    var name: String

    fn __init__(out self, stop: Int, start: Int = 0, step: Int = 1, name: String = ""):
        self.start = start
        self.stop = stop
        self.step = step
        self.name = name

    fn __len__(self) -> Int:
        if self.step > 0:
            return max(0, (self.stop - self.start + self.step - 1) // self.step)
        return 0

    fn __getitem__(self, i: Int) -> Int:
        return self.start + i * self.step

    fn __repr__(self) -> String:
        return (
            "RangeIndex(start=" + String(self.start)
            + ", stop=" + String(self.stop)
            + ", step=" + String(self.step) + ")"
        )
