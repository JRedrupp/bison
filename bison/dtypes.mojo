struct BisonDtype(ImplicitlyCopyable, Movable):
    var name: String
    var itemsize: Int

    fn __init__(out self, name: String, itemsize: Int):
        self.name = name
        self.itemsize = itemsize

    fn __copyinit__(out self, existing: Self):
        self.name = existing.name
        self.itemsize = existing.itemsize

    fn __moveinit__(out self, deinit existing: Self):
        self.name = existing.name^
        self.itemsize = existing.itemsize

    fn __str__(self) -> String:
        return self.name

    fn __eq__(self, other: BisonDtype) -> Bool:
        return self.name == other.name

    fn __ne__(self, other: BisonDtype) -> Bool:
        return self.name != other.name


comptime int8        = BisonDtype("int8", 1)
comptime int16       = BisonDtype("int16", 2)
comptime int32       = BisonDtype("int32", 4)
comptime int64       = BisonDtype("int64", 8)
comptime uint8       = BisonDtype("uint8", 1)
comptime uint16      = BisonDtype("uint16", 2)
comptime uint32      = BisonDtype("uint32", 4)
comptime uint64      = BisonDtype("uint64", 8)
comptime float32     = BisonDtype("float32", 4)
comptime float64     = BisonDtype("float64", 8)
comptime bool_       = BisonDtype("bool", 1)
comptime object_     = BisonDtype("object", 8)
comptime datetime64_ns  = BisonDtype("datetime64[ns]", 8)
comptime timedelta64_ns = BisonDtype("timedelta64[ns]", 8)
