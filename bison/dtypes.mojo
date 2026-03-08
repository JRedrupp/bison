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


alias int8        = BisonDtype("int8", 1)
alias int16       = BisonDtype("int16", 2)
alias int32       = BisonDtype("int32", 4)
alias int64       = BisonDtype("int64", 8)
alias uint8       = BisonDtype("uint8", 1)
alias uint16      = BisonDtype("uint16", 2)
alias uint32      = BisonDtype("uint32", 4)
alias uint64      = BisonDtype("uint64", 8)
alias float32     = BisonDtype("float32", 4)
alias float64     = BisonDtype("float64", 8)
alias bool_       = BisonDtype("bool", 1)
alias object_     = BisonDtype("object", 8)
alias datetime64_ns  = BisonDtype("datetime64[ns]", 8)
alias timedelta64_ns = BisonDtype("timedelta64[ns]", 8)
