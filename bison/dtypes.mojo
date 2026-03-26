struct BisonDtype(ImplicitlyCopyable, Movable):
    var name: String
    var itemsize: Int

    def __init__(out self, name: String, itemsize: Int):
        self.name = name
        self.itemsize = itemsize

    def __init__(out self, *, copy: Self):
        self.name = copy.name
        self.itemsize = copy.itemsize

    def __init__(out self, *, deinit take: Self):
        self.name = take.name^
        self.itemsize = take.itemsize

    def __str__(self) -> String:
        return self.name

    def __eq__(self, other: BisonDtype) -> Bool:
        return self.name == other.name

    def __ne__(self, other: BisonDtype) -> Bool:
        return self.name != other.name

    def is_integer(self) -> Bool:
        """Return True if this dtype is any integer family (int8/16/32/64, uint8/16/32/64).
        """
        return (
            self.name == "int8"
            or self.name == "int16"
            or self.name == "int32"
            or self.name == "int64"
            or self.name == "uint8"
            or self.name == "uint16"
            or self.name == "uint32"
            or self.name == "uint64"
        )

    def is_float(self) -> Bool:
        """Return True if this dtype is any floating-point family (float32/float64).
        """
        return self.name == "float32" or self.name == "float64"


comptime int8 = BisonDtype("int8", 1)
comptime int16 = BisonDtype("int16", 2)
comptime int32 = BisonDtype("int32", 4)
comptime int64 = BisonDtype("int64", 8)
comptime uint8 = BisonDtype("uint8", 1)
comptime uint16 = BisonDtype("uint16", 2)
comptime uint32 = BisonDtype("uint32", 4)
comptime uint64 = BisonDtype("uint64", 8)
comptime float32 = BisonDtype("float32", 4)
comptime float64 = BisonDtype("float64", 8)
comptime bool_ = BisonDtype("bool", 1)
comptime object_ = BisonDtype("object", 8)
comptime datetime64_ns = BisonDtype("datetime64[ns]", 8)
comptime timedelta64_ns = BisonDtype("timedelta64[ns]", 8)


def dtype_from_string(name: String) raises -> BisonDtype:
    """Convert a dtype name string to the corresponding BisonDtype constant.

    Raises an error if the name does not match any known dtype.
    """
    if name == "int8":
        return int8
    if name == "int16":
        return int16
    if name == "int32":
        return int32
    if name == "int64":
        return int64
    if name == "uint8":
        return uint8
    if name == "uint16":
        return uint16
    if name == "uint32":
        return uint32
    if name == "uint64":
        return uint64
    if name == "float32":
        return float32
    if name == "float64":
        return float64
    if name == "bool":
        return bool_
    if name == "object":
        return object_
    if name == "datetime64[ns]":
        return datetime64_ns
    if name == "timedelta64[ns]":
        return timedelta64_ns
    raise Error("dtype_from_string: unknown dtype name '" + name + "'")
