def _not_implemented(method: String, detail: String = "") raises:
    if detail.byte_length() > 0:
        raise Error("bison." + method + ": not implemented (" + detail + ")")
    raise Error("bison." + method + ": not implemented")
