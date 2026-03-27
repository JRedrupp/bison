def _not_implemented(method: String, detail: String = "") raises:
    if len(detail) > 0:
        raise Error("bison." + method + ": not implemented (" + detail + ")")
    raise Error("bison." + method + ": not implemented")
