# Token kind constants for the query/eval expression grammar.
# Values are stable; do not reorder.
comptime TK_IDENT: Int = 0  # bare identifier (column name)
comptime TK_INT: Int = 1  # integer literal
comptime TK_FLOAT: Int = 2  # float literal
comptime TK_STRING: Int = 3  # quoted string literal
comptime TK_TRUE: Int = 4  # True
comptime TK_FALSE: Int = 5  # False
comptime TK_NULL: Int = 6  # None
comptime TK_LT: Int = 7  # <
comptime TK_LE: Int = 8  # <=
comptime TK_GT: Int = 9  # >
comptime TK_GE: Int = 10  # >=
comptime TK_EQ: Int = 11  # ==
comptime TK_NE: Int = 12  # !=
comptime TK_AND: Int = 13  # and
comptime TK_OR: Int = 14  # or
comptime TK_NOT: Int = 15  # not
comptime TK_LPAREN: Int = 16  # (
comptime TK_RPAREN: Int = 17  # )
comptime TK_EOF: Int = 18  # end of input


struct Token(Copyable, ImplicitlyCopyable, Movable):
    """A single lexical token from the expression source."""

    var kind: Int
    var value: String

    def __init__(out self, kind: Int, value: String):
        self.kind = kind
        self.value = value

    def __init__(out self, *, copy: Self):
        self.kind = copy.kind
        self.value = copy.value

    def __init__(out self, *, deinit take: Self):
        self.kind = take.kind
        self.value = take.value^


struct Tokenizer:
    """Converts an expression string into a stream of Tokens.

    Call `next_token()` repeatedly until `TK_EOF` is returned.
    """

    var _src: String
    var _pos: Int

    def __init__(out self, src: String):
        self._src = src
        self._pos = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _extract(self, start: Int, end: Int) raises -> String:
        """Return the byte range [start, end) as a new String."""
        var bytes = self._src.as_bytes()
        return String(from_utf8=bytes[start:end])

    def _at_end(self) -> Bool:
        return self._pos >= self._src.byte_length()

    def _is_ident_start(self, b: UInt8) -> Bool:
        # A-Z (65-90), a-z (97-122), _ (95)
        return (b >= 65 and b <= 90) or (b >= 97 and b <= 122) or b == 95

    def _is_ident_cont(self, b: UInt8) -> Bool:
        return self._is_ident_start(b) or (b >= 48 and b <= 57)

    def _skip_whitespace(mut self):
        while self._pos < self._src.byte_length():
            var b = self._src.as_bytes()[self._pos]
            if b == 32 or b == 9 or b == 10 or b == 13:  # space, tab, LF, CR
                self._pos += 1
            else:
                break

    def _read_ident_or_keyword(mut self) raises -> Token:
        var start = self._pos
        while self._pos < self._src.byte_length() and self._is_ident_cont(
            self._src.as_bytes()[self._pos]
        ):
            self._pos += 1
        var word = self._extract(start, self._pos)
        if word == "and":
            return Token(TK_AND, word^)
        elif word == "or":
            return Token(TK_OR, word^)
        elif word == "not":
            return Token(TK_NOT, word^)
        elif word == "True":
            return Token(TK_TRUE, word^)
        elif word == "False":
            return Token(TK_FALSE, word^)
        elif word == "None":
            return Token(TK_NULL, word^)
        else:
            return Token(TK_IDENT, word^)

    def _read_number(mut self) raises -> Token:
        var start = self._pos
        while (
            self._pos < self._src.byte_length()
            and self._src.as_bytes()[self._pos] >= 48
            and self._src.as_bytes()[self._pos] <= 57
        ):
            self._pos += 1
        var is_float = False
        if (
            self._pos < self._src.byte_length() and self._src.as_bytes()[self._pos] == 46
        ):  # '.'
            is_float = True
            self._pos += 1
            while (
                self._pos < self._src.byte_length()
                and self._src.as_bytes()[self._pos] >= 48
                and self._src.as_bytes()[self._pos] <= 57
            ):
                self._pos += 1
        var num_str = self._extract(start, self._pos)
        if is_float:
            return Token(TK_FLOAT, num_str^)
        else:
            return Token(TK_INT, num_str^)

    def _read_string(mut self, quote: UInt8) raises -> Token:
        self._pos += 1  # skip opening quote
        var start = self._pos
        while (
            self._pos < self._src.byte_length()
            and self._src.as_bytes()[self._pos] != quote
        ):
            self._pos += 1
        if self._pos >= self._src.byte_length():
            raise Error("invalid expression: unterminated string")
        var s = self._extract(start, self._pos)
        self._pos += 1  # skip closing quote
        return Token(TK_STRING, s^)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def next_token(mut self) raises -> Token:
        """Consume and return the next token from the source string."""
        self._skip_whitespace()
        if self._at_end():
            return Token(TK_EOF, String(""))
        var b = self._src.as_bytes()[self._pos]
        # Identifier or keyword
        if self._is_ident_start(b):
            return self._read_ident_or_keyword()
        # Number
        if b >= 48 and b <= 57:  # 0-9
            return self._read_number()
        # Quoted string
        if b == 34:  # "
            return self._read_string(34)
        if b == 39:  # '
            return self._read_string(39)
        # Two-character operators (must check before single-char)
        if b == 60:  # <
            if (
                self._pos + 1 < self._src.byte_length()
                and self._src.as_bytes()[self._pos + 1] == 61
            ):
                self._pos += 2
                return Token(TK_LE, String("<="))
            self._pos += 1
            return Token(TK_LT, String("<"))
        if b == 62:  # >
            if (
                self._pos + 1 < self._src.byte_length()
                and self._src.as_bytes()[self._pos + 1] == 61
            ):
                self._pos += 2
                return Token(TK_GE, String(">="))
            self._pos += 1
            return Token(TK_GT, String(">"))
        if b == 61:  # =
            if (
                self._pos + 1 < self._src.byte_length()
                and self._src.as_bytes()[self._pos + 1] == 61
            ):
                self._pos += 2
                return Token(TK_EQ, String("=="))
            # Single = is assignment — unsupported
            raise Error("unsupported syntax: '='")
        if b == 33:  # !
            if (
                self._pos + 1 < self._src.byte_length()
                and self._src.as_bytes()[self._pos + 1] == 61
            ):
                self._pos += 2
                return Token(TK_NE, String("!="))
            raise Error("unsupported syntax: '!'")
        if b == 40:  # (
            self._pos += 1
            return Token(TK_LPAREN, String("("))
        if b == 41:  # )
            self._pos += 1
            return Token(TK_RPAREN, String(")"))
        # Anything else is explicitly unsupported syntax
        var ch = self._extract(self._pos, self._pos + 1)
        raise Error("unsupported syntax: '" + ch + "'")
