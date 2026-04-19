from ._token import (
    Token,
    Tokenizer,
    TK_IDENT,
    TK_INT,
    TK_FLOAT,
    TK_STRING,
    TK_TRUE,
    TK_FALSE,
    TK_NULL,
    TK_LT,
    TK_LE,
    TK_GT,
    TK_GE,
    TK_EQ,
    TK_NE,
    TK_AND,
    TK_OR,
    TK_NOT,
    TK_LPAREN,
    TK_RPAREN,
    TK_EOF,
)
from ._ast import (
    ASTNode,
    ParsedExpr,
    NK_IDENT,
    NK_INT,
    NK_FLOAT,
    NK_BOOL,
    NK_NULL,
    NK_STRING,
    NK_COMPARE,
    NK_NOT,
    NK_AND,
    NK_OR,
)


struct _Parser:
    """Recursive-descent parser for the minimal query/eval grammar.

    This is a private implementation type; use the top-level `parse()`
    function instead.

    Grammar (from docs/query-eval-spec.md):
        expr       := or_expr
        or_expr    := and_expr ("or" and_expr)*
        and_expr   := not_expr ("and" not_expr)*
        not_expr   := "not" not_expr | comparison
        comparison := primary (comp_op primary)?
        comp_op    := "<" | "<=" | ">" | ">=" | "==" | "!="
        primary    := IDENT | literal | "(" expr ")"
        literal    := INT | FLOAT | BOOL | STRING | NULL
    """

    var _tokens: List[Token]
    var _pos: Int
    var _nodes: List[ASTNode]

    def __init__(out self, var tokens: List[Token]):
        self._tokens = tokens^
        self._pos = 0
        self._nodes = List[ASTNode]()

    # ------------------------------------------------------------------
    # Token stream helpers
    # ------------------------------------------------------------------

    def _peek(self) -> Token:
        """Return the current token without consuming it."""
        if self._pos < len(self._tokens):
            return self._tokens[self._pos]
        return Token(TK_EOF, String(""))

    def _advance(mut self) -> Token:
        """Consume and return the current token."""
        if self._pos >= len(self._tokens):
            return Token(TK_EOF, String(""))
        var tok = self._tokens[self._pos]
        self._pos += 1
        return tok

    def _expect(mut self, kind: Int) raises -> Token:
        """Consume the current token, raising if it is not *kind*."""
        var tok = self._peek()
        if tok.kind != kind:
            if tok.kind == TK_EOF:
                raise Error("invalid expression: unexpected end of input")
            raise Error(
                "invalid expression: unexpected token '" + tok.value + "'"
            )
        return self._advance()

    # ------------------------------------------------------------------
    # Arena helpers
    # ------------------------------------------------------------------

    def _add_node(mut self, node: ASTNode) -> Int:
        """Append *node* to the arena and return its index."""
        var idx = len(self._nodes)
        self._nodes.append(node)
        return idx

    # ------------------------------------------------------------------
    # Recursive descent
    # ------------------------------------------------------------------

    def _parse_expr(mut self) raises -> Int:
        return self._parse_or()

    def _parse_or(mut self) raises -> Int:
        var left = self._parse_and()
        while self._peek().kind == TK_OR:
            _ = self._advance()
            var right = self._parse_and()
            left = self._add_node(ASTNode(NK_OR, String("or"), left, right))
        return left

    def _parse_and(mut self) raises -> Int:
        var left = self._parse_not()
        while self._peek().kind == TK_AND:
            _ = self._advance()
            var right = self._parse_not()
            left = self._add_node(ASTNode(NK_AND, String("and"), left, right))
        return left

    def _parse_not(mut self) raises -> Int:
        if self._peek().kind == TK_NOT:
            _ = self._advance()
            var operand = self._parse_not()
            return self._add_node(ASTNode(NK_NOT, String("not"), operand, -1))
        return self._parse_comparison()

    def _parse_comparison(mut self) raises -> Int:
        var left = self._parse_primary()
        var tok = self._peek()
        var is_comp_op = (
            tok.kind == TK_LT
            or tok.kind == TK_LE
            or tok.kind == TK_GT
            or tok.kind == TK_GE
            or tok.kind == TK_EQ
            or tok.kind == TK_NE
        )
        if is_comp_op:
            var op = self._advance()
            var right = self._parse_primary()
            return self._add_node(ASTNode(NK_COMPARE, op.value, left, right))
        return left

    def _parse_primary(mut self) raises -> Int:
        var tok = self._peek()
        if tok.kind == TK_IDENT:
            _ = self._advance()
            return self._add_node(ASTNode(NK_IDENT, tok.value))
        if tok.kind == TK_INT:
            _ = self._advance()
            return self._add_node(ASTNode(NK_INT, tok.value))
        if tok.kind == TK_FLOAT:
            _ = self._advance()
            return self._add_node(ASTNode(NK_FLOAT, tok.value))
        if tok.kind == TK_TRUE:
            _ = self._advance()
            return self._add_node(ASTNode(NK_BOOL, tok.value))
        if tok.kind == TK_FALSE:
            _ = self._advance()
            return self._add_node(ASTNode(NK_BOOL, tok.value))
        if tok.kind == TK_NULL:
            _ = self._advance()
            return self._add_node(ASTNode(NK_NULL, tok.value))
        if tok.kind == TK_STRING:
            _ = self._advance()
            return self._add_node(ASTNode(NK_STRING, tok.value))
        if tok.kind == TK_LPAREN:
            _ = self._advance()
            var inner = self._parse_expr()
            _ = self._expect(TK_RPAREN)
            return inner
        if tok.kind == TK_EOF:
            raise Error("invalid expression: unexpected end of input")
        raise Error("invalid expression: unexpected token '" + tok.value + "'")

    # ------------------------------------------------------------------
    # Result extraction
    # ------------------------------------------------------------------

    def _build_result(mut self, root: Int) raises -> ParsedExpr:
        """Return a ParsedExpr from the accumulated arena."""
        return ParsedExpr(self._nodes.copy(), root)


def parse(expr: String) raises -> ParsedExpr:
    """Parse *expr* into a `ParsedExpr` arena.

    Raises `Error` with a message containing `"unsupported syntax"` for
    lexically invalid input, or `"invalid expression"` for structurally
    invalid input (for example a missing closing parenthesis).
    """
    var tz = Tokenizer(expr)
    var tokens = List[Token]()
    while True:
        var tok = tz.next_token()
        var is_eof = tok.kind == TK_EOF
        tokens.append(tok)
        if is_eof:
            break
    var p = _Parser(tokens^)
    var root = p._parse_expr()
    var remaining = p._peek()
    if remaining.kind != TK_EOF:
        raise Error(
            "invalid expression: unexpected token '" + remaining.value + "'"
        )
    return p._build_result(root)
