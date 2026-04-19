# Node kind constants for the expression AST arena.
# Values are stable; do not reorder.
comptime NK_IDENT: Int = 0  # column reference (bare identifier)
comptime NK_INT: Int = 1  # integer literal
comptime NK_FLOAT: Int = 2  # float literal
comptime NK_BOOL: Int = 3  # boolean literal (True / False)
comptime NK_NULL: Int = 4  # null literal (None)
comptime NK_STRING: Int = 5  # quoted string literal
comptime NK_COMPARE: Int = 6  # binary comparison; op stored in `value`
comptime NK_NOT: Int = 7  # unary "not"; operand index in `left`
comptime NK_AND: Int = 8  # logical "and"
comptime NK_OR: Int = 9  # logical "or"


struct ASTNode(Copyable, ImplicitlyCopyable, Movable):
    """A single node in the flat expression AST arena.

    *value* holds:
    - The source text for leaf nodes (NK_IDENT, NK_INT, NK_FLOAT, NK_BOOL,
      NK_NULL, NK_STRING).
    - The operator string (`"<"`, `"<="`, `">="`, `">"`, `"=="`, `"!="`) for
      NK_COMPARE nodes.
    - The keyword string for NK_NOT / NK_AND / NK_OR (useful for debugging).

    *left* and *right* are arena indices pointing to child nodes; -1 means
    the slot is absent.
    """

    var kind: Int
    var value: String
    var left: Int
    var right: Int

    def __init__(
        out self, kind: Int, value: String, left: Int = -1, right: Int = -1
    ):
        self.kind = kind
        self.value = value
        self.left = left
        self.right = right

    def __init__(out self, *, copy: Self):
        self.kind = copy.kind
        self.value = copy.value
        self.left = copy.left
        self.right = copy.right

    def __init__(out self, *, deinit take: Self):
        self.kind = take.kind
        self.value = take.value^
        self.left = take.left
        self.right = take.right


struct ParsedExpr(Movable):
    """The result of a successful parse.

    Nodes are stored in a flat *arena* (`List[ASTNode]`); child relationships
    are encoded as integer indices into that list.  *root* is the index of the
    top-level node.

    Use `node_at(i)` to retrieve a node by arena index.
    """

    var nodes: List[ASTNode]
    var root: Int

    def __init__(out self, var nodes: List[ASTNode], root: Int):
        self.nodes = nodes^
        self.root = root

    def __init__(out self, *, deinit take: Self):
        self.nodes = take.nodes^
        self.root = take.root

    def node_at(self, i: Int) -> ASTNode:
        """Return the node at arena index *i*."""
        return self.nodes[i]
