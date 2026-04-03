"""Expression evaluator: resolve identifiers and literals, execute comparisons.

Walks a ``ParsedExpr`` arena produced by ``parse()`` and emits a boolean
``Series`` mask.  Supports comparison predicates (``NK_COMPARE``) and logical
connectives (``NK_AND``, ``NK_OR``, ``NK_NOT``) with Kleene three-valued null
semantics.  Parenthetical grouping and precedence are handled by the parser.
"""

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
from ..series import Series
from ..dataframe import DataFrame


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------


fn _resolve_ident(name: String, df: DataFrame) raises -> Series:
    """Return the column *name* from *df*, or raise a clear evaluator error."""
    try:
        return df[name]
    except:
        raise Error("evaluator: unknown identifier '" + name + "'")


fn _flip_op(op: String) raises -> String:
    """Flip a comparison operator for 'literal op identifier' → 'identifier flipped_op literal'.
    """
    if op == "<":
        return String(">")
    elif op == "<=":
        return String(">=")
    elif op == ">":
        return String("<")
    elif op == ">=":
        return String("<=")
    elif op == "==":
        return String("==")
    elif op == "!=":
        return String("!=")
    else:
        raise Error("evaluator: unknown operator '" + op + "'")


fn _apply_numeric_op(col: Series, op: String, val: Float64) raises -> Series:
    """Apply a numeric comparison operator to *col* against scalar *val*."""
    if op == "<":
        return col.__lt__(val)
    elif op == "<=":
        return col.__le__(val)
    elif op == ">":
        return col.__gt__(val)
    elif op == ">=":
        return col.__ge__(val)
    elif op == "==":
        return col.__eq__(val)
    elif op == "!=":
        return col.__ne__(val)
    else:
        raise Error("evaluator: unknown operator '" + op + "'")


fn _apply_string_op(col: Series, op: String, val: String) raises -> Series:
    """Apply a string equality/inequality operator to *col* against scalar *val*.
    """
    if op == "==":
        return col.__eq__(val)
    elif op == "!=":
        return col.__ne__(val)
    else:
        raise Error(
            "evaluator: string comparisons only support == and !=; got '"
            + op
            + "'"
        )


fn _parse_numeric_literal(node: ASTNode) raises -> Float64:
    """Convert an NK_INT or NK_FLOAT node value to Float64."""
    return atof(node.value)


fn _eval_compare(
    node: ASTNode, lhs: ASTNode, rhs: ASTNode, df: DataFrame
) raises -> Series:
    """Evaluate a single NK_COMPARE node into a boolean Series mask.

    *lhs* and *rhs* are the already-resolved child nodes (left and right
    operands of the comparison).
    """
    # For NK_COMPARE nodes, node.value holds the operator string
    # ("<", "<=", ">", ">=", "==", "!=") as set by the parser.
    var op = node.value

    var lhs_is_ident = lhs.kind == NK_IDENT
    var rhs_is_ident = rhs.kind == NK_IDENT
    var lhs_is_numeric = (lhs.kind == NK_INT) or (lhs.kind == NK_FLOAT)
    var rhs_is_numeric = (rhs.kind == NK_INT) or (rhs.kind == NK_FLOAT)
    var lhs_is_string = lhs.kind == NK_STRING
    var rhs_is_string = rhs.kind == NK_STRING

    if lhs_is_ident and rhs_is_ident:
        # column vs column
        var left_col = _resolve_ident(lhs.value, df)
        var right_col = _resolve_ident(rhs.value, df)
        if op == "<":
            return left_col.__lt__(right_col)
        elif op == "<=":
            return left_col.__le__(right_col)
        elif op == ">":
            return left_col.__gt__(right_col)
        elif op == ">=":
            return left_col.__ge__(right_col)
        elif op == "==":
            return left_col.__eq__(right_col)
        elif op == "!=":
            return left_col.__ne__(right_col)
        else:
            raise Error("evaluator: unknown operator '" + op + "'")

    elif lhs_is_ident and rhs_is_numeric:
        var col = _resolve_ident(lhs.value, df)
        var val = _parse_numeric_literal(rhs)
        return _apply_numeric_op(col, op, val)

    elif lhs_is_ident and rhs_is_string:
        var col = _resolve_ident(lhs.value, df)
        return _apply_string_op(col, op, rhs.value)

    elif lhs_is_numeric and rhs_is_ident:
        var col = _resolve_ident(rhs.value, df)
        var val = _parse_numeric_literal(lhs)
        var flipped = _flip_op(op)
        return _apply_numeric_op(col, flipped, val)

    elif lhs_is_string and rhs_is_ident:
        var col = _resolve_ident(rhs.value, df)
        var flipped = _flip_op(op)
        return _apply_string_op(col, flipped, lhs.value)

    else:
        raise Error(
            "evaluator: comparison must involve at least one column identifier"
        )


# ------------------------------------------------------------------
# Visitor — single canonical dispatch site for ASTNode kind
# ------------------------------------------------------------------


trait ASTNodeVisitorRaises:
    """Visitor over the expression-node kind set of an ``ASTNode`` arena.

    Implement one ``on_*`` method per expression node kind.  Pass an
    instance to ``visit_ast_node_raises``, which contains the **only**
    kind-dispatch chain in the codebase for expression nodes; all
    evaluation logic should delegate here instead of writing its own
    ``node.kind == NK_*`` checks.

    Each ``on_*`` method receives the current ``node``, the full
    ``ParsedExpr`` arena (for child-node look-ups), and the ``DataFrame``
    (for identifier resolution) as borrowed context parameters, so no data
    needs to be copied into the visitor struct.  Leaf node kinds
    (``NK_IDENT``, ``NK_INT``, ``NK_FLOAT``, ``NK_BOOL``, ``NK_NULL``,
    ``NK_STRING``) are resolved inside ``on_compare`` and do not require
    their own visitor methods.
    """

    def on_compare(
        mut self, node: ASTNode, parsed: ParsedExpr, df: DataFrame
    ) raises:
        ...

    def on_not(
        mut self, node: ASTNode, parsed: ParsedExpr, df: DataFrame
    ) raises:
        ...

    def on_and(
        mut self, node: ASTNode, parsed: ParsedExpr, df: DataFrame
    ) raises:
        ...

    def on_or(
        mut self, node: ASTNode, parsed: ParsedExpr, df: DataFrame
    ) raises:
        ...


def visit_ast_node_raises[
    V: ASTNodeVisitorRaises
](mut visitor: V, node: ASTNode, parsed: ParsedExpr, df: DataFrame) raises:
    """Dispatch *visitor* to the correct ``on_*`` method based on ``node.kind``.

    This is the **only** place in the codebase that reads the ``ASTNode``
    kind discriminant for expression nodes.  Add new expression node kinds
    here *and* in ``ASTNodeVisitorRaises``; every evaluation site is then
    updated automatically because it delegates here.

    *parsed* and *df* are passed through to each ``on_*`` method as borrowed
    context, so no copies of the arena or the ``DataFrame`` are made.
    """
    if node.kind == NK_COMPARE:
        visitor.on_compare(node, parsed, df)
    elif node.kind == NK_NOT:
        visitor.on_not(node, parsed, df)
    elif node.kind == NK_AND:
        visitor.on_and(node, parsed, df)
    elif node.kind == NK_OR:
        visitor.on_or(node, parsed, df)
    else:
        raise Error(
            "evaluator: unsupported expression kind " + String(node.kind)
        )


# ------------------------------------------------------------------
# ExprEvaluator — visitor that walks the AST and builds a boolean mask
# ------------------------------------------------------------------


struct ExprEvaluator(ASTNodeVisitorRaises, Movable):
    """Visitor that evaluates an expression AST into a boolean ``Series`` mask.

    After a successful call to ``visit_ast_node_raises``, the resulting
    boolean mask is in ``result``.  The ``ParsedExpr`` arena and
    ``DataFrame`` are passed as borrowed context through ``visit_ast_node_raises``
    and the ``on_*`` methods; no copies of either are made.
    """

    var result: Series

    def __init__(out self):
        self.result = Series()

    def __init__(out self, *, deinit take: Self):
        self.result = take.result^

    def on_compare(
        mut self, node: ASTNode, parsed: ParsedExpr, df: DataFrame
    ) raises:
        var lhs = parsed.node_at(node.left)
        var rhs = parsed.node_at(node.right)
        self.result = _eval_compare(node, lhs, rhs, df)

    def on_not(
        mut self, node: ASTNode, parsed: ParsedExpr, df: DataFrame
    ) raises:
        visit_ast_node_raises(self, parsed.node_at(node.left), parsed, df)
        self.result = self.result.__invert__()

    def on_and(
        mut self, node: ASTNode, parsed: ParsedExpr, df: DataFrame
    ) raises:
        visit_ast_node_raises(self, parsed.node_at(node.left), parsed, df)
        var left_result = self.result
        visit_ast_node_raises(self, parsed.node_at(node.right), parsed, df)
        self.result = left_result.__and__(self.result)

    def on_or(
        mut self, node: ASTNode, parsed: ParsedExpr, df: DataFrame
    ) raises:
        visit_ast_node_raises(self, parsed.node_at(node.left), parsed, df)
        var left_result = self.result
        visit_ast_node_raises(self, parsed.node_at(node.right), parsed, df)
        self.result = left_result.__or__(self.result)


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def eval_expr(parsed: ParsedExpr, df: DataFrame) raises -> Series:
    """Evaluate *parsed* against *df* and return a boolean Series mask.

    Supports comparison nodes (``NK_COMPARE``) and logical connectives
    (``NK_AND``, ``NK_OR``, ``NK_NOT``) with Kleene null semantics.
    Parenthetical groupings and precedence are handled by the parser.
    """
    var evaluator = ExprEvaluator()
    visit_ast_node_raises(evaluator, parsed.node_at(parsed.root), parsed, df)
    return evaluator.result
