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


def _resolve_ident(name: String, df: DataFrame) raises -> Series:
    """Return the column *name* from *df*, or raise a clear evaluator error."""
    try:
        return df[name]
    except:
        raise Error("evaluator: unknown identifier '" + name + "'")


def _flip_op(op: String) raises -> String:
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


def _apply_numeric_op(col: Series, op: String, val: Float64) raises -> Series:
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


def _apply_int_op(col: Series, op: String, val: Int64) raises -> Series:
    """Apply a comparison operator to *col* against an integer scalar *val*.

    Routes through the ``Int64`` overloads of the Series comparison operators
    so that integer columns are compared in exact integer arithmetic rather
    than being widened to ``Float64`` first.  This avoids precision loss for
    values outside the ``Float64`` mantissa range (|v| > 2**53).
    """
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


def _apply_string_op(col: Series, op: String, val: String) raises -> Series:
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


def _apply_null_op(col: Series, op: String) raises -> Series:
    """Apply a null-check operator to *col*.

    ``==`` maps to ``isna()`` (True where the value is null) and ``!=``
    maps to ``notna()`` (True where the value is not null).  All other
    operators raise, because ordering comparisons against ``None`` are
    undefined.
    """
    if op == "==":
        return col.isna()
    elif op == "!=":
        return col.notna()
    else:
        raise Error(
            "evaluator: null (None) comparisons only support == and !=; got '"
            + op
            + "'"
        )


def _parse_bool_literal(node: ASTNode) raises -> Float64:
    """Convert an NK_BOOL node to 1.0 (True) or 0.0 (False)."""
    if node.value == "True":
        return Float64(1.0)
    elif node.value == "False":
        return Float64(0.0)
    else:
        raise Error(
            "evaluator: unexpected boolean literal value '" + node.value + "'"
        )


def _parse_float_literal(node: ASTNode) raises -> Float64:
    """Convert an NK_FLOAT node value to Float64."""
    return atof(node.value)


def _parse_int_literal(node: ASTNode) raises -> Int64:
    """Convert an NK_INT node value to Int64 without Float64 widening."""
    return Int64(Int(node.value))


def _normalize_compare_operands(
    lhs: ASTNode, op: String, rhs: ASTNode
) raises -> Tuple[ASTNode, String, ASTNode]:
    """Normalize a comparison to (col_node, op, rhs_node) form.

    If the identifier (column) is already on the left, the triple is returned
    unchanged.  If the right operand is the identifier, the operands are
    swapped and *op* is flipped via ``_flip_op`` so the column is always on
    the left side.  Raises if neither operand is an identifier.

    The returned *rhs_node* is whatever non-identifier node was paired with the
    column; callers are responsible for validating its kind.
    """
    if lhs.kind == NK_IDENT:
        return (lhs, op, rhs)
    if rhs.kind == NK_IDENT:
        return (rhs, _flip_op(op), lhs)
    raise Error(
        "evaluator: comparison must involve at least one column identifier"
    )


def _eval_compare(
    node: ASTNode, lhs: ASTNode, rhs: ASTNode, df: DataFrame
) raises -> Series:
    """Evaluate a single NK_COMPARE node into a boolean Series mask.

    *lhs* and *rhs* are the already-resolved child nodes (left and right
    operands of the comparison).

    The function first handles the column-vs-column case directly, then
    delegates to ``_normalize_compare_operands`` to ensure the identifier is
    always on the left before dispatching on the literal kind.  This removes
    the duplicated ``_resolve_ident`` calls that were previously needed for
    each literal-left branch.
    """
    # For NK_COMPARE nodes, node.value holds the operator string
    # ("<", "<=", ">", ">=", "==", "!=") as set by the parser.
    var op = node.value

    # Column vs column — both sides are identifiers; handled before normalization.
    if lhs.kind == NK_IDENT and rhs.kind == NK_IDENT:
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

    # Normalize: ensure the column identifier is on the left, flipping op if
    # the identifier was on the right.  Raises if neither side is an identifier.
    var normalized = _normalize_compare_operands(lhs, op, rhs)
    var col_node = normalized[0]
    var norm_op = normalized[1]
    var rhs_node = normalized[2]

    var col = _resolve_ident(col_node.value, df)

    if rhs_node.kind == NK_INT:
        return _apply_int_op(col, norm_op, _parse_int_literal(rhs_node))
    elif rhs_node.kind == NK_FLOAT:
        return _apply_numeric_op(col, norm_op, _parse_float_literal(rhs_node))
    elif rhs_node.kind == NK_STRING:
        return _apply_string_op(col, norm_op, rhs_node.value)
    elif rhs_node.kind == NK_NULL:
        return _apply_null_op(col, norm_op)
    elif rhs_node.kind == NK_BOOL:
        return _apply_numeric_op(col, norm_op, _parse_bool_literal(rhs_node))
    else:
        raise Error(
            "evaluator: unsupported right-hand operand kind "
            + String(rhs_node.kind)
            + " (expected INT, FLOAT, STRING, NULL, or BOOL)"
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
