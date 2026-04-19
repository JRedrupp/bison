"""Expression evaluator: resolve identifiers and literals, execute comparisons.

Walks a ``ParsedExpr`` arena produced by ``parse()`` and emits a boolean
``Series`` mask.  Supports comparison predicates (``NK_COMPARE``) and logical
connectives (``NK_AND``, ``NK_OR``, ``NK_NOT``) with Kleene three-valued null
semantics.  Parenthetical grouping and precedence are handled by the parser.

**Compiler bug #642 workaround:** The original implementation used a generic
``visit_ast_node_raises[V: ASTNodeVisitorRaises]`` visitor.  That generic
dispatch combined with AnyArray typed downcasts (``as_int64()`` etc.) on the
same call graph triggered a Mojo comptime monomorphisation deadlock.  The
visitor pattern has been replaced with a plain non-generic recursive function
``_eval_node`` that uses integer if/elif comparisons on node kinds.  No
traits, no template instantiation, no generic dispatch.
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

    Routes through the ``Int64`` overloads so that integer columns are compared
    in exact integer arithmetic rather than being widened to ``Float64`` first.
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

    ``==`` maps to ``isna()`` and ``!=`` maps to ``notna()``.
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

    Ensures the column identifier is always on the left; flips *op* if the
    identifier was originally on the right.
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
    """Evaluate a single NK_COMPARE node into a boolean Series mask."""
    var op = node.value

    # Column vs column — both sides are identifiers.
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
# Non-generic recursive evaluator
#
# Replaces the original visit_ast_node_raises[V: ASTNodeVisitorRaises]
# generic visitor that triggered Mojo compiler bug #642 when combined
# with AnyArray typed downcasts on the same call graph.  This plain
# recursive function uses only integer if/elif on NK_* constants.
# ------------------------------------------------------------------


def _eval_node(
    node_idx: Int, parsed: ParsedExpr, df: DataFrame
) raises -> Series:
    """Recursively evaluate the node at *node_idx* and return a boolean Series.
    """
    var node = parsed.node_at(node_idx)
    if node.kind == NK_COMPARE:
        var lhs = parsed.node_at(node.left)
        var rhs = parsed.node_at(node.right)
        return _eval_compare(node, lhs, rhs, df)
    elif node.kind == NK_NOT:
        return _eval_node(node.left, parsed, df).__invert__()
    elif node.kind == NK_AND:
        var left_result = _eval_node(node.left, parsed, df)
        var right_result = _eval_node(node.right, parsed, df)
        return left_result.__and__(right_result)
    elif node.kind == NK_OR:
        var left_result = _eval_node(node.left, parsed, df)
        var right_result = _eval_node(node.right, parsed, df)
        return left_result.__or__(right_result)
    else:
        raise Error(
            "evaluator: unsupported expression kind " + String(node.kind)
        )


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def eval_expr(parsed: ParsedExpr, df: DataFrame) raises -> Series:
    """Evaluate *parsed* against *df* and return a boolean Series mask.

    Supports comparison nodes (``NK_COMPARE``) and logical connectives
    (``NK_AND``, ``NK_OR``, ``NK_NOT``).  Parenthetical groupings and
    precedence are handled by the parser.
    """
    return _eval_node(parsed.root, parsed, df)
