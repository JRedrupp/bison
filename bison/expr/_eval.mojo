"""Expression evaluator: resolve identifiers and literals, execute comparisons.

Walks a ``ParsedExpr`` arena produced by ``parse()`` and emits a boolean
``Series`` mask.  Only ``NK_COMPARE`` nodes are evaluated natively; logical
connectives (NK_AND, NK_OR, NK_NOT) raise with a clear "unsupported expression
kind" message and will be wired in by later issues.
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


def _parse_numeric_literal(node: ASTNode) raises -> Float64:
    """Convert an NK_INT or NK_FLOAT node value to Float64."""
    return atof(node.value)


def _eval_compare(
    node: ASTNode, parsed: ParsedExpr, df: DataFrame
) raises -> Series:
    """Evaluate a single NK_COMPARE node into a boolean Series mask."""
    var op = node.value
    var lhs = parsed.node_at(node.left)
    var rhs = parsed.node_at(node.right)

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
            return Series(left_col._col._cmp_lt(right_col._col))
        elif op == "<=":
            return Series(left_col._col._cmp_le(right_col._col))
        elif op == ">":
            return Series(left_col._col._cmp_gt(right_col._col))
        elif op == ">=":
            return Series(left_col._col._cmp_ge(right_col._col))
        elif op == "==":
            return Series(left_col._col._cmp_eq(right_col._col))
        elif op == "!=":
            return Series(left_col._col._cmp_ne(right_col._col))
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


def _eval_node(idx: Int, parsed: ParsedExpr, df: DataFrame) raises -> Series:
    """Recursively evaluate the node at arena index *idx*."""
    var node = parsed.node_at(idx)
    if node.kind == NK_COMPARE:
        return _eval_compare(node, parsed, df)
    elif node.kind == NK_AND:
        raise Error(
            "evaluator: unsupported expression kind "
            + String(node.kind)
            + " (NK_AND — not yet implemented)"
        )
    elif node.kind == NK_OR:
        raise Error(
            "evaluator: unsupported expression kind "
            + String(node.kind)
            + " (NK_OR — not yet implemented)"
        )
    elif node.kind == NK_NOT:
        raise Error(
            "evaluator: unsupported expression kind "
            + String(node.kind)
            + " (NK_NOT — not yet implemented)"
        )
    else:
        raise Error(
            "evaluator: unsupported expression kind " + String(node.kind)
        )


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def eval_expr(parsed: ParsedExpr, df: DataFrame) raises -> Series:
    """Evaluate *parsed* against *df* and return a boolean Series mask.

    Only single ``NK_COMPARE`` nodes are supported at this stage.  Logical
    connectives (and / or / not) will be added in a later release and raise
    with a clear "unsupported expression kind" error until then.
    """
    return _eval_node(parsed.root, parsed, df)
