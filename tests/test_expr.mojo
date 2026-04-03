"""Tests for bison.expr: tokenizer, AST, parser, and evaluator."""
from std.python import Python
from std.testing import assert_true, assert_equal, TestSuite
from bison import DataFrame, Series
from bison.expr import (
    parse,
    eval_expr,
    ParsedExpr,
    ASTNode,
    Tokenizer,
    Token,
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


# ------------------------------------------------------------------
# Tokenizer tests
# ------------------------------------------------------------------


def test_tokenizer_basic() raises:
    """Each basic token kind tokenizes correctly and EOF terminates."""
    var tz = Tokenizer("a 123 3.14 < <= > >= == !=")
    var t = tz.next_token()
    assert_equal(t.kind, TK_IDENT)
    assert_equal(t.value, String("a"))

    t = tz.next_token()
    assert_equal(t.kind, TK_INT)
    assert_equal(t.value, String("123"))

    t = tz.next_token()
    assert_equal(t.kind, TK_FLOAT)
    assert_equal(t.value, String("3.14"))

    t = tz.next_token()
    assert_equal(t.kind, TK_LT)
    t = tz.next_token()
    assert_equal(t.kind, TK_LE)
    t = tz.next_token()
    assert_equal(t.kind, TK_GT)
    t = tz.next_token()
    assert_equal(t.kind, TK_GE)
    t = tz.next_token()
    assert_equal(t.kind, TK_EQ)
    t = tz.next_token()
    assert_equal(t.kind, TK_NE)

    t = tz.next_token()
    assert_equal(t.kind, TK_EOF)


def test_tokenizer_parens_and_strings() raises:
    """Parentheses and quoted strings tokenize correctly."""
    var tz = Tokenizer('( "hello" \'world\' )')
    var t = tz.next_token()
    assert_equal(t.kind, TK_LPAREN)

    t = tz.next_token()
    assert_equal(t.kind, TK_STRING)
    assert_equal(t.value, String("hello"))

    t = tz.next_token()
    assert_equal(t.kind, TK_STRING)
    assert_equal(t.value, String("world"))

    t = tz.next_token()
    assert_equal(t.kind, TK_RPAREN)

    t = tz.next_token()
    assert_equal(t.kind, TK_EOF)


def test_tokenizer_keywords() raises:
    """Keyword tokens — and/or/not/True/False/None — get the right kind."""
    var tz = Tokenizer("and or not True False None")

    var t = tz.next_token()
    assert_equal(t.kind, TK_AND)
    assert_equal(t.value, String("and"))

    t = tz.next_token()
    assert_equal(t.kind, TK_OR)
    assert_equal(t.value, String("or"))

    t = tz.next_token()
    assert_equal(t.kind, TK_NOT)
    assert_equal(t.value, String("not"))

    t = tz.next_token()
    assert_equal(t.kind, TK_TRUE)
    assert_equal(t.value, String("True"))

    t = tz.next_token()
    assert_equal(t.kind, TK_FALSE)
    assert_equal(t.value, String("False"))

    t = tz.next_token()
    assert_equal(t.kind, TK_NULL)
    assert_equal(t.value, String("None"))

    t = tz.next_token()
    assert_equal(t.kind, TK_EOF)


def test_tokenizer_ident_prefix_of_keyword() raises:
    """Identifiers that start with a keyword prefix are classified as TK_IDENT."""
    var tz = Tokenizer("and_ orval notflag Trueish")

    var t = tz.next_token()
    assert_equal(t.kind, TK_IDENT)
    assert_equal(t.value, String("and_"))

    t = tz.next_token()
    assert_equal(t.kind, TK_IDENT)
    assert_equal(t.value, String("orval"))

    t = tz.next_token()
    assert_equal(t.kind, TK_IDENT)
    assert_equal(t.value, String("notflag"))

    t = tz.next_token()
    assert_equal(t.kind, TK_IDENT)
    assert_equal(t.value, String("Trueish"))


def test_tokenizer_unsupported_char() raises:
    """Unsupported characters raise with 'unsupported syntax' in the message."""
    var raised = False
    try:
        var tz = Tokenizer("a + b")
        _ = tz.next_token()  # a
        _ = tz.next_token()  # + — should raise
    except e:
        raised = "unsupported syntax" in String(e)
    assert_true(raised)


def test_tokenizer_unterminated_string() raises:
    """An unterminated string raises with 'invalid expression' in the message."""
    var raised = False
    try:
        var tz = Tokenizer('"hello')
        _ = tz.next_token()
    except e:
        raised = "invalid expression" in String(e)
    assert_true(raised)


# ------------------------------------------------------------------
# Parser tests
# ------------------------------------------------------------------


def test_parser_simple_comparison() raises:
    """Simple comparisons parse to a single NK_COMPARE root node."""
    var expr = parse("a > 2")
    var root = expr.node_at(expr.root)
    assert_equal(root.kind, NK_COMPARE)
    assert_equal(root.value, String(">"))

    var lhs = expr.node_at(root.left)
    assert_equal(lhs.kind, NK_IDENT)
    assert_equal(lhs.value, String("a"))

    var rhs = expr.node_at(root.right)
    assert_equal(rhs.kind, NK_INT)
    assert_equal(rhs.value, String("2"))


def test_parser_comparison_operators() raises:
    """All six comparison operators produce correct NK_COMPARE nodes."""
    var ops = List[String]()
    ops.append(String("<"))
    ops.append(String("<="))
    ops.append(String(">"))
    ops.append(String(">="))
    ops.append(String("=="))
    ops.append(String("!="))

    var exprs = List[String]()
    exprs.append(String("a < 1"))
    exprs.append(String("a <= 1"))
    exprs.append(String("a > 1"))
    exprs.append(String("a >= 1"))
    exprs.append(String("a == 1"))
    exprs.append(String("a != 1"))

    for i in range(len(ops)):
        var result = parse(exprs[i])
        var root = result.node_at(result.root)
        assert_equal(root.kind, NK_COMPARE)
        assert_equal(root.value, ops[i])


def test_parser_logical_precedence() raises:
    """'and' binds tighter than 'or': a > 1 and b < 2 or c == 3 => NK_OR root."""
    var expr = parse("a > 1 and b < 2 or c == 3")
    var root_node = expr.node_at(expr.root)
    assert_equal(root_node.kind, NK_OR)

    # Left child of OR must be AND
    var and_node = expr.node_at(root_node.left)
    assert_equal(and_node.kind, NK_AND)

    # Left of AND: a > 1
    var cmp_a = expr.node_at(and_node.left)
    assert_equal(cmp_a.kind, NK_COMPARE)
    assert_equal(cmp_a.value, String(">"))

    # Right of AND: b < 2
    var cmp_b = expr.node_at(and_node.right)
    assert_equal(cmp_b.kind, NK_COMPARE)
    assert_equal(cmp_b.value, String("<"))

    # Right child of OR: c == 3
    var cmp_c = expr.node_at(root_node.right)
    assert_equal(cmp_c.kind, NK_COMPARE)
    assert_equal(cmp_c.value, String("=="))


def test_parser_not_precedence() raises:
    """'not a > 1' parses as not (a > 1) — NK_NOT root, NK_COMPARE child."""
    var expr = parse("not a > 1")
    var root_node = expr.node_at(expr.root)
    assert_equal(root_node.kind, NK_NOT)

    var child = expr.node_at(root_node.left)
    assert_equal(child.kind, NK_COMPARE)
    assert_equal(child.value, String(">"))


def test_parser_parentheses() raises:
    """Parentheses override default precedence."""
    # Without parens: a > 1 and b < 2 (a > 1) and (b < 2) is already the default
    # With parens forcing OR before AND isn't possible in this grammar,
    # but we can test that parens group correctly for 'not'
    var expr = parse("not (a == 1 or b == 2)")
    var root_node = expr.node_at(expr.root)
    assert_equal(root_node.kind, NK_NOT)

    var inner = expr.node_at(root_node.left)
    assert_equal(inner.kind, NK_OR)


def test_parser_nested_parens() raises:
    """Deeply nested parentheses resolve to the inner expression."""
    var expr = parse("((a > 0))")
    var root_node = expr.node_at(expr.root)
    assert_equal(root_node.kind, NK_COMPARE)
    assert_equal(root_node.value, String(">"))


def test_parser_null_literal() raises:
    """None literals parse as NK_NULL nodes."""
    var expr = parse("x != None")
    var root_node = expr.node_at(expr.root)
    assert_equal(root_node.kind, NK_COMPARE)
    assert_equal(root_node.value, String("!="))

    var rhs = expr.node_at(root_node.right)
    assert_equal(rhs.kind, NK_NULL)
    assert_equal(rhs.value, String("None"))


def test_parser_float_literal() raises:
    """Float literals parse as NK_FLOAT nodes."""
    var expr = parse("y > 3.14")
    var root_node = expr.node_at(expr.root)
    assert_equal(root_node.kind, NK_COMPARE)

    var rhs = expr.node_at(root_node.right)
    assert_equal(rhs.kind, NK_FLOAT)
    assert_equal(rhs.value, String("3.14"))


def test_parser_bool_literals() raises:
    """True and False parse as NK_BOOL nodes."""
    var expr_t = parse("flag == True")
    var rhs_t = expr_t.node_at(expr_t.node_at(expr_t.root).right)
    assert_equal(rhs_t.kind, NK_BOOL)
    assert_equal(rhs_t.value, String("True"))

    var expr_f = parse("flag == False")
    var rhs_f = expr_f.node_at(expr_f.node_at(expr_f.root).right)
    assert_equal(rhs_f.kind, NK_BOOL)
    assert_equal(rhs_f.value, String("False"))


def test_parser_string_literal() raises:
    """Quoted strings parse as NK_STRING nodes."""
    var expr = parse('name == "alice"')
    var rhs = expr.node_at(expr.node_at(expr.root).right)
    assert_equal(rhs.kind, NK_STRING)
    assert_equal(rhs.value, String("alice"))


def test_parser_invalid_missing_paren() raises:
    """A missing closing parenthesis raises with 'invalid expression' in the message."""
    var raised = False
    try:
        _ = parse("(a > 1")
    except e:
        raised = "invalid expression" in String(e)
    assert_true(raised)


def test_parser_unsupported_arithmetic() raises:
    """Arithmetic operators raise with 'unsupported syntax' in the message."""
    var raised = False
    try:
        _ = parse("a + b")
    except e:
        raised = "unsupported syntax" in String(e)
    assert_true(raised)


def test_parser_unsupported_assignment() raises:
    """The assignment operator '=' raises with 'unsupported syntax'."""
    var raised = False
    try:
        _ = parse("a = 1")
    except e:
        raised = "unsupported syntax" in String(e)
    assert_true(raised)


def test_parser_trailing_token() raises:
    """Extra tokens after a complete expression raise with 'invalid expression'."""
    var raised = False
    try:
        _ = parse("a > 1 b")
    except e:
        raised = "invalid expression" in String(e)
    assert_true(raised)


def test_parser_empty_expr() raises:
    """An empty expression raises with 'invalid expression'."""
    var raised = False
    try:
        _ = parse("  ")
    except e:
        raised = "invalid expression" in String(e)
    assert_true(raised)


# ------------------------------------------------------------------
# Evaluator tests
# ------------------------------------------------------------------


def test_eval_int_gt() raises:
    """Evaluator with 'a > 2' against an int column produces the correct mask."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4]}")))
    var mask = eval_expr(parse("a > 2"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(not d[0])
    assert_true(not d[1])
    assert_true(d[2])
    assert_true(d[3])


def test_eval_float_ge() raises:
    """Evaluator with 'y >= 3.5' against a float column produces the correct mask."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'y': [1.0, 2.5, 3.5, 4.0]}"))
    )
    var mask = eval_expr(parse("y >= 3.5"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(not d[0])
    assert_true(not d[1])
    assert_true(d[2])
    assert_true(d[3])


def test_eval_string_eq() raises:
    """Evaluator with 'name == \"alice\"' against a string column gives the right mask."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(
            Python.evaluate("{'name': ['alice', 'bob', 'alice', 'carol']}")
        )
    )
    var mask = eval_expr(parse('name == "alice"'), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(d[0])
    assert_true(not d[1])
    assert_true(d[2])
    assert_true(not d[3])


def test_eval_string_ne() raises:
    """Evaluator with 'name != \"bob\"' gives the inverted-bob mask."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(
            Python.evaluate("{'name': ['alice', 'bob', 'alice', 'carol']}")
        )
    )
    var mask = eval_expr(parse('name != "bob"'), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(d[0])
    assert_true(not d[1])
    assert_true(d[2])
    assert_true(d[3])


def test_eval_unknown_ident() raises:
    """Evaluator raises with 'unknown identifier' for a column not in the DataFrame."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var raised = False
    try:
        _ = eval_expr(parse("unk > 1"), df)
    except e:
        raised = "unknown identifier" in String(e)
    assert_true(raised)


def test_eval_literal_left() raises:
    """A literal on the left ('5 < a') gives the same mask as 'a > 5'."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 5, 10]}")))
    var mask_normal = eval_expr(parse("a > 5"), df)
    var mask_flipped = eval_expr(parse("5 < a"), df)
    ref dn = mask_normal._col._data[List[Bool]]
    ref df2 = mask_flipped._col._data[List[Bool]]
    for i in range(3):
        assert_equal(dn[i], df2[i])


def test_eval_all_numeric_ops() raises:
    """All six comparison operators produce correct boolean masks."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'x': [1, 2, 3]}")))
    # x < 2  → [T, F, F]
    var lt = eval_expr(parse("x < 2"), df)
    assert_true(lt._col._data[List[Bool]][0])
    assert_true(not lt._col._data[List[Bool]][1])
    assert_true(not lt._col._data[List[Bool]][2])
    # x <= 2 → [T, T, F]
    var le = eval_expr(parse("x <= 2"), df)
    assert_true(le._col._data[List[Bool]][0])
    assert_true(le._col._data[List[Bool]][1])
    assert_true(not le._col._data[List[Bool]][2])
    # x > 2  → [F, F, T]
    var gt = eval_expr(parse("x > 2"), df)
    assert_true(not gt._col._data[List[Bool]][0])
    assert_true(not gt._col._data[List[Bool]][1])
    assert_true(gt._col._data[List[Bool]][2])
    # x >= 2 → [F, T, T]
    var ge = eval_expr(parse("x >= 2"), df)
    assert_true(not ge._col._data[List[Bool]][0])
    assert_true(ge._col._data[List[Bool]][1])
    assert_true(ge._col._data[List[Bool]][2])
    # x == 2 → [F, T, F]
    var eq = eval_expr(parse("x == 2"), df)
    assert_true(not eq._col._data[List[Bool]][0])
    assert_true(eq._col._data[List[Bool]][1])
    assert_true(not eq._col._data[List[Bool]][2])
    # x != 2 → [T, F, T]
    var ne = eval_expr(parse("x != 2"), df)
    assert_true(ne._col._data[List[Bool]][0])
    assert_true(not ne._col._data[List[Bool]][1])
    assert_true(ne._col._data[List[Bool]][2])


def test_eval_column_vs_column() raises:
    """Column-vs-column comparisons use Series overloads without touching _col."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 5, 3], 'b': [2, 4, 3]}"))
    )
    # a < b  → [T, F, F]
    var lt = eval_expr(parse("a < b"), df)
    ref lt_d = lt._col._data[List[Bool]]
    assert_true(lt_d[0])
    assert_true(not lt_d[1])
    assert_true(not lt_d[2])
    # a > b  → [F, T, F]
    var gt = eval_expr(parse("a > b"), df)
    ref gt_d = gt._col._data[List[Bool]]
    assert_true(not gt_d[0])
    assert_true(gt_d[1])
    assert_true(not gt_d[2])
    # a == b → [F, F, T]
    var eq = eval_expr(parse("a == b"), df)
    ref eq_d = eq._col._data[List[Bool]]
    assert_true(not eq_d[0])
    assert_true(not eq_d[1])
    assert_true(eq_d[2])
    # a != b → [T, T, F]
    var ne = eval_expr(parse("a != b"), df)
    ref ne_d = ne._col._data[List[Bool]]
    assert_true(ne_d[0])
    assert_true(ne_d[1])
    assert_true(not ne_d[2])
    # a <= b → [T, F, T]
    var le = eval_expr(parse("a <= b"), df)
    ref le_d = le._col._data[List[Bool]]
    assert_true(le_d[0])
    assert_true(not le_d[1])
    assert_true(le_d[2])
    # a >= b → [F, T, T]
    var ge = eval_expr(parse("a >= b"), df)
    ref ge_d = ge._col._data[List[Bool]]
    assert_true(not ge_d[0])
    assert_true(ge_d[1])
    assert_true(ge_d[2])
def test_eval_and() raises:
    # a > 1 and a < 4 → [F, T, T, F]
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4]}")))
    var mask = eval_expr(parse("a > 1 and a < 4"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(not d[0])
    assert_true(d[1])
    assert_true(d[2])
    assert_true(not d[3])


def test_eval_or() raises:
    # a < 2 or a > 3 → [T, F, F, T]
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4]}")))
    var mask = eval_expr(parse("a < 2 or a > 3"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(d[0])
    assert_true(not d[1])
    assert_true(not d[2])
    assert_true(d[3])


def test_eval_not() raises:
    # not a > 2 → [T, T, F]
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}")))
    var mask = eval_expr(parse("not a > 2"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(d[0])
    assert_true(d[1])
    assert_true(not d[2])


def test_eval_chained_and() raises:
    # a > 0 and b > 0 with b = [4, 0, 5] → [T, F, T]
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [4, 0, 5]}")))
    var mask = eval_expr(parse("a > 0 and b > 0"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(d[0])
    assert_true(not d[1])
    assert_true(d[2])


def test_eval_mixed_parens() raises:
    # a > 1 and (b > 5 or a > 2) with a=[1,2,3], b=[10,1,10]
    # row 0: F and (T or F) = F and T = F
    # row 1: T and (F or F) = T and F = F
    # row 2: T and (T or T) = T and T = T
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1, 2, 3], 'b': [10, 1, 10]}")))
    var mask = eval_expr(parse("a > 1 and (b > 5 or a > 2)"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(not d[0])
    assert_true(not d[1])
    assert_true(d[2])


def test_eval_null_and() raises:
    # a = [1.0, None, 3.0]; expr "a > 1 and a < 5"
    # row 0: F and T = F (non-null)
    # row 1: null and null = null
    # row 2: T and T = T (non-null)
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}")))
    var mask = eval_expr(parse("a > 1 and a < 5"), df)
    assert_true(len(mask._col._null_mask) > 0)
    assert_true(not mask._col._null_mask[0])
    assert_true(not mask._col._data[List[Bool]][0])
    assert_true(mask._col._null_mask[1])
    assert_true(not mask._col._null_mask[2])
    assert_true(mask._col._data[List[Bool]][2])


def test_eval_null_or() raises:
    # a = [1.0, None, 6.0]; expr "a < 2 or a > 5"
    # row 0: T or F = T (non-null)
    # row 1: null or null = null
    # row 2: F or T = T (non-null)
    var pd = Python.import_module("pandas")
    var df = DataFrame(pd.DataFrame(Python.evaluate("{'a': [1.0, None, 6.0]}")))
    var mask = eval_expr(parse("a < 2 or a > 5"), df)
    assert_true(len(mask._col._null_mask) > 0)
    assert_true(not mask._col._null_mask[0])
    assert_true(mask._col._data[List[Bool]][0])
    assert_true(mask._col._null_mask[1])
    assert_true(not mask._col._null_mask[2])
    assert_true(mask._col._data[List[Bool]][2])


# ------------------------------------------------------------------
# DataFrame.query integration tests
# ------------------------------------------------------------------


def test_query_simple_numeric() raises:
    """df.query("a > 0.5") filters rows natively; result matches direct bool-mask filter."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [0.1, 0.6, 0.9, 0.3], 'b': [1, 2, 3, 4]}"))
    )
    var result = df.query("a > 0.5")
    # Rows at index 1 (0.6) and 2 (0.9) pass the filter.
    assert_equal(result.shape()[0], 2)
    assert_equal(result.shape()[1], 2)
    var col_a = result["a"]
    assert_true(col_a._col._data[List[Float64]][0] > 0.5)
    assert_true(col_a._col._data[List[Float64]][1] > 0.5)


def test_query_logical_and() raises:
    """df.query("a > 1 and b < 4") applies compound logical filter natively."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4], 'b': [10, 3, 5, 2]}"))
    )
    var result = df.query("a > 1 and b < 4")
    # Row 1: a=2 > 1 and b=3 < 4 → pass
    # Row 3: a=4 > 1 and b=2 < 4 → pass
    assert_equal(result.shape()[0], 2)


def test_query_string_eq() raises:
    """df.query("cat == 'foo'") filters on a string column natively."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(
            Python.evaluate("{'cat': ['foo', 'bar', 'foo'], 'val': [1, 2, 3]}")
        )
    )
    var result = df.query("cat == 'foo'")
    assert_equal(result.shape()[0], 2)


def test_query_unknown_column_raises() raises:
    """df.query referencing a missing column raises with 'unknown identifier'."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}"))
    )
    var raised = False
    try:
        _ = df.query("z > 1")
    except e:
        raised = "unknown identifier" in String(e)
    assert_true(raised)


def test_query_unsupported_syntax_raises() raises:
    """df.query with unsupported syntax (e.g. '+') raises with 'unsupported syntax'."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}"))
    )
    var raised = False
    try:
        _ = df.query("a + 1 > 2")
    except e:
        raised = "unsupported syntax" in String(e)
    assert_true(raised)


# ------------------------------------------------------------------
# DataFrame.eval integration tests
# ------------------------------------------------------------------


def test_df_eval_simple_numeric() raises:
    """df.eval("a > 5") returns a boolean Series with the correct mask."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 6, 3, 8], 'b': [10, 20, 30, 40]}"))
    )
    var mask = df.eval("a > 5")
    ref d = mask._col._data[List[Bool]]
    assert_true(not d[0])
    assert_true(d[1])
    assert_true(not d[2])
    assert_true(d[3])


def test_df_eval_logical_and() raises:
    """df.eval("a > 1 and b < 4") returns the correct compound boolean mask."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 2, 3, 4], 'b': [10, 3, 5, 2]}"))
    )
    var mask = df.eval("a > 1 and b < 4")
    ref d = mask._col._data[List[Bool]]
    # Row 0: a=1 not > 1 → False
    assert_true(not d[0])
    # Row 1: a=2 > 1 and b=3 < 4 → True
    assert_true(d[1])
    # Row 2: a=3 > 1 but b=5 not < 4 → False
    assert_true(not d[2])
    # Row 3: a=4 > 1 and b=2 < 4 → True
    assert_true(d[3])


def test_df_eval_string_eq() raises:
    """df.eval('cat == "foo"') returns the correct boolean mask for a string column."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(
            Python.evaluate("{'cat': ['foo', 'bar', 'foo'], 'val': [1, 2, 3]}")
        )
    )
    var mask = df.eval('cat == "foo"')
    ref d = mask._col._data[List[Bool]]
    assert_true(d[0])
    assert_true(not d[1])
    assert_true(d[2])


def test_df_eval_unknown_column_raises() raises:
    """df.eval referencing a missing column raises with 'unknown identifier'."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}"))
    )
    var raised = False
    try:
        _ = df.eval("z > 1")
    except e:
        raised = "unknown identifier" in String(e)
    assert_true(raised)


def test_df_eval_unsupported_syntax_raises() raises:
    """df.eval with unsupported syntax ('+') raises with 'unsupported syntax'."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}"))
    )
    var raised = False
    try:
        _ = df.eval("a + 1 > 2")
    except e:
        raised = "unsupported syntax" in String(e)
    assert_true(raised)


def test_df_eval_not() raises:
    """df.eval("not a > 2") returns the inverted mask."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1, 2, 3]}"))
    )
    var mask = df.eval("not a > 2")
    ref d = mask._col._data[List[Bool]]
    assert_true(d[0])
    assert_true(d[1])
    assert_true(not d[2])


# ------------------------------------------------------------------
# NK_NULL evaluation tests
# ------------------------------------------------------------------


def test_eval_null_eq() raises:
    """col == None returns isna() mask: True where the value is None."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}"))
    )
    var mask = eval_expr(parse("a == None"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(not d[0])
    assert_true(d[1])
    assert_true(not d[2])


def test_eval_null_ne() raises:
    """col != None returns notna() mask: True where the value is not None."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}"))
    )
    var mask = eval_expr(parse("a != None"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(d[0])
    assert_true(not d[1])
    assert_true(d[2])


def test_eval_null_flipped_eq() raises:
    """None == col (flipped) behaves identically to col == None."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, None, 3.0]}"))
    )
    var mask = eval_expr(parse("None == a"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(not d[0])
    assert_true(d[1])
    assert_true(not d[2])


def test_eval_null_invalid_op_raises() raises:
    """col < None raises with a clear message (ordering against None is undefined)."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'a': [1.0, 2.0]}"))
    )
    var raised = False
    try:
        _ = eval_expr(parse("a < None"), df)
    except e:
        raised = "null (None) comparisons only support == and !=" in String(e)
    assert_true(raised)


# ------------------------------------------------------------------
# NK_BOOL evaluation tests
# ------------------------------------------------------------------


def test_eval_bool_eq_true() raises:
    """flag == True returns True for rows where the bool column is True."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'flag': [True, False, True]}"))
    )
    var mask = eval_expr(parse("flag == True"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(d[0])
    assert_true(not d[1])
    assert_true(d[2])


def test_eval_bool_eq_false() raises:
    """flag == False returns True for rows where the bool column is False."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'flag': [True, False, True]}"))
    )
    var mask = eval_expr(parse("flag == False"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(not d[0])
    assert_true(d[1])
    assert_true(not d[2])


def test_eval_bool_ne_true() raises:
    """flag != True returns True for rows where the bool column is False."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'flag': [True, False, True]}"))
    )
    var mask = eval_expr(parse("flag != True"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(not d[0])
    assert_true(d[1])
    assert_true(not d[2])


def test_eval_bool_flipped_eq() raises:
    """True == flag (flipped) behaves identically to flag == True."""
    var pd = Python.import_module("pandas")
    var df = DataFrame(
        pd.DataFrame(Python.evaluate("{'flag': [True, False, True]}"))
    )
    var mask = eval_expr(parse("True == flag"), df)
    ref d = mask._col._data[List[Bool]]
    assert_true(d[0])
    assert_true(not d[1])
    assert_true(d[2])


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
