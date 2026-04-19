# Query/Eval Minimal Grammar Specification

Status: Shipped — covers the first native query/eval milestone (#497, #498, #499). Part of parent issue #491.

This document defines the minimal grammar and semantics for the first native
`DataFrame.query()` and `DataFrame.eval()` release.

For in-scope syntax, behavior should match pandas as closely as possible.

## 1. Supported Syntax

### 1.1 Expression forms

The initial release supports the following forms:

- Column references by bare identifier name.
- Literal values: integer, float, boolean, string, null.
- Comparisons: `<`, `<=`, `>`, `>=`, `==`, `!=`.
- Logical operators: `not`, `and`, `or`.
- Parentheses for grouping.

### 1.2 Minimal grammar (EBNF)

```text
expr          := or_expr
or_expr       := and_expr ("or" and_expr)*
and_expr      := not_expr ("and" not_expr)*
not_expr      := "not" not_expr | comparison
comparison    := primary (comp_op primary)?
comp_op       := "<" | "<=" | ">" | ">=" | "==" | "!="
primary       := IDENT | literal | "(" expr ")"
literal       := INT | FLOAT | BOOL | STRING | NULL
IDENT         := /[A-Za-z_][A-Za-z0-9_]*/
BOOL          := "True" | "False"
NULL          := "None"
```

Notes:

- Comparison chaining (for example `a < b < c`) is out of scope for this
  minimal release.
- The same grammar applies to both `query` and `eval` in this phase.

## 2. Operator Precedence And Associativity

Highest to lowest precedence:

1. Parentheses: `( ... )`
2. Unary logical negation: `not`
3. Comparisons: `<`, `<=`, `>`, `>=`, `==`, `!=`
4. Logical conjunction: `and`
5. Logical disjunction: `or`

Associativity:

- `and` and `or` are left-associative.
- `not` is right-associative as a unary operator.

Examples:

- `a > 1 and b < 2 or c == 3` parses as `((a > 1 and b < 2) or c == 3)`.
- `not a > 1` parses as `not (a > 1)`.

## 3. Null Semantics

The engine uses three-valued boolean logic for expression composition where
null can appear in intermediate masks.

### 3.1 `and` truth table

| left | right | result |
|------|-------|--------|
| True | True | True |
| True | False | False |
| True | Null | Null |
| False | True | False |
| False | False | False |
| False | Null | False |
| Null | True | Null |
| Null | False | False |
| Null | Null | Null |

### 3.2 `or` truth table

| left | right | result |
|------|-------|--------|
| True | True | True |
| True | False | True |
| True | Null | True |
| False | True | True |
| False | False | False |
| False | Null | Null |
| Null | True | True |
| Null | False | Null |
| Null | Null | Null |

### 3.3 `not` truth table

| input | result |
|-------|--------|
| True | False |
| False | True |
| Null | Null |

### 3.4 Comparison with nulls

- Any comparison where either side is null evaluates to Null.
- Nulls are not equal to non-null values.
- Null compared with null also evaluates to Null in this phase.

## 4. Explicit Out-Of-Scope Syntax

The following are explicitly unsupported in the minimal release:

- Function calls: `f(a)`, `abs(a)`, `len(a)`.
- Attribute access: `a.str.len`, `obj.attr`.
- Indexing and slicing: `a[0]`, `a[1:3]`.
- Assignment expressions: `a = b`, `a := b`.
- Membership and identity operators: `in`, `not in`, `is`, `is not`.
- Arithmetic operators and expressions: `a + b`, `a * 2`, `-a`.
- Comparison chaining: `a < b < c`.
- Engine and parser keyword options in expression text.

## 5. Error Behavior

### 5.1 Unsupported syntax

- Parse-time unsupported grammar must raise `Error` with a message containing
  `unsupported syntax` and the failing token or construct.

### 5.2 Invalid expression structure

- Parse errors (for example missing parenthesis) must raise `Error` with a
  message containing `invalid expression`.

### 5.3 Unknown identifiers

- Referencing a missing column must raise `Error` with a message containing
  `unknown column` and the identifier.

### 5.4 Type errors during evaluation

- Invalid logical/comparison operand types must raise `Error` with a message
  containing `type error` and the operator.

### 5.5 Null handling failures

- Internal null-mask shape mismatches or impossible null states should raise
  `Error` with a message containing `null semantics`.

## 6. Examples

### 6.1 Valid expressions

- `a > 2`
- `(a > 2) and (b <= 10)`
- `not (flag == True)`
- `(x != None) or (y == 0)`
- `a == 1 or b == 2 and c == 3`

### 6.2 Invalid expressions

- `a + b` (arithmetic out of scope)
- `len(a) > 0` (function call out of scope)
- `a[0] == 1` (indexing out of scope)
- `a < b < c` (comparison chaining out of scope)
- `(a > 1` (invalid expression)

## 7. Follow-up Implementation Issues

This spec is the semantic baseline for parser and runtime implementation issues
in the #491 thread. Implementation issues must reference this document and
update it if behavior changes.
