---
name: session-to-issues
description: 'Triage SESSION.md tech-debt and bug notes against open GitHub issues and create new issues for anything not already tracked. Use when: syncing session notes to GitHub, filing tech debt, converting SESSION.md entries to issues, cross-referencing session findings with issue tracker.'
argument-hint: 'Optional: path to SESSION.md if not at repo root'
---

# Session Notes → GitHub Issues

Reads `SESSION.md`, fetches all open GitHub issues, and creates new issues for
any entry that has no match. Updates `SESSION.md` with the assigned issue number
so the entry is not filed twice.

## When to Use

- After a coding session to promote findings to the issue tracker
- When SESSION.md grows new `###` entries that haven't been filed yet
- When asked to "sync session notes", "file tech debt", or "create issues from session"

## Entry Format

SESSION.md entries follow this schema:

```markdown
### <Short title>

- **File**: `path/to/file.mojo` (line N if relevant)
- **Type**: Tech Debt | Bug | Refactoring | Design Pattern
- **Classification**: <refactoring.guru name>
- **Details**: What the problem is and what the fix should be.
```

A filed entry is annotated with its issue number:

```markdown
### <Short title> <!-- #42 -->
```

Entries already carrying an issue number are **skipped**.

## Procedure

### 1. Read SESSION.md

Read the file at the repo root (or the path provided as the argument).
Collect every `###` heading that does **not** already contain `<!-- #N -->`.

### 2. Fetch open issues

```bash
gh issue list --limit 200 --state open --json number,title,body
```

Build an in-memory list of `(number, title, body)` triples.

### 3. Match each entry

For each untracked entry:

1. Construct a **candidate title** using the prefix table below.
2. Check whether any open issue title contains the SESSION.md heading text
   (case-insensitive substring match is sufficient).
3. If a match is found, annotate the entry with that issue number and skip
   creation.
4. If no match is found, proceed to Step 4.

**Title prefix table** (derive from the `Type` field):

| Type         | Prefix      |
|--------------|-------------|
| Bug          | `bug:`      |
| Tech Debt    | `tech debt:` |
| Refactoring  | `refactor:` |
| Design Pattern | `chore:`  |
| (other)      | `chore:`    |

Full title format: `<prefix> <SESSION.md heading text>`

Example: `tech debt: \`copy()\` dispatch is duplicated in Column`

### 4. Create the GitHub issue

```bash
gh issue create \
  --title "<title>" \
  --body "<body>" \
  --label "<label>"
```

**Body template:**

```
**File**: `<File field>`
**Classification**: <Classification field>

<Details field>
```

**Label mapping** (only apply if the label already exists in the repo):

| Type         | Label        |
|--------------|--------------|
| Bug          | `bug`        |
| Tech Debt    | `tech-debt`  |
| Refactoring  | `refactor`   |
| (other)      | _(no label)_ |

To check available labels before applying:

```bash
gh label list --json name | jq -r '.[].name'
```

If the target label does not exist, omit `--label` rather than erroring.

### 5. Annotate SESSION.md

After a successful `gh issue create`, capture the returned issue URL, extract
the number, and append it to the `###` heading in SESSION.md:

```
### Original heading text <!-- #<number> -->
```

Use `replace_string_in_file` to make this edit precisely.

### 6. Report

Print a summary table:

| Entry | Action | Issue |
|-------|--------|-------|
| `copy() dispatch is duplicated` | created | #54 |
| `Float64 conversion lossy` | already tracked | #51 |

## Edge Cases

- **Duplicate headings**: If two SESSION.md entries have identical heading
  text, process them as separate issues unless the second already carries an
  annotation from processing the first.
- **No SESSION.md**: Report the missing file and stop; do not create it.
- **`gh` not authenticated**: The `gh issue list` call will fail. Advise the
  user to run `gh auth login` and stop cleanly.
- **Rate limits / network errors**: Report the error for that entry, skip it,
  and continue with the rest.
