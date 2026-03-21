---
name: ship-and-sync
description: 'Finalize implementation work by syncing with main, rebasing, creating a branch, committing and pushing, and opening a PR that includes a Session Notes Needing Issues section from SESSION.md.'
argument-hint: 'Optional: issue number, branch name, and PR title'
---

# Ship And Sync

Use this workflow when coding is complete and the next step is to ship changes cleanly and surface session notes in the PR for issue creation.

## Inputs
- Optional issue number, such as `69`
- Optional branch name and PR title

## Procedure
1. Confirm repository state.
2. Verify `SESSION.md` exists at the repo root.
3. Check `git status --short --branch` and list changed files.
4. If there are uncommitted changes, stash them with a named stash.
5. Sync main with remote:
   - `git fetch origin`
   - `git checkout main`
   - `git pull --ff-only origin main`
6. Rebase work onto updated main:
   - If work was stashed on main, create a new branch from main and pop the stash.
   - If work is on an existing feature branch with commits, run `git rebase origin/main`.
7. Run targeted validation for modified files (tests/checks/format as appropriate).
8. Stage only intended files and commit with an issue-referenced message.
9. Collect untracked SESSION.md entries:
   - Read `SESSION.md` and collect every `###` heading that does **not** already carry `<!-- #N -->`.
   - For each such entry, capture the heading line and all bullet fields beneath it.
10. Push branch to origin and open a PR against `main`:
    - Build the PR body with the standard implementation description first.
    - If any untracked entries were collected in step 9, append a `## Session Notes Needing Issues` section containing the full text of each entry (heading + bullet fields).
    - If there are no untracked entries, omit the section.

## Decision Points
- If `git pull --ff-only` fails, stop and resolve divergence before continuing.
- If stash pop or rebase produces conflicts, resolve conflicts before committing.
- If GitHub authentication fails (`gh` commands), stop and ask user to run `gh auth login`.
- If `SESSION.md` does not exist, omit the session-notes section from the PR body and report that state.

## Completion Checks
- Branch is based on latest `origin/main`.
- Commit contains only intended files.
- PR exists and references the relevant issue.
- PR body contains a `## Session Notes Needing Issues` section if there were any untracked SESSION.md entries.
- Final summary includes branch name, commit hash, and PR number/url.
