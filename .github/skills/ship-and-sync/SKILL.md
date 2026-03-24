---
name: ship-and-sync
description: 'Finalize implementation work by syncing with main, rebasing, creating a branch, committing and pushing, opening a PR, then running session-to-issues to file SESSION.md notes.'
argument-hint: 'Optional: issue number, branch name, and PR title'
---

# Ship And Sync

Use this workflow when coding is complete and the next step is to ship changes cleanly and sync session notes to GitHub issues.

## Inputs
- Optional issue number, such as `69`
- Optional branch name and PR title

## Procedure
1. Review the current session's work and update `SESSION.md` with any tech debt, bugs, or refactoring opportunities observed during the session that have not yet been recorded. Add entries under the appropriate section following the entry format in CLAUDE.md before proceeding.
2. Confirm repository state.
3. Verify `SESSION.md` exists at the repo root.
4. Check `git status --short --branch` and list changed files.
5. If there are uncommitted changes, stash them with a named stash.
6. Sync main with remote:
   - `git fetch origin`
   - `git checkout main`
   - `git pull --ff-only origin main`
7. Rebase work onto updated main:
   - If work was stashed on main, create a new branch from main and pop the stash.
   - If work is on an existing feature branch with commits, run `git rebase origin/main`.
8. Run targeted validation for modified files (tests/checks/format as appropriate).
9. Stage only intended files and commit with an issue-referenced message.
10. Push branch to origin and open a PR against `main`.
11. Run the `session-to-issues` skill to sync untracked `SESSION.md` entries into GitHub issues.
12. If new issues are created or matched, ensure `SESSION.md` headings are annotated with `<!-- #N -->`.

## Decision Points
- If `git pull --ff-only` fails, stop and resolve divergence before continuing.
- If stash pop or rebase produces conflicts, resolve conflicts before committing.
- If GitHub authentication fails (`gh` commands), stop and ask user to run `gh auth login`.
- If no untracked `SESSION.md` entries exist, skip issue creation and report that state.

## Completion Checks
- Branch is based on latest `origin/main`.
- Commit contains only intended files.
- PR exists and references the relevant issue.
- `SESSION.md` notes are linked to issues or confirmed already tracked.
- Final summary includes branch name, commit hash, PR number/url, and issue sync results.
