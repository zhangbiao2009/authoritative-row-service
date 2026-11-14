# In-memory authoritative row service: Go implementation

## Sources of truth

- [design.md](design.md): what to build. Section 0 is the working agreement; read it first.
- [tla/RowService.tla](tla/RowService.tla) and [correctness-properties.md](correctness-properties.md): the verified
  protocol and the properties it must keep.
- [tla-mapping.md](tla-mapping.md): which Go code and which test corresponds to each TLA variable, action, invariant,
  mutation and design hole.

If the design, the model and the code disagree, or something is missing or does not make sense: stop and ask. Never
change design.md, the TLA model or correctness-properties.md on your own (design.md 0.6).

## Workflow: one milestone at a time

Milestones are in design.md 12.2. For each one:

1. Write the plan for this milestone only. Wait for the user to approve it.
2. Implement with TDD: the failing test first.
3. Review for correctness (`/code-review`), fix.
4. Simplify (`/simplify`), then run the tests again.
5. Check every acceptance criterion of the milestone in 12.2, and the mutation and hole rows of tla-mapping.md
   sections 6 and 7 that belong to it. For each mutation, break the rule with a temporary patch, show a test fails,
   revert.
6. Report what was checked, with the command output. The user reviews the diff and decides when to commit.

Do not start the next milestone, and do not write code for a later one, without the user.

## Correctness rules that look like complexity

The deliberate choices in design.md 0.5, the lock rules in 6.4, and every check listed in tla-mapping.md section 6
were each shown necessary by the TLA model. Do not remove, merge or "simplify" them, even when they look redundant:
re-checking `dead` or `snap` after taking a lock, retry-until-sure loops, keeping `unsure` after a fence, never
upserting, enqueueing after releasing `mu`. When a simplification touches one of them, leave it and say why.

## Keeping the code simple

- The skeletons in design.md section 7 are the target shape. One function per operation, as there; do not split code
  into one function per TLA action. Mark each action's step with `// TLA: <Action>` instead.
- Directory layout as in design.md section 3. Do not add packages, layers or files it does not need.
- No interface unless it has two implementations now. `db.DB` (PostgreSQL and `fakedb`) is the only expected one.
- No configuration options, option structs, builders, managers, generics or helpers that the design does not call for.
  No code for later milestones or hypothetical needs.
- Errors: the sentinel errors of design.md 5.2, wrapped with `%w`. No error type hierarchies.
- Dependencies: standard library (`log/slog` for logging), plus `github.com/jackc/pgx/v5`,
  `github.com/anishathalye/porcupine`, `google.golang.org/protobuf`, `github.com/testcontainers/testcontainers-go`.
  Ask before adding anything else.
- Test-only code lives in `_test.go` files or `internal/db/fakedb`. Failpoint hooks are nil in production; use one
  mechanism for all of them; no build tags.
- Comments: the invariants (I1-I16) and each lock's role go in code comments (design.md 0.4); functions that require a
  lock held end in `Locked` (6.4). Do not comment what the code already says.

## Commands

Run from this directory. Go 1.25.

```
gofmt -l .
go vet ./...
go test -race ./...
```

All three must be clean before reporting a step done. Never claim a test passes without running it.

The TLA model: `cd tla && python3 suite.py quick` (about 13 minutes) after any change to the model. Changing the model
needs the user's approval.
