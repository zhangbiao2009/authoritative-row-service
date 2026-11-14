# DB-authoritative row service: Go implementation

## Sources of truth

- [design.md](design.md): what to build. Section 0 is the working agreement; read it first.
- [tla/RowCache.tla](tla/RowCache.tla): the verified protocol. Its properties are stated in
  [alternative-design.md](alternative-design.md) 11.2 and listed in design.md 12.3.
- [tla-mapping.md](tla-mapping.md): which Go code and which test corresponds to each TLA variable, action, property,
  mutation and design hole.

alternative-design.md is the record of how the design was chosen; where it and design.md differ, design.md wins.

If the design, the model and the code disagree, or something is missing or does not make sense: stop and ask. Never
change design.md, alternative-design.md or the TLA model on your own (design.md 0.5).

## Workflow: one milestone at a time

Milestones are in design.md 12.2. For each one:

1. Write the plan for this milestone only, in `docs/plans/<date>-m<N>-<topic>.md`. Wait for the user to approve it.
2. Implement with TDD: the failing test first.
3. Review for correctness (`/code-review`), fix.
4. Simplify (`/simplify`), then run the tests again.
5. Check every acceptance criterion of the milestone in 12.2, and the rows of tla-mapping.md sections 6 and 7 that
   belong to it. For each mutation, break the rule with a temporary patch, show a test fails, revert.
6. Report what was checked, with the command output. The user reviews the diff and decides when to commit.

Do not start the next milestone, and do not write code for a later one, without the user.

## Correctness rules that look like complexity

The deliberate choices in design.md 0.4, the lock rules in 6.4, and every check in tla-mapping.md section 6 are there
on purpose. Do not remove, merge or "simplify" them, even when they look redundant:

- `op` stays held from before a statement is sent until its outcome is settled: through the fence and read loops of
  `fenceAndReadLocked`, and on `errAbandoned`. `runSync` has no `defer` for `op` or the answer.
- After an ambiguous result: fence, then read, then check the owner, then decide. The update decision compares `ver`
  with the loaded `ver` + 1.
- Zero rows or a primary key conflict goes to `unexpectedLocked`; it is never "already done".
- Create loads first and looks the gen up in the dedup table before `ALREADY_EXISTS`. Never upsert.
- `AND gen = ?` in UPDATE and DELETE, and the re-check of `snap` in `ensureLoadedLocked`, are not needed by the model
  and are kept anyway (tla-mapping.md 6.2). No test fails without them; that is not a reason to remove them.
- Re-checking `dead` after taking `op`; releasing `op` before answering.

When a simplification touches one of them, leave it and say why.

## Keeping the code simple

- The skeletons in design.md section 7 are the target shape. One function per operation, as there; do not split code
  into one function per TLA action. Mark each action's step with `// TLA: <Action>` instead.
- Directory layout as in design.md section 3. Do not add packages, layers or files it does not need.
- This is its own module (`module rowcache`). Copy what M1 needs from `../memory-authoritative` and trim it to this
  design; never import from that module, and do not change it.
- There is no per-entry `mu`, no dirty state, no flusher (design.md 13.1). Do not reintroduce them.
- No interface unless it has two implementations now. `db.DB` (PostgreSQL and `fakedb`) is the only expected one.
- No configuration options, option structs, builders, managers, generics or helpers that the design does not call for.
  No code for later milestones or hypothetical needs.
- Errors: the sentinel errors of design.md 5.2, wrapped with `%w`, and `*db.AmbiguousError`. No error type hierarchies.
- Dependencies: standard library (`log/slog` for logging), plus `github.com/jackc/pgx/v5` (with `pgxpool`),
  `github.com/anishathalye/porcupine`, `google.golang.org/protobuf`, `github.com/testcontainers/testcontainers-go`.
  Ask before adding anything else.
- Test-only code lives in `_test.go` files or `internal/db/fakedb`. Failpoint hooks are nil in production; use one
  mechanism for all of them; no build tags.
- Comments: the invariants (I1-I14) and each lock's role go in code comments (design.md 0.3); functions that require
  `op` held end in `Locked` (6.4). Do not comment what the code already says.

## Commands

Run from this directory. Go 1.25. (`go.mod` is created in M1.)

```
gofmt -l .
go vet ./...
go test -race ./...
```

All three must be clean before reporting a step done. Never claim a test passes without running it.

The TLA model: `cd tla && python3 suite.py quick` (48 runs, about 10 minutes) after any change to the model. Changing
the model needs the user's approval.
