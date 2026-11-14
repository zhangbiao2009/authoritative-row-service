----------------------------- MODULE RowCache -----------------------------
\* TLA+ model of the alternative design in ../alternative-design.md: PostgreSQL is authoritative, every write goes
\* to the DB before it is acknowledged, and memory is a cache of the row.
\*
\* It is the counterpart of ../../memory-authoritative/tla/RowService.tla and keeps its structure, names and method,
\* so that the two can be compared. What is the same: one key; the entry in the shard map and the dead protocol; the
\* immutable snapshot, the op lock, lock-free Get and load; runSync continuing after the client gives up; the SDK
\* retrying create with the same gen; a DB that executes late, loses responses, rejects; crash and restart; a
\* superseded instance. What is different:
\*   - no dirty set, no flush queue, no flusher: an entry is never ahead of the DB
\*   - Update takes op and goes through runSync like create and delete (alternative-design.md 3.1)
\*   - one statement per operation. If its outcome is ambiguous, the session is terminated (the connection fence),
\*     then the row is read and the answer is decided from what is there; nothing is retried (3.2)
\*   - any entry whose op is free may be evicted (3.3)
\*
\* Correctness criterion: an abstract sequential state abs, changed at each operation's linearization point, which
\* is the store of the row the DB returned into the snapshot. Memory must equal abs; the DB must equal abs except
\* for the one write that is in progress under op.
\*
\* Mutations (constant Mut) and reachability witnesses (W_*) work as in RowService.tla; suite.py runs them.

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Clients,            \* clients; each client also has its own runSync background goroutine
    Cols,               \* business columns
    MaxEntry,           \* max number of entries allocated (new ones are needed after eviction or crash)
    MaxGen,             \* max number of new creates (each create uses a new gen)
    MaxVal,             \* max number of updates (each update writes a new value)
    MaxOps,             \* max number of operations each client issues
    MaxStmtTimeouts,    \* max number of DB statements with an ambiguous outcome (Go-side timeout, lost response)
    MaxDrops,           \* max number of DB statements that are definitely not executed
    MaxClientTimeouts,  \* max number of times a client gives up waiting before the outcome is known
    MaxCrashes,         \* max number of service crashes (or supersessions)
    MaxFlips,           \* max number of times a cross-row constraint starts or stops rejecting writes of this key
    MaxQueueTimeouts,   \* max number of requests that time out while still queued for op
    MaxZombieStmts,     \* max number of statements a superseded instance sends after the new one is up
    RetryCreate,        \* whether the SDK retries an ambiguous create with the same gen
    RetryWhileBusy,     \* whether the SDK may retry while its first attempt is still running
    FixDedup,           \* the gen dedup table (design.md hole 2)
    FixFence,           \* the startup fence (design.md hole 3)
    FixRetryAnswer,     \* a create retry that finds another life consults the dedup table (design.md hole 6)
    FixSdkRetry,        \* after an ambiguous attempt the SDK never reports "not executed" as final (design.md hole 7)
    FixEpoch,           \* the owner epoch in every write (design.md hole 8)
    Mut                 \* "none", or the name of one planted bug

MutNames == {
    "none",
    "NoConnFence",         \* ambiguous outcome: read the row without terminating the session first
    "ReleaseOnAmbiguous",  \* ambiguous outcome: answer "unknown" and release op
    "AmbIsFailure",        \* ambiguous outcome: fence, then answer "failed" without reading
    "UpdateNoVerCompare",  \* ambiguous update: after the fence, take "the row is there" as "my update was applied"
    "EvictNoDead",         \* eviction removes the entry from the map without setting dead
    "LoadNoRecheck",       \* load does not re-check snap after acquiring op
    "DedupDeletedWithRow", \* deleting the business row also deletes its dedup record
    "LeakOpOnNotFound",    \* update/delete of a missing row returns without releasing op
    "NoGenCheck"           \* UPDATE / DELETE without "AND gen = ?". Expected NOT to be caught: see suite.py claims
}

ASSUME MaxEntry >= 1
ASSUME Mut \in MutNames

VARIABLES
    \* whether the service is running; epoch + 1 on each crash
    up, epoch,
    \* shard map: mapE is the entry for this key in the map (0 means none)
    mapE, nextEntry, eEp,
    \* entry fields (indexed by entry number)
    snap, dead, opH,
    \* clients. retry = the current operation is an SDK retry of an ambiguous create
    pc, cop, ce, cret, ops, lastSeen, unkGen, cwait, retry,
    \* each client's runSync background goroutine. bgr / bver = gen and ver of the snapshot the statement was built from
    bpc, be, bop, bres, bgr, bver,
    \* evictor
    vpc, ve,
    \* DB: this row, the dedup table, the owner epoch row, and whether a cross-row constraint currently rejects writes
    db, dedup, dbEpoch, blocked,
    \* statements sent but not yet executed; whether each executor is waiting for a result, and the result it received
    net, wait, resp,
    \* counters for the various limits
    stmtTO, drops, cto, crashes, nextGen, nextVal, flips, qto, zsent,
    \* ghost variables for checking correctness
    abs, absKnown, preCrash, preAlt, deletedGens, insSent, delSent, violation, seen,
    \* ghost: what callers were told, and what the DB did.
    \*   failedGens  creates (by gen) whose call ended in "failed"
    \*   okUpd / failedUpd  updates (by value) whose call ended in "ok" / "failed"
    \*   appliedUpd  updates (by value) whose statement changed the row
    failedGens, okUpd, failedUpd, appliedUpd

sysV == <<up, epoch>>
mapV == <<mapE, nextEntry, eEp>>
entV == <<snap, dead, opH>>
cliV == <<pc, cop, ce, cret, ops, lastSeen, unkGen, cwait, retry>>
bgV  == <<bpc, be, bop, bres, bgr, bver>>
evV  == <<vpc, ve>>
dbV  == <<db, dedup, dbEpoch, blocked>>
netV == <<net, wait, resp>>
budV == <<stmtTO, drops, cto, crashes, nextGen, nextVal, flips, qto, zsent>>
ghV  == <<abs, absKnown, preCrash, preAlt, deletedGens, insSent, delSent, violation, seen>>
ansV == <<failedGens, okUpd, failedUpd, appliedUpd>>
vars == <<sysV, mapV, entV, cliV, bgV, evV, dbV, netV, budV, ghV, ansV>>

-----------------------------------------------------------------------------
\* values and records

Entries == 1..MaxEntry

\* A column value is <<gen, n>>: it carries the gen of the life the writer meant, to detect "data of an old object written into a new object".
Val0       == [col \in Cols |-> <<0, 0>>]
InitVal(g) == [col \in Cols |-> <<g, 0>>]

\* entry.snap: nil = not loaded; absent = definitely does not exist; row = current row
NIL    == [k |-> "nil",    gen |-> 0, ver |-> 0, val |-> Val0]
ABSENT == [k |-> "absent", gen |-> 0, ver |-> 0, val |-> Val0]
RowR(g, v, vl) == [k |-> "row", gen |-> g, ver |-> v, val |-> vl]

\* the abstract state cares only about: existence, gen, and the column values (ver is an implementation detail)
Proj(r)   == [ex |-> r.k = "row", gen |-> r.gen, val |-> r.val]
AbsAbsent == Proj(ABSENT)

DbAbsent    == [ex |-> FALSE, gen |-> 0, ver |-> 0, val |-> Val0]
RowOfDb(d)  == IF d.ex THEN RowR(d.gen, d.ver, d.val) ELSE ABSENT
DbRow       == RowOfDb(db)
DbProj      == Proj(DbRow)

\* holder of the op lock
FREE  == <<"free">>
CL(c) == <<"cl", c>>     \* the client itself (load on the Get path)
BG(c) == <<"bg", c>>     \* the client's runSync background goroutine (create / delete / update)
EV    == <<"ev">>        \* the evictor
ZB    == <<"zombie">>    \* a superseded instance (only ever the sender of a statement)
DBProcs == {BG(c) : c \in Clients}   \* executors that send write statements to the DB

AnyCol == CHOOSE col \in Cols : TRUE
NoOp   == [t |-> "none", col |-> AnyCol, egen |-> 0, gen |-> 0, v |-> 0]
NoResp == [r |-> "none", row |-> NIL]

Symm == Permutations(Clients)

-----------------------------------------------------------------------------
Init ==
    /\ up = TRUE /\ epoch = 1
    /\ mapE = 0 /\ nextEntry = 1 /\ eEp = [e \in Entries |-> 0]
    /\ snap = [e \in Entries |-> NIL]
    /\ dead = [e \in Entries |-> FALSE]
    /\ opH = [e \in Entries |-> FREE]
    /\ pc = [c \in Clients |-> "idle"]
    /\ cop = [c \in Clients |-> NoOp]
    /\ ce = [c \in Clients |-> 0]
    /\ cret = [c \in Clients |-> "none"]
    /\ ops = [c \in Clients |-> MaxOps]
    /\ lastSeen = [c \in Clients |-> 0]
    /\ unkGen = [c \in Clients |-> 0]
    /\ cwait = [c \in Clients |-> FALSE]
    /\ retry = [c \in Clients |-> FALSE]
    /\ bpc = [c \in Clients |-> "idle"]
    /\ be = [c \in Clients |-> 0]
    /\ bop = [c \in Clients |-> NoOp]
    /\ bres = [c \in Clients |-> "none"]
    /\ bgr = [c \in Clients |-> 0]
    /\ bver = [c \in Clients |-> 0]
    /\ vpc = "V_pick" /\ ve = 0
    /\ db = DbAbsent /\ dedup = {} /\ dbEpoch = 1 /\ blocked = FALSE
    /\ net = {}
    /\ wait = [p \in DBProcs |-> FALSE]
    /\ resp = [p \in DBProcs |-> NoResp]
    /\ stmtTO = 0 /\ drops = 0 /\ cto = 0 /\ crashes = 0 /\ nextGen = 0 /\ nextVal = 0
    /\ flips = 0 /\ qto = 0 /\ zsent = 0
    /\ abs = AbsAbsent /\ absKnown = TRUE /\ preCrash = AbsAbsent /\ preAlt = AbsAbsent
    /\ deletedGens = {} /\ insSent = {} /\ delSent = {}
    /\ violation = "none"
    /\ seen = [c \in Clients |-> {}]
    /\ failedGens = {} /\ okUpd = {} /\ failedUpd = {} /\ appliedUpd = {}

-----------------------------------------------------------------------------
\* helper definitions

\* getOrCreate: the double map lookup is equivalent to one atomic step.
GetOrCreate(c) ==
    IF mapE # 0
    THEN /\ ce' = [ce EXCEPT ![c] = mapE]
         /\ UNCHANGED <<mapE, nextEntry, eEp>>
    ELSE /\ nextEntry <= MaxEntry
         /\ mapE' = nextEntry
         /\ nextEntry' = nextEntry + 1
         /\ eEp' = [eEp EXCEPT ![nextEntry] = epoch]
         /\ ce' = [ce EXCEPT ![c] = nextEntry]

\* client operation ends: local variables are reset to initial values
EndOp(c) ==
    /\ pc' = [pc EXCEPT ![c] = "idle"]
    /\ cop' = [cop EXCEPT ![c] = NoOp]
    /\ ce' = [ce EXCEPT ![c] = 0]
    /\ cret' = [cret EXCEPT ![c] = "none"]

\* For a client running Get, record every value the abstract state has taken since the Get began;
\* the value the Get reads must be one of them.
InGet(c) == cop[c].t = "get" /\ pc[c] # "idle"
NewSeen(newAbs) == [c \in Clients |-> IF InGet(c) THEN seen[c] \cup {newAbs} ELSE seen[c]]

\* linearization point: the abstract state becomes newAbs
LP(newAbs) == abs' = newAbs /\ seen' = NewSeen(newAbs)

\* The first state read from the DB after a crash. "A crash loses nothing" (alternative-design.md section 1): it is
\* the abstract state at the crash, or that state with the one write that was in progress under op applied.
Plausible(obs) == obs = preCrash \/ obs = preAlt

\* Called when loading from the DB into memory. On the first load after a crash, rebuild abs from the loaded state.
Observe(obs) ==
    IF absKnown
    THEN UNCHANGED <<abs, absKnown, seen, violation>>
    ELSE /\ abs' = obs
         /\ absKnown' = TRUE
         /\ seen' = NewSeen(obs)
         /\ violation' = IF Plausible(obs) THEN violation ELSE "PostCrashStateImplausible"

\* Executor p sends a write statement, then waits for the result. live = the sender is still waiting for it.
\* ep = the sender's epoch; pre = already in flight when a newer instance bumped the owner epoch (see Bump).
SendStmt(p, kind, g, col, v) ==
    /\ net' = net \cup {[kind |-> kind, gen |-> g, col |-> col, v |-> v,
                         src |-> p, ep |-> epoch, live |-> TRUE, pre |-> FALSE]}
    /\ wait' = [wait EXCEPT ![p] = TRUE]

\* runSync's fn ends: release op; if the client is still waiting, hand it the result
\* (rel = FALSE only under the mutation LeakOpOnNotFound)
FinishOp(c, res, rel) ==
    /\ opH' = IF rel THEN [opH EXCEPT ![be[c]] = FREE] ELSE opH
    /\ bpc' = [bpc EXCEPT ![c] = IF cwait[c] THEN "B_done" ELSE "idle"]
    /\ bres' = [bres EXCEPT ![c] = IF cwait[c] THEN res ELSE "none"]
    /\ bop' = [bop EXCEPT ![c] = IF cwait[c] THEN @ ELSE NoOp]
    /\ be' = [be EXCEPT ![c] = 0]
    /\ bgr' = [bgr EXCEPT ![c] = 0]
    /\ bver' = [bver EXCEPT ![c] = 0]

Finish(c, res) == FinishOp(c, res, TRUE)

\* What the SDK tells its caller. "exists", "notfound" and "rejected" are verdicts on the data; "fail" (the statement
\* was not executed, and after a fence never will be) and "deadline" (timed out while queued) say that this request
\* did nothing. For a create that is a retry of an ambiguous attempt, the last two are not final (FixSdkRetry).
\* Update is never retried by the SDK, so each of these answers is the final answer of an update.
Failure(res) == res \in {"exists", "notfound", "rejected", "fail", "deadline"}
NotFinal(c, res) == FixSdkRetry /\ retry[c] /\ res \in {"fail", "deadline"}
SdkAnswer(c, op, res) ==
    /\ failedGens' = IF op.t = "create" /\ Failure(res) /\ ~NotFinal(c, res)
                     THEN failedGens \cup {op.gen} ELSE failedGens
    /\ unkGen' = IF op.t = "create" /\ NotFinal(c, res) /\ RetryCreate
                 THEN [unkGen EXCEPT ![c] = op.gen] ELSE unkGen
    /\ okUpd' = IF op.t = "upd" /\ res = "ok" THEN okUpd \cup {op.v} ELSE okUpd
    /\ failedUpd' = IF op.t = "upd" /\ Failure(res) THEN failedUpd \cup {op.v} ELSE failedUpd
    /\ retry' = [retry EXCEPT ![c] = FALSE]

-----------------------------------------------------------------------------
\* client: choose the next operation

ClientStart(c) ==
    /\ up /\ pc[c] = "idle" /\ ops[c] > 0
    /\ ops' = [ops EXCEPT ![c] = @ - 1]
    /\ \/ \* Get
          /\ cop' = [cop EXCEPT ![c] = [NoOp EXCEPT !.t = "get"]]
          /\ pc' = [pc EXCEPT ![c] = "G_goc"]
          /\ seen' = [seen EXCEPT ![c] = IF absKnown THEN {abs} ELSE {}]
          /\ UNCHANGED <<unkGen, nextGen, nextVal>>
       \/ \* Update: write a new value to some column; may carry a previously seen gen
          \E col \in Cols, eg \in {0, lastSeen[c]} :
             /\ bpc[c] = "idle" /\ nextVal < MaxVal
             /\ nextVal' = nextVal + 1
             /\ cop' = [cop EXCEPT ![c] = [t |-> "upd", col |-> col, egen |-> eg,
                                           gen |-> 0, v |-> nextVal + 1]]
             /\ pc' = [pc EXCEPT ![c] = "X_goc"]
             /\ UNCHANGED <<unkGen, nextGen, seen>>
       \/ \* Create: the SDK generates a new gen
          /\ bpc[c] = "idle" /\ nextGen < MaxGen
          /\ nextGen' = nextGen + 1
          /\ cop' = [cop EXCEPT ![c] = [NoOp EXCEPT !.t = "create", !.gen = nextGen + 1]]
          /\ pc' = [pc EXCEPT ![c] = "X_goc"]
          /\ UNCHANGED <<unkGen, nextVal, seen>>
       \/ \* the SDK retries an ambiguous create (same gen), possibly while its first attempt is still running
          /\ RetryCreate /\ (bpc[c] = "idle" \/ RetryWhileBusy) /\ unkGen[c] # 0
          /\ cop' = [cop EXCEPT ![c] = [NoOp EXCEPT !.t = "create", !.gen = unkGen[c]]]
          /\ unkGen' = [unkGen EXCEPT ![c] = 0]
          /\ pc' = [pc EXCEPT ![c] = "X_goc"]
          /\ UNCHANGED <<nextGen, nextVal, seen>>
       \/ \* Delete: may carry a previously seen gen
          \E eg \in {0, lastSeen[c]} :
             /\ bpc[c] = "idle"
             /\ cop' = [cop EXCEPT ![c] = [NoOp EXCEPT !.t = "delete", !.egen = eg]]
             /\ pc' = [pc EXCEPT ![c] = "X_goc"]
             /\ UNCHANGED <<unkGen, nextGen, nextVal, seen>>
    \* a create that reuses the gen of an ambiguous attempt is an SDK retry (a first create always draws a new gen)
    /\ retry' = [retry EXCEPT ![c] = cop'[c].t = "create" /\ cop'[c].gen = unkGen[c]]
    /\ UNCHANGED <<sysV, mapV, entV, ce, cret, lastSeen, cwait, bgV, evV, dbV, netV,
                   stmtTO, drops, cto, crashes, flips, qto, zsent,
                   abs, absKnown, preCrash, preAlt, deletedGens, insSent, delSent, violation, ansV>>

\* first step of each operation: getOrCreate
Goc(c) ==
    /\ up /\ pc[c] \in {"G_goc", "X_goc"}
    /\ GetOrCreate(c)
    /\ pc' = [pc EXCEPT ![c] = IF pc[c] = "G_goc" THEN "G_read" ELSE "X_lock"]
    /\ UNCHANGED <<sysV, entV, cop, cret, ops, lastSeen, unkGen, cwait, retry,
                   bgV, evV, dbV, netV, budV, ghV, ansV>>

-----------------------------------------------------------------------------
\* Get: lock-free read of snap; if not loaded, load, then start over

G_read(c) ==
    /\ up /\ pc[c] = "G_read"
    /\ LET r == snap[ce[c]] IN
       IF r.k = "nil"
       THEN /\ pc' = [pc EXCEPT ![c] = "L_lock"]
            /\ cret' = [cret EXCEPT ![c] = "G_goc"]
            /\ UNCHANGED <<cop, ce, lastSeen, violation, seen>>
       ELSE \* linearization point: snap.Load(). dead is not checked (dead protocol rule 3)
            /\ EndOp(c)
            /\ lastSeen' = [lastSeen EXCEPT ![c] = IF r.k = "row" THEN r.gen ELSE @]
            /\ violation' = IF seen[c] # {} /\ Proj(r) \notin seen[c]
                            THEN "GetNotLinearizable" ELSE violation
            /\ seen' = [seen EXCEPT ![c] = {}]
    /\ UNCHANGED <<sysV, mapV, entV, ops, unkGen, cwait, retry, bgV, evV, dbV, netV, budV,
                   abs, absKnown, preCrash, preAlt, deletedGens, insSent, delSent, ansV>>

\* load: queue up to acquire op (equivalent to singleflight)
L_lock(c) ==
    /\ up /\ pc[c] = "L_lock" /\ opH[ce[c]] = FREE
    /\ opH' = [opH EXCEPT ![ce[c]] = CL(c)]
    /\ pc' = [pc EXCEPT ![c] = "L_load"]
    /\ UNCHANGED <<sysV, mapV, snap, dead, cop, ce, cret, ops, lastSeen, unkGen, cwait, retry,
                   bgV, evV, dbV, netV, budV, ghV, ansV>>

\* load: check dead, re-check snap, then SELECT and write into snap
L_load(c) ==
    /\ up /\ pc[c] = "L_load"
    /\ LET e == ce[c] IN
       /\ opH' = [opH EXCEPT ![e] = FREE]
       /\ IF dead[e] \/ (snap[e].k # "nil" /\ Mut # "LoadNoRecheck")
          THEN UNCHANGED <<snap, abs, absKnown, seen, violation>>
          ELSE /\ snap' = [snap EXCEPT ![e] = DbRow]
               /\ Observe(DbProj)
    /\ pc' = [pc EXCEPT ![c] = cret[c]]
    /\ UNCHANGED <<sysV, mapV, dead, cop, ce, cret, ops, lastSeen, unkGen, cwait, retry,
                   bgV, evV, dbV, netV, budV, preCrash, preAlt, deletedGens, insSent, delSent, ansV>>

-----------------------------------------------------------------------------
\* client part of Create / Delete / Update: lockOpLive, then hand off to runSync

X_lock(c) ==
    \* bpc[c] = "idle" always holds here: the client's own background task holds op for as long as it runs.
    /\ up /\ pc[c] = "X_lock" /\ opH[ce[c]] = FREE /\ bpc[c] = "idle"
    /\ LET e == ce[c] IN
       IF dead[e]
       THEN \* found dead after acquiring op: release it, look up the map again
            /\ pc' = [pc EXCEPT ![c] = "X_goc"]
            /\ UNCHANGED <<opH, bgV, cwait>>
       ELSE /\ opH' = [opH EXCEPT ![e] = BG(c)]
            /\ bpc' = [bpc EXCEPT ![c] = "B_1"]
            /\ be' = [be EXCEPT ![c] = e]
            /\ bop' = [bop EXCEPT ![c] = cop[c]]
            /\ cwait' = [cwait EXCEPT ![c] = TRUE]
            /\ pc' = [pc EXCEPT ![c] = "X_wait"]
            /\ UNCHANGED <<bres, bgr, bver>>
    /\ UNCHANGED <<sysV, mapV, snap, dead, cop, ce, cret, ops, lastSeen, unkGen, retry,
                   evV, dbV, netV, budV, ghV, ansV>>

\* wait for the background result; or the client's ctx expires first (ErrOutcomeUnknown) and the background continues
X_wait(c) ==
    /\ up /\ pc[c] = "X_wait"
    /\ EndOp(c)
    /\ cwait' = [cwait EXCEPT ![c] = FALSE]
    /\ \/ /\ bpc[c] = "B_done"
          /\ bpc' = [bpc EXCEPT ![c] = "idle"]
          /\ bop' = [bop EXCEPT ![c] = NoOp]
          /\ bres' = [bres EXCEPT ![c] = "none"]
          /\ lastSeen' = [lastSeen EXCEPT ![c] =
                            IF bop[c].t = "create" /\ bres[c] = "ok" THEN bop[c].gen ELSE @]
          /\ SdkAnswer(c, bop[c], bres[c])
          /\ UNCHANGED cto
       \/ /\ bpc[c] # "B_done" /\ cto < MaxClientTimeouts
          /\ cto' = cto + 1
          /\ unkGen' = [unkGen EXCEPT ![c] = IF RetryCreate /\ cop[c].t = "create"
                                             THEN cop[c].gen ELSE @]
          /\ retry' = [retry EXCEPT ![c] = FALSE]
          /\ UNCHANGED <<bpc, bop, bres, lastSeen, failedGens, okUpd, failedUpd>>
    /\ UNCHANGED <<sysV, mapV, entV, ops, be, bgr, bver, evV, dbV, netV,
                   stmtTO, drops, crashes, nextGen, nextVal, flips, qto, zsent, ghV, appliedUpd>>

\* The request times out while it is still queued for op: DEADLINE_EXCEEDED, "this request was not executed".
X_qtimeout(c) ==
    /\ up /\ pc[c] = "X_lock" /\ opH[ce[c]] # FREE /\ qto < MaxQueueTimeouts
    /\ qto' = qto + 1
    /\ EndOp(c)
    /\ SdkAnswer(c, cop[c], "deadline")
    /\ UNCHANGED <<sysV, mapV, entV, ops, lastSeen, cwait, bgV, evV, dbV, netV,
                   stmtTO, drops, cto, crashes, nextGen, nextVal, flips, zsent, ghV, appliedUpd>>

-----------------------------------------------------------------------------
\* runSync background: one rule for every write (alternative-design.md 3.1, 3.2)

\* B_1: with op held, load if needed; decide from memory what can be decided; otherwise send the one statement.
B_1(c) ==
    /\ up /\ bpc[c] = "B_1"
    /\ LET e    == be[c]
           cur  == snap[e]
           op   == bop[c]
           kind == IF op.t = "create" THEN (IF FixDedup THEN "insd" ELSE "ins")
                   ELSE IF op.t = "delete" THEN "del" ELSE "upd"
           g    == IF op.t = "create" THEN op.gen ELSE cur.gen
       IN
       IF cur.k = "nil" THEN
            \* load first: every write is built from the loaded snapshot
            /\ snap' = [snap EXCEPT ![e] = DbRow]
            /\ Observe(DbProj)
            /\ UNCHANGED <<opH, bgV, net, wait, insSent, delSent>>
       ELSE IF op.t = "create" /\ cur.k = "row" THEN
            \* already exists. Same gen: a retry whose earlier attempt succeeded. Another life, but the dedup table
            \* holds this gen: the create took effect earlier and that life was deleted since (hole 6).
            /\ Finish(c, IF cur.gen = op.gen \/ (FixRetryAnswer /\ op.gen \in dedup) THEN "ok" ELSE "exists")
            /\ UNCHANGED <<snap, net, wait, insSent, delSent, abs, absKnown, seen, violation>>
       ELSE IF op.t # "create" /\ (cur.k = "absent" \/ (op.egen # 0 /\ cur.gen # op.egen)) THEN
            /\ FinishOp(c, "notfound", Mut # "LeakOpOnNotFound")
            /\ UNCHANGED <<snap, net, wait, insSent, delSent, abs, absKnown, seen, violation>>
       ELSE
            \* INSERT (with the dedup record), or DELETE / UPDATE ... WHERE id AND gen = <gen of the snapshot>
            /\ SendStmt(BG(c), kind, g, op.col, op.v)
            /\ bgr' = [bgr EXCEPT ![c] = g]
            /\ bver' = [bver EXCEPT ![c] = cur.ver]
            /\ insSent' = IF op.t = "create" THEN insSent \cup {g} ELSE insSent
            /\ delSent' = IF op.t = "delete" THEN delSent \cup {g} ELSE delSent
            /\ bpc' = [bpc EXCEPT ![c] = "B_2"]
            /\ UNCHANGED <<snap, opH, be, bop, bres, abs, absKnown, seen, violation>>
    /\ UNCHANGED <<sysV, mapV, dead, cliV, evV, dbV, resp, budV, preCrash, preAlt, deletedGens, ansV>>

\* B_2: the result of the statement.
B_2(c) ==
    /\ up /\ bpc[c] = "B_2" /\ ~wait[BG(c)]
    /\ resp' = [resp EXCEPT ![BG(c)] = NoResp]     \* take the result
    /\ LET e  == be[c]
           op == bop[c]
           r  == resp[BG(c)]
       IN
       IF r.r = "ok" THEN
            \* linearization point: store the row the DB returned (RETURNING), inside mu
            /\ snap' = [snap EXCEPT ![e] = r.row]
            /\ LP(Proj(r.row))
            /\ deletedGens' = IF op.t = "delete" THEN deletedGens \cup {bgr[c]} ELSE deletedGens
            /\ Finish(c, "ok")
            /\ UNCHANGED <<net, wait, violation>>
       ELSE IF r.r = "claimed" THEN
            \* create: this gen was inserted before and the row was deleted since; the create took effect back then
            /\ Finish(c, "ok")
            /\ UNCHANGED <<snap, abs, seen, deletedGens, net, wait, violation>>
       ELSE IF r.r \in {"zero", "conflict", "dup"} THEN
            \* 0 rows affected, or a primary key conflict: cannot happen while op is held and the entry is loaded
            \* (3.1). ErrInternal: drop the snapshot so that the next access reloads.
            /\ violation' = "UnexpectedResult"
            /\ snap' = [snap EXCEPT ![e] = NIL]
            /\ Finish(c, "internal")
            /\ UNCHANGED <<abs, seen, deletedGens, net, wait>>
       ELSE IF r.r \in {"fail", "rej"} THEN
            \* definitely not executed, or rejected by a constraint: a synchronous error, and nothing is in flight
            /\ Finish(c, IF r.r = "fail" THEN "fail" ELSE "rejected")
            /\ UNCHANGED <<snap, abs, seen, deletedGens, net, wait, violation>>
       ELSE IF Mut = "ReleaseOnAmbiguous" THEN
            \* mutation: ambiguous outcome, answer "unknown" and release op
            /\ Finish(c, "unknown")
            /\ UNCHANGED <<snap, abs, seen, deletedGens, net, wait, violation>>
       ELSE
            \* ambiguous outcome (3.2 step 1): terminate the session the statement was sent on and confirm it is gone.
            \* After that the statement has either executed or never will. op stays held.
            /\ net' = IF Mut = "NoConnFence" THEN net ELSE {s \in net : s.src # BG(c)}
            /\ bpc' = [bpc EXCEPT ![c] = "B_3"]
            /\ UNCHANGED <<snap, opH, be, bop, bres, bgr, bver, abs, seen, deletedGens, wait, violation>>
    /\ UNCHANGED <<sysV, mapV, dead, cliV, evV, dbV, budV, absKnown, preCrash, preAlt, insSent, delSent, ansV>>

\* B_3 (3.2 step 2): after the fence, SELECT the row (and, for a create, the dedup record) and decide from what is there.
\*   create: the row with my gen is there, or my gen is in the dedup table -> it took effect
\*   delete: the row I read is gone                                        -> it took effect
\*   update: the row is there with ver = (ver I loaded) + 1                -> it took effect
\* Otherwise it did not take effect and never will: a definite failure.
B_3(c) ==
    /\ up /\ bpc[c] = "B_3"
    /\ LET e    == be[c]
           op   == bop[c]
           mine == db.ex /\ db.gen = bgr[c]
           applied == IF op.t = "create" THEN mine \/ (FixDedup /\ op.gen \in dedup)
                      ELSE IF op.t = "delete" THEN ~mine
                      ELSE mine /\ (db.ver = bver[c] + 1 \/ Mut = "UpdateNoVerCompare")
       IN
       IF applied /\ Mut # "AmbIsFailure" THEN
            /\ snap' = [snap EXCEPT ![e] = DbRow]
            /\ LP(DbProj)
            /\ deletedGens' = IF op.t = "delete" THEN deletedGens \cup {bgr[c]} ELSE deletedGens
            /\ Finish(c, "ok")
       ELSE /\ Finish(c, "fail")
            /\ UNCHANGED <<snap, abs, seen, deletedGens>>
    /\ UNCHANGED <<sysV, mapV, dead, cliV, evV, dbV, netV, budV,
                   absKnown, preCrash, preAlt, insSent, delSent, violation, ansV>>

-----------------------------------------------------------------------------
\* evictor (dead protocol). Any entry whose op is free may be evicted, at any moment (3.3).

V_evict ==
    /\ up /\ vpc = "V_pick" /\ mapE # 0 /\ nextEntry <= MaxEntry
    /\ LET e == mapE IN
       /\ opH[e] = FREE /\ ~dead[e]
       /\ dead' = IF Mut = "EvictNoDead" THEN dead ELSE [dead EXCEPT ![e] = TRUE]
       /\ opH' = [opH EXCEPT ![e] = EV]
       /\ ve' = e
       /\ vpc' = "V_rm"
    /\ UNCHANGED <<sysV, mapV, snap, cliV, bgV, dbV, netV, budV, ghV, ansV>>

\* after releasing mu (the gap), remove it from the map inside shard.mu, then release op.
\* Once nobody holds a pointer to it, clear its snap (only to reduce the state count; nobody can see it again).
V_remove ==
    /\ up /\ vpc = "V_rm"
    /\ mapE' = IF mapE = ve THEN 0 ELSE mapE
    /\ opH' = [opH EXCEPT ![ve] = FREE]
    /\ snap' = IF \A c \in Clients : ce[c] # ve THEN [snap EXCEPT ![ve] = NIL] ELSE snap
    /\ vpc' = "V_pick"
    /\ ve' = 0
    /\ UNCHANGED <<sysV, nextEntry, eEp, dead, cliV, bgV, dbV, netV, budV, ghV, ansV>>

-----------------------------------------------------------------------------
\* DB (PostgreSQL)

\* the owner-epoch condition (design.md I16). A statement in flight at the bump may have read the old epoch (pre).
EpochOk(s) == ~FixEpoch \/ s.ep = dbEpoch \/ s.pre

GenOk(s) == db.gen = s.gen \/ Mut = "NoGenCheck"

\* the statement finds what it needs to change the row
WouldApply(s) ==
    IF s.kind = "ins" THEN ~db.ex
    ELSE IF s.kind = "insd" THEN s.gen \notin dedup /\ ~db.ex
    ELSE db.ex /\ GenOk(s)          \* del, upd

\* a cross-row constraint (unique index, foreign key) rejects it now; the whole statement is rolled back
Rejected(s) == blocked /\ WouldApply(s)

Applies(s) == EpochOk(s) /\ ~Rejected(s) /\ WouldApply(s)

\* new DB state after a statement that applies. The DB increments ver itself: UPDATE ... SET col = ?, ver = ver + 1.
Effect(s) ==
    IF s.kind \in {"ins", "insd"} THEN [ex |-> TRUE, gen |-> s.gen, ver |-> 0, val |-> InitVal(s.gen)]
    ELSE IF s.kind = "del" THEN DbAbsent
    ELSE [db EXCEPT !.ver = @ + 1, !.val = [@ EXCEPT ![s.col] = <<s.gen, s.v>>]]

ExecDb(s) == IF Applies(s) THEN Effect(s) ELSE db

ExecResult(s) ==
    IF ~EpochOk(s) THEN "zero"
    ELSE IF Rejected(s) THEN "rej"
    ELSE IF s.kind = "ins" THEN (IF db.ex THEN "dup" ELSE "ok")
    ELSE IF s.kind = "insd" THEN (IF s.gen \in dedup THEN "claimed" ELSE IF db.ex THEN "conflict" ELSE "ok")
    ELSE IF WouldApply(s) THEN "ok" ELSE "zero"

\* execute one statement (possibly a late execution). If the sender is still waiting, hand it the result, with the
\* row as RETURNING gives it; the response may also be lost.
DbExec ==
    /\ \E s \in net :
         /\ db' = ExecDb(s)
         /\ dedup' = IF s.kind = "insd" /\ EpochOk(s) /\ ~Rejected(s) THEN dedup \cup {s.gen}
                     ELSE IF s.kind = "del" /\ Mut = "DedupDeletedWithRow" /\ Applies(s) THEN dedup \ {db.gen}
                     ELSE dedup
         /\ appliedUpd' = IF s.kind = "upd" /\ Applies(s) /\ s.src # ZB THEN appliedUpd \cup {s.v} ELSE appliedUpd
         /\ net' = net \ {s}
         /\ IF s.live
            THEN /\ wait' = [wait EXCEPT ![s.src] = FALSE]
                 /\ \/ /\ resp' = [resp EXCEPT ![s.src] =
                                     [r |-> ExecResult(s),
                                      row |-> IF ExecResult(s) = "ok" THEN RowOfDb(ExecDb(s)) ELSE NIL]]
                       /\ UNCHANGED stmtTO
                    \/ /\ stmtTO < MaxStmtTimeouts       \* executed, but the response was lost
                       /\ stmtTO' = stmtTO + 1
                       /\ resp' = [resp EXCEPT ![s.src] = [r |-> "amb", row |-> NIL]]
            ELSE UNCHANGED <<resp, wait, stmtTO>>
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, evV, dbEpoch, blocked,
                   drops, cto, crashes, nextGen, nextVal, flips, qto, zsent, ghV,
                   failedGens, okUpd, failedUpd>>

\* Go-side ctx timeout: the sender stops waiting; the statement is still in flight and may execute later
DbTimeout ==
    /\ stmtTO < MaxStmtTimeouts
    /\ \E s \in net :
         /\ s.live
         /\ net' = (net \ {s}) \cup {[s EXCEPT !.live = FALSE]}
         /\ wait' = [wait EXCEPT ![s.src] = FALSE]
         /\ resp' = [resp EXCEPT ![s.src] = [r |-> "amb", row |-> NIL]]
    /\ stmtTO' = stmtTO + 1
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, evV, dbV,
                   drops, cto, crashes, nextGen, nextVal, flips, qto, zsent, ghV, ansV>>

\* statement definitely not executed (statement_timeout, connection refused), or never reaches the DB
DbDrop ==
    /\ drops < MaxDrops
    /\ \E s \in net :
         /\ net' = net \ {s}
         /\ IF s.live
            THEN /\ wait' = [wait EXCEPT ![s.src] = FALSE]
                 /\ resp' = [resp EXCEPT ![s.src] = [r |-> "fail", row |-> NIL]]
            ELSE UNCHANGED <<resp, wait>>
    /\ drops' = drops + 1
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, evV, dbV,
                   stmtTO, cto, crashes, nextGen, nextVal, flips, qto, zsent, ghV, ansV>>

\* another row changes, so that a cross-row constraint starts or stops rejecting writes of this key
EnvFlip ==
    /\ flips < MaxFlips
    /\ flips' = flips + 1
    /\ blocked' = ~blocked
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, evV, db, dedup, dbEpoch, netV,
                   stmtTO, drops, cto, crashes, nextGen, nextVal, qto, zsent, ghV, ansV>>

-----------------------------------------------------------------------------
\* crash and restart

\* the write that is in progress under op with its statement sent (at most one: OneWriterPerKey)
Pending == {c \in Clients : bpc[c] \in {"B_2", "B_3"}}
ApplyOp(op, g, a) ==
    IF op.t = "create" THEN (IF a.ex THEN a ELSE [ex |-> TRUE, gen |-> op.gen, val |-> InitVal(op.gen)])
    ELSE IF op.t = "delete" THEN (IF a.ex /\ a.gen = g THEN AbsAbsent ELSE a)
    ELSE IF op.t = "upd" THEN (IF a.ex /\ a.gen = g
                               THEN [a EXCEPT !.val = [@ EXCEPT ![op.col] = <<g, op.v>>]] ELSE a)
    ELSE a

\* The process crashes, or is superseded by a newer instance. The cache is lost; statements already sent stay in flight.
Crash ==
    /\ up /\ crashes < MaxCrashes /\ nextEntry <= MaxEntry
    /\ up' = FALSE
    /\ epoch' = epoch + 1
    /\ crashes' = crashes + 1
    /\ mapE' = 0
    /\ eEp' = [e \in Entries |-> 0]
    /\ snap' = [e \in Entries |-> NIL]
    /\ dead' = [e \in Entries |-> FALSE]
    /\ opH' = [e \in Entries |-> FREE]
    /\ pc' = [c \in Clients |-> "idle"]
    /\ cop' = [c \in Clients |-> NoOp]
    /\ ce' = [c \in Clients |-> 0]
    /\ cret' = [c \in Clients |-> "none"]
    /\ cwait' = [c \in Clients |-> FALSE]
    /\ retry' = [c \in Clients |-> FALSE]
    /\ unkGen' = [c \in Clients |->
                    IF RetryCreate /\ pc[c] # "idle" /\ cop[c].t = "create"
                    THEN cop[c].gen ELSE unkGen[c]]
    /\ bpc' = [c \in Clients |-> "idle"]
    /\ be' = [c \in Clients |-> 0]
    /\ bop' = [c \in Clients |-> NoOp]
    /\ bres' = [c \in Clients |-> "none"]
    /\ bgr' = [c \in Clients |-> 0]
    /\ bver' = [c \in Clients |-> 0]
    /\ vpc' = "V_pick" /\ ve' = 0
    /\ net' = {[s EXCEPT !.live = FALSE] : s \in net}
    /\ wait' = [p \in DBProcs |-> FALSE]
    /\ resp' = [p \in DBProcs |-> NoResp]
    /\ preCrash' = IF absKnown THEN abs ELSE preCrash
    /\ preAlt' = IF ~absKnown THEN preAlt
                 ELSE IF Pending = {} THEN abs
                 ELSE LET c == CHOOSE x \in Pending : TRUE IN ApplyOp(bop[c], bgr[c], abs)
    /\ absKnown' = FALSE
    /\ seen' = [c \in Clients |-> {}]
    /\ UNCHANGED <<nextEntry, ops, lastSeen, dbV,
                   stmtTO, drops, cto, nextGen, nextVal, flips, qto, zsent,
                   abs, deletedGens, insSent, delSent, violation, ansV>>

\* A superseded instance keeps writing after the new one is up. It sends the statements that do the most damage if
\* they take effect: an UPDATE, an INSERT or a DELETE for any gen handed out so far. ep = 0 never matches the epoch row.
ZombieSend ==
    /\ up /\ crashes > 0 /\ zsent < MaxZombieStmts
    /\ zsent' = zsent + 1
    /\ \E g \in 1..nextGen, kind \in {"upd", "del", IF FixDedup THEN "insd" ELSE "ins"} :
         net' = net \cup {[kind |-> kind, gen |-> g, col |-> AnyCol, v |-> MaxVal + 1,
                           src |-> ZB, ep |-> 0, live |-> FALSE, pre |-> FALSE]}
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, evV, dbV, wait, resp,
                   stmtTO, drops, cto, crashes, nextGen, nextVal, flips, qto, ghV, ansV>>

\* first step of startup: take over the owner epoch row
Bump ==
    /\ FixEpoch /\ ~up /\ dbEpoch # epoch
    /\ dbEpoch' = epoch
    /\ net' = {[s EXCEPT !.pre = @ \/ s.ep = dbEpoch] : s \in net}
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, evV, db, dedup, blocked, wait, resp, budV, ghV, ansV>>

\* the startup fence: terminate the previous process's sessions
FenceDrop ==
    /\ FixFence /\ ~up /\ (FixEpoch => dbEpoch = epoch)
    /\ \E s \in net : s.ep < epoch /\ net' = net \ {s}
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, evV, dbV, wait, resp, budV, ghV, ansV>>

Restart ==
    /\ ~up /\ (FixEpoch => dbEpoch = epoch)
    /\ FixFence => ~(\E s \in net : s.ep < epoch)
    /\ up' = TRUE
    /\ UNCHANGED <<epoch, mapV, entV, cliV, bgV, evV, dbV, netV, budV, ghV, ansV>>

-----------------------------------------------------------------------------
\* all client operations are done and background tasks are idle: allowed to stop here (not a deadlock)
Done ==
    /\ up
    /\ \A c \in Clients : pc[c] = "idle" /\ ops[c] = 0 /\ bpc[c] = "idle"
    /\ UNCHANGED vars

Next ==
    \/ \E c \in Clients :
         \/ ClientStart(c) \/ Goc(c) \/ G_read(c) \/ L_lock(c) \/ L_load(c)
         \/ X_lock(c) \/ X_wait(c) \/ X_qtimeout(c)
         \/ B_1(c) \/ B_2(c) \/ B_3(c)
    \/ V_evict \/ V_remove
    \/ DbExec \/ DbTimeout \/ DbDrop \/ EnvFlip
    \/ Crash \/ ZombieSend \/ Bump \/ FenceDrop \/ Restart
    \/ Done

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* invariants

\* assertions inside actions: Get linearizability, the state after a crash, results that must not happen
NoViolation == violation = "none"

\* loaded entry in the map
LiveLoaded == up /\ mapE # 0 /\ ~dead[mapE] /\ snap[mapE].k # "nil"

\* memory agrees with the abstract state: all linearization points line up
MemMatchesAbs == (LiveLoaded /\ absKnown) => Proj(snap[mapE]) = abs

\* The cache is coherent: whenever nobody holds op, the loaded snapshot is exactly the DB row (gen, ver and values),
\* even if a statement were still in flight. A violation is a cache entry that stays stale until it is evicted.
CacheCoherent == (LiveLoaded /\ opH[mapE] = FREE) => DbRow = snap[mapE]

\* The invariant of alternative-design.md 3.2: a statement of this instance is in flight only while its sender holds
\* op for the key. (Statements of a superseded instance are the business of the owner epoch.)
StmtOnlyUnderOp ==
    up => \A s \in net :
            \/ s.src = ZB
            \/ /\ s.ep = epoch
               /\ \E c \in Clients : s.src = BG(c) /\ bpc[c] \in {"B_2", "B_3"} /\ opH[be[c]] = BG(c)

\* The DB is the abstract state, except while a write is in progress under op.
DbMatchesAbs == (up /\ absKnown /\ \A c \in Clients : bpc[c] \notin {"B_2", "B_3"}) => DbProj = abs

\* a life that has been deleted never reappears, in the DB or in memory
NoResurrection ==
    /\ db.ex => db.gen \notin deletedGens
    /\ (up /\ mapE # 0 /\ snap[mapE].k = "row") => snap[mapE].gen \notin deletedGens

\* a confirmed create does not disappear (unless someone sent its DELETE)
CreateDurable ==
    (absKnown /\ abs.ex) => ((db.ex /\ db.gen = abs.gen) \/ abs.gen \in delSent)

\* at most one write SQL per key is awaiting its result
OneWriterPerKey == Cardinality({p \in DBProcs : wait[p]}) <= 1

\* in the current process, an entry that is not dead must be in the map
LiveEntryInMap ==
    \A e \in 1..(nextEntry - 1) : (up /\ eEp[e] = epoch /\ ~dead[e]) => mapE = e

\* every column value in the DB was written for the life that currently owns the row
DbValsBelongToGen == db.ex => \A col \in Cols : db.val[col][1] = db.gen

\* once the service is up, no statement that a previous process sent before the fence is still in flight
FencedRestart == (FixFence /\ up) => \A s \in net : s.ep = epoch \/ s.src = ZB

\* a create whose call ended in "failed" is never inserted, before or after the answer
CreateFailureIsFinal == FixDedup => failedGens \cap dedup = {}

\* The same promise for update, which this design can make and the write-behind design does not need to:
\* "ok" means the statement changed the row; "failed" means it did not and never will.
UpdateAnswerSound == okUpd \subseteq appliedUpd /\ failedUpd \cap appliedUpd = {}

\* action property: within one life, the DB ver never goes back, and a value never changes without a larger ver
DbVerMonotonic ==
    [][(db.ex /\ db'.ex /\ db'.gen = db.gen) =>
         /\ db'.ver >= db.ver
         /\ (db'.val # db.val => db'.ver > db.ver)]_db

-----------------------------------------------------------------------------
\* liveness (weak fairness on every step of work that has started; no fairness on faults or on starting work)

Fairness ==
    /\ \A c \in Clients :
         /\ WF_vars(Goc(c)) /\ WF_vars(G_read(c)) /\ WF_vars(L_lock(c)) /\ WF_vars(L_load(c))
         /\ WF_vars(X_lock(c)) /\ WF_vars(X_wait(c))
         /\ WF_vars(B_1(c)) /\ WF_vars(B_2(c)) /\ WF_vars(B_3(c))
    /\ WF_vars(V_remove)
    /\ WF_vars(DbExec) /\ WF_vars(Bump) /\ WF_vars(FenceDrop) /\ WF_vars(Restart)

FairSpec == Spec /\ Fairness

\* every request a client has issued eventually gets an answer
EventuallyResponds == \A c \in Clients : pc[c] # "idle" ~> pc[c] = "idle"

\* every write running in the background eventually reaches a definite outcome
BackgroundSettles ==
    \A c \in Clients : bpc[c] \in {"B_1", "B_2", "B_3"} ~> bpc[c] \in {"idle", "B_done"}

\* op is never held forever
NoLockLeak == \A e \in Entries : opH[e] # FREE ~> opH[e] = FREE

-----------------------------------------------------------------------------
\* reachability witnesses: each claims that a situation never occurs, and TLC must report each one as violated

W_Delete               == deletedGens = {}
W_Recreate             == ~(abs.ex /\ deletedGens # {})
W_Update               == okUpd = {}
W_Evict                == \A e \in Entries : ~dead[e]
W_Reload               == nextEntry <= 2
W_Crash                == crashes = 0
W_CrashWithStmtInFlight == ~(~up /\ net # {})
W_CrashWithWritePending == ~(~up /\ preAlt # preCrash)
W_AmbStmtInFlight      == ~(\E c \in Clients : bpc[c] = "B_2" /\ ~wait[BG(c)] /\ resp[BG(c)].r = "amb"
                                               /\ \E s \in net : s.src = BG(c) /\ Applies(s))
W_FenceThenApplied     == ~(\E c \in Clients : bpc[c] = "B_3" /\ bop[c].t = "upd"
                                               /\ db.ex /\ db.gen = bgr[c] /\ db.ver = bver[c] + 1)
W_FenceThenNotApplied  == ~(\E c \in Clients : bpc[c] = "B_3" /\ bop[c].t = "upd"
                                               /\ db.ex /\ db.gen = bgr[c] /\ db.ver = bver[c])
W_CreateAmb            == ~(\E c \in Clients : bpc[c] = "B_3" /\ bop[c].t = "create")
W_DeleteAmb            == ~(\E c \in Clients : bpc[c] = "B_3" /\ bop[c].t = "delete")
W_NotExecuted          == ~(\E c \in Clients : bpc[c] = "B_2" /\ ~wait[BG(c)] /\ resp[BG(c)].r = "fail")
W_DedupClaimed         == ~(\E c \in Clients : resp[BG(c)].r = "claimed")
W_RetryOfDeletedGen    == ~(\E c \in Clients : cop[c].t = "create" /\ cop[c].gen \in deletedGens)
W_GetDuringWrite       == ~(\E c, d \in Clients : InGet(c) /\ pc[c] = "G_read" /\ bpc[d] = "B_2" /\ ce[c] = be[d])
W_ClientGaveUp         == cto = 0

\* situations that need the faults of the ext environments of suite.py
W_Rejected             == ~(\E c \in Clients : bpc[c] = "B_2" /\ ~wait[BG(c)] /\ resp[BG(c)].r = "rej")
W_AmbWhileBlocked      == ~(\E c \in Clients : bpc[c] = "B_2" /\ ~wait[BG(c)] /\ resp[BG(c)].r = "amb" /\ blocked
                                               /\ \E s \in net : s.src = BG(c) /\ WouldApply(s))
W_RetryFindsOtherLife  == ~(\E c \in Clients : bpc[c] = "B_1" /\ bop[c].t = "create" /\ snap[be[c]].k = "row"
                                               /\ snap[be[c]].gen # bop[c].gen /\ bop[c].gen \in dedup)
W_RetryQueuedBehindSelf == ~(\E c \in Clients : retry[c] /\ pc[c] = "X_lock" /\ opH[ce[c]] = BG(c))
W_QueueTimeout         == qto = 0
W_ZombieStmt           == ~(up /\ \E s \in net : s.src = ZB)
W_ZombieStoppedByEpoch == ~(up /\ \E s \in net : s.src = ZB /\ ~EpochOk(s) /\ WouldApply(s))

=============================================================================
