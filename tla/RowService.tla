----------------------------- MODULE RowService -----------------------------
\* TLA+ model of the core concurrency protocol of the "In-Memory Authoritative Row Service" in design.md.
\*
\* Models only one key: all correctness issues in the design are per-key. Modeled:
\*   - allocation and eviction of the entry in the shard map (dead protocol, 7.7)
\*   - the entry's snap / dirty / queued / dead / op lock (5.2, 6)
\*   - Get, Update, Create, Delete, and runSync continuing after the client gives up (7.2-7.5)
\*   - the flusher: op is held until the write completes; on failure the mask is merged back and re-enqueued (7.6)
\*   - a DB with "late execution": a statement may still execute after the Go side times out; responses may be lost;
\*     a statement may be definitely rejected (8.1, 8.4)
\*   - clients giving up waiting; the SDK retries create with the same gen (7.10)
\*   - service crash and restart (7.8)
\*
\* Modeling granularity: the critical sections of mu, shard.mu and qmu do no IO and never block, so each is modeled
\* as one atomic step; the gap "after releasing a lock and before acquiring it again" (Section 10) is split into
\* separate steps, and TLC exhaustively explores all possible interleavings within it.
\*
\* Correctness criterion: maintain an abstract sequential state abs. Each operation modifies abs at the
\* linearization point given in Section 9; memory and DB must both agree with abs (see the invariants at the end of the file).
\*
\* Update is modeled only as "assign a new value to one column". Increment (INCR) computes the new value from the old one
\* inside mu, on the same path and in the same atomic step as assignment, with an identical concurrency structure, so it is not modeled separately.
\*
\* To bound the state count, local variables are reset to fixed initial values once they are no longer needed (clear
\* cop/ce when an operation ends, be/bop when the background task ends, resp once the result is taken, etc.). This does not change behavior.
\*
\* The properties checked here, and how each one is exercised, are specified in ../correctness-properties.md:
\*   - safety invariants and one action property (Spec)
\*   - liveness properties under weak fairness (FairSpec, live.cfg)
\*   - mutations: the constant Mut plants exactly one known bug; each must be caught (suite.py mutants)
\*   - reachability witnesses W_*: each must be violated, proving the model reaches that situation (suite.py witnesses)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Clients,            \* clients; each client also has its own runSync background goroutine
    Cols,               \* business columns
    MaxEntry,           \* max number of entries allocated (new ones are needed after eviction or crash)
    MaxGen,             \* max number of new creates (each create uses a new gen)
    MaxVal,             \* max number of updates (each update writes a new value)
    MaxOps,             \* max number of operations each client issues
    MaxStmtTimeouts,    \* max number of DB statements with an ambiguous outcome (Go-side timeout, lost response)
    MaxDrops,           \* max number of DB statements that are definitely rejected or never executed
    MaxClientTimeouts,  \* max number of times a client gives up waiting before a create/delete outcome is known
    MaxCrashes,         \* max number of service crashes
    InsertCanBeLate,    \* whether an INSERT can still execute late after a Go-side timeout (precondition of hole 1)
    RetryCreate,        \* whether the SDK retries an ambiguous create with the same gen (precondition of hole 2)
    FixDedup,           \* fix for holes 1, 2: gen dedup table + create loads first
    FixFence,           \* fix for hole 3: on restart, first clear the sessions the previous process left on PG
    FixUnsureRetry,     \* fix for hole 4: after any ambiguous attempt, a definite failure cannot be taken as the final result
    Mut                 \* "none", or the name of one planted bug (see MutNames and correctness-properties.md section 6)

\* Each mutation breaks exactly one rule of the design; with Mut = "none" the model is the design itself.
MutNames == {
    "none",
    "NoVerCheck",          \* flush UPDATE without "AND ver < ?"
    "NoGenCheck",          \* flush UPDATE / DELETE without "AND gen = ?"
    "FlushUpsert",         \* flush is an upsert: inserts the row when it does not exist
    "DedupDeletedWithRow", \* deleting the business row also deletes its dedup record
    "NoMaskMerge",         \* a failed flush does not merge its column mask back
    "EvictDirty",          \* eviction does not check dirty
    "EvictNoDead",         \* eviction removes the entry from the map without setting dead
    "UpdateIgnoresDead",   \* Update does not check dead
    "LoadNoRecheck",       \* load does not re-check snap after acquiring op
    "FlushNoOp",           \* the flusher writes without holding op
    "ReleaseOnAmbiguous",  \* create/delete: on an ambiguous outcome, give up and release op
    "SelectConfirms",      \* create/delete: on an ambiguous outcome, decide by one SELECT
    "CreateNoLoad",        \* create inserts without loading first
    "DeleteKeepsDirty",    \* a successful delete keeps the dirty mask
    "LeakOpOnNotFound"     \* delete of a missing row returns without releasing op
}

ASSUME MaxEntry >= 1
ASSUME Mut \in MutNames

VARIABLES
    \* whether the service is running; epoch + 1 on each crash
    up, epoch,
    \* shard map: mapE is the entry for this key in the map (0 means none)
    mapE, nextEntry, eEp,
    \* entry fields (indexed by entry number)
    snap, dirty, queued, dead, opH,
    \* clients
    pc, cop, ce, cret, ops, lastSeen, unkGen, cwait,
    \* each client's runSync background goroutine
    bpc, be, bop, bres, bgr, bamb,
    \* flusher and flush queue
    fpc, fbatch, fretry, fitem, queue,
    \* evictor
    vpc, ve,
    \* DB: the state of this row in the DB, and the dedup table (of the fix)
    db, dedup,
    \* statements sent but not yet executed; whether each executor is waiting for a result, and the result it received
    net, wait, resp,
    \* counters for the various limits
    stmtTO, drops, cto, crashes, nextGen, nextVal,
    \* ghost variables for checking correctness
    abs, absKnown, preCrash, deletedGens, insSent, delSent, violation, seen

sysV == <<up, epoch>>
mapV == <<mapE, nextEntry, eEp>>
entV == <<snap, dirty, queued, dead, opH>>
cliV == <<pc, cop, ce, cret, ops, lastSeen, unkGen, cwait>>
bgV  == <<bpc, be, bop, bres, bgr, bamb>>
flV  == <<fpc, fbatch, fretry, fitem, queue>>
evV  == <<vpc, ve>>
dbV  == <<db, dedup>>
netV == <<net, wait, resp>>
budV == <<stmtTO, drops, cto, crashes, nextGen, nextVal>>
ghV  == <<abs, absKnown, preCrash, deletedGens, insSent, delSent, violation, seen>>
vars == <<sysV, mapV, entV, cliV, bgV, flV, evV, dbV, netV, budV, ghV>>

-----------------------------------------------------------------------------
\* values and records

Entries == 1..MaxEntry

\* A column value is <<gen, n>>: it carries the gen of the life that wrote it, to detect "data of an old object written into a new object".
Val0       == [col \in Cols |-> <<0, 0>>]
InitVal(g) == [col \in Cols |-> <<g, 0>>]

\* entry.snap: nil = not loaded; absent = definitely does not exist; row = current row
NIL    == [k |-> "nil",    gen |-> 0, ver |-> 0, val |-> Val0]
ABSENT == [k |-> "absent", gen |-> 0, ver |-> 0, val |-> Val0]
RowR(g, v, vl) == [k |-> "row", gen |-> g, ver |-> v, val |-> vl]

\* the abstract state cares only about: existence, gen, and the column values (ver is an implementation detail)
Proj(r)   == [ex |-> r.k = "row", gen |-> r.gen, val |-> r.val]
AbsAbsent == Proj(ABSENT)

DbAbsent == [ex |-> FALSE, gen |-> 0, ver |-> 0, val |-> Val0]
DbRow    == IF db.ex THEN RowR(db.gen, db.ver, db.val) ELSE ABSENT
DbProj   == Proj(DbRow)

\* holder of the op lock
FREE  == <<"free">>
CL(c) == <<"cl", c>>     \* the client itself (load)
BG(c) == <<"bg", c>>     \* the client's runSync background goroutine (create/delete)
FL    == <<"fl">>        \* the flusher
EV    == <<"ev">>        \* the evictor
DBProcs == {BG(c) : c \in Clients} \cup {FL}   \* executors that send write statements to the DB

AnyCol == CHOOSE col \in Cols : TRUE
NoOp   == [t |-> "none", col |-> AnyCol, egen |-> 0, gen |-> 0, v |-> 0]
NoItem == [e |-> 0, r |-> NIL, m |-> {}]

Symm == Permutations(Clients)

-----------------------------------------------------------------------------
Init ==
    /\ up = TRUE /\ epoch = 1
    /\ mapE = 0 /\ nextEntry = 1 /\ eEp = [e \in Entries |-> 0]
    /\ snap = [e \in Entries |-> NIL]
    /\ dirty = [e \in Entries |-> {}]
    /\ queued = [e \in Entries |-> FALSE]
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
    /\ bpc = [c \in Clients |-> "idle"]
    /\ be = [c \in Clients |-> 0]
    /\ bop = [c \in Clients |-> NoOp]
    /\ bres = [c \in Clients |-> "none"]
    /\ bgr = [c \in Clients |-> 0]
    /\ bamb = [c \in Clients |-> FALSE]
    /\ fpc = "F_take" /\ fbatch = {} /\ fretry = {} /\ fitem = NoItem /\ queue = {}
    /\ vpc = "V_pick" /\ ve = 0
    /\ db = DbAbsent /\ dedup = {}
    /\ net = {}
    /\ wait = [p \in DBProcs |-> FALSE]
    /\ resp = [p \in DBProcs |-> "none"]
    /\ stmtTO = 0 /\ drops = 0 /\ cto = 0 /\ crashes = 0 /\ nextGen = 0 /\ nextVal = 0
    /\ abs = AbsAbsent /\ absKnown = TRUE /\ preCrash = AbsAbsent
    /\ deletedGens = {} /\ insSent = {} /\ delSent = {}
    /\ violation = "none"
    /\ seen = [c \in Clients |-> {}]

-----------------------------------------------------------------------------
\* helper definitions

\* getOrCreate (7.1): the double map lookup is equivalent to one atomic step.
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
\* the value the Get reads must be one of them (a read can be linearized at any point within its interval).
InGet(c) == cop[c].t = "get" /\ pc[c] # "idle"
NewSeen(newAbs) == [c \in Clients |-> IF InGet(c) THEN seen[c] \cup {newAbs} ELSE seen[c]]

\* linearization point: the abstract state becomes newAbs
LP(newAbs) == abs' = newAbs /\ seen' = NewSeen(newAbs)

\* Whether the first state observed from the DB after a crash and restart makes sense:
\*   - a life whose delete was confirmed must not appear
\*   - a life that appears must have had its INSERT sent by someone
\*   - all column values belong to this life
\*   - if a life that existed before the crash is gone, someone must have sent its DELETE
\* Updates within the flush window may be lost (values may be stale); the requirements allow this.
Plausible(obs) ==
    /\ obs.ex => obs.gen \notin deletedGens
    /\ obs.ex => obs.gen \in insSent
    /\ obs.ex => \A col \in Cols : obs.val[col][1] = obs.gen
    /\ (preCrash.ex /\ ~(obs.ex /\ obs.gen = preCrash.gen)) => preCrash.gen \in delSent

\* Called when loading from the DB into memory. On the first load after a crash, rebuild abs from the loaded state.
Observe(obs) ==
    IF absKnown
    THEN UNCHANGED <<abs, absKnown, seen, violation>>
    ELSE /\ abs' = obs
         /\ absKnown' = TRUE
         /\ seen' = NewSeen(obs)
         /\ violation' = IF Plausible(obs) THEN violation ELSE "PostCrashStateImplausible"

\* Executor p sends a write statement to the DB, then waits for the result. live = the sender is still waiting for this statement's result.
\* (Statements are unnumbered: at any moment each executor has at most one live statement.)
SendStmt(p, kind, g, ver, vl, m) ==
    /\ net' = net \cup {[kind |-> kind, gen |-> g, ver |-> ver, val |-> vl, mask |-> m,
                         src |-> p, ep |-> epoch, live |-> TRUE]}
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
    /\ bamb' = [bamb EXCEPT ![c] = FALSE]

Finish(c, res) == FinishOp(c, res, TRUE)

\* Create's linearization point (I7): after the DB has definitely succeeded, snap.Store(row) inside mu
CreateLP(c, row, obsBefore) ==
    /\ snap' = [snap EXCEPT ![be[c]] = row]
    /\ dirty' = [dirty EXCEPT ![be[c]] = {}]
    /\ LP(Proj(row))
    /\ absKnown' = TRUE
    /\ violation' = IF absKnown \/ Plausible(obsBefore) THEN violation
                    ELSE "PostCrashStateImplausible"

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
       \/ \* Update: write a new value to some column; may carry a previously seen gen (7.3)
          \E col \in Cols, eg \in {0, lastSeen[c]} :
             /\ nextVal < MaxVal
             /\ nextVal' = nextVal + 1
             /\ cop' = [cop EXCEPT ![c] = [t |-> "upd", col |-> col, egen |-> eg,
                                           gen |-> 0, v |-> nextVal + 1]]
             /\ pc' = [pc EXCEPT ![c] = "U_goc"]
             /\ UNCHANGED <<unkGen, nextGen, seen>>
       \/ \* Create: the SDK generates a new gen
          /\ bpc[c] = "idle" /\ nextGen < MaxGen
          /\ nextGen' = nextGen + 1
          /\ cop' = [cop EXCEPT ![c] = [NoOp EXCEPT !.t = "create", !.gen = nextGen + 1]]
          /\ pc' = [pc EXCEPT ![c] = "X_goc"]
          /\ UNCHANGED <<unkGen, nextVal, seen>>
       \/ \* the SDK retries an ambiguous create (same gen)
          /\ RetryCreate /\ bpc[c] = "idle" /\ unkGen[c] # 0
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
    /\ UNCHANGED <<sysV, mapV, entV, ce, cret, lastSeen, cwait, bgV, flV, evV, dbV, netV,
                   stmtTO, drops, cto, crashes,
                   abs, absKnown, preCrash, deletedGens, insSent, delSent, violation>>

\* first step of each operation: getOrCreate
GocNext(x) == CASE x = "G_goc" -> "G_read"
                [] x = "U_goc" -> "U_mu"
                [] x = "X_goc" -> "X_lock"

Goc(c) ==
    /\ up /\ pc[c] \in {"G_goc", "U_goc", "X_goc"}
    /\ GetOrCreate(c)
    /\ pc' = [pc EXCEPT ![c] = GocNext(pc[c])]
    /\ UNCHANGED <<sysV, entV, cop, cret, ops, lastSeen, unkGen, cwait,
                   bgV, flV, evV, dbV, netV, budV, ghV>>

-----------------------------------------------------------------------------
\* Get (7.2): lock-free read of snap; if not loaded, load, then start over

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
    /\ UNCHANGED <<sysV, mapV, entV, ops, unkGen, cwait, bgV, flV, evV, dbV, netV, budV,
                   abs, absKnown, preCrash, deletedGens, insSent, delSent>>

\* load: queue up to acquire op (equivalent to singleflight)
L_lock(c) ==
    /\ up /\ pc[c] = "L_lock" /\ opH[ce[c]] = FREE
    /\ opH' = [opH EXCEPT ![ce[c]] = CL(c)]
    /\ pc' = [pc EXCEPT ![c] = "L_load"]
    /\ UNCHANGED <<sysV, mapV, snap, dirty, queued, dead,
                   cop, ce, cret, ops, lastSeen, unkGen, cwait,
                   bgV, flV, evV, dbV, netV, budV, ghV>>

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
    /\ UNCHANGED <<sysV, mapV, dirty, queued, dead,
                   cop, ce, cret, ops, lastSeen, unkGen, cwait,
                   bgV, flV, evV, dbV, netV, budV, preCrash, deletedGens, insSent, delSent>>

-----------------------------------------------------------------------------
\* Update (7.3): takes only mu; the read-modify-write happens entirely within one critical section

U_mu(c) ==
    /\ up /\ pc[c] = "U_mu"
    /\ LET e   == ce[c]
           old == snap[e]
           op  == cop[c]
           nv  == <<old.gen, op.v>>
           nr  == [old EXCEPT !.ver = old.ver + 1, !.val = [old.val EXCEPT ![op.col] = nv]]
       IN
       IF dead[e] /\ Mut # "UpdateIgnoresDead" THEN
            \* eviction is removing it: look up the map again
            /\ pc' = [pc EXCEPT ![c] = "U_goc"]
            /\ UNCHANGED <<snap, dirty, queued, cop, ce, cret, lastSeen, abs, seen>>
       ELSE IF old.k = "nil" THEN
            \* release mu first, then load
            /\ pc' = [pc EXCEPT ![c] = "L_lock"]
            /\ cret' = [cret EXCEPT ![c] = "U_goc"]
            /\ UNCHANGED <<snap, dirty, queued, cop, ce, lastSeen, abs, seen>>
       ELSE IF old.k = "absent" \/ (op.egen # 0 /\ old.gen # op.egen) THEN
            \* NotFound
            /\ EndOp(c)
            /\ UNCHANGED <<snap, dirty, queued, lastSeen, abs, seen>>
       ELSE
            \* linearization point: snap.Store(&nr) inside mu
            /\ snap' = [snap EXCEPT ![e] = nr]
            /\ dirty' = [dirty EXCEPT ![e] = @ \cup {op.col}]
            /\ queued' = [queued EXCEPT ![e] = TRUE]
            /\ lastSeen' = [lastSeen EXCEPT ![c] = old.gen]
            /\ LP([abs EXCEPT !.val = [abs.val EXCEPT ![op.col] = nv]])
            /\ IF queued[e]
               THEN EndOp(c)
               ELSE /\ pc' = [pc EXCEPT ![c] = "U_enq"]   \* enqueue after releasing mu
                    /\ UNCHANGED <<cop, ce, cret>>
    /\ UNCHANGED <<sysV, mapV, dead, opH, ops, unkGen, cwait,
                   bgV, flV, evV, dbV, netV, budV,
                   absKnown, preCrash, deletedGens, insSent, delSent, violation>>

U_enq(c) ==
    /\ up /\ pc[c] = "U_enq"
    /\ queue' = queue \cup {ce[c]}
    /\ EndOp(c)
    /\ UNCHANGED <<sysV, mapV, entV, ops, lastSeen, unkGen, cwait,
                   bgV, fpc, fbatch, fretry, fitem, evV, dbV, netV, budV, ghV>>

-----------------------------------------------------------------------------
\* client part of Create / Delete (7.4, 7.5): lockOpLive, then hand off to runSync

X_lock(c) ==
    /\ up /\ pc[c] = "X_lock" /\ opH[ce[c]] = FREE
    /\ LET e == ce[c] IN
       IF dead[e]
       THEN \* found dead after acquiring op: release it, look up the map again
            /\ pc' = [pc EXCEPT ![c] = "X_goc"]
            /\ UNCHANGED <<opH, bgV, cwait>>
       ELSE /\ opH' = [opH EXCEPT ![e] = BG(c)]
            /\ bpc' = [bpc EXCEPT ![c] = IF cop[c].t = "create" THEN "B_c1" ELSE "B_d1"]
            /\ be' = [be EXCEPT ![c] = e]
            /\ bop' = [bop EXCEPT ![c] = cop[c]]
            /\ cwait' = [cwait EXCEPT ![c] = TRUE]
            /\ pc' = [pc EXCEPT ![c] = "X_wait"]
            /\ UNCHANGED <<bres, bgr, bamb>>
    /\ UNCHANGED <<sysV, mapV, snap, dirty, queued, dead,
                   cop, ce, cret, ops, lastSeen, unkGen,
                   flV, evV, dbV, netV, budV, ghV>>

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
          /\ UNCHANGED <<unkGen, cto>>
       \/ /\ bpc[c] # "B_done" /\ cto < MaxClientTimeouts
          /\ cto' = cto + 1
          /\ unkGen' = [unkGen EXCEPT ![c] = IF RetryCreate /\ cop[c].t = "create"
                                             THEN cop[c].gen ELSE @]
          /\ UNCHANGED <<bpc, bop, bres, lastSeen>>
    /\ UNCHANGED <<sysV, mapV, entV, ops, be, bgr, bamb,
                   flV, evV, dbV, netV, stmtTO, drops, crashes, nextGen, nextVal, ghV>>

-----------------------------------------------------------------------------
\* runSync background: createLocked + insertUntilSure (7.4)

B_c1(c) ==
    /\ up /\ bpc[c] = "B_c1"
    /\ LET e   == be[c]
           cur == snap[e]
           g   == bop[c].gen
       IN
       IF FixDedup /\ cur.k = "nil" /\ Mut # "CreateNoLoad" THEN
            \* the fix: load first, and use memory to decide whether it already exists
            /\ snap' = [snap EXCEPT ![e] = DbRow]
            /\ Observe(DbProj)
            /\ UNCHANGED <<opH, bgV, net, wait, insSent>>
       ELSE IF cur.k = "row" THEN
            \* already exists: the same gen means a client retry whose earlier attempt already succeeded (idempotent)
            /\ Finish(c, IF cur.gen = g THEN "ok" ELSE "exists")
            /\ UNCHANGED <<snap, net, wait, insSent, abs, absKnown, seen, violation>>
       ELSE
            /\ SendStmt(BG(c), IF FixDedup THEN "insd" ELSE "ins", g, 0, InitVal(g), {})
            /\ insSent' = insSent \cup {g}
            /\ bpc' = [bpc EXCEPT ![c] = "B_c2"]
            /\ UNCHANGED <<snap, opH, be, bop, bres, bgr, bamb,
                           abs, absKnown, seen, violation>>
    /\ UNCHANGED <<sysV, mapV, dirty, queued, dead, cliV, flV, evV, dbV, resp, budV,
                   preCrash, deletedGens, delSent>>

B_c2(c) ==
    /\ up /\ bpc[c] = "B_c2" /\ ~wait[BG(c)]
    /\ resp' = [resp EXCEPT ![BG(c)] = "none"]     \* take the result
    /\ LET e    == be[c]
           g    == bop[c].gen
           r    == resp[BG(c)]
           kind == IF FixDedup THEN "insd" ELSE "ins"
           mine == db.ex /\ db.gen = g
       IN
       IF r = "ok" THEN
            \* INSERT ... RETURNING succeeded
            /\ CreateLP(c, RowR(g, 0, InitVal(g)), AbsAbsent)
            /\ Finish(c, "ok")
            /\ UNCHANGED <<net, wait>>
       ELSE IF r = "amb" /\ Mut = "ReleaseOnAmbiguous" THEN
            \* mutation: return "unknown" and release op; memory keeps saying "absent"
            /\ Finish(c, "unknown")
            /\ UNCHANGED <<snap, dirty, net, wait, abs, absKnown, seen, violation>>
       ELSE IF r = "amb" /\ Mut = "SelectConfirms" /\ mine THEN
            \* mutation: one SELECT sees the row, so it is taken as created
            /\ CreateLP(c, DbRow, DbProj)
            /\ Finish(c, "ok")
            /\ UNCHANGED <<net, wait>>
       ELSE IF r = "amb" /\ Mut = "SelectConfirms" THEN
            \* mutation: one SELECT does not see the row, so it is taken as not created
            /\ Finish(c, "fail")
            /\ UNCHANGED <<snap, dirty, net, wait, abs, absKnown, seen, violation>>
       ELSE IF (r = "dup" \/ r = "claimed") /\ mine THEN
            \* conflict, and SELECT sees its own gen: an earlier "ambiguous" INSERT actually succeeded
            /\ CreateLP(c, DbRow, DbProj)
            /\ Finish(c, "ok")
            /\ UNCHANGED <<net, wait>>
       ELSE IF r = "claimed" THEN
            \* the fix: this gen was inserted long ago and later deleted; the create already took effect back then
            /\ Finish(c, "ok")
            /\ UNCHANGED <<snap, dirty, net, wait, abs, absKnown, seen, violation>>
       ELSE IF r = "dup" /\ db.ex THEN
            \* DB holds a different life: load it into memory along the way
            /\ snap' = [snap EXCEPT ![e] = DbRow]
            /\ Observe(DbProj)
            /\ Finish(c, "exists")
            /\ UNCHANGED <<dirty, net, wait>>
       ELSE IF r = "conflict" THEN
            \* should not happen under the fix: the dedup record was written but the business row was not inserted
            /\ violation' = "DedupConflict"
            /\ Finish(c, "exists")
            /\ UNCHANGED <<snap, dirty, net, wait, abs, absKnown, seen>>
       ELSE IF r = "fail" /\ ~(FixUnsureRetry /\ bamb[c]) THEN
            \* definite failure: memory unchanged (the current design: regardless of whether there were earlier ambiguous attempts)
            /\ Finish(c, "fail")
            /\ UNCHANGED <<snap, dirty, net, wait, abs, absKnown, seen, violation>>
       ELSE
            \* ambiguous outcome; or after a conflict SELECT sees that it does not exist;
            \* or (the fix) there was an earlier ambiguous attempt and this one definitely failed: retry with the same gen
            /\ SendStmt(BG(c), kind, g, 0, InitVal(g), {})
            /\ bamb' = [bamb EXCEPT ![c] = @ \/ r = "amb"]
            /\ UNCHANGED <<snap, dirty, opH, bpc, be, bop, bres, bgr,
                           abs, absKnown, seen, violation>>
    /\ UNCHANGED <<sysV, mapV, queued, dead, cliV, flV, evV, dbV, budV,
                   preCrash, deletedGens, insSent, delSent>>

-----------------------------------------------------------------------------
\* runSync background: deleteLocked + deleteUntilSure (7.5)

B_d1(c) ==
    /\ up /\ bpc[c] = "B_d1"
    /\ LET e  == be[c]
           r  == snap[e]
           eg == bop[c].egen
       IN
       IF r.k = "nil" THEN
            \* ensureLoadedLocked
            /\ snap' = [snap EXCEPT ![e] = DbRow]
            /\ Observe(DbProj)
            /\ UNCHANGED <<opH, bgV, net, wait, delSent>>
       ELSE IF r.k = "absent" \/ (eg # 0 /\ r.gen # eg) THEN
            /\ FinishOp(c, "notfound", Mut # "LeakOpOnNotFound")
            /\ UNCHANGED <<snap, net, wait, delSent, abs, absKnown, seen, violation>>
       ELSE
            \* (1) delete from the DB first: DELETE ... WHERE id AND gen. mu is not held meanwhile, so updates can proceed
            /\ bgr' = [bgr EXCEPT ![c] = r.gen]
            /\ SendStmt(BG(c), "del", r.gen, 0, Val0, {})
            /\ delSent' = delSent \cup {r.gen}
            /\ bpc' = [bpc EXCEPT ![c] = "B_d2"]
            /\ UNCHANGED <<snap, opH, be, bop, bres, bamb, abs, absKnown, seen, violation>>
    /\ UNCHANGED <<sysV, mapV, dirty, queued, dead, cliV, flV, evV, dbV, resp, budV,
                   preCrash, deletedGens, insSent>>

B_d2(c) ==
    /\ up /\ bpc[c] = "B_d2" /\ ~wait[BG(c)]
    /\ resp' = [resp EXCEPT ![BG(c)] = "none"]     \* take the result
    /\ LET e == be[c]
           r == resp[BG(c)]
       IN
       IF r = "ok" \/ (r = "amb" /\ Mut = "SelectConfirms" /\ ~(db.ex /\ db.gen = bgr[c])) THEN
            \* (2) linearization point: replace with a tombstone in the same mu critical section, and discard dirty data
            \* (under the mutation SelectConfirms, also when one SELECT after an ambiguous attempt no longer sees the row)
            /\ snap' = [snap EXCEPT ![e] = ABSENT]
            /\ dirty' = IF Mut = "DeleteKeepsDirty" THEN dirty ELSE [dirty EXCEPT ![e] = {}]
            /\ LP(AbsAbsent)
            /\ deletedGens' = deletedGens \cup {bgr[c]}
            /\ Finish(c, "ok")
            /\ UNCHANGED <<net, wait>>
       ELSE IF r = "amb" /\ Mut \in {"ReleaseOnAmbiguous", "SelectConfirms"} THEN
            \* mutation: give up (ReleaseOnAmbiguous), or one SELECT still sees the row so it is taken as failed (SelectConfirms)
            /\ Finish(c, IF Mut = "ReleaseOnAmbiguous" THEN "unknown" ELSE "fail")
            /\ UNCHANGED <<snap, dirty, abs, seen, deletedGens, net, wait>>
       ELSE IF r = "fail" /\ ~(FixUnsureRetry /\ bamb[c]) THEN
            \* definite failure: the row is still there, dirty is kept (the current design: regardless of whether there were earlier ambiguous attempts)
            /\ Finish(c, "fail")
            /\ UNCHANGED <<snap, dirty, abs, seen, deletedGens, net, wait>>
       ELSE
            \* ambiguous outcome; or (the fix) there was an earlier ambiguous attempt and this one definitely failed: retry with the same gen
            /\ SendStmt(BG(c), "del", bgr[c], 0, Val0, {})
            /\ bamb' = [bamb EXCEPT ![c] = @ \/ r = "amb"]
            /\ UNCHANGED <<snap, dirty, opH, bpc, be, bop, bres, bgr, abs, seen, deletedGens>>
    /\ UNCHANGED <<sysV, mapV, queued, dead, cliV, flV, evV, dbV, budV,
                   absKnown, preCrash, insSent, delSent, violation>>

-----------------------------------------------------------------------------
\* flusher (7.6)

\* take the whole queue
F_take ==
    /\ up /\ fpc = "F_take" /\ queue # {}
    /\ fbatch' = queue
    /\ queue' = {}
    /\ fpc' = "F_scan"
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, fretry, fitem, evV, dbV, netV, budV, ghV>>

\* per entry: tryLockOp, then take a snapshot and clear dirty inside mu
F_scan ==
    /\ up /\ fpc = "F_scan" /\ fbatch # {}
    /\ LET e == CHOOSE x \in fbatch : TRUE IN
       /\ fbatch' = fbatch \ {e}
       /\ IF opH[e] # FREE /\ Mut # "FlushNoOp" THEN
               \* create/delete/load in progress: handle it in the next round; queued stays TRUE
               /\ fretry' = fretry \cup {e}
               /\ UNCHANGED <<opH, dirty, queued, fitem>>
          ELSE IF dead[e] \/ snap[e].k # "row" \/ dirty[e] = {} THEN
               /\ queued' = [queued EXCEPT ![e] = FALSE]
               /\ UNCHANGED <<opH, dirty, fretry, fitem>>
          ELSE
               \* op is held until this batch has been written
               /\ opH' = IF Mut = "FlushNoOp" THEN opH ELSE [opH EXCEPT ![e] = FL]
               /\ fitem' = [e |-> e, r |-> snap[e], m |-> dirty[e]]
               /\ dirty' = [dirty EXCEPT ![e] = {}]
               /\ queued' = [queued EXCEPT ![e] = FALSE]
               /\ UNCHANGED fretry
    /\ UNCHANGED <<sysV, mapV, snap, dead, cliV, bgV, fpc, queue, evV, dbV, netV, budV, ghV>>

\* requeueAll(retry), then send UPDATE ... SET <columns in mask>, ver WHERE id AND gen AND ver < ?
F_send ==
    /\ up /\ fpc = "F_scan" /\ fbatch = {}
    /\ queue' = queue \cup fretry
    /\ fretry' = {}
    /\ IF fitem = NoItem
       THEN /\ fpc' = "F_take"
            /\ UNCHANGED <<net, wait>>
       ELSE /\ fpc' = "F_wait"
            /\ SendStmt(FL, "upd", fitem.r.gen, fitem.r.ver, fitem.r.val, fitem.m)
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, fbatch, fitem, evV, dbV, resp, budV, ghV>>

\* write done: on success release op; on failure or ambiguous outcome, merge the mask back and re-enqueue
F_done ==
    /\ up /\ fpc = "F_wait" /\ ~wait[FL]
    /\ resp' = [resp EXCEPT ![FL] = "none"]     \* take the result
    /\ LET e == fitem.e IN
       /\ opH' = IF Mut = "FlushNoOp" THEN opH ELSE [opH EXCEPT ![e] = FREE]
       /\ IF resp[FL] = "ok"
          THEN UNCHANGED <<dirty, queued, queue>>
          ELSE /\ dirty' = IF Mut = "NoMaskMerge" THEN dirty ELSE [dirty EXCEPT ![e] = @ \cup fitem.m]
               /\ queued' = [queued EXCEPT ![e] = TRUE]
               /\ queue' = IF queued[e] THEN queue ELSE queue \cup {e}
    /\ fitem' = NoItem
    /\ fpc' = "F_take"
    /\ UNCHANGED <<sysV, mapV, snap, dead, cliV, bgV, fbatch, fretry, evV, dbV,
                   net, wait, budV, ghV>>

-----------------------------------------------------------------------------
\* evictor (7.7, dead protocol). Idle TTL and whether capacity is exceeded are matters of time and policy;
\* here it may attempt eviction at any moment, covering all possible timings.

\* tryEvict: tryLockOp, confirm dirty == 0 inside mu, set dead
V_evict ==
    /\ up /\ vpc = "V_pick" /\ mapE # 0 /\ nextEntry <= MaxEntry
    /\ LET e == mapE IN
       /\ opH[e] = FREE /\ ~dead[e] /\ (dirty[e] = {} \/ Mut = "EvictDirty")
       /\ dead' = IF Mut = "EvictNoDead" THEN dead ELSE [dead EXCEPT ![e] = TRUE]
       /\ opH' = [opH EXCEPT ![e] = EV]
       /\ ve' = e
       /\ vpc' = "V_rm"
    /\ UNCHANGED <<sysV, mapV, snap, dirty, queued, cliV, bgV, flV, dbV, netV, budV, ghV>>

\* after releasing mu (the gap), remove it from the map inside shard.mu, then release op.
\* Once nobody holds a pointer to it, clear its snap (only to reduce the state count; nobody can see it again).
V_remove ==
    /\ up /\ vpc = "V_rm"
    /\ mapE' = IF mapE = ve THEN 0 ELSE mapE
    /\ opH' = [opH EXCEPT ![ve] = FREE]
    /\ snap' = IF \A c \in Clients : ce[c] # ve THEN [snap EXCEPT ![ve] = NIL] ELSE snap
    /\ vpc' = "V_pick"
    /\ ve' = 0
    /\ UNCHANGED <<sysV, nextEntry, eEp, dirty, queued, dead,
                   cliV, bgV, flV, dbV, netV, budV, ghV>>

-----------------------------------------------------------------------------
\* DB (PostgreSQL). All writes are conditional writes (I6).

NewRow(s) == [ex |-> TRUE, gen |-> s.gen, ver |-> 0, val |-> s.val]

\* new DB state after a statement executes
ExecDb(s) ==
    CASE s.kind = "ins"  -> IF db.ex THEN db ELSE NewRow(s)          \* ON CONFLICT DO NOTHING
      [] s.kind = "insd" -> IF s.gen \notin dedup /\ ~db.ex THEN NewRow(s) ELSE db
      [] s.kind = "upd"  -> IF db.ex /\ (db.gen = s.gen \/ Mut = "NoGenCheck")
                                  /\ (db.ver < s.ver \/ Mut = "NoVerCheck")
                            THEN [db EXCEPT !.ver = s.ver,
                                            !.val = [col \in Cols |->
                                                       IF col \in s.mask THEN s.val[col]
                                                       ELSE db.val[col]]]
                            ELSE IF ~db.ex /\ Mut = "FlushUpsert"
                            THEN [ex |-> TRUE, gen |-> s.gen, ver |-> s.ver, val |-> s.val]
                            ELSE db
      [] s.kind = "del"  -> IF db.ex /\ (db.gen = s.gen \/ Mut = "NoGenCheck") THEN DbAbsent ELSE db

\* result of executing a statement (an UPDATE/DELETE affecting 0 rows also counts as success)
ExecResult(s) ==
    CASE s.kind = "ins"  -> IF db.ex THEN "dup" ELSE "ok"
      [] s.kind = "insd" -> IF s.gen \in dedup THEN "claimed"
                            ELSE IF db.ex THEN "conflict" ELSE "ok"
      [] OTHER           -> "ok"

\* execute one statement (possibly a late execution). If the sender is still waiting, hand it the result; the response may also be lost.
DbExec ==
    /\ \E s \in net :
         /\ db' = ExecDb(s)
         /\ dedup' = CASE s.kind = "insd" -> dedup \cup {s.gen}
                       [] s.kind = "del" /\ Mut = "DedupDeletedWithRow" /\ db.ex /\ db.gen = s.gen
                                         -> dedup \ {s.gen}
                       [] OTHER -> dedup
         /\ net' = net \ {s}
         /\ IF s.live
            THEN /\ wait' = [wait EXCEPT ![s.src] = FALSE]
                 /\ \/ /\ resp' = [resp EXCEPT ![s.src] = ExecResult(s)]
                       /\ UNCHANGED stmtTO
                    \/ /\ stmtTO < MaxStmtTimeouts       \* executed, but the response was lost
                       /\ stmtTO' = stmtTO + 1
                       /\ resp' = [resp EXCEPT ![s.src] = "amb"]
            ELSE UNCHANGED <<resp, wait, stmtTO>>
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, flV, evV,
                   drops, cto, crashes, nextGen, nextVal, ghV>>

\* Go-side ctx timeout: the sender stops waiting and treats it as an ambiguous outcome; the statement is still in flight and may execute later
DbTimeout ==
    /\ stmtTO < MaxStmtTimeouts
    /\ \E s \in net :
         /\ s.live
         /\ s.kind \in {"ins", "insd"} => InsertCanBeLate
         /\ net' = (net \ {s}) \cup {[s EXCEPT !.live = FALSE]}
         /\ wait' = [wait EXCEPT ![s.src] = FALSE]
         /\ resp' = [resp EXCEPT ![s.src] = "amb"]
    /\ stmtTO' = stmtTO + 1
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, flV, evV, dbV,
                   drops, cto, crashes, nextGen, nextVal, ghV>>

\* statement definitely rejected (statement_timeout, connection failure, etc.; not executed), or never reaches the DB
DbDrop ==
    /\ drops < MaxDrops
    /\ \E s \in net :
         /\ net' = net \ {s}
         /\ IF s.live
            THEN /\ wait' = [wait EXCEPT ![s.src] = FALSE]
                 /\ resp' = [resp EXCEPT ![s.src] = "fail"]
            ELSE UNCHANGED <<resp, wait>>
    /\ drops' = drops + 1
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, flV, evV, dbV,
                   stmtTO, cto, crashes, nextGen, nextVal, ghV>>

-----------------------------------------------------------------------------
\* crash and restart (7.8)

\* process crash: all memory is lost and all goroutines vanish; statements already sent are still in flight, but nobody is waiting for them anymore.
\* a client with a create in progress sees the connection drop; the outcome is ambiguous.
Crash ==
    /\ up /\ crashes < MaxCrashes /\ nextEntry <= MaxEntry
    /\ up' = FALSE
    /\ epoch' = epoch + 1
    /\ crashes' = crashes + 1
    /\ mapE' = 0
    /\ eEp' = [e \in Entries |-> 0]
    /\ snap' = [e \in Entries |-> NIL]
    /\ dirty' = [e \in Entries |-> {}]
    /\ queued' = [e \in Entries |-> FALSE]
    /\ dead' = [e \in Entries |-> FALSE]
    /\ opH' = [e \in Entries |-> FREE]
    /\ pc' = [c \in Clients |-> "idle"]
    /\ cop' = [c \in Clients |-> NoOp]
    /\ ce' = [c \in Clients |-> 0]
    /\ cret' = [c \in Clients |-> "none"]
    /\ cwait' = [c \in Clients |-> FALSE]
    /\ unkGen' = [c \in Clients |->
                    IF RetryCreate /\ pc[c] # "idle" /\ cop[c].t = "create"
                    THEN cop[c].gen ELSE unkGen[c]]
    /\ bpc' = [c \in Clients |-> "idle"]
    /\ be' = [c \in Clients |-> 0]
    /\ bop' = [c \in Clients |-> NoOp]
    /\ bres' = [c \in Clients |-> "none"]
    /\ bgr' = [c \in Clients |-> 0]
    /\ bamb' = [c \in Clients |-> FALSE]
    /\ fpc' = "F_take" /\ fbatch' = {} /\ fretry' = {} /\ fitem' = NoItem /\ queue' = {}
    /\ vpc' = "V_pick" /\ ve' = 0
    /\ net' = {[s EXCEPT !.live = FALSE] : s \in net}
    /\ wait' = [p \in DBProcs |-> FALSE]
    /\ resp' = [p \in DBProcs |-> "none"]
    /\ preCrash' = IF absKnown THEN abs ELSE preCrash
    /\ absKnown' = FALSE
    /\ seen' = [c \in Clients |-> {}]
    /\ UNCHANGED <<nextEntry, ops, lastSeen, dbV,
                   stmtTO, drops, cto, nextGen, nextVal,
                   abs, deletedGens, insSent, delSent, violation>>

\* the fix: on restart, pg_terminate_backend the previous process's sessions. With the sessions gone,
\* statements sent by the previous process and not yet executed will never execute.
FenceDrop ==
    /\ FixFence /\ ~up
    /\ \E s \in net : s.ep < epoch /\ net' = net \ {s}
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, flV, evV, dbV, wait, resp, budV, ghV>>

Restart ==
    /\ ~up
    /\ FixFence => ~(\E s \in net : s.ep < epoch)
    /\ up' = TRUE
    /\ UNCHANGED <<epoch, mapV, entV, cliV, bgV, flV, evV, dbV, netV, budV, ghV>>

-----------------------------------------------------------------------------
\* all client operations are done and background tasks are idle: allowed to stop here (not a deadlock)
Done ==
    /\ up
    /\ \A c \in Clients : pc[c] = "idle" /\ ops[c] = 0 /\ bpc[c] = "idle"
    /\ fpc = "F_take" /\ queue = {}
    /\ UNCHANGED vars

Next ==
    \/ \E c \in Clients :
         \/ ClientStart(c) \/ Goc(c) \/ G_read(c) \/ L_lock(c) \/ L_load(c)
         \/ U_mu(c) \/ U_enq(c) \/ X_lock(c) \/ X_wait(c)
         \/ B_c1(c) \/ B_c2(c) \/ B_d1(c) \/ B_d2(c)
    \/ F_take \/ F_scan \/ F_send \/ F_done
    \/ V_evict \/ V_remove
    \/ DbExec \/ DbTimeout \/ DbDrop
    \/ Crash \/ FenceDrop \/ Restart
    \/ Done

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* invariants

\* assertions inside actions: Get linearizability, post-crash state, dedup table conflict
NoViolation == violation = "none"

\* intolerable (1): a life that has been deleted (the delete's linearization point has occurred) must not reappear in the DB or in memory
NoResurrection ==
    /\ db.ex => db.gen \notin deletedGens
    /\ (up /\ mapE # 0 /\ snap[mapE].k = "row") => snap[mapE].gen \notin deletedGens

\* loaded entry in the map
LiveLoaded == up /\ mapE # 0 /\ ~dead[mapE] /\ snap[mapE].k # "nil"

\* memory (the loaded entry in the map) agrees with the abstract state: all linearization points line up
MemMatchesAbs == (LiveLoaded /\ absKnown) => Proj(snap[mapE]) = abs

\* corollary of I4: when the entry is clean, nobody holds op, and no statement is in flight, the DB and memory are identical.
\* A violation means an old value overwrote a new one, or memory and the DB diverged permanently (intolerable (2)(3)).
QuiescentConsistent ==
    (LiveLoaded /\ dirty[mapE] = {} /\ opH[mapE] = FREE /\ net = {}) => DbRow = snap[mapE]

\* when memory does not have this row (or it is not loaded yet) and everything has stopped, the DB must equal the abstract state
AllIdle ==
    /\ net = {}
    /\ \A c \in Clients : bpc[c] \in {"idle", "B_done"}
    /\ fitem = NoItem
DbMatchesAbsWhenIdle ==
    (up /\ absKnown /\ AllIdle /\ (mapE = 0 \/ snap[mapE].k = "nil" \/ dirty[mapE] = {}))
        => DbProj = abs

\* intolerable (4): a confirmed create does not disappear (unless someone sent its DELETE)
CreateDurable ==
    (absKnown /\ abs.ex) => ((db.ex /\ db.gen = abs.gen) \/ abs.gen \in delSent)

\* I5: for the same key, at most one write SQL is awaiting its result at any moment
OneWriterPerKey == Cardinality({p \in DBProcs : wait[p]}) <= 1

\* a dirty entry is always in the flush pipeline and is never forgotten
DirtyImpliesQueued == (up /\ mapE # 0 /\ dirty[mapE] # {}) => queued[mapE]
QueuedIsTracked ==
    (up /\ mapE # 0 /\ queued[mapE]) =>
        \/ mapE \in queue \cup fbatch \cup fretry
        \/ \E c \in Clients : pc[c] = "U_enq" /\ ce[c] = mapE

\* I1: in the current process, an entry that is not dead must be in the map
LiveEntryInMap ==
    \A e \in 1..(nextEntry - 1) : (up /\ eEp[e] = epoch /\ ~dead[e]) => mapE = e

\* I10: an entry being evicted (dead) never holds dirty data, and nothing can make it dirty afterwards
NoDirtyEviction == \A e \in Entries : dead[e] => dirty[e] = {}

\* every column value in the DB was written by the life that currently owns the row
\* (a violation means "data of an old object written into a new object", 8.2 example 2)
DbValsBelongToGen == db.ex => \A col \in Cols : db.val[col][1] = db.gen

\* the DB is never newer than memory for the life memory holds (a violation means a write memory never made reached the DB)
DbNotAhead ==
    (LiveLoaded /\ snap[mapE].k = "row" /\ db.ex /\ db.gen = snap[mapE].gen) => db.ver <= snap[mapE].ver

\* I4 in its strongest form: whenever nobody holds op and the entry is clean, memory and the DB are identical,
\* even while late statements are still in flight (they must all be harmless by then).
\* While a flush is in progress the entry is already clean but op is held, which is why op appears here.
CleanIsDurable ==
    (LiveLoaded /\ dirty[mapE] = {} /\ opH[mapE] = FREE) => DbRow = snap[mapE]

\* I15: once the service is up, no statement from a previous process is still in flight
FencedRestart == (FixFence /\ up) => \A s \in net : s.ep = epoch

\* action property: within one life, the DB ver never goes back, and a value never changes without a larger ver
\* (a violation means a stale UPDATE overwrote a newer one, 8.2 example 1)
DbVerMonotonic ==
    [][(db.ex /\ db'.ex /\ db'.gen = db.gen) =>
         /\ db'.ver >= db.ver
         /\ (db'.val # db.val => db'.ver > db.ver)]_db

-----------------------------------------------------------------------------
\* liveness (correctness-properties.md section 5)
\*
\* Fairness: every step of an operation that has started, of the flusher, of an eviction already under way, of DB execution,
\* and of a restart eventually happens if it stays enabled. Faults (DbTimeout, DbDrop, Crash, a client giving up) and the
\* choice of new operations and of starting an eviction get no fairness: they may or may not happen. All faults are bounded
\* by the budgets above, which models "the DB is not unavailable forever".

Fairness ==
    /\ \A c \in Clients :
         /\ WF_vars(Goc(c)) /\ WF_vars(G_read(c)) /\ WF_vars(L_lock(c)) /\ WF_vars(L_load(c))
         /\ WF_vars(U_mu(c)) /\ WF_vars(U_enq(c)) /\ WF_vars(X_lock(c)) /\ WF_vars(X_wait(c))
         /\ WF_vars(B_c1(c)) /\ WF_vars(B_c2(c)) /\ WF_vars(B_d1(c)) /\ WF_vars(B_d2(c))
    /\ WF_vars(F_take) /\ WF_vars(F_scan) /\ WF_vars(F_send) /\ WF_vars(F_done)
    /\ WF_vars(V_remove)
    /\ WF_vars(DbExec) /\ WF_vars(FenceDrop) /\ WF_vars(Restart)

FairSpec == Spec /\ Fairness

\* L1: every request a client has issued eventually gets an answer (success, failure, or "unknown")
EventuallyResponds == \A c \in Clients : pc[c] # "idle" ~> pc[c] = "idle"

\* L2: every create/delete running in the background eventually reaches a definite outcome
BackgroundSettles ==
    \A c \in Clients : bpc[c] \in {"B_c1", "B_c2", "B_d1", "B_d2"} ~> bpc[c] \in {"idle", "B_done"}

\* L3: dirty data is eventually flushed (or lost in a crash, which the requirements allow)
EventuallyFlushed == \A e \in Entries : dirty[e] # {} ~> dirty[e] = {}

\* L4: op is never held forever (no lock leak; an entry can always become evictable again)
NoLockLeak == \A e \in Entries : opH[e] # FREE ~> opH[e] = FREE

-----------------------------------------------------------------------------
\* reachability witnesses (correctness-properties.md section 7). Each W_x claims that a situation never occurs;
\* TLC MUST report each one as violated. If one holds, the model never reaches that situation, and every property
\* that relies on it passes vacuously.

W_Delete               == deletedGens = {}
W_Recreate             == ~(abs.ex /\ deletedGens # {})
W_Evict                == \A e \in Entries : ~dead[e]
W_Reload               == nextEntry <= 2
W_Crash                == crashes = 0
W_CrashWithStmtInFlight == ~(~up /\ net # {})
W_LateStmt             == \A s \in net : s.live
W_EffectiveLateStmt    == ~(\E s \in net : ~s.live /\ ExecDb(s) # db)
W_LateInsertOfDeletedGen == ~(\E s \in net : s.kind \in {"ins", "insd"} /\ s.gen \in deletedGens)
W_OldUpdateAtRestart   == ~(~up /\ \E s \in net : s.ep < epoch /\ s.kind = "upd")
W_RetryOfDeletedGen    == ~(\E c \in Clients : cop[c].t = "create" /\ cop[c].gen \in deletedGens)
W_DedupClaimed         == ~(\E c \in Clients : resp[BG(c)] = "claimed")
W_AmbThenNotExecuted   == ~(\E c \in Clients : bamb[c] /\ ~wait[BG(c)] /\ resp[BG(c)] = "fail")
W_FlushFailed          == ~(fpc = "F_wait" /\ ~wait[FL] /\ resp[FL] \in {"amb", "fail"})
W_UpdateDuringFlush    == ~(fitem # NoItem /\ dirty[fitem.e] # {})
W_UpdateDuringDelete   == ~(\E c \in Clients : bpc[c] = "B_d2" /\ dirty[be[c]] # {})
W_ClientGaveUp         == cto = 0

-----------------------------------------------------------------------------
\* state constraint used only by hole3.cfg: old statements still in flight at crash time may only be UPDATEs,
\* so that TLC produces the UPDATE version of the counterexample described in design.md 13.2
\* (without the constraint, TLC first finds the shorter INSERT version; see hole3_insert.cfg).
OldStmtsAreUpdates == \A s \in net : s.ep < epoch => s.kind = "upd"

=============================================================================
