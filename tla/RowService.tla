----------------------------- MODULE RowService -----------------------------
\* design.md 中"内存权威行数据服务"核心并发协议的 TLA+ 模型。
\*
\* 只建模一个 key：设计中所有的正确性问题都是 per-key 的。建模了：
\*   - shard map 中 entry 的分配和淘汰（dead 协议，7.7）
\*   - entry 的 snap / dirty / queued / dead / op 锁（5.2、6）
\*   - Get、Update、Create、Delete，以及 runSync 在客户端放弃之后继续执行（7.2～7.5）
\*   - 刷盘器：op 一直持有到写完，失败时并回掩码、重新入队（7.6）
\*   - 会"迟到执行"的 DB：Go 侧超时之后语句仍可能执行；响应可能丢失；
\*     语句可能被确定拒绝（8.1、8.4）
\*   - 客户端放弃等待；SDK 用同一个 gen 重试 create（7.10）
\*   - 服务崩溃与重启（7.8）
\*
\* 建模粒度：mu、shard.mu、qmu 的临界区不做 IO、不阻塞，每个临界区建模成一个
\* 原子步骤；"放锁之后、再次拿锁之前"的空档（第 10 节）拆成单独的步骤，
\* 由 TLC 穷举其中所有可能的交错。
\*
\* 正确性的判断标准：维护一个抽象的顺序状态 abs。每个操作在第 9 节给出的
\* 线性化点上修改 abs；内存、DB 都必须和 abs 对得上（见文件末尾的不变式）。
\*
\* Update 只建模"给一列赋一个新值"。自增（INCR）在 mu 里基于旧值计算新值，
\* 和赋值走的是同一条路径、同一个原子步骤，并发结构完全一样，所以没有单独建模。
\*
\* 为了控制状态数，局部变量在用完之后都恢复成固定的初始值（操作结束时清空
\* cop/ce、后台结束时清空 be/bop、结果被取走后清空 resp 等）。这不改变行为。

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Clients,            \* 客户端；每个客户端还有一个自己的 runSync 后台 goroutine
    Cols,               \* 业务列
    MaxEntry,           \* 最多分配几个 entry（淘汰、崩溃之后要分配新的）
    MaxGen,             \* 最多几次新的 create（每次 create 一个新 gen）
    MaxVal,             \* 最多几次 update（每次 update 写一个新值）
    MaxOps,             \* 每个客户端最多发起几个操作
    MaxStmtTimeouts,    \* DB 语句"结果不确定"的次数上限（Go 侧超时、响应丢失）
    MaxDrops,           \* DB 语句被确定拒绝、或者永远不执行的次数上限
    MaxClientTimeouts,  \* 客户端在 create/delete 结果确定之前放弃等待的次数上限
    MaxCrashes,         \* 服务崩溃的次数上限
    InsertCanBeLate,    \* INSERT 在 Go 侧超时之后，是否还可能迟到执行（漏洞 1 的前提）
    RetryCreate,        \* 结果不确定的 create，SDK 是否用同一个 gen 重试（漏洞 2 的前提）
    FixDedup,           \* 修复漏洞 1、2：gen 去重表 + create 先加载
    FixFence,           \* 修复漏洞 3：重启时先清掉旧进程留在 PG 上的会话
    FixUnsureRetry      \* 修复漏洞 4：出现过不确定的尝试之后，确定失败不能当作最终结果

ASSUME MaxEntry >= 1

VARIABLES
    \* 服务是否在运行；每次崩溃 epoch + 1
    up, epoch,
    \* shard map：mapE 是 map 中这个 key 对应的 entry（0 表示没有）
    mapE, nextEntry, eEp,
    \* entry 的字段（下标是 entry 编号）
    snap, dirty, queued, dead, opH,
    \* 客户端
    pc, cop, ce, cret, ops, lastSeen, unkGen, cwait,
    \* 每个客户端的 runSync 后台 goroutine
    bpc, be, bop, bres, bgr, bamb,
    \* 刷盘器和刷盘队列
    fpc, fbatch, fretry, fitem, queue,
    \* 淘汰器
    vpc, ve,
    \* DB：这一行在 DB 中的状态，以及（修复方案的）去重表
    db, dedup,
    \* 已经发出、还没执行的语句；每个执行者是否在等结果、等到的结果
    net, wait, resp,
    \* 各种次数上限的计数
    stmtTO, drops, cto, crashes, nextGen, nextVal,
    \* 用于检查正确性的幽灵变量
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
\* 值和记录

Entries == 1..MaxEntry

\* 列的值是 <<gen, n>>：带上写入它的那条生命的 gen，用来发现"旧对象的数据写进新对象"。
Val0       == [col \in Cols |-> <<0, 0>>]
InitVal(g) == [col \in Cols |-> <<g, 0>>]

\* entry.snap：nil = 未加载；absent = 确定不存在；row = 当前行
NIL    == [k |-> "nil",    gen |-> 0, ver |-> 0, val |-> Val0]
ABSENT == [k |-> "absent", gen |-> 0, ver |-> 0, val |-> Val0]
RowR(g, v, vl) == [k |-> "row", gen |-> g, ver |-> v, val |-> vl]

\* 抽象状态只关心：存在与否、gen、各列的值（ver 是实现细节）
Proj(r)   == [ex |-> r.k = "row", gen |-> r.gen, val |-> r.val]
AbsAbsent == Proj(ABSENT)

DbAbsent == [ex |-> FALSE, gen |-> 0, ver |-> 0, val |-> Val0]
DbRow    == IF db.ex THEN RowR(db.gen, db.ver, db.val) ELSE ABSENT
DbProj   == Proj(DbRow)

\* op 锁的持有者
FREE  == <<"free">>
CL(c) == <<"cl", c>>     \* 客户端自己（load）
BG(c) == <<"bg", c>>     \* 客户端的 runSync 后台 goroutine（create/delete）
FL    == <<"fl">>        \* 刷盘器
EV    == <<"ev">>        \* 淘汰器
DBProcs == {BG(c) : c \in Clients} \cup {FL}   \* 会向 DB 发写语句的执行者

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
\* 辅助定义

\* getOrCreate（7.1）：两次查 map 的写法等价于一个原子步骤。
GetOrCreate(c) ==
    IF mapE # 0
    THEN /\ ce' = [ce EXCEPT ![c] = mapE]
         /\ UNCHANGED <<mapE, nextEntry, eEp>>
    ELSE /\ nextEntry <= MaxEntry
         /\ mapE' = nextEntry
         /\ nextEntry' = nextEntry + 1
         /\ eEp' = [eEp EXCEPT ![nextEntry] = epoch]
         /\ ce' = [ce EXCEPT ![c] = nextEntry]

\* 客户端的操作结束：局部变量恢复初始值
EndOp(c) ==
    /\ pc' = [pc EXCEPT ![c] = "idle"]
    /\ cop' = [cop EXCEPT ![c] = NoOp]
    /\ ce' = [ce EXCEPT ![c] = 0]
    /\ cret' = [cret EXCEPT ![c] = "none"]

\* 正在进行 Get 的客户端，记录它开始以来抽象状态出现过的所有值；
\* Get 读到的值必须是其中之一（读操作可以线性化在它区间内的任意时刻）。
InGet(c) == cop[c].t = "get" /\ pc[c] # "idle"
NewSeen(newAbs) == [c \in Clients |-> IF InGet(c) THEN seen[c] \cup {newAbs} ELSE seen[c]]

\* 线性化点：抽象状态变为 newAbs
LP(newAbs) == abs' = newAbs /\ seen' = NewSeen(newAbs)

\* 崩溃重启之后，第一次从 DB 观察到的状态是否说得通：
\*   - 已经确认删除的生命不能出现
\*   - 出现的生命一定有人发过它的 INSERT
\*   - 各列的值都属于这条生命
\*   - 崩溃前存在的生命如果不见了，一定有人发过它的 DELETE
\* 刷盘窗口内的 update 可以丢（值可以是旧的），这是需求允许的。
Plausible(obs) ==
    /\ obs.ex => obs.gen \notin deletedGens
    /\ obs.ex => obs.gen \in insSent
    /\ obs.ex => \A col \in Cols : obs.val[col][1] = obs.gen
    /\ (preCrash.ex /\ ~(obs.ex /\ obs.gen = preCrash.gen)) => preCrash.gen \in delSent

\* 从 DB 加载到内存时调用。崩溃之后的第一次加载，用加载到的状态重建 abs。
Observe(obs) ==
    IF absKnown
    THEN UNCHANGED <<abs, absKnown, seen, violation>>
    ELSE /\ abs' = obs
         /\ absKnown' = TRUE
         /\ seen' = NewSeen(obs)
         /\ violation' = IF Plausible(obs) THEN violation ELSE "PostCrashStateImplausible"

\* 执行者 p 向 DB 发一条写语句，然后等结果。live = 发送者还在等这条语句的结果。
\* （语句没有编号：同一时刻每个执行者最多只有一条 live 的语句。）
SendStmt(p, kind, g, ver, vl, m) ==
    /\ net' = net \cup {[kind |-> kind, gen |-> g, ver |-> ver, val |-> vl, mask |-> m,
                         src |-> p, ep |-> epoch, live |-> TRUE]}
    /\ wait' = [wait EXCEPT ![p] = TRUE]

\* runSync 的 fn 结束：释放 op；如果客户端还在等，就把结果交给它
Finish(c, res) ==
    /\ opH' = [opH EXCEPT ![be[c]] = FREE]
    /\ bpc' = [bpc EXCEPT ![c] = IF cwait[c] THEN "B_done" ELSE "idle"]
    /\ bres' = [bres EXCEPT ![c] = IF cwait[c] THEN res ELSE "none"]
    /\ bop' = [bop EXCEPT ![c] = IF cwait[c] THEN @ ELSE NoOp]
    /\ be' = [be EXCEPT ![c] = 0]
    /\ bgr' = [bgr EXCEPT ![c] = 0]
    /\ bamb' = [bamb EXCEPT ![c] = FALSE]

\* Create 的线性化点（I7）：DB 确定成功之后，在 mu 里 snap.Store(row)
CreateLP(c, row, obsBefore) ==
    /\ snap' = [snap EXCEPT ![be[c]] = row]
    /\ dirty' = [dirty EXCEPT ![be[c]] = {}]
    /\ LP(Proj(row))
    /\ absKnown' = TRUE
    /\ violation' = IF absKnown \/ Plausible(obsBefore) THEN violation
                    ELSE "PostCrashStateImplausible"

-----------------------------------------------------------------------------
\* 客户端：选择下一个操作

ClientStart(c) ==
    /\ up /\ pc[c] = "idle" /\ ops[c] > 0
    /\ ops' = [ops EXCEPT ![c] = @ - 1]
    /\ \/ \* Get
          /\ cop' = [cop EXCEPT ![c] = [NoOp EXCEPT !.t = "get"]]
          /\ pc' = [pc EXCEPT ![c] = "G_goc"]
          /\ seen' = [seen EXCEPT ![c] = IF absKnown THEN {abs} ELSE {}]
          /\ UNCHANGED <<unkGen, nextGen, nextVal>>
       \/ \* Update：给某一列写一个新值；可以带上之前看到的 gen（7.3）
          \E col \in Cols, eg \in {0, lastSeen[c]} :
             /\ nextVal < MaxVal
             /\ nextVal' = nextVal + 1
             /\ cop' = [cop EXCEPT ![c] = [t |-> "upd", col |-> col, egen |-> eg,
                                           gen |-> 0, v |-> nextVal + 1]]
             /\ pc' = [pc EXCEPT ![c] = "U_goc"]
             /\ UNCHANGED <<unkGen, nextGen, seen>>
       \/ \* Create：SDK 生成一个新的 gen
          /\ bpc[c] = "idle" /\ nextGen < MaxGen
          /\ nextGen' = nextGen + 1
          /\ cop' = [cop EXCEPT ![c] = [NoOp EXCEPT !.t = "create", !.gen = nextGen + 1]]
          /\ pc' = [pc EXCEPT ![c] = "X_goc"]
          /\ UNCHANGED <<unkGen, nextVal, seen>>
       \/ \* SDK 重试一个结果不确定的 create（同一个 gen）
          /\ RetryCreate /\ bpc[c] = "idle" /\ unkGen[c] # 0
          /\ cop' = [cop EXCEPT ![c] = [NoOp EXCEPT !.t = "create", !.gen = unkGen[c]]]
          /\ unkGen' = [unkGen EXCEPT ![c] = 0]
          /\ pc' = [pc EXCEPT ![c] = "X_goc"]
          /\ UNCHANGED <<nextGen, nextVal, seen>>
       \/ \* Delete：可以带上之前看到的 gen
          \E eg \in {0, lastSeen[c]} :
             /\ bpc[c] = "idle"
             /\ cop' = [cop EXCEPT ![c] = [NoOp EXCEPT !.t = "delete", !.egen = eg]]
             /\ pc' = [pc EXCEPT ![c] = "X_goc"]
             /\ UNCHANGED <<unkGen, nextGen, nextVal, seen>>
    /\ UNCHANGED <<sysV, mapV, entV, ce, cret, lastSeen, cwait, bgV, flV, evV, dbV, netV,
                   stmtTO, drops, cto, crashes,
                   abs, absKnown, preCrash, deletedGens, insSent, delSent, violation>>

\* 各操作的第一步：getOrCreate
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
\* Get（7.2）：无锁读 snap；未加载时去 load，然后从头再来

G_read(c) ==
    /\ up /\ pc[c] = "G_read"
    /\ LET r == snap[ce[c]] IN
       IF r.k = "nil"
       THEN /\ pc' = [pc EXCEPT ![c] = "L_lock"]
            /\ cret' = [cret EXCEPT ![c] = "G_goc"]
            /\ UNCHANGED <<cop, ce, lastSeen, violation, seen>>
       ELSE \* 线性化点：snap.Load()。不检查 dead（dead 协议规则 3）
            /\ EndOp(c)
            /\ lastSeen' = [lastSeen EXCEPT ![c] = IF r.k = "row" THEN r.gen ELSE @]
            /\ violation' = IF seen[c] # {} /\ Proj(r) \notin seen[c]
                            THEN "GetNotLinearizable" ELSE violation
            /\ seen' = [seen EXCEPT ![c] = {}]
    /\ UNCHANGED <<sysV, mapV, entV, ops, unkGen, cwait, bgV, flV, evV, dbV, netV, budV,
                   abs, absKnown, preCrash, deletedGens, insSent, delSent>>

\* load：排队拿 op（相当于 singleflight）
L_lock(c) ==
    /\ up /\ pc[c] = "L_lock" /\ opH[ce[c]] = FREE
    /\ opH' = [opH EXCEPT ![ce[c]] = CL(c)]
    /\ pc' = [pc EXCEPT ![c] = "L_load"]
    /\ UNCHANGED <<sysV, mapV, snap, dirty, queued, dead,
                   cop, ce, cret, ops, lastSeen, unkGen, cwait,
                   bgV, flV, evV, dbV, netV, budV, ghV>>

\* load：检查 dead、二次检查 snap，然后 SELECT 并写入 snap
L_load(c) ==
    /\ up /\ pc[c] = "L_load"
    /\ LET e == ce[c] IN
       /\ opH' = [opH EXCEPT ![e] = FREE]
       /\ IF dead[e] \/ snap[e].k # "nil"
          THEN UNCHANGED <<snap, abs, absKnown, seen, violation>>
          ELSE /\ snap' = [snap EXCEPT ![e] = DbRow]
               /\ Observe(DbProj)
    /\ pc' = [pc EXCEPT ![c] = cret[c]]
    /\ UNCHANGED <<sysV, mapV, dirty, queued, dead,
                   cop, ce, cret, ops, lastSeen, unkGen, cwait,
                   bgV, flV, evV, dbV, netV, budV, preCrash, deletedGens, insSent, delSent>>

-----------------------------------------------------------------------------
\* Update（7.3）：只拿 mu，读-改-写完整地在一个临界区里

U_mu(c) ==
    /\ up /\ pc[c] = "U_mu"
    /\ LET e   == ce[c]
           old == snap[e]
           op  == cop[c]
           nv  == <<old.gen, op.v>>
           nr  == [old EXCEPT !.ver = old.ver + 1, !.val = [old.val EXCEPT ![op.col] = nv]]
       IN
       IF dead[e] THEN
            \* 淘汰正在摘除它：重新查 map
            /\ pc' = [pc EXCEPT ![c] = "U_goc"]
            /\ UNCHANGED <<snap, dirty, queued, cop, ce, cret, lastSeen, abs, seen>>
       ELSE IF old.k = "nil" THEN
            \* 先放掉 mu，再去 load
            /\ pc' = [pc EXCEPT ![c] = "L_lock"]
            /\ cret' = [cret EXCEPT ![c] = "U_goc"]
            /\ UNCHANGED <<snap, dirty, queued, cop, ce, lastSeen, abs, seen>>
       ELSE IF old.k = "absent" \/ (op.egen # 0 /\ old.gen # op.egen) THEN
            \* NotFound
            /\ EndOp(c)
            /\ UNCHANGED <<snap, dirty, queued, lastSeen, abs, seen>>
       ELSE
            \* 线性化点：mu 里的 snap.Store(&nr)
            /\ snap' = [snap EXCEPT ![e] = nr]
            /\ dirty' = [dirty EXCEPT ![e] = @ \cup {op.col}]
            /\ queued' = [queued EXCEPT ![e] = TRUE]
            /\ lastSeen' = [lastSeen EXCEPT ![c] = old.gen]
            /\ LP([abs EXCEPT !.val = [abs.val EXCEPT ![op.col] = nv]])
            /\ IF queued[e]
               THEN EndOp(c)
               ELSE /\ pc' = [pc EXCEPT ![c] = "U_enq"]   \* 放掉 mu 之后再入队
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
\* Create / Delete 的客户端部分（7.4、7.5）：lockOpLive，然后交给 runSync

X_lock(c) ==
    /\ up /\ pc[c] = "X_lock" /\ opH[ce[c]] = FREE
    /\ LET e == ce[c] IN
       IF dead[e]
       THEN \* 拿到 op 之后发现 dead：放掉，重新查 map
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

\* 等后台的结果；或者客户端的 ctx 先到期（ErrOutcomeUnknown），后台继续执行
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
\* runSync 后台：createLocked + insertUntilSure（7.4）

B_c1(c) ==
    /\ up /\ bpc[c] = "B_c1"
    /\ LET e   == be[c]
           cur == snap[e]
           g   == bop[c].gen
       IN
       IF FixDedup /\ cur.k = "nil" THEN
            \* 修复方案：先加载，用内存判断是否已经存在
            /\ snap' = [snap EXCEPT ![e] = DbRow]
            /\ Observe(DbProj)
            /\ UNCHANGED <<opH, bgV, net, wait, insSent>>
       ELSE IF cur.k = "row" THEN
            \* 已经存在：同一个 gen 说明是客户端重试、之前那次已经成功（幂等）
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
    /\ resp' = [resp EXCEPT ![BG(c)] = "none"]     \* 取走结果
    /\ LET e    == be[c]
           g    == bop[c].gen
           r    == resp[BG(c)]
           kind == IF FixDedup THEN "insd" ELSE "ins"
           mine == db.ex /\ db.gen = g
       IN
       IF r = "ok" THEN
            \* INSERT ... RETURNING 成功
            /\ CreateLP(c, RowR(g, 0, InitVal(g)), AbsAbsent)
            /\ Finish(c, "ok")
            /\ UNCHANGED <<net, wait>>
       ELSE IF (r = "dup" \/ r = "claimed") /\ mine THEN
            \* 冲突，SELECT 看到的是自己的 gen：之前某次"不确定"的 INSERT 其实成功了
            /\ CreateLP(c, DbRow, DbProj)
            /\ Finish(c, "ok")
            /\ UNCHANGED <<net, wait>>
       ELSE IF r = "claimed" THEN
            \* 修复方案：这个 gen 早就插入过、之后又被删了，create 当时已经生效
            /\ Finish(c, "ok")
            /\ UNCHANGED <<snap, dirty, net, wait, abs, absKnown, seen, violation>>
       ELSE IF r = "dup" /\ db.ex THEN
            \* DB 里是别的生命：顺便加载进内存
            /\ snap' = [snap EXCEPT ![e] = DbRow]
            /\ Observe(DbProj)
            /\ Finish(c, "exists")
            /\ UNCHANGED <<dirty, net, wait>>
       ELSE IF r = "conflict" THEN
            \* 修复方案下不应该出现：去重记录写入了，但业务行没插进去
            /\ violation' = "DedupConflict"
            /\ Finish(c, "exists")
            /\ UNCHANGED <<snap, dirty, net, wait, abs, absKnown, seen>>
       ELSE IF r = "fail" /\ ~(FixUnsureRetry /\ bamb[c]) THEN
            \* 确定失败：内存不变（当前设计：不管之前有没有过不确定的尝试）
            /\ Finish(c, "fail")
            /\ UNCHANGED <<snap, dirty, net, wait, abs, absKnown, seen, violation>>
       ELSE
            \* 结果不确定；或者冲突之后 SELECT 看到不存在；
            \* 或者（修复方案）之前有过不确定的尝试、这次确定失败：用同一个 gen 重试
            /\ SendStmt(BG(c), kind, g, 0, InitVal(g), {})
            /\ bamb' = [bamb EXCEPT ![c] = @ \/ r = "amb"]
            /\ UNCHANGED <<snap, dirty, opH, bpc, be, bop, bres, bgr,
                           abs, absKnown, seen, violation>>
    /\ UNCHANGED <<sysV, mapV, queued, dead, cliV, flV, evV, dbV, budV,
                   preCrash, deletedGens, insSent, delSent>>

-----------------------------------------------------------------------------
\* runSync 后台：deleteLocked + deleteUntilSure（7.5）

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
            /\ Finish(c, "notfound")
            /\ UNCHANGED <<snap, net, wait, delSent, abs, absKnown, seen, violation>>
       ELSE
            \* ① 先删 DB：DELETE ... WHERE id AND gen。这期间不持有 mu，update 可以继续
            /\ bgr' = [bgr EXCEPT ![c] = r.gen]
            /\ SendStmt(BG(c), "del", r.gen, 0, Val0, {})
            /\ delSent' = delSent \cup {r.gen}
            /\ bpc' = [bpc EXCEPT ![c] = "B_d2"]
            /\ UNCHANGED <<snap, opH, be, bop, bres, bamb, abs, absKnown, seen, violation>>
    /\ UNCHANGED <<sysV, mapV, dirty, queued, dead, cliV, flV, evV, dbV, resp, budV,
                   preCrash, deletedGens, insSent>>

B_d2(c) ==
    /\ up /\ bpc[c] = "B_d2" /\ ~wait[BG(c)]
    /\ resp' = [resp EXCEPT ![BG(c)] = "none"]     \* 取走结果
    /\ LET e == be[c]
           r == resp[BG(c)]
       IN
       IF r = "ok" THEN
            \* ② 线性化点：同一个 mu 临界区里换成墓碑，并丢弃脏数据
            /\ snap' = [snap EXCEPT ![e] = ABSENT]
            /\ dirty' = [dirty EXCEPT ![e] = {}]
            /\ LP(AbsAbsent)
            /\ deletedGens' = deletedGens \cup {bgr[c]}
            /\ Finish(c, "ok")
            /\ UNCHANGED <<net, wait>>
       ELSE IF r = "fail" /\ ~(FixUnsureRetry /\ bamb[c]) THEN
            \* 确定失败：行还在，dirty 保留（当前设计：不管之前有没有过不确定的尝试）
            /\ Finish(c, "fail")
            /\ UNCHANGED <<snap, dirty, abs, seen, deletedGens, net, wait>>
       ELSE
            \* 结果不确定；或者（修复方案）之前有过不确定的尝试、这次确定失败：用同一个 gen 重试
            /\ SendStmt(BG(c), "del", bgr[c], 0, Val0, {})
            /\ bamb' = [bamb EXCEPT ![c] = @ \/ r = "amb"]
            /\ UNCHANGED <<snap, dirty, opH, bpc, be, bop, bres, bgr, abs, seen, deletedGens>>
    /\ UNCHANGED <<sysV, mapV, queued, dead, cliV, flV, evV, dbV, budV,
                   absKnown, preCrash, insSent, delSent, violation>>

-----------------------------------------------------------------------------
\* 刷盘器（7.6）

\* 取走整个队列
F_take ==
    /\ up /\ fpc = "F_take" /\ queue # {}
    /\ fbatch' = queue
    /\ queue' = {}
    /\ fpc' = "F_scan"
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, fretry, fitem, evV, dbV, netV, budV, ghV>>

\* 逐个 entry：tryLockOp，然后在 mu 里取快照、清 dirty
F_scan ==
    /\ up /\ fpc = "F_scan" /\ fbatch # {}
    /\ LET e == CHOOSE x \in fbatch : TRUE IN
       /\ fbatch' = fbatch \ {e}
       /\ IF opH[e] # FREE THEN
               \* 正在 create/delete/load：下一轮再处理，queued 仍为 TRUE
               /\ fretry' = fretry \cup {e}
               /\ UNCHANGED <<opH, dirty, queued, fitem>>
          ELSE IF dead[e] \/ snap[e].k # "row" \/ dirty[e] = {} THEN
               /\ queued' = [queued EXCEPT ![e] = FALSE]
               /\ UNCHANGED <<opH, dirty, fretry, fitem>>
          ELSE
               \* op 一直持有到这一批写完
               /\ opH' = [opH EXCEPT ![e] = FL]
               /\ fitem' = [e |-> e, r |-> snap[e], m |-> dirty[e]]
               /\ dirty' = [dirty EXCEPT ![e] = {}]
               /\ queued' = [queued EXCEPT ![e] = FALSE]
               /\ UNCHANGED fretry
    /\ UNCHANGED <<sysV, mapV, snap, dead, cliV, bgV, fpc, queue, evV, dbV, netV, budV, ghV>>

\* requeueAll(retry)，然后发 UPDATE ... SET <mask 中的列>, ver WHERE id AND gen AND ver < ?
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

\* 写完：成功就放掉 op；失败或不确定，把掩码并回去、重新入队
F_done ==
    /\ up /\ fpc = "F_wait" /\ ~wait[FL]
    /\ resp' = [resp EXCEPT ![FL] = "none"]     \* 取走结果
    /\ LET e == fitem.e IN
       /\ opH' = [opH EXCEPT ![e] = FREE]
       /\ IF resp[FL] = "ok"
          THEN UNCHANGED <<dirty, queued, queue>>
          ELSE /\ dirty' = [dirty EXCEPT ![e] = @ \cup fitem.m]
               /\ queued' = [queued EXCEPT ![e] = TRUE]
               /\ queue' = IF queued[e] THEN queue ELSE queue \cup {e}
    /\ fitem' = NoItem
    /\ fpc' = "F_take"
    /\ UNCHANGED <<sysV, mapV, snap, dead, cliV, bgV, fbatch, fretry, evV, dbV,
                   net, wait, budV, ghV>>

-----------------------------------------------------------------------------
\* 淘汰器（7.7，dead 协议）。空闲 TTL、容量是否超限都是时间和策略问题，
\* 这里让它在任何时刻都可以尝试淘汰，覆盖所有可能的时机。

\* tryEvict：tryLockOp，在 mu 里确认 dirty == 0，设置 dead
V_evict ==
    /\ up /\ vpc = "V_pick" /\ mapE # 0 /\ nextEntry <= MaxEntry
    /\ LET e == mapE IN
       /\ opH[e] = FREE /\ ~dead[e] /\ dirty[e] = {}
       /\ dead' = [dead EXCEPT ![e] = TRUE]
       /\ opH' = [opH EXCEPT ![e] = EV]
       /\ ve' = e
       /\ vpc' = "V_rm"
    /\ UNCHANGED <<sysV, mapV, snap, dirty, queued, cliV, bgV, flV, dbV, netV, budV, ghV>>

\* 放掉 mu 之后（空档），在 shard.mu 里从 map 中摘除，然后放掉 op。
\* 已经没有人拿着它的指针时，把它的 snap 清掉（只是为了减少状态数，没有人能再看到它）。
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
\* DB（PostgreSQL）。所有写入都是条件写（I6）。

NewRow(s) == [ex |-> TRUE, gen |-> s.gen, ver |-> 0, val |-> s.val]

\* 语句执行之后 DB 的新状态
ExecDb(s) ==
    CASE s.kind = "ins"  -> IF db.ex THEN db ELSE NewRow(s)          \* ON CONFLICT DO NOTHING
      [] s.kind = "insd" -> IF s.gen \notin dedup /\ ~db.ex THEN NewRow(s) ELSE db
      [] s.kind = "upd"  -> IF db.ex /\ db.gen = s.gen /\ db.ver < s.ver
                            THEN [db EXCEPT !.ver = s.ver,
                                            !.val = [col \in Cols |->
                                                       IF col \in s.mask THEN s.val[col]
                                                       ELSE db.val[col]]]
                            ELSE db
      [] s.kind = "del"  -> IF db.ex /\ db.gen = s.gen THEN DbAbsent ELSE db

\* 语句执行的结果（UPDATE/DELETE 影响 0 行也算成功）
ExecResult(s) ==
    CASE s.kind = "ins"  -> IF db.ex THEN "dup" ELSE "ok"
      [] s.kind = "insd" -> IF s.gen \in dedup THEN "claimed"
                            ELSE IF db.ex THEN "conflict" ELSE "ok"
      [] OTHER           -> "ok"

\* 执行一条语句（可能是迟到执行的）。如果发送者还在等，把结果交给它；响应也可能丢失。
DbExec ==
    /\ \E s \in net :
         /\ db' = ExecDb(s)
         /\ dedup' = IF s.kind = "insd" THEN dedup \cup {s.gen} ELSE dedup
         /\ net' = net \ {s}
         /\ IF s.live
            THEN /\ wait' = [wait EXCEPT ![s.src] = FALSE]
                 /\ \/ /\ resp' = [resp EXCEPT ![s.src] = ExecResult(s)]
                       /\ UNCHANGED stmtTO
                    \/ /\ stmtTO < MaxStmtTimeouts       \* 执行了，但响应丢了
                       /\ stmtTO' = stmtTO + 1
                       /\ resp' = [resp EXCEPT ![s.src] = "amb"]
            ELSE UNCHANGED <<resp, wait, stmtTO>>
    /\ UNCHANGED <<sysV, mapV, entV, cliV, bgV, flV, evV,
                   drops, cto, crashes, nextGen, nextVal, ghV>>

\* Go 侧 ctx 超时：发送者不再等了，按"结果不确定"处理；语句还在路上，之后可能执行
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

\* 语句被确定拒绝（statement_timeout、连接失败等，没有执行），或者永远到不了 DB
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
\* 崩溃与重启（7.8）

\* 进程崩溃：内存全部丢失，所有 goroutine 消失；已经发出的语句还在路上，但没有人在等了。
\* 正在进行 create 的客户端看到连接断开，结果不确定。
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

\* 修复方案：重启时 pg_terminate_backend 旧进程的会话。会话没了，
\* 旧进程发出、还没执行的语句就永远不会执行。
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
\* 所有客户端的操作都做完了，后台也空闲：允许停在这里（不算死锁）
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
\* 不变式

\* 各个动作中的断言：Get 的线性一致性、崩溃之后的状态、去重表冲突
NoViolation == violation = "none"

\* 不可容忍①：已经删除（delete 的线性化点已经发生）的生命，不能重新出现在 DB 或内存里
NoResurrection ==
    /\ db.ex => db.gen \notin deletedGens
    /\ (up /\ mapE # 0 /\ snap[mapE].k = "row") => snap[mapE].gen \notin deletedGens

\* map 中已加载的 entry
LiveLoaded == up /\ mapE # 0 /\ ~dead[mapE] /\ snap[mapE].k # "nil"

\* 内存（map 中已加载的 entry）和抽象状态一致：所有线性化点都对得上
MemMatchesAbs == (LiveLoaded /\ absKnown) => Proj(snap[mapE]) = abs

\* I4 的推论：entry 是干净的、没有人持有 op、也没有还在路上的语句时，DB 和内存完全一致。
\* 违反它说明旧值覆盖了新值，或者内存和 DB 永久地分叉了（不可容忍②③）。
QuiescentConsistent ==
    (LiveLoaded /\ dirty[mapE] = {} /\ opH[mapE] = FREE /\ net = {}) => DbRow = snap[mapE]

\* 内存中没有这一行（或者还没加载）、而且一切都停下来时，DB 必须等于抽象状态
AllIdle ==
    /\ net = {}
    /\ \A c \in Clients : bpc[c] \in {"idle", "B_done"}
    /\ fitem = NoItem
DbMatchesAbsWhenIdle ==
    (up /\ absKnown /\ AllIdle /\ (mapE = 0 \/ snap[mapE].k = "nil" \/ dirty[mapE] = {}))
        => DbProj = abs

\* 不可容忍④：已经确认的 create 不会消失（除非有人发过它的 DELETE）
CreateDurable ==
    (absKnown /\ abs.ex) => ((db.ex /\ db.gen = abs.gen) \/ abs.gen \in delSent)

\* I5：同一个 key 同一时刻最多只有一条在等结果的写 SQL
OneWriterPerKey == Cardinality({p \in DBProcs : wait[p]}) <= 1

\* 脏的 entry 一定在刷盘的流水线上，不会被遗忘
DirtyImpliesQueued == (up /\ mapE # 0 /\ dirty[mapE] # {}) => queued[mapE]
QueuedIsTracked ==
    (up /\ mapE # 0 /\ queued[mapE]) =>
        \/ mapE \in queue \cup fbatch \cup fretry
        \/ \E c \in Clients : pc[c] = "U_enq" /\ ce[c] = mapE

\* I1：当前进程中没有 dead 的 entry 一定在 map 中
LiveEntryInMap ==
    \A e \in 1..(nextEntry - 1) : (up /\ eEp[e] = epoch /\ ~dead[e]) => mapE = e

-----------------------------------------------------------------------------
\* 只用于 hole3.cfg 的状态约束：崩溃时还在路上的旧语句只能是 UPDATE，
\* 让 TLC 给出 design.md 13.2 中描述的那个 UPDATE 版本的反例
\*（不加约束时，TLC 会先找到更短的 INSERT 版本，见 hole3_insert.cfg）。
OldStmtsAreUpdates == \A s \in net : s.ep < epoch => s.kind = "upd"

=============================================================================
