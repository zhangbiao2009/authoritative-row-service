---
aliases: [内存权威行数据服务设计, 行数据服务, write-behind 行存储]
tags: [design, concurrency, go, write-behind, fencing-token, linearizability]
created: 2026-11-11
---

# 内存权威行数据服务：设计文档

> [!info] 文档用途
> 这份文档记录了这个服务的需求、设计、背后的原则和权衡，用来交给 Claude Code 实现。第 0 节是写给实现者的工作约定，第 14～15 节是设计方法论和权衡，供日后回顾。

---

## 0. 给实现者（Claude Code）的工作约定

1. 先通读全文，尤其是 **第 4 节（不变式）** 和 **第 6 节（锁设计）**。
2. 开工前，先就 **第 13 节（待确认问题）** 向用户逐条确认，不要自行假设。
3. 按 **第 12 节的里程碑** 推进。每个里程碑都必须通过 `go test -race` 和该阶段要求的并发测试。
4. 把第 4 节的不变式、每把锁的角色（是否叶子锁、保护什么）写进代码注释。
5. 下面这些是**刻意的设计**，不要为了"简化"把它们删掉：
   - DB 写入一律是条件写（gen/ver），**禁止 upsert**
   - create/delete 结果不确定时，持有 op 用同样的参数重试，直到结果确定
   - `mu`、`shard.mu`、`qmu` 是叶子锁，`op` 是唯一可以嵌套其他锁的锁
   - 入队在放掉 `mu` 之后进行，队列不能阻塞
   - 墓碑（逻辑删除）和 dead 协议（物理删除）分开
   - 毒行进入隔离区并告警，绝不能静默丢弃（7.9）
6. 发现设计中说不通或有遗漏的地方，先停下来问用户，不要自行修改设计。

---

## 1. 背景与需求

- 这个服务是数据的**权威来源**，不是缓存。数据库只是为它提供接近实时的持久化。
- 数据是数据库表中的**一行（多列）**，不是单纯的 key-value。
- 四种操作：

| 操作 | 什么时候返回 | 原因 |
|---|---|---|
| Create | DB **确定成功**之后 | 用户创建的东西绝不能消失 |
| Delete | DB **确定成功**之后 | 删掉的东西绝不能重新出现 |
| Update | 改完内存**立即返回**，之后异步刷盘 | 频率很高，要求尽可能快 |
| Get | 内存命中直接返回；未命中时从 DB 加载 | — |

- Update 可能只修改一行中的**一列或几列**。
- 服务管理**多张业务方的表**，表结构是**动态的**：服务在运行时从 PG 的系统目录读取表结构，不在代码里写死。唯一的要求是表上必须有 gen、ver 两列（列名可配置）。
- 尽量不限制业务方：主键支持整数、字符串、UUID，以及复合主键（见 5.3）。
- **可以容忍**：服务崩溃重启时，丢失刷盘窗口内最近的一小部分 update。
- **不可容忍**：
  - 已经确认的 create 消失
  - 已经确认 delete 的数据重新出现
  - 旧值覆盖新值，并且长期停留在 DB 中
  - 旧对象的数据写进了同一个 id 重新创建的新对象
- 语言：Go（暂不考虑其他语言）。

---

## 2. 前提假设

- 同一个 key 在任一时刻只由**一个服务实例**负责：单实例部署，或者多实例按 key 路由（见 11.3）。
- 这些表**只由本服务写入**。业务方的其他程序如果绕过本服务直接写这些表，内存就不再是权威，数据会被覆盖或者出现不一致。**这是接入本服务的硬性前提，必须和业务方约定清楚。**
- **内存放不下全量数据**，只保留工作集：
  - 一个 entry 超过 `IdleTTL` 没有被访问，就要从内存中清除（前提是干净的，见 7.7）
  - entry 总数超过 `MaxEntries` 时，按近似 LRU 强制淘汰
  - 冷数据第一次被访问时，从 DB 加载
- 数据库是 **PostgreSQL**，驱动使用 `github.com/jackc/pgx/v5`（PostgreSQL 带来的影响见 8.3～8.5）。

---

## 3. 总体结构

```
Service
 ├── tables map[表名]*Store     按配置的表名单，启动时从 PG 系统目录加载 schema（见 5.3）
 │    └── Store（一张表）
 │         ├── schema            运行期间不可变
 │         └── shards[N]         N 取 2 的幂（如 256），按 maphash(key) & (N-1) 定位
 │              ├── mu  (RWMutex)   保护 map 结构；叶子锁
 │              ├── m   map[Key]*entry   Key 是主键的规范化编码
 │              ├── qmu (Mutex)     保护刷盘队列；叶子锁
 │              └── queue []*entry  刷盘队列（不阻塞）
 ├── 刷盘器                     后台定时，按 shard 批量把脏行写回 DB（所有表共用 worker 池）
 ├── 淘汰器                     按空闲 TTL 清除干净条目；超出 MaxEntries 时按近似 LRU 淘汰
 └── DB 接口                    真实实现（PostgreSQL / pgx）+ 带故障注入的假实现
```

每个 key 在内存中有一个固定的**槽位**（entry）。增、删、改在内存里都只是替换这个槽位中的不可变快照指针，所以"存在与否"本身就是值的一部分：

```
nil    → Row / absent   (load)
absent → Row            (create)
Row    → Row'           (update)
Row    → absent         (delete，墓碑)
```

建议的目录结构：

```
/cmd/server               入口、API 层（协议待确认）
/internal/store           entry、shard、Get/Create/Update/Delete、淘汰
/internal/store/flush.go  刷盘器
/internal/db              DB 接口、错误分类、PostgreSQL（pgx）实现
/internal/db/fakedb       故障注入的假 DB（测试用）
/internal/store/*_test.go 压力测试、Porcupine 线性一致性测试、failpoint 测试
```

---

## 4. 核心不变式

所有代码都必须维持下面这些不变式，并把它们写在代码注释中。

| 编号 | 不变式 |
|---|---|
| I1 | 每个 key 在 map 中**最多有一个** entry。没有 dead 的 entry 一定在 map 中；dead 的 entry 只会在从 map 摘除前的极短时间内仍留在 map 中。 |
| I2 | 已经发布的 `*Row` **永远不会被修改**。任何修改都是新建一个 Row，再替换指针。 |
| I3 | 写 `snap` 必须持有 `mu`。涉及 nil/absent 的转换（load、create、delete）还必须同时持有 `op`。Update 只做 Row→Row 的转换。 |
| I4 | 在持有 `op` 的前提下，如果 `dirty == 0`，那么 DB 中这一行和内存一致（或者两边都不存在）。 |
| I5 | 同一个 key 在任一时刻，最多只有一条由本服务发出、仍在等待结果的写 SQL。超时之后迟到执行的写，由 gen/ver 挡住。 |
| I6 | DB 写入一律是条件写：`UPDATE ... WHERE id AND gen AND ver <`、`DELETE ... WHERE id AND gen`、INSERT 依靠唯一键。**禁止 upsert。** |
| I7 | Create/Delete 只有在 DB 结果**确定成功**之后才修改内存，而且修改内存（线性化点）发生在给客户端返回之前。 |
| I8 | 只有 `op` 可以在持有期间去拿其他锁。`mu`、`shard.mu`、`qmu` 都是叶子锁。 |
| I9 | 写 `dead` 必须同时持有 `op` 和 `mu`；读 `dead` 持有其中任意一把即可。 |
| I10 | 有脏数据（`dirty != 0`）的 entry 不能淘汰。 |
| I11 | schema 在运行期间不可变。`Row.Vals` 按 schema 中的列序号存放，切片本身和其中的每个值都不可修改（I2 的延伸）。 |

---

## 5. 数据结构

### 5.1 表结构

业务方的表接入之前，需要加上两列（列名可配置，默认 `row_gen` / `row_ver`；本文的 SQL 为了简洁写成 gen / ver）：

```sql
ALTER TABLE orders
  ADD COLUMN row_gen BIGINT NOT NULL DEFAULT 0,  -- 化身号：每次 create 生成新值，按"相等"比较
  ADD COLUMN row_ver BIGINT NOT NULL DEFAULT 0;  -- 版本号：每次 update +1，单调递增
ALTER TABLE orders SET (fillfactor = 80);        -- 在页内给 HOT 更新留出空间（见 8.5）
```

- PG 11 及以上版本，加一个带常量默认值的列不会重写整张表，可以在线执行。
- 已有的行 gen 都是 0，这没有问题：gen 只需要和同一个 key 的其他生命不同，新建的行会用非 0 的随机数。
- gen、ver 不要建索引；业务列的索引尽量避开频繁更新的列，否则 UPDATE 无法走 HOT。

本文后面的示例统一使用一张示例表 `items(id, gen, ver, name, score)`。

### 5.2 Go 结构

```go
package store

var (
	ErrNotFound       = errors.New("not found")
	ErrAlreadyExists  = errors.New("already exists")
	ErrOutcomeUnknown = errors.New("outcome unknown") // create/delete 在客户端超时前未能确定结果
	ErrBusy           = errors.New("busy")            // 背压
)

// Key 是主键的规范化编码（见 5.3），可以直接作为 map 的 key。
type Key string

// Row 一旦发布就不可变（I2、I11）。
type Row struct {
	Key  Key
	Gen  int64 // 化身号：区分同一个 key 的不同"生命"
	Ver  int64 // 版本号：区分同一个生命里修改的先后
	Vals []any // 业务列的值，下标是 schema 中的列序号；切片和其中的值都不可修改
}

// 列位图：第 i 位表示第 i 列（i < 63）；第 63 位表示"序号 ≥ 63 的某一列脏了"，
// 刷盘时把序号 ≥ 63 的列全部写一遍。这样任意列数的表都能支持，列很多时只是多写一些。
func colBit(i int) uint64 {
	if i >= 63 {
		return 1 << 63
	}
	return 1 << uint(i)
}

// absent 是哨兵，表示确定不存在（墓碑或负缓存）。
var absent = &Row{}

type entry struct {
	id Key

	// 无锁读者直接读取；写入必须持有 mu（I3）。
	// nil = 未加载；absent = 确定不存在；其他 = 当前行
	snap atomic.Pointer[Row]

	// op：跨 DB IO 的 per-key 锁。容量为 1 的 channel，可以配合 ctx 取消。
	// 串行化本 key 的 load / create / delete / 刷盘。
	// 唯一允许在持有期间再去拿其他锁的锁（I8）。
	op chan struct{}

	// mu：叶子锁。持有期间不拿其他锁、不阻塞、不做 IO、不打日志、不调用外部代码。
	mu         sync.Mutex
	dirty      uint64    // 脏列位图（mu）
	queued     bool      // 是否已经在刷盘队列中（mu）
	poisoned   bool      // 刷盘被 DB 确定拒绝，进入隔离区（mu，见 7.9）
	dead       bool      // 已被摘除；写需要同时持有 op+mu，读持有其一即可（I9）
	dirtySince time.Time // dirty 从 0 变为非 0 的时间（mu）

	// 最近访问时间（粗粒度时钟，单位秒），用于空闲 TTL 和近似 LRU。
	lastAccess atomic.Int64

	// 只在持有 op 时访问
	lastLoadErr   error
	lastLoadErrAt time.Time
}

type shard struct {
	mu sync.RWMutex // 叶子锁：只保护 map 结构
	m  map[Key]*entry

	qmu   sync.Mutex // 叶子锁：只保护刷盘队列
	queue []*entry   // 因为有 queued 标记，长度不会超过 entry 数；不能用会阻塞的 channel
}

type Store struct {
	shards     []*shard
	db         db.DB
	cfg        Config
	nowSec     atomic.Int64    // 粗粒度时钟：后台 ticker 每秒更新一次
	entryCount atomic.Int64
	dirtyCount atomic.Int64
	inflight   sync.WaitGroup  // 正在进行的 create/delete
	bgCtx      context.Context // 服务生命周期的 ctx，关闭时取消
}
```

> [!warning] 浅拷贝陷阱
> `nr := *old` 只复制结构体本身，`Vals` 切片仍然和旧行共享底层数组。修改之前必须先 `nr.Vals = slices.Clone(old.Vals)`，再替换其中的元素。各个值本身（`[]byte`、`json.RawMessage` 等）也不可修改，要改就整体换成一个新值。

### 5.3 动态 schema、多表与主键

**加载 schema**

- 需要管理的表在配置中列出（表名白名单），服务不接受对名单之外的表的访问。
- 启动时从 PG 系统目录（`pg_attribute`、`pg_index`、`pg_constraint`、`pg_trigger`，或者 `information_schema`）读取：列名、类型（OID 和 `format_type`）、是否 NOT NULL、默认值、是否是生成列、主键由哪些列组成，以及 CHECK 约束、外键和触发器。
- 校验以下几点，不满足就拒绝加载这张表，并给出明确的错误信息：
  - 必须有 gen、ver 两列，类型为 `BIGINT NOT NULL`
  - 必须有主键，并且主键列的类型在支持范围内
  - 所有列的类型都在支持的类型列表里
- 加载完成后，schema 在运行期间**不可变**（I11）。
- SQL 由 schema 动态生成，表名和列名一律用 `pgx.Identifier{...}.Sanitize()` 转义。生成好的 SQL 按（表，列位图）缓存。

**支持的类型（v1）**

| PG 类型 | Go 中的表示 |
|---|---|
| bool | bool |
| int2 / int4 / int8 | int64（写入时检查范围） |
| float4 / float8 | float64 |
| numeric | pgtype.Numeric（或者十进制字符串） |
| text / varchar(n) / char(n) | string（写入时检查长度） |
| bytea | []byte |
| uuid | [16]byte |
| date / timestamp / timestamptz | time.Time |
| json / jsonb | json.RawMessage（保留原始字节，不解析成 map） |

数组、复合类型、枚举等以后再加。所有值都视为不可变：`[]byte`、`json.RawMessage` 这类引用类型，API 层收到时先复制一份；返回给调用方之后，调用方不能修改。

**主键：内部统一编码，不限制业务方**

- 内部把主键统一编码成一个 `Key`（Go 的 string，可以存放任意字节），既作为 map 的 key，也作为 shard 哈希（`hash/maphash`）的输入。
- 编码规则：
  - int2 / int4 / int8：8 字节大端整数
  - text / varchar：UTF-8 字节
  - uuid：16 字节
  - 复合主键：把各列的编码按"长度前缀 + 内容"依次拼接
- 编码是确定性的：同一个主键值，永远得到同一个 Key。
- 这样整数、字符串、UUID、复合主键都能支持。代价是 string 作为 map key 时，要按字节计算哈希，比 int64 稍慢，但和一次 DB 往返相比可以忽略。如果以后压测发现这里是瓶颈，再为单列整数主键加一条快速路径。
- **v1 要求 create 时由调用方提供主键**，不支持由 DB 生成的主键（serial / identity）。原因：create 结果不确定时，要用同一个主键和 gen 重试；如果主键由 DB 生成，重试就会插入一条新行，只能改为按 gen 查找（需要在 gen 上建唯一索引）。gen 在插入之后不会再变，所以给它建索引不影响 HOT。这个能力可以以后再加。

**表上的其他对象**

- **默认值**：create 用 `INSERT ... RETURNING` 把 DB 填充的默认值读回内存，保证内存和 DB 一致。
- **生成列**（`GENERATED ALWAYS AS`）：不能写入，而且依赖的列被修改之后，内存里的值就过时了。v1 不加载生成列，也不对外提供。
- **会修改行的触发器**：触发器在 UPDATE 时做的修改（比如 `updated_at = now()`），内存里看不到，内存和 DB 会不一致。要和业务方约定：接入的表不能有修改行数据的触发器；`updated_at` 这类列，由本服务在 update 时赋值。schema 加载时检测到触发器，要打警告日志并把它们列出来。
- **CHECK 约束和外键**：会导致刷盘时出现毒行（见 7.9）。schema 加载时检测到，也要打警告日志。

**表结构变更**

- v1：表结构变更需要**重启服务**。优雅关闭时会刷完所有脏行，重启后重新加载 schema。
- 增加一个可以为空、或者带默认值的列，是兼容的变更：即使暂时不重启，旧的 schema 也不会写错这一列（INSERT 时它取默认值，UPDATE 不会碰它），只是暂时读不到。删除列、改名、改类型是不兼容的变更，必须先停服务，再改表。
- 后台定期（比如每分钟）对比系统目录和已加载的 schema，发现不一致就告警。

**多表**

- 每张表一个 `Store`，有自己的 schema 和 shard。下文的代码骨架就是一个 `Store`。
- 刷盘 worker 池和淘汰器由所有表共用，按 shard 轮流处理。
- IdleTTL、MaxEntries 等配置可以按表覆盖全局默认值（见 11.4）。

---

## 6. 锁设计

### 6.1 每把锁的角色

| 锁 | 保护什么 | 范围 | 持有时间 | 是否跨 IO | 角色 |
|---|---|---|---|---|---|
| `shard.mu` | map 结构（查找、插入、删除） | 很多 key | 极短 | 否 | 叶子锁 |
| `shard.qmu` | 刷盘队列 | 很多 key | 极短 | 否 | 叶子锁 |
| `e.mu` | 行的读-改-写、dirty、queued、dead | 1 个 key | 极短 | 否 | 叶子锁 |
| `e.op` | 本 key 的所有 DB 操作串行化 | 1 个 key | 长 | **是** | 根（唯一可以嵌套的锁） |
| `e.snap`（atomic） | 读者无锁读取一致的整行 | 1 个 key | — | — | 不是锁 |

### 6.2 锁顺序

规则只有一句：**只有 `op` 可以在持有期间去拿其他锁**；其他锁都是叶子锁，持有期间什么都不拿。

锁依赖图只有 `op → mu`、`op → shard.mu`、`op → qmu` 这几条边，没有环，所以不会死锁。

### 6.3 这样设计的理由

- **shard.mu 必须是叶子锁**：它的范围很大。持有期间只要等待任何东西，这个 shard 上所有 key 的请求都会跟着排队。
- **update 只拿 mu，永远不等 IO**：update 和刷盘的延迟要求不同，不能共用一把锁。判断方法：一把锁的所有使用者里最慢的那一个，会把所有等这把锁的人都拖到同样慢。
- **读者不拿任何锁**：不可变快照加原子发布（copy-on-write）。读者之间不写任何共享内存，在热点 key 上也能线性扩展。即使是 RWMutex 的读锁，也会因为争抢同一条 cache line 而成为瓶颈。
- **查找时用"先放手、再验证"，而不是 crabbing**：在 shard 锁里找到 entry 后**马上放掉** shard 锁，再去锁 entry，拿到锁后检查 dead。不用 crabbing（持有 shard 锁的同时去锁 entry）的原因：
  - 对 op 用 crabbing：等于持有 shard 锁去等别人的 DB IO，整个 shard 都会被卡住
  - 对 mu 用 crabbing：热点 key 的争抢会扩散到整个 shard（Go 的 RWMutex 在有写者排队时，会挡住新来的读者）
  - Go 有 GC，空档期间手里的指针不会悬空，所以事后验证是安全的

### 6.4 禁止事项（code review 检查清单）

- 持有 `mu` / `shard.mu` / `qmu` 时：不拿其他锁、不做 channel 收发、不做 IO、不打日志（`log.Logger` 内部有锁，而且会做 IO）、不调用外部回调。
- 持有 `shard.mu` 时，不能去等 `mu` 或 `op`。
- 持有 `mu` 时，不能去等 `op`。如果需要加载数据，必须先放掉 `mu`，加载完后从头再走一遍。
- 要求调用方已持有锁的函数，统一使用 `xxxLocked` 后缀。
- 每次拿到 `op` 或 `mu` 之后，先检查 `dead`。

---

## 7. 各操作流程（代码骨架）

> 下面是骨架，用来说明流程和锁的使用，细节由实现者补全。骨架里的 `Store` 对应一张表，`id` 是编码后的主键（`Key` 类型），业务列存放在 `Row.Vals` 中。

### 7.1 辅助函数

```go
func (e *entry) lockOp(ctx context.Context) error {
	select {
	case e.op <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *entry) tryLockOp() bool {
	select {
	case e.op <- struct{}{}:
		return true
	default:
		return false
	}
}

func (e *entry) unlockOp() { <-e.op }

// touch 记录访问时间。读的是粗粒度时钟 s.nowSec，避免每次命中都调用 time.Now()；
// 值没有变化就不写，避免每次命中都去写同一条 cache line。
func (s *Store) touch(e *entry) {
	now := s.nowSec.Load()
	if e.lastAccess.Load() != now {
		e.lastAccess.Store(now)
	}
}

func (s *Store) getOrCreate(id Key) *entry {
	sh := s.shardFor(id)
	sh.mu.RLock()
	e := sh.m[id]
	sh.mu.RUnlock()
	if e != nil {
		return e
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e = sh.m[id]; e == nil { // 二次检查
		e = &entry{id: id, op: make(chan struct{}, 1)}
		e.lastAccess.Store(s.nowSec.Load()) // 新建的 entry 不能一出生就被当成空闲的
		sh.m[id] = e
		s.entryCount.Add(1)
	}
	return e
}

// lockOpLive 拿到一个没有 dead 的 entry 的 op。
func (s *Store) lockOpLive(ctx context.Context, id Key) (*entry, error) {
	for {
		e := s.getOrCreate(id)
		if err := e.lockOp(ctx); err != nil {
			return nil, err
		}
		if !e.dead { // 持有 op 即可读取 dead（I9）
			return e, nil
		}
		e.unlockOp()
	}
}

func (s *Store) markDirtyLocked(e *entry, mask uint64) {
	if e.dirty == 0 {
		s.dirtyCount.Add(1)
		e.dirtySince = time.Now()
	}
	e.dirty |= mask
}

func (s *Store) clearDirtyLocked(e *entry) {
	if e.dirty != 0 {
		s.dirtyCount.Add(-1)
	}
	e.dirty = 0
}

// enqueue 必须在放掉 mu 之后调用（qmu 是叶子锁，队列不阻塞）。
func (s *Store) enqueue(e *entry) {
	sh := s.shardFor(e.id)
	sh.qmu.Lock()
	sh.queue = append(sh.queue, e)
	sh.qmu.Unlock()
}
```

### 7.2 Get 与 load

```go
func (s *Store) Get(ctx context.Context, id Key) (*Row, error) {
	for {
		e := s.getOrCreate(id)
		s.touch(e)
		if r := e.snap.Load(); r != nil { // 线性化点
			if r == absent {
				return nil, ErrNotFound
			}
			return r, nil // 共享的不可变对象，调用方只读
		}
		if err := s.load(ctx, e); err != nil {
			return nil, err
		}
		// 加载完回到开头。如果 entry 被淘汰了，getOrCreate 会拿到新的 entry。
	}
}

// load 供 Get 和 Update 使用。并发的未命中都会排在 op 上，相当于 singleflight。
func (s *Store) load(ctx context.Context, e *entry) error {
	if err := e.lockOp(ctx); err != nil {
		return err // 客户端已经放弃，退出排队
	}
	defer e.unlockOp()
	if e.dead {
		return nil // 调用方会重新查 map
	}
	_, err := s.ensureLoadedLocked(ctx, e)
	return err
}

// ensureLoadedLocked 要求调用方持有 op。
func (s *Store) ensureLoadedLocked(ctx context.Context, e *entry) (*Row, error) {
	if r := e.snap.Load(); r != nil {
		return r, nil // 二次检查：排队期间别人已经加载好了
	}
	// 刚刚失败过就直接返回错误，避免排队的请求一个接一个地等超时
	if e.lastLoadErr != nil && time.Since(e.lastLoadErrAt) < s.cfg.LoadErrTTL {
		return nil, e.lastLoadErr
	}
	r, err := s.db.Select(ctx, e.id)
	switch {
	case errors.Is(err, db.ErrNoRows):
		r = absent
	case err != nil:
		if ctx.Err() == nil { // 客户端自己取消的不缓存，只缓存 DB 侧的错误
			e.lastLoadErr, e.lastLoadErrAt = err, time.Now()
		}
		return nil, err
	}
	e.lastLoadErr = nil
	e.mu.Lock()
	e.snap.Store(r)
	e.mu.Unlock()
	return r, nil
}
```

- 读操作迟到执行没有危害（不改变任何东西），所以 SELECT 不需要 gen/ver。
- 查询不存在的 key 也会留下一个 absent 条目（负缓存）。这类条目和墓碑使用更短的 `NegativeIdleTTL`，防止大量随机 key 把内存撑大。

### 7.3 Update（只改内存）

```go
// Change 由 API 层根据请求和 schema 构造。构造时已经完成类型转换和校验（见 5.3、7.9）。
type Change struct {
	Cols []int  // 要修改的列（schema 中的序号）
	Vals []any  // 新值，和 Cols 一一对应；构造时已经复制，之后不再修改
	Mask uint64 // Cols 对应的列位图（colBit 的按位或）
	Gen  int64  // 可选：客户端期望的 gen，0 表示不检查
}

func (s *Store) Update(ctx context.Context, id Key, ch Change) error {
	if s.dirtyCount.Load() > s.cfg.MaxDirty {
		return ErrBusy // 背压（可以改成短暂等待；对已经是脏行的再次更新可以放行）
	}
	for {
		e := s.getOrCreate(id)
		s.touch(e)
		e.mu.Lock()
		if e.dead {
			e.mu.Unlock()
			runtime.Gosched() // 淘汰正在摘除它，稍等再查 map
			continue
		}
		old := e.snap.Load()
		if old == nil {
			e.mu.Unlock() // 先放掉 mu，再去拿 op（mu 是叶子锁）
			if err := s.load(ctx, e); err != nil {
				return err
			}
			continue
		}
		if old == absent || (ch.Gen != 0 && old.Gen != ch.Gen) {
			e.mu.Unlock()
			return ErrNotFound
		}
		nr := *old
		nr.Vals = slices.Clone(old.Vals) // 只复制切片本身；各个值不可变，可以共享
		for i, c := range ch.Cols {
			nr.Vals[c] = ch.Vals[i]
		}
		nr.Ver = old.Ver + 1
		e.snap.Store(&nr) // 线性化点
		s.markDirtyLocked(e, ch.Mask)
		need := !e.queued
		e.queued = true
		e.mu.Unlock()
		if need {
			s.enqueue(e) // 放掉 mu 之后再入队
		}
		return nil
	}
}
```

- 读-改-写**完整地**放在 `mu` 里面，所以两个请求修改不同的列也不会丢更新。如果需要自增这类读改写语义，可以给 Change 加一个操作类型，在 mu 里基于 old 的值计算新值。
- delete 进行中到达的 update**允许成功**（见 15 节）。它的线性化点在 delete 之前，delete 成功时会把它的改动一起丢弃。
- `Change.Gen` 用来区分同一个 id 的不同生命：如果一个 update 本来是要改旧对象的，它不会改到删除后重新创建的新对象上。需不需要待确认。

### 7.4 Create（同步）

```go
// runSync 在持有 op 的情况下，在后台 goroutine 中执行 fn，直到结果确定才释放 op。
// 如果客户端的 ctx 先到期，先给客户端返回 ErrOutcomeUnknown，后台继续执行。
func (s *Store) runSync(ctx context.Context, e *entry, fn func(*entry) error) error {
	done := make(chan error, 1)
	s.inflight.Add(1)
	go func() {
		defer s.inflight.Done()
		defer e.unlockOp()
		done <- fn(e)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ErrOutcomeUnknown
	}
}

func (s *Store) Create(ctx context.Context, in *Row) error {
	e, err := s.lockOpLive(ctx, in.Key)
	if err != nil {
		return err
	}
	return s.runSync(ctx, e, func(e *entry) error { return s.createLocked(e, in) })
}

func (s *Store) createLocked(e *entry, in *Row) error {
	cur := e.snap.Load()
	if cur != nil && cur != absent {
		if in.Gen != 0 && cur.Gen == in.Gen {
			return nil // 客户端重试，之前那次已经成功了（幂等）
		}
		return ErrAlreadyExists
	}
	nr := *in
	if nr.Gen == 0 {
		nr.Gen = s.newGen() // 非 0 的 63 位随机数，只需要和同 id 的其他生命不同
	}
	nr.Ver = 0
	got, err := s.insertUntilSure(s.bgCtx, &nr)
	if errors.Is(err, ErrAlreadyExists) {
		e.mu.Lock()
		e.snap.Store(got) // DB 里已经有别的生命，顺便加载进内存
		e.mu.Unlock()
		return err
	}
	if err != nil {
		return err // 确定失败，内存不变
	}
	// got 是 INSERT ... RETURNING 返回的整行（或者确认时 SELECT 到的行），
	// 包含 DB 填充的默认值，所以内存和 DB 完全一致。
	e.mu.Lock()
	e.snap.Store(got) // 线性化点：DB 确定成功之后、返回之前（I7）
	s.clearDirtyLocked(e)
	e.mu.Unlock()
	s.touch(e)
	return nil
}

// insertUntilSure 用同一个 gen 重试，直到结果确定。
// 成功时返回 DB 中的整行（INSERT ... RETURNING，或者确认时 SELECT 到的行）。
func (s *Store) insertUntilSure(ctx context.Context, r *Row) (*Row, error) {
	for attempt := 0; ; attempt++ {
		got, err := s.db.Insert(ctx, r) // ON CONFLICT DO NOTHING；冲突时返回 db.ErrDuplicate
		if err == nil {
			return got, nil
		}
		if errors.Is(err, db.ErrDuplicate) {
			cur, serr := s.db.Select(ctx, r.Key)
			switch {
			case serr == nil && cur.Gen == r.Gen:
				return cur, nil // 之前某次"超时"的插入其实已经成功了
			case serr == nil:
				return cur, ErrAlreadyExists
			case errors.Is(serr, db.ErrNoRows):
				// 状态还没稳定下来，继续重试
			default:
				err = serr
			}
		}
		if err != nil && !errors.Is(err, db.ErrDuplicate) && !db.IsAmbiguous(err) {
			return nil, err // 确定失败
		}
		if werr := s.backoff(ctx, attempt); werr != nil {
			return nil, werr // 服务正在关闭
		}
	}
}
```

- 创建期间到达的 Get，如果看到 nil 就会排在 op 上等；如果看到 absent，就返回不存在（线性化在 create 之前）。总之不会看到还没落库的行。
- 结果不确定时**不能用 SELECT 来"确认"一下就算了**：SELECT 看到不存在，不代表那条迟到的 INSERT 之后不会落地。必须用同一个 gen 重试，直到结果确定。gen 本身就起到了幂等 id 的作用。
- 客户端收到 `ErrOutcomeUnknown` 之后，可以带上同一个 gen 重试 create，这样是幂等的。

### 7.5 Delete（同步）

```go
func (s *Store) Delete(ctx context.Context, id Key) error {
	e, err := s.lockOpLive(ctx, id) // 会等正在进行的刷盘完成
	if err != nil {
		return err
	}
	return s.runSync(ctx, e, s.deleteLocked)
}

func (s *Store) deleteLocked(e *entry) error {
	r, err := s.ensureLoadedLocked(s.bgCtx, e)
	if err != nil {
		return err
	}
	if r == absent {
		return ErrNotFound
	}
	// ① 先删 DB。这里不持有 mu，update 可以继续写内存。
	if err := s.deleteUntilSure(s.bgCtx, e.id, r.Gen); err != nil {
		return err // 确定失败：行还在，dirty 保留，之后照常刷盘
	}
	// ② 线性化点：在同一个 mu 临界区里换成墓碑，并丢弃脏数据
	e.mu.Lock()
	e.snap.Store(absent)
	s.clearDirtyLocked(e)
	e.mu.Unlock()
	s.touch(e) // 墓碑从删除那一刻开始计算 NegativeIdleTTL
	return nil // ③ 生效之后才返回
}

// deleteUntilSure：DELETE FROM items WHERE id=? AND gen=?
// 影响 0 行或 1 行都算成功（DELETE 是幂等的）。结果不确定就重试，确定失败就返回错误。
```

- **为什么 delete 返回后 update 一定访问不到**：Update 的读-改-写和第②步都在同一把 `mu` 里，两者只能一先一后。Update 在②之前，它的改动会被 `dirty=0` 丢弃；Update 在②之后，它会看到 absent，返回 NotFound。
- ②必须在 DB 成功**之后**（否则 DB 删除失败时，内存和 DB 就不一致了），并且在**返回之前**。
- 不从 map 中删除 entry，而是换成墓碑。原因：
  1. 已经拿着这个 entry 指针、正在排队的请求，能马上看到"已删除"
  2. 起到负缓存的作用（删除之后客户端可能还会继续发 update）
  3. delete 的路径很简单，物理删除统一交给淘汰来做

### 7.6 刷盘（write-behind）

每个刷盘 worker 负责一组固定的 shard（比如 shard i 交给 worker i % W），每隔 `FlushInterval` 处理一轮：

```go
type flushItem struct {
	e     *entry
	row   *Row
	mask  uint64
	since time.Time
}

func (s *Store) flushShard(sh *shard) {
	sh.qmu.Lock()
	batch := sh.queue
	sh.queue = nil
	sh.qmu.Unlock()

	var items []flushItem
	var retry []*entry
	for _, e := range batch {
		if !e.tryLockOp() {
			retry = append(retry, e) // 正在 create/delete/load，下一轮再处理；queued 仍为 true
			continue
		}
		e.mu.Lock()
		r, mask := e.snap.Load(), e.dirty
		if e.dead || r == nil || r == absent || mask == 0 {
			e.queued = false
			e.mu.Unlock()
			e.unlockOp()
			continue
		}
		since := e.dirtySince
		s.clearDirtyLocked(e)
		e.queued = false
		e.mu.Unlock()
		items = append(items, flushItem{e, r, mask, since}) // op 一直持有到这一批写完
	}
	s.requeueAll(sh, retry)

	// 一个事务里执行多条：
	// UPDATE <表> SET <mask 中的列>, ver=? WHERE <主键>=? AND gen=? AND ver < ?
	// 超时或任何失败都按失败处理（条件写是幂等的，重写是安全的）。
	// 如果是"确定失败"（比如违反约束），说明这一批里有毒行，要逐行重试把它找出来（见 7.9）。
	err := s.db.UpdateBatch(s.bgCtx, items)
	for _, it := range items {
		if err != nil {
			e := it.e
			e.mu.Lock()
			s.markDirtyLocked(e, it.mask) // 把掩码并回去，下次带着最新值重写
			if it.since.Before(e.dirtySince) {
				e.dirtySince = it.since // 保留最早的脏时间，监控才准确
			}
			need := !e.queued
			e.queued = true
			e.mu.Unlock()
			if need {
				s.enqueue(e)
			}
		}
		it.e.unlockOp()
	}
	// DB 持续失败时，对这个 shard 做指数退避
}
```

要点：

- **合并写**：两次刷盘之间，不管 update 了多少次，同一行只写一次 DB。积压量的上限是"脏行数"而不是"update 次数"。
- 刷盘期间新来的 update 会让这一行重新入队，但下一次刷盘要等 op。所以同一个 key 同一时刻只有一个 UPDATE 在执行（I5）。
- 批量刷盘时，op 用 TryLock，避免和 create/delete 互相等待。每个 shard 只有一个 worker，所以 worker 之间不会争抢同一个 key。
- 影响 0 行**不是错误**：可能是之前一次"超时"的写其实已经成功了，也可能是这一行已经被删除。记一个 `flush_noop_rows` 指标就行。
- PostgreSQL 的 RowsAffected 是匹配并被更新的行数（值没有变化也算在内），不存在 MySQL 那种"只统计实际改变的行"的问题。
- 批量写入的具体 SQL 见 8.3。

### 7.7 淘汰（物理删除，dead 协议）

**安全的条件只有一个**：在持有 op 的前提下，`dirty == 0`，也就是内存和 DB 完全一致（I4）。只要满足这个条件，被淘汰的 entry 以后再被访问，也只是一次未命中，重新加载回来的状态完全一样。哪些 key 以后还会被访问，只影响命中率，不影响正确性。脏的 entry 即使已经空闲也不淘汰，等刷盘之后的下一轮再处理。

淘汰有两种触发方式：

| 触发 | 条件 | 做法 |
|---|---|---|
| 空闲清除 | `now - lastAccess > IdleTTL`（absent 或者没加载成功的条目，用 `NegativeIdleTTL`） | 每个 shard 每隔 `EvictInterval` 抽样检查一次 |
| 容量淘汰 | `entryCount > MaxEntries`（高水位） | 抽样，淘汰样本中最久没有被访问的，直到降到低水位（比如 0.9 × MaxEntries） |

**为什么用抽样，而不是全表扫描或者 LRU 链表：**

- 全表扫描要在 RLock 下遍历整个大 map。这期间 getOrCreate 的写锁只能等着，而 Go 的 RWMutex 在有写者排队时，还会挡住新来的读者，整个 shard 都会卡顿。抽样让每次持锁的时间有一个上限。
- LRU 链表要求每次命中都去移动链表节点，这是写操作，需要加锁，会破坏"命中路径只读"。
- 这正是 Redis 的做法：主动过期用随机抽样，`allkeys-lru` 用抽样来近似 LRU。
- 代价：过期的 entry 不会在 TTL 到点时被精确清除，会稍晚一些。TTL 是内存策略，不是正确性要求，可以接受。

```go
const sampleK = 64

// idleSweepShard：Redis 式抽样过期。样本里过期的比例超过 25%，就立刻再来一轮。
func (s *Store) idleSweepShard(sh *shard) {
	now := s.nowSec.Load()
	expired := func(e *entry) bool { return s.idleExpired(e, now) }
	for round := 0; round < s.cfg.MaxSweepRounds; round++ {
		sampled := 0
		cands := make([]*entry, 0, sampleK)
		sh.mu.RLock()
		for _, e := range sh.m { // Go 的 map 每次遍历起点都是随机的，取前 K 个当作随机样本
			if expired(e) {
				cands = append(cands, e)
			}
			if sampled++; sampled == sampleK {
				break
			}
		}
		sh.mu.RUnlock() // 放掉 shard 锁之后，再去碰 entry 的锁
		for _, e := range cands {
			s.tryEvict(sh, e, expired)
		}
		if len(cands)*4 <= sampled {
			return // 过期比例不超过 25%，结束这一轮
		}
	}
}

func (s *Store) idleExpired(e *entry, now int64) bool {
	ttl := s.cfg.IdleTTLSec
	if r := e.snap.Load(); r == nil || r == absent {
		ttl = s.cfg.NegativeIdleTTLSec
	}
	return now-e.lastAccess.Load() > ttl
}

// tryEvict 执行 dead 协议。拿到锁之后用 eligible 再检查一次：采样之后，它可能又被访问了。
func (s *Store) tryEvict(sh *shard, e *entry, eligible func(*entry) bool) bool {
	if !e.tryLockOp() {
		return false // 有 DB 操作正在进行
	}
	defer e.unlockOp()

	e.mu.Lock()
	ok := !e.dead && e.dirty == 0 && eligible(e) // eligible 只做 atomic 读，是纯内存操作
	if ok {
		e.dead = true // 同时持有 op 和 mu（I9）
	}
	e.mu.Unlock() // 先放掉 mu，保持 mu 是叶子锁
	if !ok {
		return false
	}

	sh.mu.Lock() // 仍然持有 op
	if sh.m[e.id] == e { // 防御性检查
		delete(sh.m, e.id)
		s.entryCount.Add(-1)
	}
	sh.mu.Unlock()
	return true
}
```

容量淘汰（`capacityEvictShard`）的流程相同：在 RLock 下取 K 个样本，放锁之后按 `lastAccess` 从早到晚排序，逐个调用 `tryEvict`（eligible 直接返回 true），直到这个 shard 淘汰够配额为止。如果容量已经超限，而样本里的 entry 都是脏的、无法淘汰，说明刷盘跟不上了，这时要靠背压（11.1）来限制增长。

**dead 协议的三条规则：**

1. **摘除者**：同时持有 op 和 mu，确认 `dirty == 0`，设置 `dead = true`，然后从 map 中删除。
2. **所有拿锁的人**：拿到 op 或 mu 之后，先检查 dead。如果是 dead，就放掉锁，重新 getOrCreate。
3. **无锁读者**：不检查 dead。它读到的是摘除那一刻的最新值，把线性化点放在摘除时刻即可，仍然是线性一致的。

- 从放掉 mu 到从 map 中删除之间的几微秒里，Update 可能拿到同一个 dead 的 e，空转几次（有 `Gosched` 缓解）。这是让 mu 保持叶子锁的代价，可以接受。
- `s.nowSec` 由一个后台 ticker 每秒更新一次。空闲 TTL 的精度是秒级，完全够用。

### 7.8 优雅关闭

1. API 层停止接收新请求。
2. 等待正在进行的 create/delete 完成（`s.inflight.Wait()`，此时 `bgCtx` 还没有取消）。
3. 循环刷盘，直到所有队列都为空、`dirtyCount == 0`，或者到达关闭截止时间。
4. 取消 `bgCtx`，退出。

如果关闭时还有未确定的 create/delete 被中断，这没有关系：重启之后内存是空的，所有数据都会从 DB 重新加载，自然和 DB 一致。

### 7.9 毒行：刷盘时被 DB 确定拒绝的行

这是 write-behind 固有的问题：update 已经向客户端确认成功了，但刷盘时 DB 明确拒绝了它（违反 CHECK 约束、外键、NOT NULL、长度超限、类型错误等）。这样的行无论重试多少次都会失败。而且在一个事务里批量写入时，只要有一行失败，整批都会回滚，一个毒行会拖垮同一批的所有行。

处理办法：

1. **尽量在内存里提前拒绝**：API 层构造 Change 时按 schema 校验类型、NOT NULL、varchar(n) 长度、整数范围。能在 update 时拒绝的，就不要留到刷盘时才发现。CHECK 约束（任意 SQL 表达式）和外键在 Go 里没法完整校验，所以要和业务方约定：通过本服务更新的列，尽量不要带 CHECK 约束和外键。
2. **隔离**：一整批"确定失败"时，把这一批拆成逐行写入（每行一个事务，或者使用 SAVEPOINT），找出确定失败的那几行，其他行正常写入。
3. **隔离区**：把毒行标记为 `poisoned`（受 mu 保护）。它继续保持脏状态，但不再进入普通批次，而是按较长的退避时间单独重试。因为后续的 update 有可能修正这个值，所以每次有新的 update 时也触发一次重试。同时上报告警指标 `poisoned_rows`，并记录 DB 返回的错误。
4. **绝不静默丢弃**：提供管理接口，由人来决定怎么处理：丢弃内存中的改动并从 DB 重新加载（等于撤销已经确认过的 update），或者修正数据后重试。
5. 毒行不会被淘汰，因为它是脏的（I10）。

---

## 8. DB 写入与 fencing（gen/ver）

### 8.1 根本问题：超时不等于没执行

Go 这边 ctx 超时，只说明我们不再等结果了，那条 SQL 可能还卡在 DB 的行锁等待或者网络上，之后照样会执行。这时如果释放 op，从另一条连接发出下一条 SQL，DB 实际的执行顺序就不再等于我们发出的顺序。

这不是 Go 的问题，而是分布式系统的基本事实：一个请求除了成功和失败之外，还有第三种结果，叫"**未知**"。

**进程内的顺序靠锁来保证，跨进程的顺序靠数据里携带的版本号来保证。** gen/ver 的作用就是 fencing token（见 DDIA 第 8 章）：由存储端来拒绝过期的写入，而不是依赖客户端的发送顺序。

per-key 串行化和 gen/ver **两者都要保留**：串行化处理正常情况，gen/ver 兜住超时这种少见但后果严重的情况。

### 8.2 三个反例（都是 gen/ver 要挡住的）

**例 1：没有 ver，旧值覆盖新值**

```
1. update score=10 → 内存 Ver=5
2. 刷盘 A：UPDATE SET score=10 WHERE id=42      （卡在 DB 里）
3. A 在 Go 侧超时 → 当作失败：并回掩码、重新入队、释放 op
4. update score=20 → 内存 Ver=6
5. 刷盘 B：UPDATE SET score=20 WHERE id=42      （立刻执行）DB: 20 ✓
6. A 终于执行 → DB: 10 ✗（内存是 20；重启之后用户的修改就丢了）
```

加上 `AND ver < 5` 后，A 影响 0 行。丢掉 A 是安全的，因为 A 的掩码已经并回去了，B 一定包含 A 的所有列，而且值更新。这正是失败时必须把 mask 并回去的原因。

**例 2：没有 gen，旧数据写进新对象**

```
1. 刷盘 A：UPDATE SET score=10, ver=5 WHERE id=42 AND ver<5   （卡住、超时）
2. delete 42 成功
3. 重新 create 42 → INSERT gen=200, ver=0                       成功
4. A 终于执行：新行 ver=0 < 5 → score=10 被写进新对象 ✗
```

加上 `AND gen = 100` 后，A 影响 0 行。gen 区分的是同一个 id 的不同生命，ver 区分的是同一个生命里的先后。

**例 3：迟到的 DELETE 删掉了新建的对象（直接违反"创建的东西绝不能消失"）**

```
1. DELETE A（连接 1）卡住，Go 侧超时
2. 重试 DELETE B（连接 2）成功 → 返回"删除成功"
3. 重新 create 42 → INSERT gen=200 成功 → 返回"创建成功"
4. A 终于执行 → 刚创建的对象被删掉了 ✗
```

加上 `AND gen = 100` 后，A 影响 0 行。

### 8.3 SQL 模板（PostgreSQL）

下面以示例表 `items(id, gen, ver, name, score)` 说明。实际的 SQL 根据 schema 动态生成（见 5.3），列名以配置为准。

```sql
-- 加载
SELECT id, gen, ver, name, score FROM items WHERE id = $1

-- 创建：ON CONFLICT DO NOTHING 不会修改已有的行，是允许的（禁止的是 DO UPDATE）。
-- 返回 0 行表示主键冲突，这时再 SELECT gen，判断是不是自己之前那次插入（同一个 gen）。
INSERT INTO items (id, gen, ver, name, score) VALUES ($1, $2, 0, $3, $4)
ON CONFLICT (id) DO NOTHING
RETURNING id, gen, ver, name, score   -- 返回整行，包括 DB 填充的默认值

-- 删除（幂等；影响 0 行也算成功）
DELETE FROM items WHERE id = $1 AND gen = $2
```

刷盘有两种写法，都只需要一次网络往返：

1. **pgx.Batch（建议先用这种）**：每行一条 UPDATE，只写 mask 中的列，放进一个 `pgx.Batch`，在一个事务里通过 pipeline 一次发出（`tx.SendBatch`）：

   ```sql
   UPDATE items SET score = $1, ver = $2 WHERE id = $3 AND gen = $4 AND ver < $2
   ```

2. **单条语句 + unnest（批量很大时）**：一次写整行。

   ```sql
   UPDATE items AS t
   SET name = v.name, score = v.score, ver = v.ver
   FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::bigint[])
        AS v(id, gen, ver, name, score)
   WHERE t.id = v.id AND t.gen = v.gen AND t.ver < v.ver
   ```

   因为内存是权威，而且只有本服务写这张表，所以写整行也是正确的。在 PG 中，UPDATE 本来就会重写整行（见 8.5），只写脏列节省的只是网络传输和 SQL 解析，并没有减少磁盘写入。

   但有两个例外，这时只能用 pgx.Batch：一是有数组类型列的表，因为 unnest 会把多维数组展开；二是有很大的 TOAST 列（大段 text、jsonb）的表，因为把一个没有改动的大值重新写一遍，PG 会重新写它的 TOAST 数据，代价很高。

- **绝对禁止** `INSERT ... ON CONFLICT DO UPDATE`（upsert）：迟到的刷盘会把已经删除的行"复活"。
- 同一批里的行按 id 排序后再写。每个 shard 只有一个 worker，create/delete 和刷盘又通过 op 串行化，所以不同事务之间本来就不会争抢同一行。按 id 排序是额外的保险，防止以后出现 DB 层的行锁死锁。

### 8.4 错误分类：`db.IsAmbiguous(err)`（pgx）

| 错误 | 分类 | 说明 |
|---|---|---|
| `pgconn.SafeToRetry(err) == true` | 确定失败 | pgx 保证语句还没有发出去 |
| `*pgconn.PgError`，SQLSTATE `23505` | 确定失败（重复键） | 映射为 `db.ErrDuplicate` |
| `*pgconn.PgError`，`57014`（被取消，包括 statement_timeout 触发） | 确定失败 | 语句已经回滚 |
| `*pgconn.PgError`，`55P03`（lock_timeout） | 确定失败 | 语句已经回滚 |
| `*pgconn.PgError`，`40001` / `40P01` | 确定失败 | 事务已经回滚，可以重试 |
| 其他 `*pgconn.PgError` | 确定失败 | 服务端明确返回了错误 |
| ctx 超时或取消、连接中断、`io.EOF`、其他网络错误 | **结果不确定** | 语句可能已经执行，也可能稍后才执行 |
| COMMIT 时出现网络错误 | **结果不确定** | 事务可能已经提交 |
| 拿不准的错误 | **结果不确定** | 所有写入都是条件写或幂等的，重试总是安全的 |

**用服务端超时，把大部分"不确定"变成"确定失败"。** Go 侧 ctx 超时是"结果不确定"最常见的来源。如果让 PG 服务端先超时，客户端收到的就是一个明确的错误（`57014` 或 `55P03`），语句已经确定回滚：

```sql
SET LOCAL statement_timeout = '2s';  -- 必须小于 Go 侧 ctx 的超时（比如 3s）
SET LOCAL lock_timeout = '1s';
```

可以在每个事务开头用 `SET LOCAL` 设置，也可以在角色级别设置（`ALTER ROLE <服务账号> SET statement_timeout = '2s'`）。角色级别最可靠，经过 PgBouncer 时也有效（见 8.5）。另外：

- PG 14 及以上版本可以设置 `client_connection_check_interval`：客户端断开之后，服务端能更早发现并中止正在执行的查询，进一步减少迟到执行。
- 这些措施只能**减少**结果不确定的情况，**不能消除**它：比如 COMMIT 已经成功了，但响应在网络中丢失。所以 gen/ver 和"重试到确定"仍然必须保留。
- ctx 取消时，pgx 会中断这条连接上的等待（取决于配置，可能还会向服务端发送取消请求），这条连接随后会被关闭。服务端的那条语句可能已经执行完，也可能还在执行。所以 Go 侧的 ctx 超时一律按"结果不确定"处理。

### 8.5 PostgreSQL 对设计的其他影响

1. **MVCC 写放大**：PG 的每次 UPDATE 都会写一个新版本的整行，旧版本变成死元组，等 VACUUM 回收。update 频率高时，会带来表膨胀和 autovacuum 的压力。对策：
   - **合并写是最主要的手段**：DB 的写入量约等于"脏行数 × 刷盘次数"，和 update 的次数无关。`FlushInterval` 越大，写放大越小，但崩溃时丢失的窗口也越大，需要权衡。
   - **让 UPDATE 走 HOT**（heap-only tuple，不需要更新索引）：被更新的列（包括 `ver`）都不建索引；表的 `fillfactor` 设为 70～90，在页内留出空间。HOT 更新的比例可以在 `pg_stat_user_tables.n_tup_hot_upd` 中观察。
   - 对这张表单独调低 autovacuum 的触发阈值（比如 `ALTER TABLE items SET (autovacuum_vacuum_scale_factor = 0.01)`），让它更频繁地运行、每次的工作量更小。
2. **RowsAffected 的语义**：PG 返回的是匹配并被更新的行数，用 `CommandTag.RowsAffected()` 判断 0 行还是 1 行是准确的。
3. **事务隔离级别**：默认的 READ COMMITTED 就够了。所有写入都是按主键的单行条件写，正确性靠 gen/ver 保证，不依赖隔离级别。
4. **连接池和 PgBouncer**：本服务自己用 `pgxpool` 维护连接池，实例数量也不多，**建议直接连接 PG，不经过 PgBouncer**。如果运维要求必须经过 PgBouncer（transaction 模式），设计不用改，但有几处配置要注意：
   - **预编译语句**：pgx 默认使用命名的预编译语句，在 transaction 模式下会出错。解决办法：把 PgBouncer 升级到 1.21 或更高版本，并打开 `max_prepared_statements`；或者让 pgx 使用 `QueryExecModeExec` / `QueryExecModeCacheDescribe`，不使用命名的预编译语句。
   - **会话级设置不可靠**：transaction 模式下，同一个客户端连接上的不同事务，可能落在不同的服务端连接上，所以 `SET statement_timeout` 和 pgxpool 的 `AfterConnect` 钩子都不能保证生效。超时改为在角色级别设置，或者在每个事务里用 `SET LOCAL` 设置。
   - ctx 超时会导致连接被关闭重建。DB 变慢时，连接重建会很频繁，连接池要留出余量。
5. **gen 的生成**：在 Go 侧生成非 0 的 63 位随机数即可，不需要用 PG 的 sequence，省一次往返。

---

## 9. 线性一致性与线性化点

**定义**：每个操作都在它的"请求到响应"这段时间内的**某一个瞬间**生效。按这些瞬间排出来的顺序，必须和一个单线程系统依次执行的结果一致，并且所有客户端看到的是同一个顺序。时间上有重叠的操作，谁先谁后都可以；但如果 A 的响应已经回来之后 B 才发出请求，A 就必须排在 B 前面。

- 写操作的线性化点 = 效果**开始对其他请求可见、并且不能再撤回**的那一刻，而且必须是一个原子的瞬间。
- 读操作的线性化点 = 它观察状态的那一刻。
- "开始执行"不等于"生效"。delete 正在执行时到达的 update，它的线性化点在 delete 的线性化点之前，所以 update 本来就排在 delete 前面。

| 操作 | 线性化点 |
|---|---|
| Get（命中） | `snap.Load()` |
| Get（读到已摘除的 entry） | 摘除的那一刻 |
| Update | `mu` 里的 `snap.Store(&nr)` |
| Create | DB 确定成功之后，`mu` 里的 `snap.Store(row)` |
| Delete | DB 确定成功之后，`mu` 里的 `snap.Store(absent)` |

所有线性化点都是对同一个 atomic 指针的原子操作，天然排成一个全局顺序。

---

## 10. 空档清单

"放锁之后、再次拿锁之前"的空档，是并发 bug 最集中的地方。每个空档都必须有对应的补救措施：

| 空档 | 期间可能发生什么 | 补救措施 |
|---|---|---|
| 放掉 shard.mu → 拿 entry 的锁 | entry 被淘汰 | 检查 dead |
| 排队等 op | 别人已经加载好了 | 二次检查 snap |
| Update 放掉 mu → load → 重来 | 任何事都可能发生 | 从头完整地再走一遍 |
| 刷盘取了快照 → 写 DB | 有新的 update 进来 | 重新入队；下一次刷盘要等 op |
| DB 超时 → 下一条 SQL | 旧 SQL 迟到执行 | gen/ver 条件写；create/delete 重试到结果确定 |
| tryEvict 放掉 mu → 从 map 删除 | Update 拿到 dead 的 e | Update 检查 dead，Gosched 后重试 |
| DB DELETE 执行中（只持有 op） | update 写入内存 | ②在 mu 里清掉 dirty |

新增代码如果引入了新的空档，必须把它加进这张表，并写清楚补救措施。

---

## 11. 运维

### 11.1 背压

- 脏行不能淘汰，所以内存占用的下限就是脏行数。DB 变慢时，脏行会越积越多。
- 当 `dirtyCount > MaxDirty` 时，新的 update 返回 `ErrBusy`（或者短暂等待）。对已经是脏行的再次更新不会增加积压，可以放行。

### 11.2 监控指标

- entry 总数、墓碑数、负缓存条目数
- `dirtyCount`、**最老脏行的年龄**：这就是崩溃时最多会丢失的数据窗口，必须配置告警
- 刷盘延迟、批大小、失败次数、`flush_noop_rows`
- load 错误次数、op 等待时长
- 结果不确定的 create/delete 次数、`ErrOutcomeUnknown` 次数
- 淘汰次数、命中率

线上可以做后台抽样巡检：随机抽一些干净的行和 DB 对比，发现不一致就告警。

### 11.3 多实例（以后再做）

- 既然内存是权威，就必须保证同一个 key 只有一个实例在写：按 key 路由（一致性哈希），再加租约（lease）。
- 切主时，旧实例可能还在刷盘。在条件写里再加一个 owner epoch（这才是 Kleppmann 原文中 fencing token 的场景），挡住旧实例的写入。
- 新的 owner 从 DB 加载数据。崩溃时丢失的窗口，就是原 owner 还没刷盘的那部分。

### 11.4 配置项

| 配置 | 含义 | 参考值 |
|---|---|---|
| `Tables` | 要管理的表名单；IdleTTL、NegativeIdleTTL、MaxEntries 等可以按表覆盖全局默认值 | — |
| `GenColumn` / `VerColumn` | gen、ver 的列名 | `row_gen` / `row_ver` |
| `NumShards` | 每张表的 shard 数量，取 2 的幂 | 256 |
| `FlushInterval` | 刷盘间隔；决定崩溃时最多丢多少数据，也决定 PG 的写放大 | 100ms～1s（待定） |
| `FlushBatchMax` | 每批最多写多少行 | 500 |
| `IdleTTL` | 空闲多久之后从内存清除 | 待定 |
| `NegativeIdleTTL` | 墓碑和负缓存条目的空闲 TTL | 比 IdleTTL 短，比如 1～5 分钟 |
| `EvictInterval` | 空闲抽样的间隔 | 1s |
| `MaxSweepRounds` | 每次抽样最多连续几轮 | 16 |
| `MaxEntries` | 每张表的 entry 数量上限（高水位），由内存预算决定 | 待定 |
| `MaxDirty` | 脏行数上限，超过就背压 | 待定 |
| `LoadErrTTL` | 加载失败之后，多长时间内直接返回同一个错误 | 100ms |
| `DBTimeout` | Go 侧 DB 调用的超时 | 3s |
| `StatementTimeout` / `LockTimeout` | PG 服务端超时，必须小于 `DBTimeout`；建议在角色级别设置 | 2s / 1s |
| `QueryExecMode` | pgx 的执行模式；经过 PgBouncer 时见 8.5 | 默认 |
| `SchemaCheckInterval` | 后台对比系统目录和已加载 schema 的间隔 | 1min |

---

## 12. 测试计划与里程碑

### 12.1 测试手段（按投入从小到大）

1. **`go test -race`**：所有测试一直开着。它只能发现数据竞争，发现不了逻辑错误。
2. **故障注入的假 DB（`fakedb`）**：
   - 语义和真实 SQL 一致：条件 UPDATE/DELETE、主键唯一约束
   - 可以注入：随机延迟、随机的确定失败、连接错误（结果不确定）、在执行前或执行后失败
   - **能模拟"超时但之后才执行"**：立刻向调用方返回 ctx 超时，但这条语句在之后某个随机时刻才在 DB 中生效。这是最重要的一项，真实的 DB 很难稳定复现这种情况
   - 按顺序记录所有实际生效的语句，方便事后检查
   - 可以注入"确定失败"（比如模拟违反约束），用来测试毒行的隔离（7.9）
3. **高争用压力测试**：用很少的 key（比如 4 个）、很多 goroutine（比如 64 个），随机执行增删查改。结束之后优雅关闭并全部刷盘，然后对比内存和 DB：每一行完全一致（包括 gen、ver），absent 当且仅当 DB 中不存在。
4. **Porcupine 线性一致性检查**（`github.com/anishathalye/porcupine`）：
   - 记录每个操作的请求时间、响应时间和返回值，按 key 分区检查
   - 顺序模型：每个 key 的状态是 `{exists, gen, cols}`；Update 按 mask 修改列
   - 结果未知的操作（`ErrOutcomeUnknown`）：把它的返回时间设为历史的末尾，模型对这类操作接受"已发生"和"未发生"两种情况
   - `ErrBusy` 按"确定没有发生"处理
5. **崩溃恢复测试**：在压力测试进行中丢弃整个 Store，用同一个 fakedb 新建一个 Store。检查：已确认的 create 仍然存在（除非之后又被确认删除）；已确认 delete 的数据不会重新出现（除非之后又被重新创建）；只允许丢失最近的 update。
6. **failpoint 测试**：在关键的空档处埋测试钩子（生产环境中为 nil），强制制造特定的交错：
   - DB DELETE 成功之后、`Store(absent)` 之前，插入一个 Update
   - 刷盘取完快照之后、写 DB 之前，插入 Update 和 Delete
   - tryEvict 设置 dead 之后、从 map 删除之前，插入 Update
   - 刷盘超时、之后迟到执行，期间插入 delete 加 create
   - 空闲抽样选出候选之后、tryEvict 之前，插入 Get/Update（验证 eligible 的二次检查和 dead 协议）
7. **`testing/synctest`**（Go 1.25+）：用虚拟时间测试刷盘间隔、超时、IdleTTL、NegativeIdleTTL、LoadErrTTL 这类和时间相关的逻辑（`nowSec` 的 ticker 在 synctest 中同样使用虚拟时间）。
8. **（可选）TLA+ 模型**：把核心协议（per-key 状态、刷盘、会迟到执行的 DB）写成一个小模型，比如 2 个客户端、1 个 key，用 TLC 穷举所有交错。

### 12.2 里程碑

| 里程碑 | 内容 | 验收标准 |
|---|---|---|
| M1 | 项目骨架、DB 接口、schema 抽象（fakedb 中直接构造 schema）、主键编码、fakedb（先不加故障注入）、shard/entry、Get + load | `-race` 单元测试通过；主键编码覆盖整数、字符串、UUID、复合主键 |
| M2 | Update + 单 key 刷盘 + 刷盘队列 | 压力测试通过；结束后内存和 DB 一致 |
| M3 | Create/Delete（含 runSync、结果不确定时重试到确定、gen/ver 条件写）；fakedb 加上故障注入 | 压力测试加故障注入通过；第 8.2 节三个反例的 failpoint 测试通过 |
| M4 | Porcupine 线性一致性测试 | 长时间随机运行没有违反 |
| M5 | 淘汰：空闲 TTL 抽样清除、容量上限淘汰、dead 协议 | failpoint 测试加压力测试通过；压力测试中把 IdleTTL 设得很短，让淘汰和读写高频交错 |
| M6 | 批量刷盘、毒行隔离（7.9）、背压、优雅关闭、监控指标、崩溃恢复测试 | 崩溃恢复测试通过；注入确定失败时，毒行被隔离，同一批的其他行正常写入 |
| M7 | PostgreSQL 适配（pgx、pgxpool、服务端超时）、从系统目录加载 schema、类型映射与校验、`IsAmbiguous` 分类、schema 漂移检测、API 层 | 针对真实 PG 的集成测试通过（可以用 testcontainers-go 启动 PG）；检查 HOT 更新的比例 |

---

## 13. 待确认问题（开工前请向用户确认）

**已经确认：**

- 内存放不下全量数据，只保留工作集，按空闲 TTL 清除（见 7.7）。IdleTTL、内存预算都是配置项。
- 数据库是 PostgreSQL（见 8.3～8.5）。
- 表结构是动态的，运行时从系统目录读取；表上必须有 gen、ver 两列（见 5.3）。
- 主键不限制为整数：内部统一编码成 `Key`，支持整数、字符串、UUID、复合主键（见 5.3）。

**还需要确认：**

1. **5.3 中支持的类型列表够用吗？** 业务表里有没有数组、枚举、复合类型这类列？
2. **需要支持由 DB 生成的主键吗？**（serial / identity；v1 默认不支持，要求 create 时由调用方提供主键）
3. **业务方能接受 5.3 和 7.9 中的约定吗？** 只有本服务写这些表；不使用会修改行的触发器；通过本服务更新的列，尽量不带 CHECK 约束和外键；表结构变更需要重启服务。
4. **对外的 API 协议是什么？**（gRPC 还是 HTTP）返回给客户端的是否需要包含 gen/ver？
5. **刷盘间隔和批大小设多少？** 也就是可以接受丢多长时间的数据（这同时影响 PG 的写放大）。
6. **PostgreSQL 是什么版本？实际部署时会经过 PgBouncer 吗？**（`client_connection_check_interval` 需要 PG 14 或更高版本；建议直接连接，见 8.5）
7. **现在是单实例，还是需要支持多实例？**
8. **Update 是否需要带 gen？** 也就是要不要区分同一个 key 的不同生命。
9. **Update 有没有"自增"这类读改写语义？**
10. **delete 进行中到达的 update，是否必须失败？**（默认允许成功，见 15 节）
11. **Go 是什么版本？**（`testing/synctest` 需要 1.25 或更高）

---

## 14. 设计方法论（锁设计原则）

### 14.1 核心思想

**锁保护的是不变式，锁的形状由操作的延迟要求决定。** 锁是从需求推导出来的：需求一变，锁就得跟着变。最初的缓存版本设计中，一把 per-key 锁就够了，因为那时写操作本来就要等 DB；改成 write-behind 之后，update 不能再等 IO，这把锁就必须拆开。

### 14.2 可复用的八个步骤

1. 列出所有操作，写清楚它们的延迟要求和一致性要求。
2. 列出不变式，为每条不变式找一把守护它的锁。说不清楚守护什么的锁，要么是多余的，要么放错了位置。
3. 为每条不变式找到最小的保护范围（空间粒度）。
4. 看每个范围内最慢的操作是什么（时间粒度）。**范围大的锁，持有时间必须短；要跨 IO 的锁，范围只能缩到一个 key。**
5. 延迟要求不同的操作，拆到不同的锁上。
6. 读路径尽量无锁：用不可变数据加原子发布。
7. 定好锁顺序（见 14.3），每次拿锁之后重新验证状态。
8. 跨出进程的部分（DB、网络）不靠锁，改用版本号（fencing token）。

### 14.3 锁的粒度是二维的

|  | 持有时间短 | 持有时间长（跨 IO） |
|---|---|---|
| **范围大**（很多 key） | ✓ shard.mu | ✗ 绝对禁止 |
| **范围小**（一个 key） | ✓ e.mu | ✓ e.op |

### 14.4 怎么确定锁顺序

1. **画锁依赖图**：检查每一条代码路径，"持有 A 时，要不要去等 B？"如果要，就画一条 A→B 的边。只有"拿着一把锁去等另一把"才算边；先后各拿一次、中间不重叠的不算。
2. **图里没有环，就不会死锁**。对图做拓扑排序，就得到了锁的顺序。证明：如果有环，沿着环走一圈，等级会一路严格递增，最后回到自己，矛盾。
3. **如果某条边会造成环，就拆掉它**：
   - 先放锁，再去拿另一把，代价是拿到之后必须重新验证（dead 协议本质上就是"放锁的代价"）
   - 用 TryLock：从不等待，就不会出现在环里
   - 调整结构，让这条路径不需要同时持有两把锁
4. **选择边的方向时的经验**：
   - **叶子锁放在最里层**：持有期间不拿别的锁、不阻塞、不做 IO。叶子锁可以在任何地方拿，不会造成死锁（它没有出边，不可能构成环）；持有时间可以预测；可以和别的代码组合使用。
   - 持有时间长、需要嵌套别的锁的，放在外层。
   - 同一级别的多把锁，按一个稳定的 key 排序后再加锁（比如转账时永远先锁 id 小的账户）。
   - 树形结构中，先锁父节点再锁子节点（前提是子节点的锁持有时间很短）。
5. **锁的顺序不是由粒度粗细决定的，而是由"谁需要拿着谁去等谁"决定的。** 粗粒度的锁不一定在外层。shard.mu 范围最大，却适合做最内层的叶子锁。
6. **叶子锁的身份要写进注释并且主动维护**。最常见的破坏方式是"隐藏的锁"：在临界区里调用的函数内部又加了锁（比如打日志）。
7. **Go 特有的陷阱**：持有锁时做阻塞的 channel 操作（channel 也相当于一把锁）；`sync.Mutex` 不可重入；对 RWMutex 递归加读锁（中间如果有写者在排队，就会死锁）。
8. **排查工具**：`-race` 发现不了死锁。局部死锁用 `/debug/pprof/goroutine?debug=2` 查看所有 goroutine 卡在哪里；开发阶段可以把锁换成 `github.com/sasha-s/go-deadlock`，它会检测锁顺序反转。Linux 内核的 lockdep 也是同样的思路。
9. **另一派做法**：不预防，而是检测，然后回滚（数据库就是这样做的，因为事务要锁哪些行事先并不知道）。在服务代码里，要拿哪些锁在写代码时就能确定，所以用顺序来预防更简单、更可靠。

### 14.5 "先放手、再验证"与 crabbing

- **Crabbing（交替加锁）**：锁父 → 锁子 → 放父。持有时间重叠，子节点不可能在中间被摘掉，不需要验证。代价是两把锁形成了嵌套，而且子锁上的争抢会扩散到父锁。
- **先放手、再验证**：锁父 → 放父 → 锁子 → 检查 dead。中间有空档，靠事后验证来补救。查找路径上两把锁不嵌套，淘汰路径才能放心地反过来加锁。
- 在没有 GC 的语言里，空档期间对象可能已经被释放（use-after-free），所以那边要么用 crabbing，要么用引用计数保活（Linux 的 lockref 就是"引用计数 + dead 标记"）。Go 有 GC，所以"先放手、再验证"是安全的。

### 14.6 dead 协议是通用技术

它要解决的问题：通过共享的索引找到对象，再去锁对象，"找到"和"锁上"之间存在时间窗口。通用写法是：

```
for {
    obj := 查索引()
    lock(obj)
    if obj 仍然有效 { break }
    unlock(obj)
}
```

同样的做法出现在：Java ConcurrentHashMap 的 `putVal`（锁住桶头之后，检查它是否还是桶头）、Go `sync.Map` 的 `expunged` 哨兵、Linux 的 `lockref_mark_dead()` 和 `I_FREEING`、Harris 无锁链表的"先标记、再摘除"。

### 14.7 逻辑删除与物理删除分离

先打标记（墓碑），让所有人都能看到"已删除"；之后再在安全的时机（淘汰时，走 dead 协议）真正回收。这等于把并发中最难处理的"结构变化"（从 map 中插入或删除）变成了简单的"值变化"（替换指针）。结构变化只剩下 getOrCreate 和淘汰两处，并且都被集中隔离起来。

### 14.8 锁只在进程内有效

锁能保证的只是自己内存里的顺序。一旦跨出进程，就会出现"结果未知"。面对"未知"，有两条路：

1. 等到结果确定（KILL 掉连接，再确认它已经消失）：可以做到，但很慢、很繁琐，而且依赖具体的数据库。
2. 让存储端拒绝过期的请求（fencing token，也就是 gen/ver）：不管网络怎么乱，结果都是对的。本设计选择这一条。

### 14.9 怎么保证并发程序写对

- 锁的作用是**减少交错**：不需要考虑每行代码之间的交错，只需要考虑临界区之间的先后顺序，以及放锁之后的空档。
- 把空档做少，并且全部列出来（第 10 节）。
- 用不变式、线性化点和状态表来推理，而不是去枚举所有交错。
- 用工具去找你没想到的情况（第 12 节）：TLA+ 检查设计（穷举，但只针对模型），Porcupine 检查实现（真实代码，但只覆盖测试跑到的交错）。两者互补。

---

## 15. 关键权衡

| 决策 | 选择 | 放弃了什么 / 代价 | 理由 |
|---|---|---|---|
| Update 的持久化方式 | write-behind（先改内存再异步刷盘） | 崩溃时会丢失刷盘窗口内的 update | update 频率很高，必须快；这部分丢失可以容忍 |
| Create/Delete 的持久化方式 | 同步写，结果确定才返回 | 延迟较高 | 创建的东西不能消失，删除的东西不能复活 |
| 读路径 | copy-on-write，用不可变快照加 atomic 发布 | 每次 update 都要分配内存，带来 GC 压力 | 读完全无锁，热点 key 上也能线性扩展。如果压测发现分配是瓶颈（写远多于读、行又很大），再考虑 copy-on-read（RWMutex 原地修改、读时复制） |
| 锁的拆分 | op（跨 IO）和 mu（叶子锁）分开 | 多了一把锁 | update 永远不用等 IO |
| 查找方式 | 先放手、再验证（dead） | 需要 dead 协议 | 不让热点 key 的争抢扩散到整个 shard；shard.mu 保持叶子锁 |
| mu 是否做成叶子锁 | 是（淘汰时先放 mu 再拿 shard.mu） | 淘汰的几微秒里，Update 可能空转 | 规则简化为"只有 op 可以嵌套" |
| 删除方式 | 墓碑 + 延迟物理删除 | 墓碑占用内存，需要 TTL 清理 | 已经拿着指针的人能马上看到；负缓存；delete 的路径简单 |
| delete 进行中到达的 update | 允许成功，delete 成功后一起丢弃 | 客户端可能会觉得意外（update 成功了，数据随后被删掉） | 线性一致；不增加状态；update 不变慢。另外两种选择：返回可重试错误（多一种错误要处理），或者等 delete 完成（update 变慢） |
| 串行化和 gen/ver | 两者都保留 | 多两个列和 WHERE 条件 | 串行化处理正常情况，gen/ver 兜住超时；迟到的写会造成永久性的错误，而且会违反硬性需求 |
| create/delete 结果不确定时 | 持有 op 重试到结果确定 | DB 长时间不可用时，这个 key 的其他 DB 操作会被阻塞 | SELECT 确认不可靠（挡不住迟到执行）；gen 让 INSERT 重试变成幂等 |
| 刷盘写什么 | 默认只写 mask 中的脏列（pgx.Batch）；批量很大时可以写整行（unnest） | 需要维护位图 | 网络传输和解析更少。在 PG 中 UPDATE 总是重写整行，减少磁盘写放大主要靠合并写 |
| 内存模式 | 工作集：空闲 TTL 清除 + 容量上限 | 冷数据第一次访问要查 DB | 数据量放不下内存 |
| 淘汰算法 | Redis 式随机抽样（过期 + 近似 LRU） | 过期清除不精确，会稍晚一些 | 每次持锁时间有上限；命中路径只写一个粗粒度时间戳，不用维护 LRU 链表 |
| 服务端超时 | statement_timeout / lock_timeout 小于 Go 侧超时 | 需要仔细配置两层超时 | 把大部分"结果不确定"变成"确定失败"，减少重试 |
| 主键类型 | 统一编码成 `Key`（string），支持整数、字符串、UUID、复合主键 | string 作为 map key，哈希比 int64 稍慢 | 不限制业务方；性能差距和 DB 往返相比可以忽略，必要时再给单列整数主键加快速路径 |
| DB 生成的主键 | v1 不支持，要求调用方提供主键 | 业务方要自己生成主键（UUID、snowflake 等） | 结果不确定时，重试必须能确定是不是同一行；要支持的话需要在 gen 上建唯一索引 |
| 表结构变更 | v1 需要重启服务；后台检测漂移并告警 | 改表需要安排停机或者滚动重启 | 运行期间 schema 不可变，推理简单；在线切换 schema 需要按表暂停写入、刷盘、清空，复杂度高 |
| 毒行 | 提前校验 + 逐行隔离 + 隔离区 + 告警，绝不静默丢弃 | 已经确认过的 update 有可能需要人工处理 | write-behind 加上 DB 约束，必然会出现"确认之后才失败"的情况，只能尽量减少并且让它可见 |
| PgBouncer | 建议直接连接 PG | — | 服务自己有连接池；少一层就少一个"结果不确定"的来源，也不用处理预编译语句和会话设置的兼容问题 |

---

## 16. 参考资料

- Martin Kleppmann，《Designing Data-Intensive Applications》：第 8 章 fencing token，第 9 章线性一致性
- Herlihy & Wing, *Linearizability: A Correctness Condition for Concurrent Objects*, 1990
- Porcupine：`github.com/anishathalye/porcupine`
- Hillel Wayne，《Practical TLA+》；Leslie Lamport 的 TLA+ 视频课程
- Go 源码：`sync/map.go`（`expunged` 哨兵）
- Java 源码：`ConcurrentHashMap.putVal`（加锁之后验证）
- Linux 内核：`lib/lockref.c`（`lockref_mark_dead`）、lockdep
- Timothy Harris, *A Pragmatic Implementation of Non-Blocking Linked-Lists*, 2001
- `github.com/sasha-s/go-deadlock`
