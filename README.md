# Depurge

面向**并发控制科研**的以太坊主网交易离线重放沙盒。从预制 dataset 目录读取真实交易数据和完整状态快照（witness），在基于 go-ethereum `core/vm` 的内存 EVM 中按原始顺序离线重放，精确采集每笔交易的执行耗时与 **slot 级读写集**（含深层嵌套子调用），以结构化 JSONL 格式输出，供后续科研分析。

## 核心特性

- **离线重放**：从 dataset 读取区块（header + transactions + witness），在内存 StateDB 中按 `stateAnchor=pre-first-user-tx` 语义重放，同区块内交易顺序提交状态，区块间独立初始化。
- **slot 级读写集采集**：自定义 StateDB + `tracing.Hooks` 双层埋点（方案 D），默认采集 storage slot / balance / nonce 的读写（含值与旧值），粒度可通过 CLI 调整。
- **深层嵌套调用树**：追踪 CALL/CALLCODE/DELEGATECALL/STATICCALL/CREATE/CREATE2 全部子调用，读写集按 `frame_id` 父子关联组织为 `call_tree`，支持子调用级冲突分析。
- **per-tx 无锁 Recorder**：每笔交易独立 Recorder 实例，交易结束 Freeze，天然无竞争，不污染并发实验加速比。
- **双轨输出**：`call_tree`（树形，并发研究核心）+ `flat_rwset`（对齐 dataset 自带 rwsets，baseline 对比）。
- **耗时精确记录**：纳秒级 EVM 执行打点，支持 `--runs N` 每区块整管线多轮运行取平均（串行 / Vegeta / Depurge 共用，摊平测量噪声）。
- **真 MPT 树**：可选挂载 go-ethereum 真 trie，在每笔交易/区块结束时重算 state root 与 storage root，真实模拟链上 MPT 树更新开销。
- **链上三段耗时**：每笔交易分别计时 EVM 执行（`ApplyMessage`）、MPT 树更新（`CommitMPT`）、receipt 构建（`GetLogs` + `CreateBloom`），支持与链上完整耗时口径对比。

## 构建

```bash
go build -o depurge ./cmd/depurge
```

要求 Go 1.22+，依赖 go-ethereum v1.15.11（支持 EIP-7702 delegation）。

## 用法

### 串行重放（`--replay-serial`）

```bash
# 重放整个 dataset（默认同时运行串行与 vegeta；depurge 默认关闭，见算法开关）
./depurge replay --dataset datasets/test-24000000-24000009

# 只跑串行执行算法
./depurge replay --dataset datasets/test-24000000-24000009 \
  --replay-serial --replay-vegeta=false

# 重放指定区块范围，并与链上 canonical 对比
./depurge replay --dataset datasets/test-24000000-24000009 \
  --blocks 24000000-24000002 --compare

# 每区块整管线多轮运行取平均（串行 / vegeta / depurge 共用，摊平测量噪声）
./depurge replay --dataset datasets/test-24000000-24000009 \
  --blocks 24000000 --runs 5

# 纯性能基准（跳过读写集采集）
./depurge replay --dataset datasets/test-24000000-24000009 --no-record

# 账户级粒度（粗粒度分区实验）
./depurge replay --dataset datasets/test-24000000-24000009 \
  --rwset-granularity account

# 每笔交易提交一次 MPT（pre-Byzantium 老语义）
./depurge replay --dataset datasets/test-24000000-24000009 --mpt-per-tx
```

### Vegeta 并行执行（`--replay-vegeta`）

```bash
# 只跑 Vegeta 并行算法（预执行猜读写集 → 贪心聚簇 → 冲突 DAG → 波次并行验证 → 串行兜底）
./depurge replay --dataset datasets/test-24000000-24000009 \
  --blocks 24000000 --replay-serial=false --replay-vegeta

# 调 worker 数与定边/兜底顺序
./depurge replay --dataset datasets/test-24000000-24000009 \
  --blocks 24000000 --replay-serial=false \
  --parallelism 10 --edge-order new --serial-order block \
  --filter-nonce --filter-coinbase --runs 5
```

### Depurge 并行执行（`--replay-depurge`）

针对 Vegeta 两大缺点的改进算法：①预执行读写集偏移 → abort → 串行退化；②波次调度的
straggler（慢交易拖整波）。算法四步：

1. **step1 保守读写集获取（三臂）**：为每笔交易获取 `spec_rw`，获取失败或为空的交易进 step4。
2. **step2 建图**：以读写集 key 为队列，交易按原始序入队（不区分读/写集）。
3. **step3 事件驱动并行**：交易在其所有相关 key 队列都处于队头时就绪，多个就绪交易并行执行
   （**无波次屏障**，提交即解锁后继，慢交易只拖其后继不拖无关交易）。执行得到真实读写集
   `real_rw`，须满足 `spec_rw ⊇ real_rw` 才可提交，否则 abort。队列中 B 在 A 之后，则 B
   必然看到 A 提交后的状态（B 解锁时 A 已合并进共享状态）。
4. **step4 串行兜底**：step1 失败/空集 + abort 的交易，按原始序基于并行提交后的状态串行执行。

三臂（`--spec-arm`）决定 step1 的读写集来源：

| 臂 | 策略 | 说明 |
|----|------|------|
| `A` | LLM 优先 + 回退 | LLM 静态分析成功的交易用 LLM 集调度，失败的回退预执行集 |
| `B` | 并集 | 预执行读写集 ∪ LLM 读写集（LLM 失败则仅预执行） |
| `C` | 纯预执行（默认） | 等价于 vegeta 的读写集来源，对照组 |

A/B 臂依赖 `llm/mainnet_rw`（`--llm-dir`）。LLM 只产 storage slot，桥接层会统一 key 格式
（`slot:<小写>:<slot>` → recorder 的 `storage:<EIP-55>:<slot>`）并合成 sender balance key
（gas 真实调度键）。LLM 失败逐类统计：`not-contract`（EOA 调用）/`no-contract`（无分析）/
`no-selector`/`decode-fail`/`no-storage`（缺 storage 布局）/`unresolved`（含动态键）/`empty`。

```bash
# 只跑 Depurge（C 臂 = 纯预执行读写集）
./depurge replay --dataset datasets/mainnet-21498532-21499531 \
  --blocks 21498532-21498536 --replay-serial=false --replay-vegeta=false --replay-depurge

# A 臂（LLM 优先 + 回退）与 B 臂（并集）
./depurge replay --dataset datasets/mainnet-21498532-21499531 \
  --blocks 21498532-21498536 --replay-depurge --spec-arm A --llm-dir llm/mainnet_rw

# 三算法同跑（串行 → vegeta → depurge），多轮取平均
./depurge replay --dataset datasets/mainnet-21498532-21499531 \
  --blocks 21498532 --replay-depurge --spec-arm B --runs 5
```

### 预执行 vs 串行执行读写集差异（`TestPreExecuteVsSerialRWSet`）

对比同一区块在两种执行模式下的每笔交易读写集差异（`internal/replay/preexec_test.go`）：

- **串行**：`replayBlock`，交易共享状态，前一笔的提交影响后一笔。
- **预执行**：`PreExecute`，每笔交易从同一份 witness 初始状态独立执行（独立快照 + 独立满额 GasPool + 跳过 nonce 校验）。

对每笔交易做 `FlatReadKeys` / `FlatWriteKeys` 的集合差运算，输出：
- `serial-only`：仅串行执行读到/写到的 key
- `pre-only`：仅预执行读到/写到的 key

每笔差异交易最多展示 10 个 key（`maxKeysShown`），并按区块与全局汇总不一致交易数。

```bash
# 默认 dataset（datasets/test-24000000-24000009），全部区块
go test ./internal/replay/ -run TestPreExecuteVsSerialRWSet -v

# 指定 dataset 与区块范围（注意 -args 之后的 flag 传给测试二进制）
go test ./internal/replay/ -run TestPreExecuteVsSerialRWSet -v \
  -args -dataset datasets/mainnet-21498532-21499531 -blocks 21498532-21498541
```

测试 flag：`-dataset`（默认 `../../datasets/test-24000000-24000009`）、`-blocks`（如 `24000001-24000003` 或单个块号，空则全部）。读写集采集口径与主流程一致（同一套 `AccessRecorder`：slot + balance + nonce）。

### LLM 读写集 soundness 评测（`llmsoundness`）

独立工具（`cmd/llmsoundness` + `internal/llmsoundness`），用 dataset 自带的 **canonical rwsets**
（链上真实读写，ground truth）评测 `llm/mainnet_rw/` 里 **LLM 静态分析**产出的读写集的保守度：

- **recall（召回）**：canonical 实际访问的 storage key，有多少被 LLM 声明覆盖。`1-recall = 漏报率`。
- **precision（精确）**：LLM 声明的 key，有多少实际被访问。`1-precision = 多报率`。

LLM 分析以抽象声明 `(account, field)` 给出（如 `{account:"msg.sender", field:"balances"}`）。
评测核心是把抽象声明**实例化**成具体 slot key（`slot:<lower-addr>:<slot>`，与 dataset 对齐），
再与 canonical 做集合对比。实例化支持：

- **嵌套 mapping 链式解析**：LLM 对同一 field 按 key 层级「由外到内」输出多条记录，按 mapping
  深度分块、逐块链式计算 `keccak(pad(k_i) || pad(prev))`（如 USDT `allowed[from][msg.sender]`）。
- **struct 展开**：mapping/inplace 命中 struct 时，展开全部成员 word slot（去重）。
- **原始类型兜底**：`t_bool`/`t_uintN`/`t_address` 等原始标量常不出现在 types 表，按 base slot 发。
- 仅解析 `t_address` 类型的 key；非地址 key（tokenId/poolId 等）静态不可知，记 `dynamic-key` unresolved。

```bash
go run ./cmd/llmsoundness \
  --llm-dir llm/mainnet_rw \
  --dataset datasets/mainnet-21498532-21499531 \
  --blocks 21498532-21498541
```

| 参数 | 默认 | 说明 |
|------|------|------|
| `--llm-dir` | `llm/mainnet_rw` | LLM 分析目录（每合约一个子目录） |
| `--dataset` | （必填） | dataset 目录（提供 canonical rwsets） |
| `--blocks` | 全部 | 区块范围，如 `21498532-21498541` |
| `--min-tx` | `0` | 只报命中交易数 ≥ N 的合约 |

**双口径指标**：报告同时给出「全部合约」与「仅带 storage 布局」两套全局指标。缺 `storage.json`
的合约所有 field 都无法实例化（必然全漏报），单列以免拉低整体解读；逐合约表有 `sto` 列标记。

**unresolved 分类**：`unknown-field`（LLM 声明的 field 不在 storage 布局，含把函数名当 field 的
幻觉）、`unknown-type`、`dynamic-key`（key 静态不可知，如 tokenId）、`partial-chain`（嵌套 mapping
欠声明，只给了部分层级的 key）。

**已知限制**（属静态分析 + LLM 输出格式的本质边界，非实现 bug）：

- 动态 key（`_positions[tokenId]` 的 tokenId、poolId 等）无法静态解析 → 漏报。典型如 NPM。
- LLM 对 nested mapping 欠声明（只给外层 key，如 WBTC `allowed`）→ `partial-chain` 漏报。
- `multicall` 的 LLM 分析是内层调用的保守并集且全标 `global` → 大量 `dynamic-key`/`partial-chain`。
- precision 偏低多为**保守过近似**：LLM 按源码声明了 `paused`/`deprecated` 等读，但具体执行路径
  未触发（实测 USDT extra 集中于这类，slot 实例化本身正确）。
- `owner`/`params.*`/`opInfo.*` 等 account 符号暂不解析（需运行时状态或 struct 参数解码，收益低）。

单元测试 `internal/llmsoundness/instance_test.go` 锁定链式 slot、struct 展开、原始类型兜底、
非地址 key 四个关键行为。

### CLI 参数

| 参数 | 默认 | 说明 |
|------|------|------|
| `--dataset` | （必填） | dataset 目录路径 |
| `--output` | `<dataset>/results` | 结果输出目录 |
| `--blocks` | 全部 | 区块范围，如 `24000000-24000005` |
| `--replay-serial` | `true` | 是否运行串行执行算法（EVM/MPT/receipt 耗时） |
| `--replay-vegeta` | `true` | 是否运行 Vegeta 并行算法（各阶段耗时 + state-diff） |
| `--replay-depurge` | `false` | 是否运行 Depurge 并行算法（事件驱动调度 + state-diff） |
| `--spec-arm` | `C` | Depurge step1 读写集获取臂：`A`=LLM 优先+回退；`B`=并集；`C`=纯预执行 |
| `--llm-dir` | `llm/mainnet_rw` | Depurge A/B 臂的 LLM 静态分析目录 |
| `--runs` | `1` | 每区块整管线重复轮数，串行 / vegeta / depurge 共用（>1 取平均） |
| `--compare` | `false` | 与 dataset 自带 canonical/rwsets 对比 |
| `--no-record` | `false` | 跳过读写集采集（纯性能基准） |
| `--rwset-granularity` | `slot` | 读写集粒度：`slot` / `account` |
| `--collect-balance` | `true` | 是否采集 balance 读写 |
| `--collect-nonce` | `true` | 是否采集 nonce 读写 |
| `--mpt-per-tx` | `false` | MPT 提交口径：`true`=每笔交易提交（pre-Byzantium）；`false`=区块结束统一一次（现代主网语义） |
| `--parallelism` | `0`（=NumCPU） | worker 数，vegeta（预执行与波次验证）与 depurge（预执行与事件驱动调度）共用 |
| `--edge-order` | `new` | Vegeta DAG 冲突边定向：`new`=聚簇序；`original`=原始区块序 |
| `--serial-order` | `block` | Vegeta 串行兜底顺序：`block`=原始区块序；`hash`=交易哈希字典序 |
| `--filter-nonce` | `true` | 建图/验证时过滤 nonce 伪冲突 key（vegeta 与 depurge 共用） |
| `--filter-coinbase` | `true` | 过滤 coinbase 的 balance tip 写 key（vegeta 与 depurge 共用） |

## Dataset 格式

```
<dataset>/
├── manifest.json          # formatVersion/chainId/fromBlock/toBlock/stateAnchor/executionMode
├── blocks/
│   └── <blockNum>.json.zst   # 每区块一个 zstd 压缩 JSON
└── code/                  # （空，合约代码内联在 witness.accounts[].code）
```

区块 JSON 顶层结构：
- `header`：baseFeePerGas/beneficiary/gasLimit/number/prevRandao/timestamp 等
- `transactions`：原始签名交易数据（Legacy/2930/1559/4844）
- `witness`：`{accounts: {addr: {balance, nonce, codeHash, code, storage}}}`
- `canonical`：`{receipts: [{txHash, txIndex, status, gasUsed, logsCount}]}`
- `rwsets`：扁平读写集 `[{txHash, txIndex, readKeys[], writeKeys[]}]`

## 输出格式

每区块一个 `results/<blockNum>.jsonl`，一行一笔交易：

```json
{
  "tx_hash": "0x...",
  "tx_index": 1,
  "block_number": 24000000,
  "status": 1,
  "gas_used": 122401,
  "elapsed_ns": 185667,
  "mpt_ns": 2341,
  "receipt_ns": 512,
  "runs": [361458, 179875, 185667],
  "call_tree": {
    "frame_id": "f0",
    "type": "ROOT",
    "depth": 0,
    "children": [
      {
        "frame_id": "f1",
        "parent_id": "f0",
        "type": "CALL",
        "caller": "0x...",
        "address": "0x...",
        "depth": 1,
        "accesses": [
          {"frame_id":"f1","address":"0x...","kind":"storage","slot":"0x...","value":"0x...","op_type":"read"}
        ]
      }
    ]
  },
  "flat_read_keys": ["storage:0x...:0x...", "acct:0x...:balance"],
  "flat_write_keys": ["storage:0x...:0x..."],
  "stats": {"frame_count": 13, "max_depth": 6, "access_count": 62, "read_count": 40, "write_count": 22}
}
```

字段说明（耗时三段，纳秒）：
- `elapsed_ns`：EVM 执行耗时（`core.ApplyMessage`，单次或多 run 中位数）
- `mpt_ns`：MPT 树更新耗时（`CommitMPT`；默认口径为区块级提交均摊到每笔交易）
- `receipt_ns`：receipt 构建耗时（`GetLogs` + `CreateBloom`）

## 运行概要（run-summary.log）

执行命令时会在当前目录（cwd）**覆盖写** `run-summary.log`；命令内按开关依次运行的多个算法（串行 → vegeta → depurge）**追加写**到同一文件，算法间以 `====` 分隔、每算法以 `>>> Replay <名字> <<<` 为标题。

### 串行算法输出（`--replay-serial`）

```
Replay started at: 2026-08-30T10:33:44+08:00
===================================================
>>> Replay Depurge <<<
Dataset range    : 24000000 - 24000009
runs             : 5 (per-block pipeline rounds, averaged)
---------------------------------------------------
block 24000000   : 100 txs | EVM: 12.345ms | MPT: 5.678ms | receipt: 1.234ms
...
---------------------------------------------------
Total EVM exec   : 123.456ms
Total MPT update : 56.789ms
Total receipt    : 12.345ms
---------------------------------------------------
Chain-equivalent  : 192.590ms (EVM+MPT+receipt)
Serial exec only  : 123.456ms (EVM only)
Total elapsed     : 210.000ms
===================================================
```

其中 `Chain-equivalent` = EVM 执行 + MPT 树更新 + receipt 构建（链上一笔交易完整耗时的对应口径），`Serial exec only` 为纯 EVM 执行，二者可对比。

### Vegeta 算法输出（`--replay-vegeta`）

```
Vegeta run | parallelism=10 edge-order=new serial-order=block filter-nonce=true filter-coinbase=true runs=5
block 24000000: 232 txs | waves=18(max 144) | parallel=215 aborted=4 serial=17 | pre=17.912958ms order=378.7µs dag=167.933µs par=22.8591ms(clone 127.60149ms, merge 1.730192ms) ser=634.15µs | total=24.039883ms (excl. pre-exec) incl-pre=41.952841ms | state-diff=166 | serialized-order=MATCH
  runs(n=5): 226.958541ms, 174.095542ms, 156.508541ms, 104.62675ms, 120.199416ms (avg 156.477758ms)
  abort: tx#231: nonce too high: ...
  diff: veg-only acct:0x...:balance
-------------------------------------------------------------------
blocks=1 txs=232 | waves=18 | parallel=215 (92.7%) serial=17 (7.3%)
phase timing:
  pre-exec  : wall=17.912958ms sum=30.994449ms (excluded from total)
  order     : 378.7µs
  dag       : 167.933µs
  parallel  : wall=22.8591ms sum=22.056458ms (clone=127.60149ms merge=1.730192ms)
  serial    : wall=634.15µs sum=611.125µs
-------------------------------------------------------------------
total (order+dag+parallel+serial) : 24.039883ms
total incl. pre-exec              : 41.952841ms
block-end MPT                     : 1.487375ms (excluded from total)
state diff keys                   : 166 across all blocks
serialized-order verification     : MATCH (diff keys 0, verified outside algo timing)
-------------------------------------------------------------------
```

阶段含义：
- `pre-exec`：并行预执行（猜测保守读写集），**不计入总耗时**
- `order`：依赖排序（贪心聚簇）
- `dag`：冲突 DAG 构建
- `parallel`：波次乐观并行验证（`clone` 为状态克隆分项，`merge` 为合并提交分项）
- `serial`：串行兜底重放
- `total`：`order + dag + parallel + serial`（算法总时间，**不含预执行**）
- `state diff keys`：最终状态与串行基线不一致的 key 数（正确性诊断，`veg-only`/`serial-only` 为差异样本）
- `serialized-order`：合并提交层等价性验证（`MATCH`/`MISMATCH`，见下）

其中 `clone` 为所有 worker 克隆耗时**之和**（总和口径），而 `parallel wall` 为并发覆盖后的墙钟（墙钟口径），故 `clone` 可能大于 `parallel wall`——这正是「全状态 Clone 实现最新视图」的实现开销，真实系统用 MVCC 规避。

### Depurge 算法输出（`--replay-depurge`）

```
>>> Replay Depurge <<<
Depurge run | arm=C parallelism=10 runs=1
block 21498532: 151 txs | sched=145 committed=144 aborted=1 serial=7 | spec=46.47075ms graph=109.542µs par=118.628042ms(clone 489.770374ms, merge 104.860377ms) ser=2.178208ms | total=120.915792ms (excl. spec) incl-spec=167.386542ms | state-diff=2 | reexec=0.7% par-avg=53.01 peak=82
  abort: tx#1: rw-set violation: [storage:0x...:0x... ...]
  diff: depurge-only acct:0x...:balance
-------------------------------------------------------------------
blocks=2 txs=299 | committed=287 (96.0%) aborted=3 serial=12 (4.0%)
re-execution rate: 1.03% (aborted 3 / scheduled 290)
spec acquisition: no-rwset=9 empty-spec=0
LLM analysis (arm A): tried=781 ok=77 | fail: not-contract=0 no-contract=573 no-selector=0 decode-fail=0 no-storage=67 unresolved=63 empty=1
phase timing:
  spec      : wall=72.673417ms (excluded from total; pre-exec sum=105.580245ms)
  graph     : 208.292µs
  parallel  : wall=162.521542ms sum=64.88024ms (clone=618.90525ms merge=137.182337ms) avg-par=51.89 peak=95 dispatches=105
  serial    : wall=2.960375ms sum=2.872209ms
-------------------------------------------------------------------
total (graph+parallel+serial) : 165.690209ms
total incl. spec              : 238.363626ms
block-end MPT                 : 2.9ms (excluded from total)
state diff keys               : 8 across all blocks
speedup                       : 0.29x (baseline wall 48.5ms / total 165.7ms)
-------------------------------------------------------------------
```

阶段与指标含义：
- `spec`：step1 保守读写集获取（预执行 + A/B 臂的 LLM 实例化），**不计入总耗时**；`pre-exec sum` 为预执行各交易耗时之和
- `graph`：step2 key 队列建图
- `parallel`：step3 事件驱动并行（`clone`/`merge` 为克隆/合并分项；`avg-par` 为平均并行度=在飞交易数对时间的积分/墙钟；`peak` 为峰值在飞数；`dispatches` 为就绪调度次数）
- `serial`：step4 串行兜底（含 step1 失败/空集 + abort 交易）
- `total`：`graph + parallel + serial`（算法总时间，**不含 spec 获取**；`incl-spec` 为含 spec 口径）
- `re-execution rate`（重执行率）：`aborted / (committed + aborted)`，即参与调度交易中被作废的比例（对应 vegeta 缺点①的度量）
- `spec acquisition`：`no-rwset`=预执行失败（无读写集）、`empty-spec`=过滤后空集，两者直接进串行兜底，不计入重执行率
- `LLM analysis`：仅 A/B 臂输出，`tried`/`ok` 与逐类失败统计
- `state diff keys`：最终状态与原始顺序串行基线的差异 key 数（`depurge-only`/`serial-only` 为样本），口径与含义同 vegeta 的 `state-diff`
- `speedup`：串行基线墙钟 / 算法总耗时

与 vegeta 的对照：两者共用预执行、过滤口径（`--filter-nonce`/`--filter-coinbase`）、合并层
（nonce +=1 累加、coinbase tip 增量）与 state-diff 基准，差异仅在读写集来源（三臂）与调度方式
（事件驱动无屏障 vs 波次屏障），便于隔离归因加速比变化。

### 正确性诊断的双口径

Vegeta 输出两类正确性诊断，分工不同：

1. **`state-diff`**：vegeta 并发最终状态 vs **原始区块序串行基线**（链上正确状态）的差异。它回答「并发执行是否偏离链上正确状态」。`veg-only`/`serial-only` 是差异 key 样本。非零差异可能来自：预执行失败/空集交易、被过滤 key（nonce/coinbase balance）在读值上的隔离、以及乐观并发作废不级联的固有语义偏差。

2. **`serialized-order`**：vegeta 并发最终状态 vs **按波次隔离串行重放**的差异。它回答「合并提交层（`MergeCommittedFrom` / coinbase tip 增量合并 / nonce 累加）是否与串行重放等价」。验证采用**按波次隔离**口径：每个波次内每笔交易基于「波次开始快照」独立执行（复刻并发读隔离），波内执行完按提交顺序合并——这样精确复刻「并发读隔离 + 顺序合并」，只测合并层，不混入读隔离偏差。`MATCH`（diff keys 0）表示合并层与串行等价。

两类诊断共同作用：`state-diff` 测「偏离链上」、`serialized-order` 测「合并层是否等价」；前者非零是 vegeta 乐观并发的语义边界（预期内），后者非零才是合并实现的 bug。

## 并发控制科研用法

采集的 `call_tree` 可直接用于：
- **冲突分析**：`tx[i].write_set ∩ tx[j].read_set` → WAR 依赖
- **可并行分区**：按 write_set 做集合划分，互不冲突的交易可并行
- **子调用级冲突**：两个交易外层不冲突但都调用同一合约同一 slot → 仍冲突（仅 frame 级粒度可发现）
- **abort 预测**：write 的 old_value 与另一交易 read value 不一致 → 验证失败

per-tx 独立 Recorder 设计确保多交易并行重放实验时采集器无锁竞争，不污染加速比测量。

## 正确性验证

实测与 dataset 自带 `canonical.receipts` 对比（`--compare`）：
- `test-24000000-24000009`（Prague 时代）：status 100%，gasUsed 98.3%
- `mainnet-16774645-16774655`（Merge 时代）：status 100%，gasUsed 100%
