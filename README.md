# Depurge

面向**并发控制科研**的以太坊主网交易离线重放沙盒。从预制 dataset 目录读取真实交易数据和完整状态快照（witness），在基于 go-ethereum `core/vm` 的内存 EVM 中按原始顺序离线重放，精确采集每笔交易的执行耗时与 **slot 级读写集**（含深层嵌套子调用），以结构化 JSONL 格式输出，供后续科研分析。

## 核心特性

- **离线重放**：从 dataset 读取区块（header + transactions + witness），在内存 StateDB 中按 `stateAnchor=pre-first-user-tx` 语义重放，同区块内交易顺序提交状态，区块间独立初始化。
- **slot 级读写集采集**：自定义 StateDB + `tracing.Hooks` 双层埋点（方案 D），默认采集 storage slot / balance / nonce 的读写（含值与旧值），粒度可通过 CLI 调整。
- **深层嵌套调用树**：追踪 CALL/CALLCODE/DELEGATECALL/STATICCALL/CREATE/CREATE2 全部子调用，读写集按 `frame_id` 父子关联组织为 `call_tree`，支持子调用级冲突分析。
- **per-tx 无锁 Recorder**：每笔交易独立 Recorder 实例，交易结束 Freeze，天然无竞争，不污染并发实验加速比。
- **双轨输出**：`call_tree`（树形，并发研究核心）+ `flat_rwset`（对齐 dataset 自带 rwsets，baseline 对比）。
- **耗时精确记录**：纳秒级 EVM 执行打点，支持 `--runs N` 多次运行取中位数。
- **真 MPT 树**：可选挂载 go-ethereum 真 trie，在每笔交易/区块结束时重算 state root 与 storage root，真实模拟链上 MPT 树更新开销。
- **链上三段耗时**：每笔交易分别计时 EVM 执行（`ApplyMessage`）、MPT 树更新（`CommitMPT`）、receipt 构建（`GetLogs` + `CreateBloom`），支持与链上完整耗时口径对比。

## 构建

```bash
go build -o depurge ./cmd/depurge
```

要求 Go 1.22+，依赖 go-ethereum v1.15.11（支持 EIP-7702 delegation）。

## 用法

```bash
# 重放整个 dataset
./depurge replay --dataset datasets/test-24000000-24000009

# 重放指定区块范围，并与链上 canonical 对比
./depurge replay --dataset datasets/test-24000000-24000009 \
  --blocks 24000000-24000002 --compare

# 多次运行取中位数耗时
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

### CLI 参数

| 参数 | 默认 | 说明 |
|------|------|------|
| `--dataset` | （必填） | dataset 目录路径 |
| `--output` | `<dataset>/results` | 结果输出目录 |
| `--blocks` | 全部 | 区块范围，如 `24000000-24000005` |
| `--runs` | `1` | 每笔交易执行次数（>1 输出各次耗时与中位数） |
| `--compare` | `false` | 与 dataset 自带 canonical/rwsets 对比 |
| `--no-record` | `false` | 跳过读写集采集（纯性能基准） |
| `--rwset-granularity` | `slot` | 读写集粒度：`slot` / `account` |
| `--collect-balance` | `true` | 是否采集 balance 读写 |
| `--collect-nonce` | `true` | 是否采集 nonce 读写 |
| `--mpt-per-tx` | `false` | MPT 提交口径：`true`=每笔交易提交（pre-Byzantium）；`false`=区块结束统一一次（现代主网语义） |

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

执行命令时会在当前目录（cwd）覆盖写 `run-summary.log`，输出三段耗时与总对比：

```
Replay started at: 2026-08-30T10:33:44+08:00
===================================================
>>> Replay Depurge <<<
Dataset range    : 24000000 - 24000009
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
