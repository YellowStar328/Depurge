# HANDOFF: llmsoundness（LLM 读写集 soundness 离线评测）

> 交接文档。日期：2026-08-31。
> 状态：**✅ 本文档列出的 TODO 已全部完成**。构建/vet/测试通过。尚未 `git commit`（untracked，待用户决定）。

## 0. 完成摘要（2026-08-31）

全量评测（blocks 21498532-21499531，37858 笔有 canonical rwset 的交易）：

```
recall   : 82.92%   (有 storage 布局合约口径: 86.31%)
precision: 69.13%
```

- P0-1：无 storage 布局合约已从全局指标分母剔除，报告提供「全量 / 有 storage」双口径 + 逐合约 `sto` 列。
- P0-2：嵌套 mapping 链式解析 + struct 成员展开已实现（见下「实现方案」）。
- P1-1：account 符号解析已覆盖实际数据中出现的全部可解析类别；动态符号（tokenId/poolId 等非地址键）按「静态不可知」归类，属能力边界而非缺陷。
- P1-2：USDT extra 已归因——实例化正确，extra 来自 LLM 对 `paused/upgradedAddress/deprecated` 等声明字段的保守过度近似（声明了但运行时不读），非实现 bug。
- P2：dbg 测试/`.bak` 已删，`.gitignore` 已补 `/llmsoundness`，README 已加用法章节。
- 新增回归测试：`internal/llmsoundness/instance_test.go`（4 例：嵌套链、struct 展开、原始类型回退、动态键归类）。

10 块小样本指标变化：recall 69.33% → 72.29%，precision 62.38% → 66.23%（全量口径提升更明显，因大样本摊薄了长尾）。

## 1. 任务目标

用 dataset 自带的 **canonical rwsets**（链上真实读写的 ground truth）评测 `llm/mainnet_rw/` 里
**LLM 静态分析**产出的读写集的保守度：

- **recall（召回）**：canonical 实际访问的 storage key，有多少被 LLM 声明覆盖。`1-recall = 漏报率`。
- **precision（精确）**：LLM 声明的 key，有多少实际被访问。`1-precision = 多报率`。

LLM 分析以「抽象声明」形式给出：每个 selector 的 `reads/writes` 是若干 `(account, field)` 对
（如 `{account:"msg.sender", field:"balances"}`）。评测核心是把抽象声明**实例化**成具体 slot key
（`slot:<addr>:<slot>`，与 dataset key 格式对齐），再与 canonical 做集合对比。

## 2. 代码结构

| 文件 | 职责 |
|---|---|
| `internal/llmsoundness/types.go` | 加载 `llm/mainnet_rw/<addr>/` 合约目录：meta / storage / abi / funcs / `0x<selector>.json`。`TypeDesc` 含 struct `Members` 解析；`Contract.HasStorage()` 判定 storage 布局存在性。 |
| `internal/llmsoundness/instance.go` | **核心**：`(account, field)` → 具体 slot key。`Instantiate` → `instantiateField` → `instantiateChain`（mapping 链折叠）/ `emitValue`/`structSlots`（struct 展开）。 |
| `internal/llmsoundness/evaluate.go` | 遍历 dataset 每区块每交易，按 `to` 匹配 LLM 合约，解码 calldata，实例化 + 与 canonical 对比，聚合 `ContractStats`。 |
| `cmd/llmsoundness/main.go` | cobra CLI + 报告（双口径全局指标 + 逐合约明细含 `sto` 列 + unresolved Top20）。 |
| `internal/llmsoundness/instance_test.go` | 实例化回归测试 ×4。 |

### 运行方式

```bash
cd /Users/yellowstar/Desktop/code/Depurge
go run ./cmd/llmsoundness \
  --llm-dir llm/mainnet_rw \
  --dataset datasets/mainnet-21498532-21499531 \
  --blocks 21498532-21498541
```

flags：`--llm-dir`（默认 `llm/mainnet_rw`）、`--dataset`（必填）、`--blocks`（空=全部）、`--min-tx`（只报命中交易数≥N 的合约）。

> ⚠️ `llm/` 和 `datasets/` 都在 `.gitignore` 里，接手者需本地已有这两个数据目录才能跑。

## 3. 原 TODO 的实现方案（已完成，供回溯）

### P0-1 无 storage 布局合约（67/110）
- `HasStorage()` = storage 条目或 types 非空。`accumulate` 把无 storage 合约的计数排除出全局分母，单独输出「全量 / 有 storage」双口径；逐合约表 `sto` 列标记。
- 这些合约（如 Dai）所有 field 落 `unknown-field` → unresolved → recall=0%，是**数据缺口非代码 bug**；补齐 storage.json 即可自动生效。

### P0-2 嵌套 mapping / struct mapping（instance.go 重写）
- **数据形态**：LLM 对同一 field 按 mapping 深度逐层发多条 entry（外层→内层），如 USDT `allowed[from][msg.sender]` 发两条：`{account:addr1, mappingKeyChain:[msg.sender]}`（外层）+ `{account:addr2, mappingKeyChain:[msg.sender, addr1]}`（内层）。
- **链式折叠**：`instantiateChain` 按 `len(mappingKeyChain)` 升序分块，逐块折叠 `slot = keccak(pad(key) || pad(prevSlot))`，最终 `emitValue` 发末端值。只解析 `t_address` 键（取对应地址参数）；非地址键（`t_uint256` tokenId 等）静态不可知 → `dynamic-key` unresolved。
- **struct 展开**：`TypeDesc.Members`（label/slot/offset/type）已解析；`structSlots` 发 base + 各成员 word 偏移（去重），深度保护递归。实测 NPM Position 5-word 跨度、PoolKey 6-word 与 numberOfBytes 一致。
- **原始类型回退**：`t_bool`/`t_uintN`/`t_address` 常不在 types map 中，非 mapping/struct/array 时直接发 base slot（修复 WBTC `balances` 等漏发）。

### P1-1 account 符号
- `resolveAccountKey` 支持 `msg.sender/addr1/addr2`（实测数据中仅有的可解析符号）。`owner`/`params.*`/`opInfo.*` 等实测未出现或伴随动态键，归 `dynamic-key`/`unknown-account`，属静态分析能力边界。

### P1-2 USDT extra 归因
- 抽样验证：extra slot 7/11 对应 LLM 声明的 `paused/upgradedAddress/deprecated` 等字段，实例化地址与 slot 计算正确；canonical 中这些交易运行时不读它们 → **LLM 保守过度近似（可接受）**，非实例化 bug。

### P2 收尾
- 已删 `cmd/llmsoundness/dbg_test.go`、`dbg2_test.go`、`dbg3_test.go` 与 3 个 `.json.bak`；`.gitignore` 补 `/llmsoundness`；README 增「LLM 读写集 soundness 评测」章节。

## 4. 关键实现细节 / 坑（接手者必读）

- **key 格式对齐**：`CanonicalKey = "slot:" + lower(addr) + ":" + slot.Hex()`。dataset 侧地址全小写，`filterContractSlots` 用小写前缀匹配。slot 用 `common.Hash.Hex()`（带 0x、全小写）。
- **mapping slot 公式**：`keccak256(pad32(key) || pad32(base_slot))`（`mappingSlot`），嵌套即迭代折叠。
- **slot 解析**：storage.json 的 `slot` 是**十进制字符串**，`parseSlot` 用 `big.Int.SetString(s,10)`。
- **label 归一化**：`normalizeLabel` 去前导下划线（`balances`↔`_balances`），`labelIndex` 同时索引原名与归一名。
- **bytes/string**：只发 base slot（存长度），>31 字节的 keccak 数据 slot **未发**（保守但可能漏）。
- **评测匹配链**：`to`→合约；`rwByHash[lower(tx.Hash)]`→canonical rwset；`input[:8]`→selector；`ABI.MethodById`+`Inputs.Unpack`→args。任一失败计入 `NoFuncTx`/`DecodeFail`。
- **TxCount vs matched** 差值 = NoFuncTx + DecodeFail + 无 rwset 的交易。

## 5. 已知边界（非缺陷，不再处理）

- 动态键（tokenId/poolId 等非地址 mapping key）静态不可知 → unresolved，拉低 recall 属静态分析固有上限。
- multicall/批量交易的读写集是子调用并集，单 selector 声明无法覆盖全部。
- WBTC `allowed` 存在 LLM 部分链声明（只发外层不发内层），导致 approve 的 allowance slot 漏报——属 LLM 标注质量问题，正是本工具要度量的对象。
- 无 storage.json 的 67 个合约需补数据才能参评。

## 6. 数据格式参考

`llm/mainnet_rw/<addr>/`：
- `meta.json`：address / contract_name / compiler_version / verified / proxy / implementation。
- `storage.json`：`storage[]`(label/slot/offset/type/contract) + `types{tid: {encoding,label,numberOfBytes,key,value,base,members?}}`。**67 个合约缺此文件**。
- `abi.json`：标准 ABI。`funcs.json`：`[{selector,name,inputs[]}]`。
- `0x<selector>.json`：`{reads:[{account,field,mappingKeyChain?}], writes:[...]}`。

dataset 侧（`internal/dataset/block.go`）：`FlatRwSet{TxHash, ReadKeys[], WriteKeys[]}`，key 为 `slot:<lower-addr>:<slot>` 扁平串（交易级聚合，无调用帧）。
