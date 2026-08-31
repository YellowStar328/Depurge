// Package vegeta 实现 NSDI'25 论文《Vegeta: Enabling Parallel Smart Contract
// Execution in Leaderless Blockchains》speculate-order-replay 框架的纯调度逻辑：
//
//	依赖排序（按 key 贪心聚簇）→ 冲突依赖 DAG（写写/读写/写读三类冲突）→
//	波次状态机（就绪 / 提交 / 作废级联）→ 串行兜底列表排序。
//
// 本包只做集合与图运算，不依赖 EVM / dataset / state，可独立单元测试；
// 预执行（用真实 EVM 在基准状态上猜测保守读写集）由 internal/replay 集成层完成。
//
// 计时口径（由集成层负责）：算法总时间 = 依赖排序 + DAG 构建 + 批量并行验证
// + 串行重放；预执行单独计时（同时输出含/不含预执行两种总时间）。
package vegeta

import (
	"fmt"
	"sort"
	"strings"
)

// 串行兜底顺序口径。
const (
	SerialOrderBlock = "block" // 默认：作废交易按原始区块序重放（与链上串行执行严格等价）
	SerialOrderHash  = "hash"  // spec 字面口径：按交易哈希字典序（leaderless 确定性）
)

// DAG 冲突边定向口径。
const (
	EdgeOrderNew      = "new"      // 默认：冲突对按聚簇后的整体顺序定向（spec 字面："前面的交易作为后面前驱"）
	EdgeOrderOriginal = "original" // 冲突对一律按原始区块序定向（配合区块序兜底可保证最终状态与链上等价）
)

// TxInfo 参与调度的单笔交易摘要（预执行产物）。
type TxInfo struct {
	Index  int      // 原始区块序（0..n-1）
	Hash   string   // 交易哈希 hex（SerialOrderHash 排序用）
	Reads  []string // 预执行保守读集（扁平 key，与 recorder FlatReadKeys 同口径）
	Writes []string // 预执行保守写集（扁平 key）
}

// Options 调度选项。
type Options struct {
	FilterNonce    bool   // 聚簇与建边时过滤 nonce 伪冲突 key（acct:0x..:nonce）
	FilterCoinbase bool   // 聚簇/建边/验证时过滤 coinbase 的 balance tip 写 key（可交换累加，顺序无关）
	Coinbase       string // coinbase 地址（EIP-55 checksummed，与 FlatBalanceKey 格式一致）
	EdgeOrder      string // EdgeOrderNew（默认）或 EdgeOrderOriginal
	SerialOrder    string // SerialOrderBlock（默认）或 SerialOrderHash
}

// DefaultOptions 返回默认选项：过滤 nonce、聚簇序定边、区块序兜底。
func DefaultOptions() Options {
	return Options{
		FilterNonce: true,
		EdgeOrder:   EdgeOrderNew,
		SerialOrder: SerialOrderBlock,
	}
}

// Validate 校验选项取值。
func (o Options) Validate() error {
	switch o.EdgeOrder {
	case EdgeOrderNew, EdgeOrderOriginal:
	default:
		return fmt.Errorf("invalid edge order %q (expect new|original)", o.EdgeOrder)
	}
	switch o.SerialOrder {
	case SerialOrderBlock, SerialOrderHash:
	default:
		return fmt.Errorf("invalid serial order %q (expect block|hash)", o.SerialOrder)
	}
	return nil
}

func (o Options) withDefaults() Options {
	if o.EdgeOrder != EdgeOrderOriginal {
		o.EdgeOrder = EdgeOrderNew
	}
	if o.SerialOrder != SerialOrderHash {
		o.SerialOrder = SerialOrderBlock
	}
	return o
}

// IsNonceKey 判断是否为 nonce 扁平 key（acct:0x...:nonce）。
// nonce 是防重放序号、不携带业务状态：同一 sender 的每笔交易写 nonce、
// 下一笔读 nonce，会在冲突检测层制造假依赖，把同账户交易强制串行化。
func IsNonceKey(k string) bool { return strings.HasSuffix(k, ":nonce") }

// FilterKeys 按 opts 过滤 key 列表并去重排序：
//   - FilterNonce：剔除 nonce 伪冲突 key；
//   - FilterCoinbase：剔除 coinbase 的 balance key——每笔交易的 priority fee
//     tip 都写该 key（witness 实测单块可被全部交易覆盖），会把整块锁成
//     单链；tip 累积是可交换加法（顺序无关），剔除后由集成层做增量合并。
//
// 采集口径保持与 dataset rwsets 对齐，仅在检测/对比层过滤。
func FilterKeys(keys []string, opts Options) []string {
	coinbaseBalance := ""
	if opts.FilterCoinbase && opts.Coinbase != "" {
		coinbaseBalance = "acct:" + opts.Coinbase + ":balance"
	}
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if opts.FilterNonce && IsNonceKey(k) {
			continue
		}
		if coinbaseBalance != "" && k == coinbaseBalance {
			continue
		}
		set[k] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// SubsetOf 返回 actual 中不在 predicted 内的 key（结果为空即 actual ⊆ predicted）。
// 用于乐观验证：交易重执行真实触碰的读写集是否完全落在预执行预测集内。
func SubsetOf(actual, predicted []string) []string {
	pset := make(map[string]struct{}, len(predicted))
	for _, k := range predicted {
		pset[k] = struct{}{}
	}
	var miss []string
	for _, k := range actual {
		if _, ok := pset[k]; !ok {
			miss = append(miss, k)
		}
	}
	sort.Strings(miss)
	return miss
}

// ClusterOrder 依赖排序：按 key 贪心聚簇，生成交易整体顺序。
//
// 语义（用户确认）：
//  1. 统计每个 key（过滤 nonce 后）被多少笔交易访问（读或写均计）；
//  2. key 按访问交易数降序排列（同数按 key 字典序，保证确定性）——
//     链越长代表竞争越集中，优先处理；
//  3. 依次处理每个 key，把其尚未放置的交易按原始区块序追加到整体序列尾部，
//     让争夺同一份数据的交易尽量靠近（已放置的交易跳过，即一笔交易只归入
//     最先覆盖它的 key）；
//  4. 未被任何 key 覆盖的交易（如过滤 nonce 后读写集为空）按原始序追加在末尾。
//
// infos 须只含参与调度的交易（预执行成功且过滤后读写集非空）；
// 返回聚簇后的原始索引序列，是建图（BuildGraph）的基础。
func ClusterOrder(infos []TxInfo, opts Options) []int {
	opts = opts.withDefaults()

	// key -> 访问它的交易（原始索引升序：按原始序遍历保证）
	keyTxs := make(map[string][]int)
	sorted := sortedByIndex(infos)
	for _, info := range sorted {
		for _, k := range unionKeys(info, opts) {
			keyTxs[k] = append(keyTxs[k], info.Index)
		}
	}

	// key 按访问交易数降序、同数按 key 字典序
	keys := make([]string, 0, len(keyTxs))
	for k := range keyTxs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keyTxs[keys[i]]) != len(keyTxs[keys[j]]) {
			return len(keyTxs[keys[i]]) > len(keyTxs[keys[j]])
		}
		return keys[i] < keys[j]
	})

	placed := make(map[int]struct{}, len(infos))
	order := make([]int, 0, len(infos))
	for _, k := range keys {
		for _, idx := range keyTxs[k] {
			if _, ok := placed[idx]; ok {
				continue
			}
			placed[idx] = struct{}{}
			order = append(order, idx)
		}
	}
	// 未被任何 key 覆盖的交易按原始序收尾
	for _, info := range sorted {
		if _, ok := placed[info.Index]; ok {
			continue
		}
		placed[info.Index] = struct{}{}
		order = append(order, info.Index)
	}
	return order
}

type txState int

const (
	statePending   txState = iota // 待执行（波次循环中）
	stateCommitted                // 并行验证通过已提交
	stateAborted                  // 已作废（进串行兜底段）
)

// Graph 冲突依赖 DAG 与波次状态机。
//
// 边规则：共享同一 key（过滤 nonce 后）的交易对，只要任一方写该 key
// ——覆盖写写（双写）/读写（前读后写）/写读（前写后读）三类冲突——
// 即加一条"前驱 → 后继"边；双方都只读则无边。
// 边方向由 Options.EdgeOrder 决定：
//   - EdgeOrderNew：按聚簇整体序定向（spec 字面："前面的交易作为后面的前驱"）；
//   - EdgeOrderOriginal：一律按原始区块序定向（保证最终状态与链上串行等价）。
//
// 状态机：Ready() 返回所有入度为 0 的 pending 交易组成一个波次；
// 验证通过调用 Commit（后继入度减一）；验证失败调用 Invalidate（级联作废
// 全部后继，后继不得基于前驱写入缺失的视图并行提交）。
type Graph struct {
	infos map[int]TxInfo
	pos   map[int]int   // 原始索引 -> 聚簇序位置
	succs map[int][]int // 原始索引 -> 后继原始索引（升序去重）
	indeg map[int]int   // 原始索引 -> 剩余入度
	state map[int]txState

	aborted []int // 作废交易（含级联，按作废发生顺序）
	edges   int
}

// BuildGraph 基于聚簇序 order 与读写集 infos 构建冲突 DAG。
// order 与 infos 覆盖同一批参与调度的交易（原始索引一致）。
func BuildGraph(order []int, infos []TxInfo, opts Options) *Graph {
	opts = opts.withDefaults()
	g := &Graph{
		infos: make(map[int]TxInfo, len(infos)),
		pos:   make(map[int]int, len(order)),
		succs: make(map[int][]int, len(infos)),
		indeg: make(map[int]int, len(infos)),
		state: make(map[int]txState, len(infos)),
	}
	for _, info := range infos {
		g.infos[info.Index] = info
		g.state[info.Index] = statePending
	}
	for p, idx := range order {
		g.pos[idx] = p
	}

	// key -> 访问者（含是否写者）
	type accessor struct {
		idx    int
		writer bool
	}
	keyAcc := make(map[string][]accessor)
	for _, idx := range order {
		info, ok := g.infos[idx]
		if !ok {
			continue
		}
		wset := make(map[string]struct{}, len(info.Writes))
		for _, k := range info.Writes {
			wset[k] = struct{}{}
		}
		for _, k := range unionKeys(info, opts) {
			_, w := wset[k]
			keyAcc[k] = append(keyAcc[k], accessor{idx: idx, writer: w})
		}
	}

	// 建边（去重）
	edgeSet := make(map[int]map[int]struct{})
	addEdge := func(from, to int) {
		if from == to {
			return
		}
		if edgeSet[from] == nil {
			edgeSet[from] = make(map[int]struct{})
		}
		if _, ok := edgeSet[from][to]; ok {
			return
		}
		edgeSet[from][to] = struct{}{}
		g.edges++
	}
	for _, accs := range keyAcc {
		if len(accs) < 2 {
			continue
		}
		// 访问者排序：EdgeOrderNew 按聚簇位置；EdgeOrderOriginal 按原始区块序
		sort.Slice(accs, func(i, j int) bool {
			if opts.EdgeOrder == EdgeOrderOriginal {
				return accs[i].idx < accs[j].idx
			}
			return g.pos[accs[i].idx] < g.pos[accs[j].idx]
		})
		// "j 为写者 → 连所有更早访问者；j 为读者 → 连所有更早写者"，
		// 等价于：共享 key 且至少一方写即建边（WW/RW/WR）。
		for j := 1; j < len(accs); j++ {
			if accs[j].writer {
				for i := 0; i < j; i++ {
					addEdge(accs[i].idx, accs[j].idx)
				}
			} else {
				for i := 0; i < j; i++ {
					if accs[i].writer {
						addEdge(accs[i].idx, accs[j].idx)
					}
				}
			}
		}
	}
	for from, tos := range edgeSet {
		list := make([]int, 0, len(tos))
		for to := range tos {
			list = append(list, to)
		}
		sort.Ints(list)
		g.succs[from] = list
	}
	for _, tos := range g.succs {
		for _, to := range tos {
			g.indeg[to]++
		}
	}
	return g
}

// Ready 返回当前就绪波：所有入度为 0 且 pending 的交易，按聚簇序输出。
// 波次内交易两两无边（无冲突 key 交集），可安全并行重执行。
func (g *Graph) Ready() []int {
	var ready []int
	for idx, st := range g.state {
		if st == statePending && g.indeg[idx] == 0 {
			ready = append(ready, idx)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		pi, oki := g.pos[ready[i]]
		pj, okj := g.pos[ready[j]]
		if !oki {
			pi = len(g.pos)
		}
		if !okj {
			pj = len(g.pos)
		}
		if pi != pj {
			return pi < pj
		}
		return ready[i] < ready[j]
	})
	return ready
}

// Commit 提交一笔交易（并行验证通过）：后继入度减一。仅对 pending 交易有效。
func (g *Graph) Commit(idx int) {
	if g.state[idx] != statePending {
		return
	}
	g.state[idx] = stateCommitted
	for _, s := range g.succs[idx] {
		if g.state[s] == statePending {
			g.indeg[s]--
		}
	}
}

// Invalidate 作废一笔交易（验证失败）并级联作废其全部 DAG 后继（传递闭包）。
//
// 级联的必要性：前驱被作废后推迟到最后的串行兜底段，其后继若仍在并行阶段
// 基于"前驱写入缺失"的视图执行并提交，会得到与链上不一致的状态——
// 因此后继必须一并作废进串行段。返回本次新作废的原始索引（含 idx 自身）。
func (g *Graph) Invalidate(idx int) []int {
	if g.state[idx] != statePending {
		return nil
	}
	var newly []int
	queue := []int{idx}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if g.state[cur] != statePending {
			continue
		}
		g.state[cur] = stateAborted
		g.aborted = append(g.aborted, cur)
		newly = append(newly, cur)
		queue = append(queue, g.succs[cur]...)
	}
	return newly
}

// Pending 是否仍有 pending 交易（波次循环是否继续）。
func (g *Graph) Pending() bool {
	for _, st := range g.state {
		if st == statePending {
			return true
		}
	}
	return false
}

// Remaining 返回全部 pending 交易的原始索引（升序）。
// 正常流程不会用到（级联作废保证 pending 交易要么就绪要么被级联作废），
// 仅供集成层做死锁防御兜底。
func (g *Graph) Remaining() []int {
	var out []int
	for idx, st := range g.state {
		if st == statePending {
			out = append(out, idx)
		}
	}
	sort.Ints(out)
	return out
}

// Aborted 返回全部作废交易的原始索引（含级联，按作废发生顺序）。
// 串行兜底列表 = Aborted() + 初始串行段（预执行失败/空集交易，图外），
// 统一经 SortSerial 排序。
func (g *Graph) Aborted() []int {
	return append([]int(nil), g.aborted...)
}

// NodeCount / EdgeCount / CommittedCount / AbortedCount 统计信息。
func (g *Graph) NodeCount() int      { return len(g.infos) }
func (g *Graph) EdgeCount() int      { return g.edges }
func (g *Graph) CommittedCount() int { return g.count(stateCommitted) }
func (g *Graph) AbortedCount() int   { return g.count(stateAborted) }

func (g *Graph) count(st txState) int {
	n := 0
	for _, s := range g.state {
		if s == st {
			n++
		}
	}
	return n
}

// SortSerial 将串行兜底交易列表按 Options.SerialOrder 排序：
// block = 原始区块序（默认，与链上串行等价）；hash = 交易哈希字典序
// （spec 字面口径）。hashOf 返回原始索引对应的交易哈希。
func SortSerial(indices []int, hashOf func(int) string, opts Options) []int {
	out := append([]int(nil), indices...)
	opts = opts.withDefaults()
	if opts.SerialOrder == SerialOrderHash {
		sort.SliceStable(out, func(i, j int) bool {
			hi, hj := hashOf(out[i]), hashOf(out[j])
			if hi != hj {
				return hi < hj
			}
			return out[i] < out[j]
		})
	} else {
		sort.Ints(out)
	}
	return out
}

// ---- 内部工具 ----

// sortedByIndex 返回按原始索引升序的副本。
func sortedByIndex(infos []TxInfo) []TxInfo {
	out := append([]TxInfo(nil), infos...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}

// unionKeys 交易读集与写集的并集（去重、按 opts 过滤、有序）。
func unionKeys(info TxInfo, opts Options) []string {
	return FilterKeys(append(append([]string(nil), info.Reads...), info.Writes...), opts)
}
