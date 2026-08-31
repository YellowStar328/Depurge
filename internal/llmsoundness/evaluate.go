package llmsoundness

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"depurge/internal/dataset"
)

// TxEval 是单笔交易对单个合约的评测结果。
type TxEval struct {
	TxHash  string
	Block   uint64
	Sel     string
	SelName string

	// 双方 key 集合
	LlmRead  map[string]struct{}
	LlmWrite map[string]struct{}
	CanonRead  map[string]struct{}
	CanonWrite map[string]struct{}

	// 未解析计数（LLM 声称但无法实例化）
	UnresolvedRead  int
	UnresolvedWrite int

	// 归因统计
	LlmOnlyKeys map[string]struct{} // LLM 有、canonical 无（多报）
	MissKeys    map[string]struct{} // canonical 有、LLM 无（漏报）
}

// ContractStats 是单合约的聚合统计。
type ContractStats struct {
	Address    common.Address
	Name       string
	TxCount    int // 命中的交易数（含成功/失败匹配）
	MatchedTx  int // 成功 decode selector 且 LLM 有分析结果的交易数
	NoFuncTx   int // selector 在 LLM 分析里找不到的交易数
	DecodeFail int // calldata 解码失败的交易数

	// canonical 侧 key 总数（按读写）
	CanonReadKeys  int
	CanonWriteKeys int
	// LLM 侧实例化 key 总数
	LlmReadKeys  int
	LlmWriteKeys int
	// 命中（交集）key 数
	HitReadKeys  int
	HitWriteKeys int
	// 漏报（canonical 有、LLM 无）
	MissReadKeys  int
	MissWriteKeys int
	// 多报（LLM 有、canonical 无）
	ExtraReadKeys  int
	ExtraWriteKeys int
	// 未解析
	UnresolvedRead  int
	UnresolvedWrite int

	// 未解析原因聚合（跨交易）
	UnresolvedDetail map[string]int
}

func newContractStats(addr common.Address, name string) *ContractStats {
	return &ContractStats{
		Address:          addr,
		Name:             name,
		UnresolvedDetail: map[string]int{},
	}
}

// Evaluate 遍历 dataset，对每笔调用 LLM 合约的交易做 recall/precision 评测。
func Evaluate(loader *dataset.Loader, contracts map[common.Address]*Contract, blocks string) (map[common.Address]*ContractStats, error) {
	stats := make(map[common.Address]*ContractStats)
	for addr, c := range contracts {
		stats[addr] = newContractStats(addr, c.Meta.ContractName)
	}

	err := loader.ForEachBlock(blocks, func(blk *dataset.BlockData) error {
		// 按 txHash 索引 rwsets
		rwByHash := make(map[string]*dataset.FlatRwSet)
		for i := range blk.RwSets {
			rw := &blk.RwSets[i]
			rwByHash[strings.ToLower(rw.TxHash)] = rw
		}

		for i := range blk.Transactions {
			tx := &blk.Transactions[i]
			to := strings.ToLower(tx.To)
			addr := common.HexToAddress(to)
			contract, ok := contracts[addr]
			if !ok {
				continue
			}
			st := stats[addr]
			st.TxCount++

			rw := rwByHash[strings.ToLower(tx.Hash)]
			if rw == nil {
				continue
			}

			evalTx(contract, st, tx, rw, uint64(blk.Header.Number))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return stats, nil
}

// evalTx 对单笔交易评测，累加进 st。
func evalTx(c *Contract, st *ContractStats, tx *dataset.TxData, rw *dataset.FlatRwSet, blockNum uint64) {
	input := strings.TrimPrefix(tx.Input, "0x")
	if len(input) < 8 {
		return
	}
	sel := "0x" + input[:8]

	// LLM 是否有该 selector 的分析结果
	if c.Selectors[sel] == nil {
		st.NoFuncTx++
		return
	}

	// 解码 calldata：用 selector（4 字节）查 method。
	data := common.FromHex(tx.Input)
	if len(data) < 4 {
		st.DecodeFail++
		return
	}
	method, err := c.ABI.MethodById(data[:4])
	if err != nil {
		st.DecodeFail++
		return
	}
	args, err := method.Inputs.Unpack(data[4:])
	if err != nil {
		st.DecodeFail++
		return
	}

	sender := common.HexToAddress(tx.From)
	st.MatchedTx++

	// LLM 实例化（读 + 写）
	llmRead := c.Instantiate(sel, "read", sender, args)
	llmWrite := c.Instantiate(sel, "write", sender, args)

	// canonical 侧：只取该合约自身地址的 slot key
	canonRead := filterContractSlots(rw.ReadKeys, c.Address)
	canonWrite := filterContractSlots(rw.WriteKeys, c.Address)

	// 聚合统计
	accumulate(st, llmRead, llmWrite, canonRead, canonWrite)
}

// filterContractSlots 从 canonical key 列表中筛选属于该合约的 slot key。
// canonical key 里地址是全小写，因此用小写前缀匹配。
func filterContractSlots(keys []string, addr common.Address) map[string]struct{} {
	prefix := "slot:" + strings.ToLower(addr.Hex()) + ":"
	out := map[string]struct{}{}
	for _, k := range keys {
		if strings.HasPrefix(strings.ToLower(k), prefix) {
			out[k] = struct{}{}
		}
	}
	return out
}

// accumulate 把一次对比累加进 st。
func accumulate(st *ContractStats, llmRead, llmWrite *InstResult, canonRead, canonWrite map[string]struct{}) {
	// 读侧
	st.CanonReadKeys += len(canonRead)
	st.LlmReadKeys += len(llmRead.Keys)
	hitR, missR, extraR := setDiff(canonRead, llmRead.Keys)
	st.HitReadKeys += hitR
	st.MissReadKeys += missR
	st.ExtraReadKeys += extraR
	st.UnresolvedRead += llmRead.Unresolved
	mergeDetail(st.UnresolvedDetail, llmRead.UnresolvedDetail)

	// 写侧
	st.CanonWriteKeys += len(canonWrite)
	st.LlmWriteKeys += len(llmWrite.Keys)
	hitW, missW, extraW := setDiff(canonWrite, llmWrite.Keys)
	st.HitWriteKeys += hitW
	st.MissWriteKeys += missW
	st.ExtraWriteKeys += extraW
	st.UnresolvedWrite += llmWrite.Unresolved
	mergeDetail(st.UnresolvedDetail, llmWrite.UnresolvedDetail)
}

// setDiff 计算 canonical 与 llm 的交集、canonical 独有的（漏报）、llm 独有的（多报）。
func setDiff(canon, llm map[string]struct{}) (hit, miss, extra int) {
	for k := range canon {
		if _, ok := llm[k]; ok {
			hit++
		} else {
			miss++
		}
	}
	for k := range llm {
		if _, ok := canon[k]; !ok {
			extra++
		}
	}
	return
}

func mergeDetail(dst, src map[string]int) {
	for k, v := range src {
		dst[k] += v
	}
}

// Recall 计算漏报率（recall）：命中 / canonical 总数。
func (s *ContractStats) Recall() float64 {
	total := s.CanonReadKeys + s.CanonWriteKeys
	if total == 0 {
		return 1.0
	}
	hit := s.HitReadKeys + s.HitWriteKeys
	return float64(hit) / float64(total)
}

// Precision 计算多报率（precision）：命中 / LLM 实例化总数。
func (s *ContractStats) Precision() float64 {
	total := s.LlmReadKeys + s.LlmWriteKeys
	if total == 0 {
		return 1.0
	}
	hit := s.HitReadKeys + s.HitWriteKeys
	return float64(hit) / float64(total)
}

// MissRate 漏报率 = 1 - recall。
func (s *ContractStats) MissRate() float64 { return 1 - s.Recall() }

// ExtraRate 多报率 = 1 - precision。
func (s *ContractStats) ExtraRate() float64 { return 1 - s.Precision() }

// String 一行摘要。
func (s *ContractStats) String() string {
	return fmt.Sprintf("%s (%s): txs=%d matched=%d | recall=%.1f%% (miss %d/%d) precision=%.1f%% (extra %d/%d) | unresolved r=%d w=%d",
		s.Address.Hex(), s.Name, s.TxCount, s.MatchedTx,
		s.Recall()*100, s.MissReadKeys+s.MissWriteKeys, s.CanonReadKeys+s.CanonWriteKeys,
		s.Precision()*100, s.ExtraReadKeys+s.ExtraWriteKeys, s.LlmReadKeys+s.LlmWriteKeys,
		s.UnresolvedRead, s.UnresolvedWrite)
}
