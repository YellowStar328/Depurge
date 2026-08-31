// Package output 将重放结果（执行状态、耗时、读写集）以结构化
// JSONL 格式写入 results/ 目录，每区块一个文件，一行一笔交易。
package output

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/common"

	"depurge/internal/state"
)

// TxResult 单笔交易的完整重放结果。
type TxResult struct {
	TxHash        common.Hash      `json:"tx_hash"`
	TxIndex       int              `json:"tx_index"`
	BlockNumber   uint64           `json:"block_number"`
	Status        int              `json:"status"` // 1=成功 0=失败 2=消息构造错误
	GasUsed       uint64           `json:"gas_used"`
	Err           string           `json:"error,omitempty"`
	ElapsedNs     int64            `json:"elapsed_ns"`          // EVM 执行耗时（ApplyMessage，单次或多 run 中位数）
	Runs          []int64          `json:"runs,omitempty"`      // --runs N 时的各次耗时
	MptNs         int64            `json:"mpt_ns"`              // MPT 树更新耗时（CommitMPT）
	ReceiptNs     int64            `json:"receipt_ns"`          // receipt 构建耗时（GetLogs + CreateBloom）
	CallTree      *state.CallFrame `json:"call_tree,omitempty"` // 树形读写集（含深层嵌套子调用）
	FlatReadKeys  []string         `json:"flat_read_keys"`      // 扁平读集（对齐 dataset rwsets 格式）
	FlatWriteKeys []string         `json:"flat_write_keys"`     // 扁平写集
	Stats         TxStats          `json:"stats"`
	Compare       *CompareDiff     `json:"compare,omitempty"` // --compare 时输出
}

// TxStats 读写集统计摘要。
type TxStats struct {
	FrameCount  int `json:"frame_count"`  // 调用帧总数（含 ROOT）
	MaxDepth    int `json:"max_depth"`    // 最大调用深度
	AccessCount int `json:"access_count"` // 访问条目总数
	ReadCount   int `json:"read_count"`
	WriteCount  int `json:"write_count"`
}

// CompareDiff 与 dataset canonical/rwsets 的对比结果。
type CompareDiff struct {
	CanonicalStatus  *uint64  `json:"canonical_status,omitempty"`
	CanonicalGasUsed *uint64  `json:"canonical_gas_used,omitempty"`
	StatusMatch      *bool    `json:"status_match,omitempty"`
	GasUsedMatch     *bool    `json:"gas_used_match,omitempty"`
	ReadKeyDiff      *KeyDiff `json:"read_key_diff,omitempty"`
	WriteKeyDiff     *KeyDiff `json:"write_key_diff,omitempty"`
}

// KeyDiff 扁平 key 集合差异。
type KeyDiff struct {
	Missing []string `json:"missing"` // dataset 有而本地没有
	Extra   []string `json:"extra"`   // 本地有而 dataset 没有
}

// Writer 写 results/<blockNum>.jsonl。
type Writer struct {
	Dir string
}

// NewWriter 创建输出器并确保目录存在。
func NewWriter(dir string) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}
	return &Writer{Dir: dir}, nil
}

// WriteBlock 将一个区块的全部交易结果写为一个 JSONL 文件。
func (w *Writer) WriteBlock(blockNum uint64, results []TxResult) error {
	path := filepath.Join(w.Dir, fmt.Sprintf("%d.jsonl", blockNum))
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	enc := json.NewEncoder(bw)
	for i := range results {
		if err := enc.Encode(&results[i]); err != nil {
			return fmt.Errorf("encode tx %d of block %d: %w", i, blockNum, err)
		}
	}
	return bw.Flush()
}
