package dataset

// Manifest 是 dataset 目录顶层 manifest.json 的结构。
type Manifest struct {
	FormatVersion int    `json:"formatVersion"`
	ChainID       uint64 `json:"chainId"`
	FromBlock     uint64 `json:"fromBlock"`
	ToBlock       uint64 `json:"toBlock"`
	ExportedAt    string `json:"exportedAt"`
	// StateAnchor 描述 witness 状态快照的锚定时机，
	// 实测为 "pre-first-user-tx"：区块首笔用户交易前的状态。
	StateAnchor string `json:"stateAnchor"`
	// ExecutionMode 实测为 "block-local-user-tx"：每区块独立执行，
	// 区块间状态不连续（下区块从各自 witness 重新初始化）。
	ExecutionMode string `json:"executionMode"`
	SourceClient  string `json:"sourceClient"`
	HashWindow    int    `json:"hashWindow"`
}

// IsBlockLocal 报告 dataset 是否为区块独立执行模式。
func (m *Manifest) IsBlockLocal() bool {
	return m.ExecutionMode == "block-local-user-tx"
}
