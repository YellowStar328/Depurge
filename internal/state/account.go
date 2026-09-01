package state

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/holiman/uint256"
)

// emptyCodeHash 是空代码的 keccak256 哈希。
var emptyCodeHash = crypto.Keccak256Hash(nil)

// AccountState 内存账户状态。
// originStorage / originBalance / originNonce 表示本区块 witness（或上一笔交易
// Finalise 提交后）的已提交值；修改先写当前值，Finalise 时合并回 origin。
//
// CoW 物化标记（仅 CloneCoW 路径使用；深拷贝/纯内存常规路径恒为 0 即禁用）：
//   - matEpoch：该账户结构被当前属主库物化（独占）时的 epoch。
//     写入口发现 acc.matEpoch != 属主 epoch 时必须先 cowClone 替换 map 条目，
//     绝不原地改共享账户；
//   - originMatEpoch：originStorage map 被物化为属主私有时的 epoch。
//     原地写 originStorage（finalise / MergeCommittedFrom）前必须校验，
//     不匹配则整 map 拷贝后再写（槽级 CoW）。
type AccountState struct {
	Balance  *uint256.Int
	Nonce    uint64
	Code     []byte
	codeHash common.Hash // 创建/SetCode 时急算（读路径纯读，共享账户下无写竞争）

	originBalance *uint256.Int
	originNonce   uint64

	// storage
	originStorage map[common.Hash]common.Hash // 已提交值（GetCommittedState 读取；CoW 下可跨库共享）
	dirtyStorage  map[common.Hash]common.Hash // 本 tx 修改（GetState 优先读取）

	// MPT storage trie：真实 trie，Finalise 时把 dirty 槽增量 Update 进去，
	// Commit 后重算 storage root（对齐链上 MPT 树更新开销）。
	// CloneCoW 仅限纯内存模式（storageTrie 恒 nil），不存在共享 trie 问题。
	storageTrie *trie.Trie

	touched bool // 是否被本 tx touch（Finalise 时用于 EIP-158 空账户删除判断）

	created bool // 本 tx 内创建（journal 回滚时整体删除）

	matEpoch       uint64 // 账户结构物化 epoch（0 = CoW 禁用）
	originMatEpoch uint64 // originStorage 物化 epoch（0 = CoW 禁用）
}

// NewAccountState 从 witness 初始化账户状态。
func NewAccountState(balance *uint256.Int, nonce uint64, code []byte, storage map[common.Hash]common.Hash, storageTrie *trie.Trie) *AccountState {
	if balance == nil {
		balance = new(uint256.Int)
	}
	if storage == nil {
		storage = make(map[common.Hash]common.Hash)
	}
	if code == nil {
		code = []byte{}
	}
	return &AccountState{
		Balance:       new(uint256.Int).Set(balance),
		originBalance: new(uint256.Int).Set(balance),
		Nonce:         nonce,
		originNonce:   nonce,
		Code:          code,
		codeHash:      crypto.Keccak256Hash(code), // 急算：共享账户下读路径不得有写副作用
		originStorage: storage,
		dirtyStorage:  make(map[common.Hash]common.Hash),
		storageTrie:   storageTrie,
	}
}

// GetState 读取当前槽值（dirty 优先）。
func (a *AccountState) GetState(key common.Hash) common.Hash {
	if v, ok := a.dirtyStorage[key]; ok {
		return v
	}
	return a.originStorage[key]
}

// SetState 写入槽值（不落 journal，由 MemoryStateDB 统一 journal）。
func (a *AccountState) SetState(key, value common.Hash) {
	a.dirtyStorage[key] = value
	a.touched = true
}

// GetCommittedState 读取已提交槽值。
func (a *AccountState) GetCommittedState(key common.Hash) common.Hash {
	return a.originStorage[key]
}

// GetCodeHash 返回代码哈希。codeHash 在 NewAccountState/SetCode 时急算，
// 本方法纯读——CloneCoW 共享账户可能被多 worker 并发读，惰性缓存会引入写竞争。
func (a *AccountState) GetCodeHash() common.Hash {
	return a.codeHash
}

// IsEmpty 报告账户是否为空（EIP-161: balance=nonce=code=0）。
func (a *AccountState) IsEmpty() bool {
	return a.Balance.IsZero() && a.Nonce == 0 && len(a.Code) == 0
}

// finalise 提交 dirty 状态到 origin（交易结束调用）。
// 若存在 storage trie，则把 dirty 槽增量 Update 进 trie（对齐链上
// state_object.updateRoot 的存储写入），Commit 的时机由 MemoryStateDB 统一控制。
func (a *AccountState) finalise() {
	if a.storageTrie != nil {
		for k, v := range a.dirtyStorage {
			// 对齐 geth updateStorage：value 去前导零，零值删除
			if v == (common.Hash{}) {
				_ = a.storageTrie.Update(k[:], nil)
			} else {
				_ = a.storageTrie.Update(k[:], common.TrimLeftZeroes(v[:]))
			}
		}
	}
	for k, v := range a.dirtyStorage {
		a.originStorage[k] = v
	}
	a.dirtyStorage = make(map[common.Hash]common.Hash)
	a.originBalance.Set(a.Balance)
	a.originNonce = a.Nonce
	a.touched = false
	a.created = false
}

// clone 深拷贝账户状态（预执行的独立状态快照）。
//
// Balance/originBalance/Code/originStorage/dirtyStorage 逐项复制，
// 保证克隆库上的任何修改（含 journal 回滚）都不会波及原库。
// storageTrie 共享引用：纯内存模式（预执行默认）下恒为 nil；
// 带 trie 模式下 trie 基于已 Commit 的 root 重建、写前不可变，共享只读安全。
func (a *AccountState) clone() *AccountState {
	c := &AccountState{
		Balance:       new(uint256.Int).Set(a.Balance),
		Nonce:         a.Nonce,
		Code:          append([]byte(nil), a.Code...),
		codeHash:      a.codeHash,
		originBalance: new(uint256.Int).Set(a.originBalance),
		originNonce:   a.originNonce,
		storageTrie:   a.storageTrie,
		touched:       a.touched,
		created:       a.created,
	}
	c.originStorage = make(map[common.Hash]common.Hash, len(a.originStorage))
	for k, v := range a.originStorage {
		c.originStorage[k] = v
	}
	c.dirtyStorage = make(map[common.Hash]common.Hash, len(a.dirtyStorage))
	for k, v := range a.dirtyStorage {
		c.dirtyStorage[k] = v
	}
	return c
}

// cowClone 是 CloneCoW 路径的账户浅拷贝（账户级 CoW）。
//
//   - 复制：Balance/originBalance（uint256 会被原地改，必须私有）、
//     dirtyStorage（小表，复制后各自私有演进）；
//   - 共享：Code（字节码只整体替换、从不原地改）、
//     originStorage（槽级 CoW：真正原地写时再整 map 拷贝，由 originMatEpoch 追踪）、
//     storageTrie（CloneCoW 仅限纯内存模式，恒 nil）；
//   - matEpoch 置为 epoch（对新属主私有）；originMatEpoch 沿用 a 的值
//     （必 ≠ epoch，保证首次原地写 origin 前触发整 map 拷贝）。
//
// 正确性前提：共享期间任何属主都不得原地改 a——各属主写前经
// cowEnsureAccount 物化（epoch 不匹配即替换为本拷贝），a 自此冻结。
func (a *AccountState) cowClone(epoch uint64) *AccountState {
	c := &AccountState{
		Balance:        new(uint256.Int).Set(a.Balance),
		Nonce:          a.Nonce,
		Code:           a.Code, // 共享：字节码只整体替换
		codeHash:       a.codeHash,
		originBalance:  new(uint256.Int).Set(a.originBalance),
		originNonce:    a.originNonce,
		originStorage:  a.originStorage, // 共享：槽级 CoW
		storageTrie:    a.storageTrie,
		touched:        a.touched,
		created:        a.created,
		matEpoch:       epoch,
		originMatEpoch: a.originMatEpoch,
	}
	c.dirtyStorage = make(map[common.Hash]common.Hash, len(a.dirtyStorage))
	for k, v := range a.dirtyStorage {
		c.dirtyStorage[k] = v
	}
	return c
}
