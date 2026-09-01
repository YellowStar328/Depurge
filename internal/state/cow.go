package state

import (
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
)

// cowEpochCounter 全局 epoch 发号器：单调递增、每次调用唯一。
// epoch=0 保留为「CoW 禁用」哨兵（深拷贝 Clone / vegeta / 串行等常规路径）。
var cowEpochCounter atomic.Uint64

func nextCowEpoch() uint64 { return cowEpochCounter.Add(1) }

// CloneCoW 是 depurge 并行阶段使用的 CoW 惰性克隆：浅拷贝 accounts map
//（新 map + 复制 *AccountState 指针），真正写时才物化被写账户。
//
// epoch 协议（无引用计数、无生命周期管理、GC 安全）：
//   - 父库与子库各发一个全新 epoch；账户 matEpoch 记录物化时的 epoch；
//   - 父/子任一方写账户时发现 acc.matEpoch != 自己的 epoch，先 cowClone
//     替换自己的 map 条目（账户级 CoW），绝不原地改共享账户；
//   - 由此克隆后被对方观察到的账户一律冻结，读路径零拷贝且无需同步；
//   - originStorage 槽级共享：原地写（finalise / MergeCommittedFrom）前
//     经 cowEnsureOrigin 校验 originMatEpoch，不匹配先整 map 拷贝。
//
// 仅支持纯内存模式（trieDB==nil）：带 trie 模式 storageTrie 共享且有原地
// Update 路径，CoW 前提不成立。
//
// 并发：多个 worker 可在同一父库上并发调用（depurge 的 RLock 段）。期间
// accounts map 只读（合并持写锁互斥），epoch 经全局原子计数器发号，安全。
func (s *MemoryStateDB) CloneCoW() *MemoryStateDB {
	if s.trieDB != nil {
		panic("CloneCoW 仅支持纯内存模式（无 trie）")
	}
	child := &MemoryStateDB{
		accounts:         make(map[common.Address]*AccountState, len(s.accounts)),
		selfdestructed:   make(map[common.Address]struct{}),
		transientStorage: make(map[common.Address]map[common.Hash]common.Hash),
		accessAddrs:      make(map[common.Address]struct{}),
		accessSlots:      make(map[common.Address]map[common.Hash]struct{}),
	}
	for addr, acc := range s.accounts {
		child.accounts[addr] = acc // 指针共享；任一方写前各自物化
	}
	child.epoch.Store(nextCowEpoch())
	// 父库同样换新 epoch：父库后续写（合并/串行兜底）也会先 CoW，
	// 冻结子库观察到的所有账户。漏掉这步会导致父库原地改共享账户。
	s.epoch.Store(nextCowEpoch())
	return child
}

// cowEnsureAccount 确保 addr 处账户为 s 私有：epoch 不匹配（与其他库共享）
// 则 cowClone 替换 map 条目后返回。返回可直接原地写的账户。
// 前提：账户存在；epoch=0（CoW 禁用）时恒 no-op。
func (s *MemoryStateDB) cowEnsureAccount(addr common.Address) *AccountState {
	acc := s.accounts[addr]
	epoch := s.epoch.Load()
	if epoch == 0 || acc.matEpoch == epoch {
		return acc
	}
	c := acc.cowClone(epoch)
	s.accounts[addr] = c
	return c
}

// cowEnsureOrigin 确保 acc.originStorage 为 s 私有（原地写前调用）：
// epoch 不匹配则整 map 拷贝（槽级 CoW）。epoch=0 时恒 no-op。
func (s *MemoryStateDB) cowEnsureOrigin(acc *AccountState) {
	epoch := s.epoch.Load()
	if epoch == 0 || acc.originMatEpoch == epoch {
		return
	}
	m := make(map[common.Hash]common.Hash, len(acc.originStorage))
	for k, v := range acc.originStorage {
		m[k] = v
	}
	acc.originStorage = m
	acc.originMatEpoch = epoch
}

// adoptAccount 将 acc 收编为 s 私有（打当前 epoch 戳）并放入 map。
// 用于「天然私有」的新账户：新建账户、合并时的整账户深拷贝。
func (s *MemoryStateDB) adoptAccount(addr common.Address, acc *AccountState) {
	epoch := s.epoch.Load()
	acc.matEpoch = epoch
	acc.originMatEpoch = epoch
	s.accounts[addr] = acc
}

// DisableCoW 关闭 CoW 保护（epoch 归零），使后续写全部原地进行，
// 不再触发账户级/槽级物化。
//
// 安全契约：仅允许在「已无任何存活子库观察者」时调用（depurge 并行段
// wg.Wait() 之后、串行兜底之前）——此时对共享账户的写无人可观察，
// 原地写回到 epoch=0 常规路径（深拷贝 Clone / vegeta / 串行基线同构），
// 只改写入方式、不改写入值。
//
// 自愈：下一次 CloneCoW 会重新发号，matEpoch 不匹配的账户在被写前
// 自动重新物化，本调用不为未来 CoW 留任何隐患。
func (s *MemoryStateDB) DisableCoW() { s.epoch.Store(0) }
