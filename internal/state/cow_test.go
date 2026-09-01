package state

import (
	"math/big"
	"reflect"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// TestCloneCoWIsolation 验证账户级 CoW 的双向隔离：
// 克隆后任一方写账户都不影响另一方（写前各自物化，绝不原地改共享账户）。
func TestCloneCoWIsolation(t *testing.T) {
	a := testAddr("0x1111111111111111111111111111111111111111")
	parent := NewMemoryStateDBWithTrie(false)
	parent.InitAccount(a, u256v(100), 5, []byte{0x60}, map[common.Hash]common.Hash{{1}: {10}})

	child := parent.CloneCoW()

	// 子库写：balance/nonce/storage 均不影响父库
	child.SubBalance(a, u256v(10), 0)
	child.SetNonce(a, 6, 0)
	child.SetState(a, common.Hash{1}, common.Hash{11})
	if got := parent.GetBalance(a); got.Cmp(u256v(100)) != 0 {
		t.Fatalf("parent balance = %v, want 100 (child write leaked)", got)
	}
	if got := parent.GetNonce(a); got != 5 {
		t.Fatalf("parent nonce = %d, want 5", got)
	}
	if got := parent.GetState(a, common.Hash{1}); got != (common.Hash{10}) {
		t.Fatalf("parent slot = %v, want 10", got)
	}

	// 父库写：不影响子库快照
	parent.AddBalance(a, u256v(7), 0)
	if got := child.GetBalance(a); got.Cmp(u256v(90)) != 0 {
		t.Fatalf("child balance = %v, want 90 (parent write leaked)", got)
	}
	if got := parent.GetBalance(a); got.Cmp(u256v(107)) != 0 {
		t.Fatalf("parent balance = %v, want 107", got)
	}
}

// TestCloneCoWOriginSharedUntilFinalise 验证槽级 CoW 的物化时机：
// 克隆后 originStorage map 共享（零拷贝）；子库 Finalise 原地合并 dirty
// 前整 map 私有化，父库 origin 不受污染。
func TestCloneCoWOriginSharedUntilFinalise(t *testing.T) {
	a := testAddr("0x1111111111111111111111111111111111111111")
	parent := NewMemoryStateDBWithTrie(false)
	// 余额非零：避免 Finalise 按 EIP-158 空账户（nonce=balance=code=0）删除
	parent.InitAccount(a, u256v(1), 0, nil,
		map[common.Hash]common.Hash{{1}: {10}, {2}: {20}})

	child := parent.CloneCoW()
	child.SetState(a, common.Hash{1}, common.Hash{11})

	// SetState 后：账户已物化（指针不同），但 originStorage 仍共享（槽级延迟）
	pAcc, cAcc := parent.accounts[a], child.accounts[a]
	if pAcc == cAcc {
		t.Fatal("account should be CoW-copied in child after SetState")
	}
	if reflect.ValueOf(pAcc.originStorage).Pointer() != reflect.ValueOf(cAcc.originStorage).Pointer() {
		t.Fatal("originStorage should still be shared before Finalise (slot-level CoW)")
	}

	child.Finalise(true)

	// Finalise 后：子库 origin 私有化，父库 origin 完全不受污染
	if got := parent.GetState(a, common.Hash{1}); got != (common.Hash{10}) {
		t.Fatalf("parent slot1 = %v, want 10 (finalise leaked into shared origin)", got)
	}
	if got := parent.GetCommittedState(a, common.Hash{1}); got != (common.Hash{10}) {
		t.Fatalf("parent committed slot1 = %v, want 10", got)
	}
	if got := child.GetState(a, common.Hash{1}); got != (common.Hash{11}) {
		t.Fatalf("child slot1 = %v, want 11", got)
	}
	if got := child.GetCommittedState(a, common.Hash{1}); got != (common.Hash{11}) {
		t.Fatalf("child committed slot1 = %v, want 11", got)
	}
	if reflect.ValueOf(pAcc.originStorage).Pointer() == reflect.ValueOf(cAcc.originStorage).Pointer() {
		t.Fatal("originStorage should be privatized after Finalise")
	}
	// 未写的槽保持共享值可见
	if got := child.GetState(a, common.Hash{2}); got != (common.Hash{20}) {
		t.Fatalf("child slot2 = %v, want 20", got)
	}
}

// TestCloneCoWMergeNoPolluteLiveClone 验证 dispatcher 场景：
// 合并一个成员的结果进 master 时，不得污染仍存活的其它 CoW 克隆
//（对应 depurge：worker B 持克隆执行期间，worker A 的结果正在合并）。
func TestCloneCoWMergeNoPolluteLiveClone(t *testing.T) {
	a := testAddr("0x1111111111111111111111111111111111111111")
	master := NewMemoryStateDBWithTrie(false)
	master.InitAccount(a, u256v(100), 0, nil, map[common.Hash]common.Hash{{1}: {10}})

	c1 := master.CloneCoW()
	c2 := master.CloneCoW() // c2 在 c1 合并全程存活

	// c1 执行并提交
	c1.AddBalance(a, u256v(5), 0)
	c1.SetState(a, common.Hash{1}, common.Hash{11})
	c1.Finalise(true)
	wk := []string{
		"acct:" + a.String() + ":balance",
		FlatStorageKey(a, common.Hash{1}),
	}
	if err := master.MergeCommittedFrom(c1, wk, nil); err != nil {
		t.Fatalf("merge c1: %v", err)
	}

	// master 已更新
	if got := master.GetBalance(a); got.Cmp(u256v(105)) != 0 {
		t.Fatalf("master balance = %v, want 105", got)
	}
	if got := master.GetState(a, common.Hash{1}); got != (common.Hash{11}) {
		t.Fatalf("master slot = %v, want 11", got)
	}
	// c2 快照冻结在合并前
	if got := c2.GetBalance(a); got.Cmp(u256v(100)) != 0 {
		t.Fatalf("live clone balance = %v, want 100 (merge polluted live clone)", got)
	}
	if got := c2.GetState(a, common.Hash{1}); got != (common.Hash{10}) {
		t.Fatalf("live clone slot = %v, want 10", got)
	}

	// c2 基于自己的冻结快照继续执行并提交（不同槽，模拟调度无冲突）
	c2.SetState(a, common.Hash{2}, common.Hash{22})
	c2.Finalise(true)
	if err := master.MergeCommittedFrom(c2, []string{FlatStorageKey(a, common.Hash{2})}, nil); err != nil {
		t.Fatalf("merge c2: %v", err)
	}
	if got := master.GetState(a, common.Hash{2}); got != (common.Hash{22}) {
		t.Fatalf("master slot2 = %v, want 22", got)
	}
	// c1 的提交不被 c2 覆盖
	if got := master.GetState(a, common.Hash{1}); got != (common.Hash{11}) {
		t.Fatalf("master slot1 = %v, want 11 (c1 commit lost)", got)
	}
}

// TestCloneCoWAdditiveMergeWithLiveClone 验证 coinbase 增量合并在存活克隆下的正确性。
func TestCloneCoWAdditiveMergeWithLiveClone(t *testing.T) {
	cb := testAddr("0x2222222222222222222222222222222222222222")
	master := NewMemoryStateDBWithTrie(false)
	master.InitAccount(cb, u256v(100), 0, nil, nil)

	observer := master.CloneCoW() // 存活观察者

	m1 := master.CloneCoW()
	m1.AddBalance(cb, u256v(7), 0)
	m1.Finalise(true)
	wk := "acct:" + cb.String() + ":balance"
	if err := master.MergeCommittedFrom(m1, []string{wk},
		map[common.Address]*big.Int{cb: big.NewInt(7)}); err != nil {
		t.Fatalf("merge m1: %v", err)
	}
	m2 := master.CloneCoW()
	m2.AddBalance(cb, u256v(3), 0)
	m2.Finalise(true)
	if err := master.MergeCommittedFrom(m2, []string{wk},
		map[common.Address]*big.Int{cb: big.NewInt(3)}); err != nil {
		t.Fatalf("merge m2: %v", err)
	}
	if got := master.GetBalance(cb); got.Cmp(u256v(110)) != 0 {
		t.Fatalf("master balance = %v, want 110", got)
	}
	// 观察者仍见克隆时点的值
	if got := observer.GetBalance(cb); got.Cmp(u256v(100)) != 0 {
		t.Fatalf("observer balance = %v, want 100", got)
	}
}

// TestCloneCoWNewAccountIsolation 验证子库新建账户对父库不可见，合并后落库。
func TestCloneCoWNewAccountIsolation(t *testing.T) {
	b := testAddr("0x3333333333333333333333333333333333333333")
	parent := NewMemoryStateDBWithTrie(false)
	child := parent.CloneCoW()

	child.CreateContract(b)
	child.SetCode(b, []byte{0x60, 0x00})
	child.SetState(b, common.Hash{9}, common.Hash{99})
	child.Finalise(true)

	if parent.Exist(b) {
		t.Fatal("new account in child must not leak into parent")
	}
	wk := []string{FlatStorageKey(b, common.Hash{9})}
	if err := parent.MergeCommittedFrom(child, wk, nil); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if !parent.Exist(b) {
		t.Fatal("merged account should exist in parent")
	}
	if got := parent.GetState(b, common.Hash{9}); got != (common.Hash{99}) {
		t.Fatalf("merged slot = %v, want 99", got)
	}
}

// TestCloneCoWSerialFallbackOnEpochMaster 验证 master 在 CloneCoW 之后
//（epoch 已激活）直接在其上串行执行（兜底段语义）仍然正确。
func TestCloneCoWSerialFallbackOnEpochMaster(t *testing.T) {
	a := testAddr("0x1111111111111111111111111111111111111111")
	master := NewMemoryStateDBWithTrie(false)
	master.InitAccount(a, u256v(100), 0, nil, map[common.Hash]common.Hash{{1}: {10}})

	dead := master.CloneCoW() // 推进 epoch 后丢弃（模拟并行阶段结束）
	_ = dead

	master.AddBalance(a, u256v(5), 0)
	master.SetState(a, common.Hash{1}, common.Hash{99})
	master.Finalise(true)

	if got := master.GetBalance(a); got.Cmp(u256v(105)) != 0 {
		t.Fatalf("balance = %v, want 105", got)
	}
	if got := master.GetState(a, common.Hash{1}); got != (common.Hash{99}) {
		t.Fatalf("slot = %v, want 99", got)
	}
	if got := master.GetCommittedState(a, common.Hash{1}); got != (common.Hash{99}) {
		t.Fatalf("committed slot = %v, want 99", got)
	}
}

// TestDisableCoWSerialFallbackInPlace 验证串行兜底场景的 DisableCoW：
// 并行段（CloneCoW）结束、子库丢弃后关闭 CoW，master 上的写全部原地
// 进行（账户指针不变、无物化），值正确；且下一次 CloneCoW 重新发号、
// 隔离语义自愈。
func TestDisableCoWSerialFallbackInPlace(t *testing.T) {
	a := testAddr("0x1111111111111111111111111111111111111111")
	master := NewMemoryStateDBWithTrie(false)
	master.InitAccount(a, u256v(100), 0, nil, map[common.Hash]common.Hash{{1}: {10}})

	dead := master.CloneCoW() // 推进 epoch（模拟并行段）
	_ = dead                  // 并行段结束：子库已合并或作废，无存活观察者

	master.DisableCoW() // 串行兜底前置：关 CoW

	before := master.accounts[a]
	master.AddBalance(a, u256v(5), 0)
	master.SetState(a, common.Hash{1}, common.Hash{99})
	master.Finalise(true)

	if master.accounts[a] != before {
		t.Fatal("write after DisableCoW must be in-place (no account materialization)")
	}
	if got := master.GetBalance(a); got.Cmp(u256v(105)) != 0 {
		t.Fatalf("balance = %v, want 105", got)
	}
	if got := master.GetState(a, common.Hash{1}); got != (common.Hash{99}) {
		t.Fatalf("slot = %v, want 99", got)
	}
	if got := master.GetCommittedState(a, common.Hash{1}); got != (common.Hash{99}) {
		t.Fatalf("committed slot = %v, want 99", got)
	}

	// 自愈：下一轮 CloneCoW 重新发号，隔离恢复
	c := master.CloneCoW()
	c.SubBalance(a, u256v(5), 0)
	if got := master.GetBalance(a); got.Cmp(u256v(105)) != 0 {
		t.Fatalf("master balance = %v, want 105 (self-heal isolation broken)", got)
	}
	if got := c.GetBalance(a); got.Cmp(u256v(100)) != 0 {
		t.Fatalf("child balance = %v, want 100", got)
	}
}

// TestCloneCoWCodeHashEager 验证 codeHash 急算（创建时即有值，无需触发读）。
func TestCloneCoWCodeHashEager(t *testing.T) {
	a := testAddr("0x1111111111111111111111111111111111111111")
	code := []byte{1, 2, 3}
	db := NewMemoryStateDBWithTrie(false)
	db.InitAccount(a, u256v(0), 0, code, nil)

	if got := db.accounts[a].codeHash; got != crypto.Keccak256Hash(code) {
		t.Fatalf("codeHash not eager: got %v", got)
	}
	// SetCode 后同样急算
	db.SetCode(a, []byte{9, 9})
	if got := db.accounts[a].codeHash; got != crypto.Keccak256Hash([]byte{9, 9}) {
		t.Fatalf("codeHash not eager after SetCode: got %v", got)
	}
}

// TestCloneCoWPanicsWithTrie 验证带 trie 模式拒绝 CloneCoW。
func TestCloneCoWPanicsWithTrie(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("CloneCoW on trie-backed db should panic")
		}
	}()
	NewMemoryStateDB().CloneCoW()
}

// TestCloneCoWConcurrentWorkers 并发压测（配 -race 运行）：
// 模拟 depurge 并行段——多 worker 并发 CloneCoW 同一 master、各自执行，
// dispatcher 在写锁下串行合并；期间其它 worker 的克隆仍存活。
// 任何「原地改共享账户/共享 origin map」的缺陷都会被 race 检测器捕获。
func TestCloneCoWConcurrentWorkers(t *testing.T) {
	const (
		rounds  = 10
		workers = 8
	)
	contract := testAddr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	master := NewMemoryStateDBWithTrie(false)
	master.InitAccount(contract, u256v(0), 0, []byte{0x60, 0x00},
		map[common.Hash]common.Hash{{0}: {1}})

	for r := 0; r < rounds; r++ {
		var mu sync.RWMutex
		type result struct {
			db   *MemoryStateDB
			keys []string
			slot common.Hash
		}
		results := make(chan result, workers)
		var wg sync.WaitGroup

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				mu.RLock()
				db := master.CloneCoW()
				// 并发读共享账户（codeHash/code/origin 槽）：
				// 与 dispatcher 的合并并发，验证读路径无写副作用。
				_ = db.GetCodeHash(contract)
				_ = db.GetCode(contract)
				_ = db.GetState(contract, common.Hash{0})
				mu.RUnlock()

				// 各 worker 写自己的槽 + 自己的账户（模拟调度无冲突）
				slot := common.Hash{byte(r*workers + i + 1)}
				db.SetState(contract, slot, common.Hash{byte(100 + i)})
				own := common.BytesToAddress([]byte{0xbb, byte(i + 1)})
				db.AddBalance(own, u256v(uint64(i+1)), 0)
				db.Finalise(true)

				results <- result{
					db: db,
					keys: []string{
						FlatStorageKey(contract, slot),
						"acct:" + own.String() + ":balance",
					},
					slot: slot,
				}
			}(i)
		}

		// dispatcher：写锁下串行合并（此时可能仍有 worker 持克隆执行）
		go func() { wg.Wait(); close(results) }()
		for res := range results {
			mu.Lock()
			if err := master.MergeCommittedFrom(res.db, res.keys, nil); err != nil {
				t.Errorf("merge: %v", err)
			}
			mu.Unlock()
		}

		// 本轮所有写入落库
		for i := 0; i < workers; i++ {
			slot := common.Hash{byte(r*workers + i + 1)}
			if got := master.GetState(contract, slot); got != (common.Hash{byte(100 + i)}) {
				t.Fatalf("round %d slot %v = %v, want %v", r, slot, got, common.Hash{byte(100 + i)})
			}
		}
	}
}
