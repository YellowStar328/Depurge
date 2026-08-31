package state

import (
	"math/big"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

func testAddr(s string) common.Address { return common.HexToAddress(s) }

// u256v 构造 uint256 值（测试辅助）。
func u256v(v uint64) *uint256.Int { return uint256.NewInt(v) }

// TestMergeCommittedFromFields 验证字段级合并语义：
// storage 槽按 src 最终值落 master；balance 走覆盖（非 additive 时）；
// nonce 走 +=1 累加（可交换计数器，单笔交易 delta 恒为 +1）。
// 未在写集中的字段不受影响。
func TestMergeCommittedFromFields(t *testing.T) {
	addrA := testAddr("0x1111111111111111111111111111111111111111")
	master := NewMemoryStateDBWithTrie(false)
	master.InitAccount(addrA, u256v(100), 5, nil,
		map[common.Hash]common.Hash{{1}: {10}})

	// 成员库：克隆后模拟交易执行（balance-10 / nonce+1 / 槽 k1 改值 / 新槽 k2）
	src := master.Clone()
	src.SubBalance(addrA, u256v(10), 0)
	src.SetNonce(addrA, 6, 0) // master@5 执行后 +1 = 6
	src.SetState(addrA, common.Hash{1}, common.Hash{11})
	src.SetState(addrA, common.Hash{2}, common.Hash{22})
	src.Finalise(true)

	writeKeys := []string{
		"acct:" + addrA.String() + ":balance",
		"acct:" + addrA.String() + ":nonce",
		FlatStorageKey(addrA, common.Hash{1}),
		FlatStorageKey(addrA, common.Hash{2}),
	}
	if err := master.MergeCommittedFrom(src, writeKeys, nil); err != nil {
		t.Fatalf("merge: %v", err)
	}

	if got := master.GetBalance(addrA); got.Cmp(u256v(90)) != 0 {
		t.Fatalf("balance = %v, want 90", got)
	}
	// nonce +=1 累加：master@5 + 1 = 6（单笔交易，与覆盖结果一致）
	if got := master.GetNonce(addrA); got != 6 {
		t.Fatalf("nonce = %d, want 6 (master 5 += 1)", got)
	}
	if got := master.GetState(addrA, common.Hash{1}); got != (common.Hash{11}) {
		t.Fatalf("slot1 = %v, want 11", got)
	}
	if got := master.GetState(addrA, common.Hash{2}); got != (common.Hash{22}) {
		t.Fatalf("slot2 = %v, want 22", got)
	}
}

// TestMergeCommittedFromNonceAccumulate 验证 nonce +=1 累加在并发场景的正确性：
// 同一 sender 的两笔交易在同一波次并发执行（各自从相同基准 clone，
// nonce 各自 +1 得相同值），依次合并必须累加（master +=1 两次）而非覆盖
// （后者会用相同值覆盖丢失一次递增）。这是 serialized-order MISMATCH 的根因修复。
func TestMergeCommittedFromNonceAccumulate(t *testing.T) {
	sender := testAddr("0x5555555555555555555555555555555555555555")
	master := NewMemoryStateDBWithTrie(false)
	master.InitAccount(sender, u256v(1000), 100, nil, nil)

	// 两个成员各自 clone（sender nonce=100），执行后 nonce 都=101（各自 +1）
	m1 := master.Clone()
	m1.SetNonce(sender, 101, 0)
	m1.Finalise(true)
	m2 := master.Clone()
	m2.SetNonce(sender, 101, 0)
	m2.Finalise(true)

	wk := "acct:" + sender.String() + ":nonce"
	if err := master.MergeCommittedFrom(m1, []string{wk}, nil); err != nil {
		t.Fatalf("merge m1: %v", err)
	}
	if got := master.GetNonce(sender); got != 101 {
		t.Fatalf("after m1: nonce = %d, want 101 (100 += 1)", got)
	}
	if err := master.MergeCommittedFrom(m2, []string{wk}, nil); err != nil {
		t.Fatalf("merge m2: %v", err)
	}
	// 累加：101 += 1 = 102（覆盖语义会得 101，丢失第二次递增）
	if got := master.GetNonce(sender); got != 102 {
		t.Fatalf("after m2: nonce = %d, want 102 (101 += 1, accumulate not overwrite)", got)
	}
}

// TestMergeCommittedFromAdditiveBalance 验证 coinbase tip 增量累加：
// 批内两个成员各自在克隆（master@100）上累加 tip，增量由调用方在执行时
// 预计算（成员最终值 − 克隆基准），依次合并必须累加而非覆盖——
// 尤其成员 2 合并时 master 已被成员 1 推进到 107，此时重算
// "src − 当前 master" 会得到错误增量（103−107），必须用预计算值 3。
func TestMergeCommittedFromAdditiveBalance(t *testing.T) {
	cb := testAddr("0x2222222222222222222222222222222222222222")
	master := NewMemoryStateDBWithTrie(false)
	master.InitAccount(cb, u256v(100), 0, nil, nil)

	// 成员1：tip +7（克隆基准 100，最终 107，delta=7）
	m1 := master.Clone()
	m1.AddBalance(cb, u256v(7), 0)
	m1.Finalise(true)
	// 成员2：tip +3（克隆基准仍为 100，最终 103，delta=3）
	m2 := master.Clone()
	m2.AddBalance(cb, u256v(3), 0)
	m2.Finalise(true)

	wk := "acct:" + cb.String() + ":balance"
	d1 := map[common.Address]*big.Int{cb: big.NewInt(7)}
	d2 := map[common.Address]*big.Int{cb: big.NewInt(3)}
	if err := master.MergeCommittedFrom(m1, []string{wk}, d1); err != nil {
		t.Fatalf("merge m1: %v", err)
	}
	if got := master.GetBalance(cb); got.Cmp(u256v(107)) != 0 {
		t.Fatalf("after m1: balance = %v, want 107", got)
	}
	if err := master.MergeCommittedFrom(m2, []string{wk}, d2); err != nil {
		t.Fatalf("merge m2: %v", err)
	}
	if got := master.GetBalance(cb); got.Cmp(u256v(110)) != 0 {
		t.Fatalf("after m2: balance = %v, want 110（预计算增量累加不覆盖）", got)
	}
}

// TestMergeCommittedFromAccountLifecycle 验证账户新建（整拷）与删除同步。
func TestMergeCommittedFromAccountLifecycle(t *testing.T) {
	addrB := testAddr("0x3333333333333333333333333333333333333333")
	addrC := testAddr("0x4444444444444444444444444444444444444444")
	master := NewMemoryStateDBWithTrie(false)
	master.InitAccount(addrC, u256v(5), 0, nil, nil)

	// 成员：新建 addrB（SetCode）并清空 addrC（EIP-161 触发 Finalise 删除）
	src := master.Clone()
	src.CreateContract(addrB)
	src.SetCode(addrB, []byte{0x60, 0x00})
	src.SetState(addrB, common.Hash{9}, common.Hash{99})
	src.SubBalance(addrC, u256v(5), 0)
	src.SetNonce(addrC, 0, 0)
	src.Finalise(true)

	writeKeys := []string{
		FlatStorageKey(addrB, common.Hash{9}),
		"acct:" + addrC.String() + ":balance",
	}
	if err := master.MergeCommittedFrom(src, writeKeys, nil); err != nil {
		t.Fatalf("merge: %v", err)
	}

	// addrB 新建成功（含 code 与 storage）
	if !master.Exist(addrB) {
		t.Fatal("addrB should be created by merge")
	}
	if got := master.GetCode(addrB); !reflect.DeepEqual(got, []byte{0x60, 0x00}) {
		t.Fatalf("addrB code = %v", got)
	}
	if got := master.GetState(addrB, common.Hash{9}); got != (common.Hash{99}) {
		t.Fatalf("addrB slot = %v, want 99", got)
	}
	// addrC 被成员删除（写集涉及 → master 同步删除）
	if master.Exist(addrC) {
		t.Fatal("addrC should be deleted by merge")
	}
}

// TestMergeCommittedFromRejectsTrie 拒绝非纯内存模式。
func TestMergeCommittedFromRejectsTrie(t *testing.T) {
	if err := NewMemoryStateDB().MergeCommittedFrom(NewMemoryStateDB(), nil, nil); err == nil {
		t.Fatal("expect error for trie-backed db")
	}
}

// TestDiffFlatStates 验证状态 diff 语义（空账户跳过 + 同 key 不同值）。
func TestDiffFlatStates(t *testing.T) {
	a := map[string]string{"k1": "v1", "k2": "v2", "empty": "0x000...0"}
	b := map[string]string{"k1": "v1", "k2": "OTHER"}
	onlyA, onlyB := DiffFlatStates(a, b)
	if !reflect.DeepEqual(onlyA, []string{"empty", "k2"}) {
		t.Fatalf("onlyA = %v, want [empty k2]", onlyA)
	}
	if !reflect.DeepEqual(onlyB, []string{"k2"}) {
		t.Fatalf("onlyB = %v, want [k2]", onlyB)
	}
}
