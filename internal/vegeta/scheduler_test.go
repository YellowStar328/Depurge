package vegeta

import (
	"reflect"
	"testing"
)

// TestClusterOrder 验证按 key 贪心聚簇的排序结果与确定性。
//
// 布局：k1 被 {0,2,3} 访问（3 笔），k2 被 {1,3} 访问（2 笔），
// tx4 过滤 nonce 后读写集为空。
// 期望：key 降序 k1→k2；k1 放置 0,2,3（原始序）；k2 放置 1（3 已放置）；
// tx4 收尾追加。
func TestClusterOrder(t *testing.T) {
	infos := []TxInfo{
		{Index: 0, Reads: []string{"k1"}, Writes: []string{"k1"}},
		{Index: 1, Reads: []string{"k2"}},
		{Index: 2, Writes: []string{"k1"}},
		{Index: 3, Reads: []string{"k2"}, Writes: []string{"k1", "k2"}},
		{Index: 4, Reads: []string{"acct:0x1:nonce"}, Writes: []string{"acct:0x1:nonce"}},
	}
	order := ClusterOrder(infos, DefaultOptions())
	want := []int{0, 2, 3, 1, 4}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

// TestClusterOrderTieByKeyString 验证同访问数时按 key 字典序（确定性）。
func TestClusterOrderTieByKeyString(t *testing.T) {
	infos := []TxInfo{
		{Index: 0, Writes: []string{"b"}},
		{Index: 1, Writes: []string{"a"}},
		{Index: 2, Writes: []string{"b"}},
		{Index: 3, Writes: []string{"a"}},
	}
	// a 与 b 均被 2 笔访问，字典序 a 先处理 → 1,3 先放置，随后 b → 0,2
	order := ClusterOrder(infos, DefaultOptions())
	want := []int{1, 3, 0, 2}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

// TestClusterOrderInputOrderInvariant 验证输入乱序不影响结果（内部按原始序归一）。
func TestClusterOrderInputOrderInvariant(t *testing.T) {
	infos := []TxInfo{
		{Index: 3, Reads: []string{"k2"}, Writes: []string{"k1", "k2"}},
		{Index: 0, Reads: []string{"k1"}, Writes: []string{"k1"}},
		{Index: 4, Reads: []string{"k3"}},
		{Index: 2, Writes: []string{"k1"}},
		{Index: 1, Reads: []string{"k2"}},
	}
	got := ClusterOrder(infos, DefaultOptions())
	want := ClusterOrder([]TxInfo{
		{Index: 0, Reads: []string{"k1"}, Writes: []string{"k1"}},
		{Index: 1, Reads: []string{"k2"}},
		{Index: 2, Writes: []string{"k1"}},
		{Index: 3, Reads: []string{"k2"}, Writes: []string{"k1", "k2"}},
		{Index: 4, Reads: []string{"k3"}},
	}, DefaultOptions())
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cluster order input-sensitive: got %v, want %v", got, want)
	}
}

func graphFixture() ([]TxInfo, []int) {
	infos := []TxInfo{
		{Index: 0, Hash: "0xa", Writes: []string{"k1"}},
		{Index: 1, Hash: "0xd", Reads: []string{"k2"}},
		{Index: 2, Hash: "0xb", Writes: []string{"k1"}},
		{Index: 3, Hash: "0xc", Reads: []string{"k2"}, Writes: []string{"k1", "k2"}},
	}
	// TestClusterOrder 期望的聚簇序
	order := []int{0, 2, 3, 1}
	return infos, order
}

// TestBuildGraphEdgeOrderNew 验证聚簇序定边（EdgeOrderNew，spec 字面）。
//
// k1 全写：0→2、0→3、2→3；k2 写者 3（聚簇位 2）先于读者 1（位 3）→ 3→1。
func TestBuildGraphEdgeOrderNew(t *testing.T) {
	infos, order := graphFixture()
	g := BuildGraph(order, infos, DefaultOptions())

	if g.EdgeCount() != 4 {
		t.Fatalf("edges = %d, want 4", g.EdgeCount())
	}
	if !reflect.DeepEqual(g.succs[0], []int{2, 3}) {
		t.Fatalf("succs[0] = %v, want [2 3]", g.succs[0])
	}
	if !reflect.DeepEqual(g.succs[2], []int{3}) {
		t.Fatalf("succs[2] = %v, want [3]", g.succs[2])
	}
	if !reflect.DeepEqual(g.succs[3], []int{1}) {
		t.Fatalf("succs[3] = %v, want [1]", g.succs[3])
	}
	if g.indeg[1] != 1 || g.indeg[2] != 1 || g.indeg[3] != 2 || g.indeg[0] != 0 {
		t.Fatalf("indeg = %v", g.indeg)
	}
}

// TestBuildGraphEdgeOrderOriginal 验证原始区块序定边（EdgeOrderOriginal）。
//
// k2 上原始序：读者 1 在前、写者 3 在后 → 1→3（与 new 口径相反）。
func TestBuildGraphEdgeOrderOriginal(t *testing.T) {
	infos, order := graphFixture()
	opts := DefaultOptions()
	opts.EdgeOrder = EdgeOrderOriginal
	g := BuildGraph(order, infos, opts)

	if g.EdgeCount() != 4 {
		t.Fatalf("edges = %d, want 4", g.EdgeCount())
	}
	if !reflect.DeepEqual(g.succs[1], []int{3}) {
		t.Fatalf("succs[1] = %v, want [3]", g.succs[1])
	}
	if len(g.succs[3]) != 0 {
		t.Fatalf("succs[3] = %v, want empty", g.succs[3])
	}
}

// TestBuildGraphReaderPairNoEdge 验证双方都只读同一 key 不建边（同波次并行）。
func TestBuildGraphReaderPairNoEdge(t *testing.T) {
	infos := []TxInfo{
		{Index: 0, Reads: []string{"k1"}},
		{Index: 1, Reads: []string{"k1"}},
	}
	g := BuildGraph([]int{0, 1}, infos, DefaultOptions())
	if g.EdgeCount() != 0 {
		t.Fatalf("edges = %d, want 0（双读无冲突）", g.EdgeCount())
	}
	if ready := g.Ready(); !reflect.DeepEqual(ready, []int{0, 1}) {
		t.Fatalf("ready = %v, want [0 1]", ready)
	}
}

// TestBuildGraphNonceFiltered 验证 nonce 伪冲突 key 被过滤（不开过滤时则建边）。
func TestBuildGraphNonceFiltered(t *testing.T) {
	infos := []TxInfo{
		{Index: 0, Writes: []string{"acct:0x1:nonce"}},
		{Index: 1, Reads: []string{"acct:0x1:nonce"}},
	}
	// 过滤 nonce 后 tx1 的访问 key 全被剔除 → 无边（同波次并行）
	gOn := BuildGraph([]int{0, 1}, infos, DefaultOptions())
	if gOn.EdgeCount() != 0 {
		t.Fatalf("filter-nonce: edges=%d, want 0", gOn.EdgeCount())
	}
	// 不过滤：写读冲突 → 0→1
	opts := DefaultOptions()
	opts.FilterNonce = false
	gOff := BuildGraph([]int{0, 1}, infos, opts)
	if gOff.EdgeCount() != 1 || !reflect.DeepEqual(gOff.succs[0], []int{1}) {
		t.Fatalf("no-filter: edges=%d succs[0]=%v, want 1/[1]", gOff.EdgeCount(), gOff.succs[0])
	}
}

// TestWaves 验证波次推进：按批取就绪交易，提交后解锁后继。
func TestWaves(t *testing.T) {
	infos, order := graphFixture()
	g := BuildGraph(order, infos, DefaultOptions())

	w1 := g.Ready()
	if !reflect.DeepEqual(w1, []int{0}) {
		t.Fatalf("wave1 = %v, want [0]", w1)
	}
	g.Commit(0)

	w2 := g.Ready()
	if !reflect.DeepEqual(w2, []int{2}) {
		t.Fatalf("wave2 = %v, want [2]", w2)
	}
	g.Commit(2)

	w3 := g.Ready()
	if !reflect.DeepEqual(w3, []int{3}) {
		t.Fatalf("wave3 = %v, want [3]", w3)
	}
	g.Commit(3)

	w4 := g.Ready()
	if !reflect.DeepEqual(w4, []int{1}) {
		t.Fatalf("wave4 = %v, want [1]", w4)
	}
	g.Commit(1)

	if g.Pending() {
		t.Fatal("all committed, Pending should be false")
	}
	if g.CommittedCount() != 4 || g.AbortedCount() != 0 {
		t.Fatalf("committed=%d aborted=%d, want 4/0", g.CommittedCount(), g.AbortedCount())
	}
}

// TestInvalidateCascade 验证作废级联到全部 DAG 后继（传递闭包）。
//
// 菱形依赖：0→2、0→3、2→4、3→4。作废 0 应级联 {2,3,4}。
func TestInvalidateCascade(t *testing.T) {
	infos := []TxInfo{
		{Index: 0, Writes: []string{"k"}},
		{Index: 2, Writes: []string{"k"}},
		{Index: 3, Writes: []string{"k"}},
		{Index: 4, Writes: []string{"k"}},
		{Index: 9, Reads: []string{"other"}},
	}
	// 聚簇序按索引即可；k 上全写者：0→2→3→4
	g := BuildGraph([]int{0, 2, 3, 4, 9}, infos, DefaultOptions())

	newly := g.Invalidate(0)
	if !reflect.DeepEqual(newly, []int{0, 2, 3, 4}) {
		t.Fatalf("newly aborted = %v, want [0 2 3 4]", newly)
	}
	// tx9 无冲突不受级联影响，仍就绪
	if ready := g.Ready(); !reflect.DeepEqual(ready, []int{9}) {
		t.Fatalf("ready = %v, want [9]", ready)
	}
	g.Commit(9)
	if g.Pending() {
		t.Fatal("after commit(9) no pending tx should remain")
	}
	if g.AbortedCount() != 4 {
		t.Fatalf("aborted = %d, want 4", g.AbortedCount())
	}
	// 重复作废为 no-op
	if again := g.Invalidate(0); again != nil {
		t.Fatalf("re-invalidate = %v, want nil", again)
	}
	// 提交已作废交易为 no-op（已提交的仍只有 tx9）
	g.Commit(2)
	if g.CommittedCount() != 1 {
		t.Fatalf("committed = %d, want 1（仅 tx9）", g.CommittedCount())
	}
}

// TestInvalidateAfterPartialCommit 验证部分前驱已提交时作废只级联可达后继。
func TestInvalidateAfterPartialCommit(t *testing.T) {
	infos := []TxInfo{
		{Index: 0, Writes: []string{"ka"}},
		{Index: 1, Writes: []string{"kb"}},
		{Index: 2, Writes: []string{"ka", "kb"}},
		{Index: 3, Writes: []string{"ka"}},
	}
	// ka: 0→2, 0→3, 2→3；kb: 1→2
	g := BuildGraph([]int{0, 1, 2, 3}, infos, DefaultOptions())

	g.Commit(0) // 2 仍等待 1（kb），3 仍等待 2（ka）
	if ready := g.Ready(); !reflect.DeepEqual(ready, []int{1}) {
		t.Fatalf("ready = %v, want [1]", ready)
	}
	g.Commit(1) // 解锁 2
	if ready := g.Ready(); !reflect.DeepEqual(ready, []int{2}) {
		t.Fatalf("ready = %v, want [2]", ready)
	}
	// 作废 2：级联其后继 3（3 不得在 2 缺席时并行提交）
	newly := g.Invalidate(2)
	if !reflect.DeepEqual(newly, []int{2, 3}) {
		t.Fatalf("newly = %v, want [2 3]", newly)
	}
	if g.Pending() {
		t.Fatal("no pending tx should remain")
	}
}

// TestSortSerial 验证串行兜底双顺序。
func TestSortSerial(t *testing.T) {
	indices := []int{3, 1, 2}
	hashOf := func(i int) string {
		switch i {
		case 1:
			return "0xcc"
		case 2:
			return "0xaa"
		default:
			return "0xbb"
		}
	}
	block := SortSerial(indices, hashOf, DefaultOptions())
	if !reflect.DeepEqual(block, []int{1, 2, 3}) {
		t.Fatalf("block order = %v, want [1 2 3]", block)
	}
	opts := DefaultOptions()
	opts.SerialOrder = SerialOrderHash
	hash := SortSerial(indices, hashOf, opts)
	if !reflect.DeepEqual(hash, []int{2, 3, 1}) {
		t.Fatalf("hash order = %v, want [2 3 1]", hash)
	}
}

// TestSubsetOf 验证乐观验证的集合包含判断。
func TestSubsetOf(t *testing.T) {
	if miss := SubsetOf([]string{"a", "b"}, []string{"a", "b", "c"}); len(miss) != 0 {
		t.Fatalf("miss = %v, want empty", miss)
	}
	if miss := SubsetOf([]string{"a", "d"}, []string{"a", "b", "c"}); !reflect.DeepEqual(miss, []string{"d"}) {
		t.Fatalf("miss = %v, want [d]", miss)
	}
	if miss := SubsetOf(nil, []string{"a"}); len(miss) != 0 {
		t.Fatalf("miss = %v, want empty", miss)
	}
}

// TestFilterKeys 验证 key 过滤与去重（nonce 伪冲突 + coinbase balance tip）。
func TestFilterKeys(t *testing.T) {
	keys := []string{"b", "a", "acct:0x1:nonce", "b"}
	got := FilterKeys(keys, Options{FilterNonce: true})
	if !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("filtered = %v, want [a b]", got)
	}
	got = FilterKeys(keys, Options{})
	if !reflect.DeepEqual(got, []string{"a", "acct:0x1:nonce", "b"}) {
		t.Fatalf("unfiltered = %v", got)
	}

	// coinbase balance tip key 过滤（FilterCoinbase 且 Coinbase 匹配 EIP-55 格式）
	cb := "0x4838B106FCe9647Bdf1E7877BF73cE8B0BAD5f97"
	cbKeys := []string{"b", "acct:" + cb + ":balance", "acct:0x1:balance"}
	got = FilterKeys(cbKeys, Options{FilterCoinbase: true, Coinbase: cb})
	if !reflect.DeepEqual(got, []string{"acct:0x1:balance", "b"}) {
		t.Fatalf("coinbase filtered = %v", got)
	}
	// 未开启过滤或地址不匹配时保留
	got = FilterKeys(cbKeys, Options{FilterCoinbase: false, Coinbase: cb})
	if !reflect.DeepEqual(got, []string{"acct:0x1:balance", "acct:" + cb + ":balance", "b"}) {
		t.Fatalf("coinbase unfiltered = %v", got)
	}

	if !IsNonceKey("acct:0x1:nonce") {
		t.Fatal("IsNonceKey should be true")
	}
	if IsNonceKey("acct:0x1:balance") {
		t.Fatal("IsNonceKey should be false for balance")
	}
}

// TestOptionsValidate 验证选项校验。
func TestOptionsValidate(t *testing.T) {
	if err := DefaultOptions().Validate(); err != nil {
		t.Fatalf("default options invalid: %v", err)
	}
	bad := DefaultOptions()
	bad.EdgeOrder = "xxx"
	if err := bad.Validate(); err == nil {
		t.Fatal("expect error for bad edge order")
	}
	bad2 := DefaultOptions()
	bad2.SerialOrder = "xxx"
	if err := bad2.Validate(); err == nil {
		t.Fatal("expect error for bad serial order")
	}
}
