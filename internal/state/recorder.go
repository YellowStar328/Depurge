package state

import (
	"sort"

	"github.com/ethereum/go-ethereum/common"
)

// Granularity 读写集采集粒度。
type Granularity int

const (
	// GranularitySlot slot 级（默认）：每条记录含 address+slot+value+old_value+op_type。
	GranularitySlot Granularity = iota
	// GranularityAccount 账户级：仅记录被访问的账户地址（粗粒度分区实验用）。
	GranularityAccount
)

// Config 采集器配置。
type Config struct {
	Granularity    Granularity
	CollectBalance bool // 是否采集 balance 读写（flat_rwset 对齐 dataset 需要）
	CollectNonce   bool // 是否采集 nonce 读写
	CollectCode    bool // 是否采集 code 读取
}

// DefaultConfig 返回默认配置：slot 级粒度，balance/nonce 采集开启。
func DefaultConfig() Config {
	return Config{
		Granularity:    GranularitySlot,
		CollectBalance: true,
		CollectNonce:   true,
		CollectCode:    false,
	}
}

// AccessRecorder 单笔交易的读写集采集器。
//
// per-tx 独立实例设计：每笔交易 new 一个 Recorder，交易结束 Freeze()。
// 多交易并行重放实验时各 Recorder 互不共享、天然无锁，
// 不会让采集器锁竞争污染并发实验的加速比测量。
type AccessRecorder struct {
	cfg Config

	root    *CallFrame            // 顶层帧（tx 级，depth=0）
	stack   []*CallFrame          // 当前调用栈（栈顶 = 当前帧）
	frames  []*CallFrame          // 全部帧（按创建顺序）
	cur     *CallFrame            // 当前帧缓存（避免重复索引栈顶）
	entries int                   // 总访问条数

	flatRead  map[string]struct{} // 扁平读 key 去重
	flatWrite map[string]struct{} // 扁平写 key 去重

	frozen bool
}

// NewRecorder 创建 per-tx 采集器并初始化顶层帧。
func NewRecorder(cfg Config) *AccessRecorder {
	root := &CallFrame{
		FrameID: "f0",
		Type:    "ROOT",
		Depth:   0,
	}
	return &AccessRecorder{
		cfg:       cfg,
		root:      root,
		stack:     []*CallFrame{root},
		frames:    []*CallFrame{root},
		cur:       root,
		flatRead:  make(map[string]struct{}),
		flatWrite: make(map[string]struct{}),
	}
}

// NewFrame 由 FrameTracer 在 OnEnter 时调用：创建子帧并入栈。
// 返回新帧，其父帧为当前栈顶。
func (r *AccessRecorder) NewFrame(typ string, caller, addr common.Address, depth int) *CallFrame {
	if r == nil || r.frozen {
		return nil
	}
	frame := &CallFrame{
		FrameID:  "f" + itoa(len(r.frames)),
		ParentID: r.cur.FrameID,
		Type:     typ,
		Caller:   caller,
		Address:  addr,
		Depth:    depth,
	}
	r.cur.Children = append(r.cur.Children, frame)
	r.frames = append(r.frames, frame)
	r.stack = append(r.stack, frame)
	r.cur = frame
	return frame
}

// PopFrame 由 FrameTracer 在 OnExit 时调用：弹出栈顶帧。
// depth 用于一致性校验（不匹配时重同步，防止 EVM 异常路径导致栈漂移）。
func (r *AccessRecorder) PopFrame(depth int) {
	if r == nil || r.frozen {
		return
	}
	if len(r.stack) <= 1 {
		return // 只剩 ROOT
	}
	r.stack = r.stack[:len(r.stack)-1]
	r.cur = r.stack[len(r.stack)-1]
}

// FinishFrame 由 FrameTracer 在 OnExit 时调用：写入栈顶帧结果并弹栈。
// OnExit 的 depth 与 OnEnter 配对（该帧进入时的深度）。
func (r *AccessRecorder) FinishFrame(depth int, gasUsed uint64, reverted bool, errStr string) {
	if r == nil || r.frozen {
		return
	}
	if r.cur != nil && r.cur != r.root && r.cur.Depth == depth {
		r.cur.GasUsed = gasUsed
		r.cur.Reverted = reverted
		r.cur.Err = errStr
	}
	r.PopFrame(depth)
}

// SetRootResult 由 replayer 在交易结束后设置 ROOT 帧结果。
func (r *AccessRecorder) SetRootResult(gasUsed uint64, reverted bool, errStr string) {
	if r == nil {
		return
	}
	r.root.GasUsed = gasUsed
	r.root.Reverted = reverted
	r.root.Err = errStr
}

// FrameCount 返回本 tx 的帧总数（含 ROOT）。
func (r *AccessRecorder) FrameCount() int {
	if r == nil {
		return 0
	}
	return len(r.frames)
}

// EntryCount 返回访问条目总数。
func (r *AccessRecorder) EntryCount() int {
	if r == nil {
		return 0
	}
	return r.entries
}

// record 记录一条访问（call_tree + flat 双轨）。
func (r *AccessRecorder) record(kind AccessKind, addr common.Address, slot common.Hash, val, oldVal common.Hash, isWrite bool, flatKey string) {
	if r == nil || r.frozen {
		return
	}
	frame := r.cur
	if r.cfg.Granularity == GranularityAccount {
		// 账户级粒度：只记 flat（地址级）
		if isWrite {
			r.flatWrite[addr.String()] = struct{}{}
		} else {
			r.flatRead[addr.String()] = struct{}{}
		}
		return
	}
	e := AccessEntry{
		FrameID: frame.FrameID,
		Address: addr,
		Kind:    kind,
		Slot:    slot,
		Value:   val,
		OpType:  OpRead,
	}
	if isWrite {
		e.OpType = OpWrite
		e.OldValue = oldVal
	}
	frame.Accesses = append(frame.Accesses, e)
	r.entries++
	if isWrite {
		r.flatWrite[flatKey] = struct{}{}
	} else {
		r.flatRead[flatKey] = struct{}{}
	}
}

// RecordStorageRead 记录存储槽读取。
func (r *AccessRecorder) RecordStorageRead(addr common.Address, slot, val common.Hash) {
	r.record(KindStorage, addr, slot, val, common.Hash{}, false, FlatStorageKey(addr, slot))
}

// RecordStorageWrite 记录存储槽写入。
func (r *AccessRecorder) RecordStorageWrite(addr common.Address, slot, oldVal, newVal common.Hash) {
	r.record(KindStorage, addr, slot, newVal, oldVal, true, FlatStorageKey(addr, slot))
}

// RecordBalanceRead 记录余额读取。
func (r *AccessRecorder) RecordBalanceRead(addr common.Address, val common.Hash) {
	if r == nil || !r.cfg.CollectBalance {
		return
	}
	r.record(KindBalance, addr, common.Hash{}, val, common.Hash{}, false, FlatBalanceKey(addr))
}

// RecordBalanceWrite 记录余额写入。
func (r *AccessRecorder) RecordBalanceWrite(addr common.Address, oldVal, newVal common.Hash) {
	if r == nil || !r.cfg.CollectBalance {
		return
	}
	r.record(KindBalance, addr, common.Hash{}, newVal, oldVal, true, FlatBalanceKey(addr))
}

// RecordNonceRead 记录 nonce 读取。
func (r *AccessRecorder) RecordNonceRead(addr common.Address, val common.Hash) {
	if r == nil || !r.cfg.CollectNonce {
		return
	}
	r.record(KindNonce, addr, common.Hash{}, val, common.Hash{}, false, FlatNonceKey(addr))
}

// RecordNonceWrite 记录 nonce 写入。
func (r *AccessRecorder) RecordNonceWrite(addr common.Address, oldVal, newVal common.Hash) {
	if r == nil || !r.cfg.CollectNonce {
		return
	}
	r.record(KindNonce, addr, common.Hash{}, newVal, oldVal, true, FlatNonceKey(addr))
}

// RecordCodeRead 记录代码读取。
func (r *AccessRecorder) RecordCodeRead(addr common.Address) {
	if r == nil || !r.cfg.CollectCode {
		return
	}
	r.record(KindCode, addr, common.Hash{}, common.Hash{}, common.Hash{}, false, FlatCodeKey(addr))
}

// Freeze 冻结采集器：交易结束后调用，后续访问不再记录。
func (r *AccessRecorder) Freeze() {
	if r != nil {
		r.frozen = true
	}
}

// CallTree 返回调用树（ROOT 帧）。
func (r *AccessRecorder) CallTree() *CallFrame {
	if r == nil {
		return nil
	}
	return r.root
}

// FlatReadKeys 返回扁平读 key 列表。
func (r *AccessRecorder) FlatReadKeys() []string {
	if r == nil {
		return nil
	}
	return setToSlice(r.flatRead)
}

// FlatWriteKeys 返回扁平写 key 列表。
func (r *AccessRecorder) FlatWriteKeys() []string {
	if r == nil {
		return nil
	}
	return setToSlice(r.flatWrite)
}

func setToSlice(m map[string]struct{}) []string {
	if len(m) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
