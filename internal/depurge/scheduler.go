package depurge

import "sort"

// TxInfo 是单笔交易的调度输入。
type TxInfo struct {
	Index int      // 区块内原始序号
	Keys  []string // spec 读写并集（已过滤、去重）；队列不区分读集和写集
}

// Scheduler 是 key 队列事件驱动无屏障调度器（纯逻辑，不含执行）。
//
// 语义（对应算法 step2/step3）：
//   - 每个 key 一条队列，交易按区块原始顺序入队，不区分读/写；
//   - 交易就绪当且仅当它在所有所属队列中都在队头；
//   - Finish（提交或 abort）把交易从其所有队列出队，递减后继的阻塞计数，
//     立即返回新就绪交易——无全局屏障，提交即解锁后继；
//   - 队列按原始序入队，阻塞关系恒指向更小原始序号，故不存在循环等待，
//     死锁在构造上不可能；DrainRemaining 仅作防御性兜底。
//
// 并发约定：调度器状态只由单一 dispatcher 串行操作（免锁）；
// 并行执行发生在集成层的 worker 池，与本结构无关。
type Scheduler struct {
	nTx        int
	queues     map[string][]int // key → 按原始序的交易队列
	head       map[string]int   // key → 队头在队列中的位置
	keys       [][]string       // 交易 → 其 spec key 列表（未入图者为 nil）
	blocked    []int            // 交易 → 其不在队头的队列数
	done       []bool           // 交易 → 已完成（提交或 abort）
	aborted    []bool           // 交易 → 被 abort
	dispatched []bool           // 交易 → 已派发执行中（避免重复就绪）
	pending    int              // 未完成交易数
	inGraph    int              // 入图交易数
}

// BuildScheduler 按 spec 读写并集建 key 队列（step2）。
// infos 只含参与调度的交易（spec 非空）；nTx 为区块交易总数，
// 用于按原始序号索引。
func BuildScheduler(nTx int, infos []TxInfo) *Scheduler {
	s := &Scheduler{
		nTx:        nTx,
		queues:     make(map[string][]int),
		head:       make(map[string]int),
		keys:       make([][]string, nTx),
		blocked:    make([]int, nTx),
		done:       make([]bool, nTx),
		aborted:    make([]bool, nTx),
		dispatched: make([]bool, nTx),
		inGraph:    len(infos),
	}
	for _, info := range infos {
		s.keys[info.Index] = info.Keys
		for _, k := range info.Keys {
			s.queues[k] = append(s.queues[k], info.Index)
		}
		s.pending++
	}
	// 初始阻塞计数：每条队列中非队头交易各 +1。
	for _, q := range s.queues {
		for pos, idx := range q {
			if pos > 0 {
				s.blocked[idx]++
			}
		}
	}
	return s
}

// Ready 返回当前就绪交易（所有所属队列都在队头、未完成、未派发），升序。
func (s *Scheduler) Ready() []int {
	var ready []int
	for idx := 0; idx < s.nTx; idx++ {
		if len(s.keys[idx]) > 0 && !s.done[idx] && !s.dispatched[idx] && s.blocked[idx] == 0 {
			ready = append(ready, idx)
		}
	}
	return ready
}

// Dispatch 标记交易已派发（执行中），使其不再被 Ready 重复返回。
func (s *Scheduler) Dispatch(idx int) {
	s.dispatched[idx] = true
}

// Finish 完成交易（提交或 abort）：从其所有队列出队、递减后继阻塞计数，
// 返回新就绪交易（升序）。
//
// 不变式：就绪交易必在其所有队列队头，故出队时 q[head] 恒等于 idx。
func (s *Scheduler) Finish(idx int, aborted bool) []int {
	if s.done[idx] {
		return nil
	}
	s.done[idx] = true
	s.aborted[idx] = aborted
	s.pending--
	var newly []int
	for _, k := range s.keys[idx] {
		q := s.queues[k]
		h := s.head[k]
		if h >= len(q) || q[h] != idx {
			// 理论不可达（见不变式）；防御性跳过。
			continue
		}
		s.head[k] = h + 1
		if h+1 < len(q) {
			nxt := q[h+1]
			s.blocked[nxt]--
			if s.blocked[nxt] == 0 && !s.done[nxt] && !s.dispatched[nxt] {
				newly = append(newly, nxt)
			}
		}
	}
	sort.Ints(newly)
	return newly
}

// Pending 返回未完成交易数。
func (s *Scheduler) Pending() int { return s.pending }

// InGraph 返回入图（参与调度）交易数。
func (s *Scheduler) InGraph() int { return s.inGraph }

// Aborted 返回已 abort 的交易序号（升序）。
func (s *Scheduler) Aborted() []int {
	var out []int
	for idx := 0; idx < s.nTx; idx++ {
		if s.done[idx] && s.aborted[idx] {
			out = append(out, idx)
		}
	}
	return out
}

// Remaining 返回未完成交易序号（升序），供死锁防御兜底使用。
func (s *Scheduler) Remaining() []int {
	var out []int
	for idx := 0; idx < s.nTx; idx++ {
		if len(s.keys[idx]) > 0 && !s.done[idx] {
			out = append(out, idx)
		}
	}
	return out
}

// DrainRemaining 强制把全部未完成交易标记为 aborted（死锁防御兜底）。
// 调用后调度器不再继续调度，故不维护队列一致性。返回被兜底的序号（升序）。
func (s *Scheduler) DrainRemaining() []int {
	rem := s.Remaining()
	for _, idx := range rem {
		s.done[idx] = true
		s.aborted[idx] = true
		s.pending--
	}
	return rem
}
