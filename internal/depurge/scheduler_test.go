package depurge

import (
	"reflect"
	"testing"
)

func TestSchedulerSingleKeyOrdering(t *testing.T) {
	// 三笔交易共享 key "a"：只有队头（原始序最小）就绪。
	s := BuildScheduler(3, []TxInfo{
		{Index: 0, Keys: []string{"a"}},
		{Index: 1, Keys: []string{"a"}},
		{Index: 2, Keys: []string{"a"}},
	})
	if got := s.Ready(); !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("initial ready = %v, want [0]", got)
	}
	if got := s.Finish(0, false); !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("finish 0 newly = %v, want [1]", got)
	}
	s.Dispatch(1)
	if got := s.Ready(); len(got) != 0 {
		t.Fatalf("ready after dispatch = %v, want empty", got)
	}
	if got := s.Finish(1, false); !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("finish 1 newly = %v, want [2]", got)
	}
	if got := s.Finish(2, false); len(got) != 0 {
		t.Fatalf("finish 2 newly = %v, want empty", got)
	}
	if s.Pending() != 0 {
		t.Fatalf("pending = %d, want 0", s.Pending())
	}
	if got := s.Aborted(); len(got) != 0 {
		t.Fatalf("aborted = %v, want empty", got)
	}
}

func TestSchedulerIndependentKeysParallel(t *testing.T) {
	// 无共享 key 的交易应同时就绪（并行）。
	s := BuildScheduler(3, []TxInfo{
		{Index: 0, Keys: []string{"a"}},
		{Index: 1, Keys: []string{"b"}},
		{Index: 2, Keys: []string{"c"}},
	})
	if got := s.Ready(); !reflect.DeepEqual(got, []int{0, 1, 2}) {
		t.Fatalf("ready = %v, want [0 1 2]", got)
	}
}

func TestSchedulerMultiKeyHeadCondition(t *testing.T) {
	// tx0 触及 a、b；tx1 只触及 b。tx1 在 b 队列中位于 tx0 之后，
	// 必须等 tx0 完成才就绪——即使 tx1 不在 a 队列。
	s := BuildScheduler(2, []TxInfo{
		{Index: 0, Keys: []string{"a", "b"}},
		{Index: 1, Keys: []string{"b"}},
	})
	if got := s.Ready(); !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("ready = %v, want [0]", got)
	}
	if got := s.Finish(0, false); !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("newly = %v, want [1]", got)
	}
}

func TestSchedulerAbortUnlocksSuccessors(t *testing.T) {
	// abort 与提交一样出队并解锁后继。
	s := BuildScheduler(3, []TxInfo{
		{Index: 0, Keys: []string{"a"}},
		{Index: 1, Keys: []string{"a"}},
		{Index: 2, Keys: []string{"a"}},
	})
	s.Ready()
	s.Dispatch(0)
	if got := s.Finish(0, true); !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("abort 0 newly = %v, want [1]", got)
	}
	if got := s.Aborted(); !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("aborted = %v, want [0]", got)
	}
	s.Dispatch(1)
	s.Finish(1, false)
	s.Dispatch(2)
	s.Finish(2, false)
	if s.Pending() != 0 {
		t.Fatalf("pending = %d, want 0", s.Pending())
	}
}

func TestSchedulerNoDoubleDispatch(t *testing.T) {
	s := BuildScheduler(2, []TxInfo{
		{Index: 0, Keys: []string{"a", "b"}},
		{Index: 1, Keys: []string{"a", "b"}},
	})
	ready := s.Ready()
	if !reflect.DeepEqual(ready, []int{0}) {
		t.Fatalf("ready = %v, want [0]", ready)
	}
	for _, idx := range ready {
		s.Dispatch(idx)
	}
	// tx0 执行中：tx1 在两条队列都被 tx0 阻塞，且 tx0 不重复就绪。
	if got := s.Ready(); len(got) != 0 {
		t.Fatalf("ready = %v, want empty", got)
	}
	// tx0 完成：tx1 在 a、b 两条队列同时解锁，只返回一次。
	if got := s.Finish(0, false); !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("newly = %v, want [1] (once)", got)
	}
}

func TestSchedulerFullRunDeterministic(t *testing.T) {
	// 模拟完整事件驱动执行：任意完成顺序下所有交易最终完成，
	// 且同 key 队列内严格保持原始序约束。
	infos := []TxInfo{
		{Index: 0, Keys: []string{"a", "x"}},
		{Index: 1, Keys: []string{"b"}},
		{Index: 2, Keys: []string{"a", "b"}},
		{Index: 3, Keys: []string{"x", "b"}},
		{Index: 4, Keys: []string{"c"}},
	}
	s := BuildScheduler(5, infos)
	done := map[int]bool{}
	inFlight := map[int]bool{}
	for s.Pending() > 0 {
		for _, idx := range s.Ready() {
			s.Dispatch(idx)
			inFlight[idx] = true
		}
		if len(inFlight) == 0 {
			t.Fatalf("deadlock: pending=%d, no in-flight", s.Pending())
		}
		// 取任意一个在飞交易完成（这里取最大序号，制造乱序提交）。
		pick := -1
		for idx := range inFlight {
			if idx > pick {
				pick = idx
			}
		}
		delete(inFlight, pick)
		done[pick] = true
		for _, nxt := range s.Finish(pick, pick == 1) {
			if done[nxt] {
				t.Fatalf("newly ready %d already done", nxt)
			}
		}
	}
	if len(done) != 5 {
		t.Fatalf("done = %d, want 5", len(done))
	}
	if got := s.Aborted(); !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("aborted = %v, want [1]", got)
	}
}

func TestSchedulerDrainRemaining(t *testing.T) {
	s := BuildScheduler(3, []TxInfo{
		{Index: 0, Keys: []string{"a"}},
		{Index: 1, Keys: []string{"a"}},
		{Index: 2, Keys: []string{"b"}},
	})
	s.Ready()
	s.Dispatch(0)
	s.Finish(0, false)
	rem := s.DrainRemaining()
	if !reflect.DeepEqual(rem, []int{1, 2}) {
		t.Fatalf("drain = %v, want [1 2]", rem)
	}
	if s.Pending() != 0 {
		t.Fatalf("pending = %d, want 0", s.Pending())
	}
	if got := s.Aborted(); !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("aborted = %v, want [1 2]", got)
	}
}
