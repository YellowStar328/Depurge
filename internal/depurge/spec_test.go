package depurge

import (
	"reflect"
	"testing"
)

func TestBridgeLLMKey(t *testing.T) {
	// LLM canonical key（小写地址）→ recorder 格式（EIP-55 校验和地址 + 完整 slot）。
	in := "slot:0xdac17f958d2ee523a2206206994597c13d831ec7:0x1"
	want := "storage:0xdAC17F958D2ee523a2206206994597C13D831ec7:0x0000000000000000000000000000000000000000000000000000000000000001"
	got, ok := BridgeLLMKey(in)
	if !ok || got != want {
		t.Fatalf("BridgeLLMKey(%q) = %q, %v; want %q, true", in, got, ok, want)
	}

	// 非法格式一律拒绝。
	for _, bad := range []string{
		"",
		"acct:0xabc:balance",
		"slot:0xabc",
		"slot:0xabc:1", // slot 缺 0x 前缀
		"storage:0xabc:0x1",
	} {
		if _, ok := BridgeLLMKey(bad); ok {
			t.Fatalf("BridgeLLMKey(%q) accepted, want rejected", bad)
		}
	}
}

func TestSelectSpecArmC(t *testing.T) {
	pre := []string{"acct:0xA:balance", "storage:0xC:0x01"}
	got := SelectSpec(ArmC, pre, []string{"storage:0xC:0x02"}, true, "acct:0xA:balance")
	if !reflect.DeepEqual(got, []string{"acct:0xA:balance", "storage:0xC:0x01"}) {
		t.Fatalf("arm C = %v, want pure pre-exec set", got)
	}
}

func TestSelectSpecArmAFallback(t *testing.T) {
	pre := []string{"storage:0xC:0x01"}
	llm := []string{"storage:0xC:0x02"}
	senderBal := "acct:0xA:balance"

	// LLM 成功：只用 LLM 集 + sender balance（不含预执行 key）。
	got := SelectSpec(ArmA, pre, llm, true, senderBal)
	want := []string{"acct:0xA:balance", "storage:0xC:0x02"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("arm A llmOK = %v, want %v", got, want)
	}

	// LLM 失败：回退预执行集（不合成 sender balance，预执行已含真实键）。
	got = SelectSpec(ArmA, pre, nil, false, senderBal)
	if !reflect.DeepEqual(got, pre) {
		t.Fatalf("arm A fallback = %v, want %v", got, pre)
	}
}

func TestSelectSpecArmBUnion(t *testing.T) {
	pre := []string{"storage:0xC:0x01", "acct:0xA:balance"}
	llm := []string{"storage:0xC:0x01", "storage:0xC:0x02"} // 0x01 与预执行重叠
	senderBal := "acct:0xA:balance"

	got := SelectSpec(ArmB, pre, llm, true, senderBal)
	want := []string{"acct:0xA:balance", "storage:0xC:0x01", "storage:0xC:0x02"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("arm B union = %v, want %v", got, want)
	}

	// LLM 失败：并集退化为纯预执行集（输出去重排序）。
	got = SelectSpec(ArmB, pre, nil, false, senderBal)
	want = []string{"acct:0xA:balance", "storage:0xC:0x01"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("arm B llm-fail = %v, want %v", got, want)
	}

	// 预执行集为空：不入并集，即使 LLM 成功也保持空 spec
	// （交给集成层直接串行兜底，避免 newly-scheduled 增量 abort）。
	if got = SelectSpec(ArmB, nil, llm, true, senderBal); len(got) != 0 {
		t.Fatalf("arm B empty pre-exec = %v, want empty spec", got)
	}
}

func TestParseArm(t *testing.T) {
	for in, want := range map[string]Arm{"a": ArmA, "B": ArmB, "c": ArmC, " C ": ArmC} {
		got, err := ParseArm(in)
		if err != nil || got != want {
			t.Fatalf("ParseArm(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := ParseArm("D"); err == nil {
		t.Fatal("ParseArm(D) accepted, want error")
	}
}

func TestLLMFailReasonString(t *testing.T) {
	if LLMOK.String() != "ok" || LLMUnresolved.String() != "unresolved" || LLMEmpty.String() != "empty" {
		t.Fatal("unexpected reason strings")
	}
	if len(AllLLMFailReasons()) != 7 {
		t.Fatalf("AllLLMFailReasons len = %d, want 7", len(AllLLMFailReasons()))
	}
}
