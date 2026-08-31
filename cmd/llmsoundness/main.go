// llmsoundness 用 dataset 自带的 canonical rwsets 作为 ground truth，
// 评测 llm/mainnet_rw 里 LLM 静态分析产出的读写集保守度（漏报率 recall / 多报率 precision）。
package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/spf13/cobra"

	"depurge/internal/dataset"
	"depurge/internal/llmsoundness"
)

func main() {
	var (
		llmDir     string
		datasetDir string
		blocks     string
		minTx      int
	)

	rootCmd := &cobra.Command{
		Use:   "llmsoundness",
		Short: "用 canonical rwsets 评测 LLM 读写集分析的 soundness（漏报/多报率）",
		RunE: func(cmd *cobra.Command, args []string) error {
			if llmDir == "" {
				return fmt.Errorf("--llm-dir is required")
			}
			if datasetDir == "" {
				return fmt.Errorf("--dataset is required")
			}

			contracts, err := llmsoundness.LoadContracts(llmDir)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "loaded %d contracts\n", len(contracts))

			loader, err := dataset.NewLoader(datasetDir)
			if err != nil {
				return err
			}

			stats, err := llmsoundness.Evaluate(loader, contracts, blocks)
			if err != nil {
				return err
			}

			printReport(stats, contracts, minTx)
			return nil
		},
	}

	rootCmd.Flags().StringVar(&llmDir, "llm-dir", "llm/mainnet_rw", "LLM 分析结果目录")
	rootCmd.Flags().StringVar(&datasetDir, "dataset", "", "dataset 目录（含 canonical rwsets）")
	rootCmd.Flags().StringVar(&blocks, "blocks", "", "区块范围过滤，如 21498532-21499531，空=全部")
	rootCmd.Flags().IntVar(&minTx, "min-tx", 0, "只报告命中交易数 >= min-tx 的合约")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// row 是逐合约的一行统计，sto 标记该合约是否带 storage 布局。
type row struct {
	addr string
	s    *llmsoundness.ContractStats
	sto  bool
}

// totals 是一组合约的聚合计数。
type totals struct {
	count, txs, matched, canon, llm, hit, miss, extra, unres int
}

func accumulate(t *totals, r row) {
	t.count++
	t.txs += r.s.TxCount
	t.matched += r.s.MatchedTx
	t.canon += r.s.CanonReadKeys + r.s.CanonWriteKeys
	t.llm += r.s.LlmReadKeys + r.s.LlmWriteKeys
	t.hit += r.s.HitReadKeys + r.s.HitWriteKeys
	t.miss += r.s.MissReadKeys + r.s.MissWriteKeys
	t.extra += r.s.ExtraReadKeys + r.s.ExtraWriteKeys
	t.unres += r.s.UnresolvedRead + r.s.UnresolvedWrite
}

func printReport(stats map[common.Address]*llmsoundness.ContractStats, contracts map[common.Address]*llmsoundness.Contract, minTx int) {
	rows := make([]row, 0, len(stats))
	for addr, s := range stats {
		sto := false
		if c, ok := contracts[addr]; ok {
			sto = c.HasStorage()
		}
		rows = append(rows, row{addr.Hex(), s, sto})
	}
	sort.Slice(rows, func(i, j int) bool {
		// 按命中交易数降序
		return rows[i].s.MatchedTx > rows[j].s.MatchedTx
	})

	// 全局聚合：全部合约 / 仅带 storage 布局的合约。缺布局的合约所有 field
	// 都无法实例化（必然全漏报），单列出来避免拉低整体指标的解读。
	var all, withSto totals
	for _, r := range rows {
		accumulate(&all, r)
		if r.sto {
			accumulate(&withSto, r)
		}
	}

	fmt.Println()
	fmt.Println("===================================================")
	fmt.Println(">>> LLM Soundness Evaluation (canonical rwsets) <<<")
	fmt.Println("===================================================")
	fmt.Printf("contracts          : %d (带 storage 布局 %d / 缺布局 %d)\n",
		all.count, withSto.count, all.count-withSto.count)
	fmt.Printf("txs to LLM contract: %d (matched selector=%d)\n", all.txs, all.matched)
	fmt.Printf("canonical keys     : %d\n", all.canon)
	fmt.Printf("llm keys           : %d\n", all.llm)
	fmt.Printf("hit keys           : %d\n", all.hit)
	fmt.Printf("miss keys (漏报)   : %d\n", all.miss)
	fmt.Printf("extra keys (多报)  : %d\n", all.extra)
	fmt.Printf("unresolved         : %d\n", all.unres)
	fmt.Printf("---- 全局指标（全部合约） ----\n")
	fmt.Printf("recall   : %.2f%%  (漏报率 %.2f%%)\n", recallOf(all.hit, all.canon)*100, missOf(all.hit, all.canon)*100)
	fmt.Printf("precision: %.2f%%  (多报率 %.2f%%)\n", precisionOf(all.hit, all.llm)*100, extraOf(all.hit, all.llm)*100)
	fmt.Printf("---- 全局指标（仅带 storage 布局，可实例化） ----\n")
	fmt.Printf("recall   : %.2f%%  (漏报率 %.2f%%)\n", recallOf(withSto.hit, withSto.canon)*100, missOf(withSto.hit, withSto.canon)*100)
	fmt.Printf("precision: %.2f%%  (多报率 %.2f%%)\n", precisionOf(withSto.hit, withSto.llm)*100, extraOf(withSto.hit, withSto.llm)*100)
	fmt.Println()

	// 逐合约明细
	fmt.Println("---------------------------------------------------")
	fmt.Printf("%-44s %-24s %3s %6s %7s %8s %8s %8s %8s\n",
		"contract", "name", "sto", "matched", "recall", "miss", "precision", "extra", "unres")
	for _, r := range rows {
		s := r.s
		if s.MatchedTx < minTx {
			continue
		}
		stoMark := "y"
		if !r.sto {
			stoMark = "-"
		}
		fmt.Printf("%-44s %-24s %3s %6d %6.1f%% %8d %6.1f%% %8d %8d\n",
			s.Address.Hex(), s.Name, stoMark, s.MatchedTx,
			s.Recall()*100, s.MissReadKeys+s.MissWriteKeys,
			s.Precision()*100, s.ExtraReadKeys+s.ExtraWriteKeys,
			s.UnresolvedRead+s.UnresolvedWrite)
	}
	fmt.Println()

	// 未解析原因 TopN（全局）
	printUnresolvedTop(stats)
}

func printUnresolvedTop(stats map[common.Address]*llmsoundness.ContractStats) {
	agg := map[string]int{}
	for _, s := range stats {
		for k, v := range s.UnresolvedDetail {
			agg[k] += v
		}
	}
	if len(agg) == 0 {
		return
	}
	type kv struct {
		k string
		v int
	}
	list := make([]kv, 0, len(agg))
	for k, v := range agg {
		list = append(list, kv{k, v})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].v > list[j].v })
	fmt.Println("---- unresolved 原因 Top20（全局） ----")
	for i, e := range list {
		if i >= 20 {
			break
		}
		fmt.Printf("  %-48s %d\n", e.k, e.v)
	}
	fmt.Println()
}

func recallOf(hit, canon int) float64 {
	if canon == 0 {
		return 1
	}
	return float64(hit) / float64(canon)
}
func missOf(hit, canon int) float64 { return 1 - recallOf(hit, canon) }

func precisionOf(hit, llm int) float64 {
	if llm == 0 {
		return 1
	}
	return float64(hit) / float64(llm)
}
func extraOf(hit, llm int) float64 { return 1 - precisionOf(hit, llm) }
