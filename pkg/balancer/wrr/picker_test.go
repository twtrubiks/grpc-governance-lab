package wrr

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// makeNodes 造一組固定 ewt 的節點(統計用不到時 buckets/width 隨意)。
func makeNodes(weights map[string]int64) []*node {
	nodes := make([]*node, 0, len(weights))
	for addr, w := range weights {
		n := newNode(addr, w, 10, 300*time.Millisecond, time.Now)
		atomic.StoreInt64(&n.ewt, w) // 靜態:有效權重 = 設定權重
		nodes = append(nodes, n)
	}
	return nodes
}

// TestPicker_DistributionMatchesWeights 驗證命中比例正比於權重。
// 單執行緒:smooth WRR 是確定性的,比例幾乎精確。
func TestPicker_DistributionMatchesWeights(t *testing.T) {
	p := newPicker(makeNodes(map[string]int64{"a": 1, "b": 2, "c": 7}))

	const rounds = 10000
	got := map[string]int{}
	for i := 0; i < rounds; i++ {
		got[p.pick().addr]++
	}

	// 權重 1:2:7,總 10,期望比例 10%/20%/70%,容忍 ±1.5%
	expect := map[string]float64{"a": 0.10, "b": 0.20, "c": 0.70}
	for addr, want := range expect {
		ratio := float64(got[addr]) / rounds
		if ratio < want-0.015 || ratio > want+0.015 {
			t.Errorf("節點 %s 命中比例 %.3f,期望 %.2f±0.015", addr, ratio, want)
		}
	}
}

// TestPicker_Smoothness 驗證平滑性:高權重節點不會連續霸佔,
// 而是均勻穿插(這正是 smooth WRR 勝過普通 WRR 之處)。
func TestPicker_Smoothness(t *testing.T) {
	p := newPicker(makeNodes(map[string]int64{"a": 1, "b": 1, "c": 3}))

	maxStreak, streak := 0, 0
	var last string
	for i := 0; i < 100; i++ {
		addr := p.pick().addr
		if addr == last {
			streak++
		} else {
			streak = 1
			last = addr
		}
		if streak > maxStreak {
			maxStreak = streak
		}
	}
	// 權重 1:1:3,最高權重節點連續出現不應超過 2 次
	if maxStreak > 2 {
		t.Errorf("平滑性不足:最長連續命中 %d 次(> 2)", maxStreak)
	}
}

func TestPicker_Empty(t *testing.T) {
	if got := newPicker(nil).pick(); got != nil {
		t.Errorf("空 picker 應回 nil,得 %v", got)
	}
}

// TestPicker_ConcurrentRaceFree 是 M5 驗收的一半:8 goroutine 併發
// Pick 必須 -race 全過(吞吐量在 BenchmarkPicker_Pick 驗證)。
func TestPicker_ConcurrentRaceFree(t *testing.T) {
	p := newPicker(makeNodes(map[string]int64{"a": 1, "b": 2, "c": 3}))
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50000; i++ {
				if p.pick() == nil {
					t.Error("併發 Pick 不該回 nil")
					return
				}
			}
		}()
	}
	wg.Wait()
}

// BenchmarkPicker_Pick 是 M5 驗收:8 goroutine 併發下 > 100 萬 ops/s。
func BenchmarkPicker_Pick(b *testing.B) {
	p := newPicker(makeNodes(map[string]int64{"a": 10, "b": 20, "c": 30}))
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = p.pick()
		}
	})
}
