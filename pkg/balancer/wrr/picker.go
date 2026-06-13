package wrr

import (
	"sync"
	"sync/atomic"
)

// picker 在一組固定節點上做平滑加權輪詢(Nginx smooth WRR)。
//
// 節點集合變動時整個 picker 重建(rebuild);權重變動(recompute)
// 不重建,直接原子改 node.ewt——picker 持有的是 *node 指標。
//
// mu 保護 nodes 切片本身;Pick 只持 RLock,對 node.ewt/cwt 的調整
// 全用 atomic,故 Pick 之間無需互斥(CODE_QUALITY.md §4)。
type picker struct {
	mu    sync.RWMutex
	nodes []*node
}

// newPicker 以給定節點建立 picker。
func newPicker(nodes []*node) *picker {
	return &picker{nodes: nodes}
}

// pick 用平滑加權輪詢選一個節點,並累加它的 picks 計數。
// 節點集合為空時回 nil。
//
// 演算法(Nginx smooth weighted round-robin):
//  1. 每個節點 cwt += ewt
//  2. 選 cwt 最大的節點 best
//  3. best.cwt -= 所有節點 ewt 總和
//
// 性質:長期命中比例正比於各節點 ewt,且相鄰選擇平滑分散
// (不像普通 WRR 會連續打同一個高權重節點)。
func (p *picker) pick() *node {
	p.mu.RLock()
	defer p.mu.RUnlock()
	nodes := p.nodes
	if len(nodes) == 0 {
		return nil
	}

	var (
		total int64
		best  *node
		bestC int64
	)
	for _, n := range nodes {
		ewt := atomic.LoadInt64(&n.ewt)
		total += ewt
		// AddInt64 的回傳值是本 goroutine 視角的新 cwt,據此比較最大者,
		// 即使有併發 Pick 也不會讀到撕裂值
		cwt := atomic.AddInt64(&n.cwt, ewt)
		if best == nil || cwt > bestC {
			best = n
			bestC = cwt
		}
	}
	atomic.AddInt64(&best.cwt, -total)
	atomic.AddInt64(&best.picks, 1)
	return best
}
