package wrr

import (
	"math"
	"sort"
	"sync/atomic"
)

// Stat 是單一後端節點的即時快照,供 gateway 的 /debug/backends 輸出
// (demo 場景 3、5、6 全靠它看流量分布)。
type Stat struct {
	// Service 目標服務名(同一服務的多個副本共用)。
	Service string `json:"service"`
	// Addr 後端位址。
	Addr string `json:"addr"`
	// ConfigWeight 設定權重(靜態)。
	ConfigWeight int64 `json:"config_weight"`
	// EffectiveWeight 目前有效權重(動態重算後);決定流量占比。
	EffectiveWeight int64 `json:"effective_weight"`
	// Picks 累計被選中次數。
	Picks int64 `json:"picks"`
	// Requests 累計完成的請求數。
	Requests int64 `json:"requests"`
	// Fails 累計傳輸層失敗數。
	Fails int64 `json:"fails"`
	// SuccessRate 最近視窗的成功率(0~1)。
	SuccessRate float64 `json:"success_rate"`
	// LatencyMs 最近視窗的平均延遲(毫秒)。
	LatencyMs float64 `json:"latency_ms"`
	// CPUMilli server 回報的 CPU 使用率(千分比)。
	CPUMilli int64 `json:"cpu_milli"`
}

// snapshot 回傳所有節點的即時快照,按位址排序(輸出穩定)。
func (c *core) snapshot() []Stat {
	c.mu.Lock()
	nodes := make([]*node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.mu.Unlock()

	out := make([]Stat, 0, len(nodes))
	for _, n := range nodes {
		errc, req := n.errs.Value()
		latSum, latCnt := n.latency.Value()
		successRate := 1.0
		if req > 0 {
			successRate = 1 - float64(errc)/float64(req)
		}
		latencyMs := 0.0
		if latCnt > 0 {
			latencyMs = float64(latSum) / float64(latCnt) / 1000 // 微秒 → 毫秒
		}
		out = append(out, Stat{
			Service:         c.service,
			Addr:            n.addr,
			ConfigWeight:    n.weight,
			EffectiveWeight: atomic.LoadInt64(&n.ewt),
			Picks:           atomic.LoadInt64(&n.picks),
			Requests:        atomic.LoadInt64(&n.requests),
			Fails:           atomic.LoadInt64(&n.fails),
			SuccessRate:     math.Round(successRate*1000) / 1000,
			LatencyMs:       math.Round(latencyMs*100) / 100,
			CPUMilli:        atomic.LoadInt64(&n.cpuMilli),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}
