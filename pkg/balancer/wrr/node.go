package wrr

import (
	"math"
	"sync/atomic"
	"time"
)

// DefaultWeight 是節點未在 metadata 指定權重時的設定權重。
const DefaultWeight = 10

// node 是一個後端副本加上它的即時統計。
//
// 併發約定:ewt/cwt 與 server 回報的 cpu/ss 都是 atomic,
// 因此 Pick 全程只持 picker 的 RLock、不需要 per-node 鎖;
// 滑動視窗(latency/errs)有自己的鎖,只在 RPC 完成與權重重算時碰,
// 不在 Pick 熱路徑上。
type node struct {
	addr   string
	weight int64 // 設定權重(靜態,來自 registry metadata)

	// smooth WRR 執行狀態,atomic 讓 Pick 不必持寫鎖
	ewt int64 // 有效權重(動態,recompute 重算)
	cwt int64 // 當前權重(每次 Pick 調整)

	// server 透過 trailer 回報,atomic
	cpuMilli int64  // CPU 使用率千分比(0~1000),clamp 下限見 recompute
	ssBits   uint64 // server 端成功率的 float64 bits,預設 1.0

	// 最近視窗統計(供 recompute)
	latency *rollingWindow // 延遲(微秒)
	errs    *rollingWindow // 錯誤:sum=失敗數,count=總請求數

	// 累計觀測量(供 /debug/backends,單調遞增)
	picks    int64
	requests int64
	fails    int64
}

// newNode 建立節點,初始有效權重等於設定權重。
func newNode(addr string, weight int64, buckets int, width time.Duration, clock func() time.Time) *node {
	if weight <= 0 {
		weight = DefaultWeight
	}
	n := &node{
		addr:    addr,
		weight:  weight,
		ewt:     weight,
		latency: newRollingWindow(buckets, width, clock),
		errs:    newRollingWindow(buckets, width, clock),
	}
	atomic.StoreUint64(&n.ssBits, math.Float64bits(1.0)) // 未回報前假設健康
	return n
}

// record 記錄一次完成的 RPC:延遲(微秒)與是否失敗。
// 只計入傳輸層錯誤,業務錯誤(如 -404)不算節點不健康(它正常工作)。
func (n *node) record(latencyMicros int64, failed bool) {
	atomic.AddInt64(&n.requests, 1)
	ev := int64(0)
	if failed {
		ev = 1
		atomic.AddInt64(&n.fails, 1)
	}
	n.errs.Add(ev)
	n.latency.Add(latencyMicros)
}

// reportCPU 記錄 server 經 trailer 回報的 CPU 使用率(千分比)。
func (n *node) reportCPU(milli int64) {
	atomic.StoreInt64(&n.cpuMilli, milli)
}

// reportServerSuccess 記錄 server 回報的自身成功率(0~1)。
func (n *node) reportServerSuccess(ratio float64) {
	atomic.StoreUint64(&n.ssBits, math.Float64bits(ratio))
}
