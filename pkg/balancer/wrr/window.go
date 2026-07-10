package wrr

import (
	"sync"
	"time"
)

// rollingWindow 是固定桶數的滑動視窗統計(預設 10 桶 × 300ms = 3 秒)。
//
// 用途:per-node 的延遲與錯誤統計只看「最近 3 秒」,故障節點恢復後
// 舊的壞數據會隨桶過期自動淘汰,權重才回得來(demo 場景 3 的回升)。
//
// mu 保護所有欄位。Add 在每次 RPC 完成時呼叫(非熱路徑,Pick 不碰它);
// Value 在權重重算時呼叫。clock 可注入,讓測試不依賴真實時間。
type rollingWindow struct {
	mu      sync.Mutex
	buckets []bucket
	width   time.Duration
	clock   func() time.Time

	// lastIdx 是目前寫入的桶;lastStart 是該桶的起始時間(對齊到 width)。
	lastIdx   int
	lastStart time.Time
}

// bucket 是單一時間桶的累加值。
type bucket struct {
	sum   int64
	count int64
}

// newRollingWindow 建立滑動視窗;count 桶、每桶 width 寬。
func newRollingWindow(count int, width time.Duration, clock func() time.Time) *rollingWindow {
	if clock == nil {
		clock = time.Now
	}
	return &rollingWindow{
		buckets:   make([]bucket, count),
		width:     width,
		clock:     clock,
		lastStart: clock(),
	}
}

// advanceLocked 把時間游標推進到 now:跨過幾個桶就清空幾個桶,
// 跨過的桶數達到總桶數時整個視窗失效(全清)。呼叫方須持鎖。
func (w *rollingWindow) advanceLocked(now time.Time) {
	steps := int(now.Sub(w.lastStart) / w.width)
	if steps <= 0 {
		return
	}
	if steps >= len(w.buckets) {
		// 整窗作廢:游標必須直接對齊到 now。若只前進 steps 個桶寬,
		// 閒置超過兩個視窗後 lastStart 會一直落後 now 一個視窗以上,
		// 追上之前每次 advance 都再全清一輪,把剛寫入的新樣本也吃掉
		for i := range w.buckets {
			w.buckets[i] = bucket{}
		}
		w.lastStart = now
		return
	}
	for i := 0; i < steps; i++ {
		w.lastIdx = (w.lastIdx + 1) % len(w.buckets)
		w.buckets[w.lastIdx] = bucket{}
	}
	w.lastStart = w.lastStart.Add(time.Duration(steps) * w.width)
}

// Add 把一個觀測值累加進當前桶。
func (w *rollingWindow) Add(v int64) {
	w.mu.Lock()
	w.advanceLocked(w.clock())
	w.buckets[w.lastIdx].sum += v
	w.buckets[w.lastIdx].count++
	w.mu.Unlock()
}

// Value 回傳視窗內所有桶的總和與筆數。
func (w *rollingWindow) Value() (sum, count int64) {
	w.mu.Lock()
	w.advanceLocked(w.clock())
	for _, b := range w.buckets {
		sum += b.sum
		count += b.count
	}
	w.mu.Unlock()
	return sum, count
}
