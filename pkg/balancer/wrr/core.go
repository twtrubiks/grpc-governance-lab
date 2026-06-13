package wrr

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Config 是 balancer 的可調參數;零值欄位採生產預設,測試注入短週期。
type Config struct {
	// Buckets 滑動視窗桶數,預設 10。
	Buckets int
	// BucketWidth 每桶時間寬度,預設 300ms(10×300ms = 3 秒視窗)。
	BucketWidth time.Duration
	// RecalcInterval 權重重算週期,預設 3 秒。
	RecalcInterval time.Duration
	// CPUFloorMilli CPU 使用率下限(千分比),預設 10(=1%)。
	// 防止「demo 容器 CPU 趨近 0」時權重公式除以 0 爆炸。
	CPUFloorMilli int64
	// SuccessFloor 成功率下限,預設 0.1:
	// 避免冷啟動節點因偶發失敗、樣本不足被權重歸零而永遠收不到流量。
	SuccessFloor float64
	// Clock 可注入的時鐘,nil 用 time.Now。
	Clock func() time.Time
}

func (c Config) withDefaults() Config {
	if c.Buckets <= 0 {
		c.Buckets = 10
	}
	if c.BucketWidth <= 0 {
		c.BucketWidth = 300 * time.Millisecond
	}
	if c.RecalcInterval <= 0 {
		c.RecalcInterval = 3 * time.Second
	}
	if c.CPUFloorMilli <= 0 {
		c.CPUFloorMilli = 10
	}
	if c.SuccessFloor <= 0 {
		c.SuccessFloor = 0.1
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
	return c
}

// core 管理一條連線上的節點集合、picker 與權重重算,與 gRPC 解耦,
// 可獨立建構與測試。
//
// mu 保護 nodes(addr → node)與 picker 指標。
type core struct {
	cfg Config
	// service 是這條連線的目標服務名(target endpoint),供 /debug 分組。
	service string

	mu     sync.Mutex
	nodes  map[string]*node
	picker atomic.Pointer[picker]
}

// recalcLoop 週期性重算權重;退出路徑:ctx 取消(由 balancer Close 觸發)。
func (c *core) recalcLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.RecalcInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.recompute()
		}
	}
}

// newCore 建立核心。
func newCore(cfg Config) *core {
	c := &core{
		cfg:   cfg.withDefaults(),
		nodes: make(map[string]*node),
	}
	c.picker.Store(newPicker(nil))
	return c
}

// setAddrs 把節點集合對齊到 addrs:新增缺少的、移除多出的,
// 保留既有節點的統計(地址沒變的副本不該被洗掉歷史)。集合有變才重建 picker。
func (c *core) setAddrs(addrs map[string]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	changed := false
	for addr, weight := range addrs {
		if _, ok := c.nodes[addr]; !ok {
			c.nodes[addr] = newNode(addr, weight, c.cfg.Buckets, c.cfg.BucketWidth, c.cfg.Clock)
			changed = true
		}
	}
	for addr := range c.nodes {
		if _, ok := addrs[addr]; !ok {
			delete(c.nodes, addr)
			changed = true
		}
	}
	if changed {
		c.rebuildPickerLocked()
	}
}

// rebuildPickerLocked 用目前節點集合重建 picker;呼叫方須持鎖。
func (c *core) rebuildPickerLocked() {
	nodes := make([]*node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.picker.Store(newPicker(nodes))
}

// pick 選一個節點(無鎖,讀 atomic picker 指標)。
func (c *core) pick() *node {
	return c.picker.Load().pick()
}

// recompute 依最近視窗統計重算所有節點的有效權重。
//
// 權重公式直接改寫自 go-kratos(原 warden)的 WRR 計分實作
// (Apache-2.0,歸屬見專案根目錄 NOTICE);思路受 EWMA/P2C 啟發:
//
//	score = sqrt( cs · ss² · 1e9 / (lag · cpu) )
//	ewt   = max(1, score · 設定權重)
//
// 直覺:成功率(client cs、server ss)越高、延遲 lag 與 CPU 越低,
// 分數越高、拿到的流量越多。ss 取平方,讓 server 自評的健康度權重更大。
// 沒有任何樣本的節點沿用「平均分數」,避免冷啟動被邊緣化。
func (c *core) recompute() {
	c.mu.Lock()
	nodes := make([]*node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.mu.Unlock()

	scores := make([]float64, len(nodes))
	var totalScore float64
	var scored int
	for i, n := range nodes {
		errc, req := n.errs.Value()
		latSum, latCnt := n.latency.Value()
		if req == 0 || latCnt == 0 {
			scores[i] = 0 // 無樣本,稍後補平均分
			continue
		}
		cs := clampSuccess(1-float64(errc)/float64(req), req, c.cfg.SuccessFloor)
		ss := clampFloor(math.Float64frombits(atomic.LoadUint64(&n.ssBits)), c.cfg.SuccessFloor)
		lag := math.Max(float64(latSum)/float64(latCnt), 1) // 微秒,下限 1
		cpu := float64(maxInt64(atomic.LoadInt64(&n.cpuMilli), c.cfg.CPUFloorMilli))

		scores[i] = math.Sqrt(cs * ss * ss * 1e9 / (lag * cpu))
		totalScore += scores[i]
		scored++
	}

	// 全場都沒有樣本:維持靜態設定權重,不動
	if scored == 0 {
		return
	}
	avg := totalScore / float64(scored)
	for i, n := range nodes {
		s := scores[i]
		if s <= 0 {
			s = avg // 無樣本節點給平均分,公平參與下一輪
		}
		ewt := int64(s * float64(n.weight))
		if ewt < 1 {
			ewt = 1 // 永不歸零,留一線流量讓壞節點有機會證明自己恢復了
		}
		atomic.StoreInt64(&n.ewt, ewt)
	}
}

// clampSuccess 對 client 端成功率取下限,並對冷啟動(樣本極少)額外保護。
func clampSuccess(cs float64, req int64, floor float64) float64 {
	if cs <= 0 {
		return floor
	}
	// 樣本少又成功率偏低時,別急著重判——可能只是運氣差
	if cs <= 2*floor && req <= 5 {
		return 2 * floor
	}
	return cs
}

func clampFloor(v, floor float64) float64 {
	if v < floor {
		return floor
	}
	return v
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
