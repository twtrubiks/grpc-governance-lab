package discovery

import "sync"

// guard 實作自我保護(類 Eureka self-preservation)。
//
// 推導:單一節點停止心跳,大概率是它真的死了;但「大量節點的心跳
// 同時消失」更可能是註冊中心自己被網路分區隔離——此時把所有節點
// 剔除,訂閱者會拿到空列表、流量瞬間歸零,比「保留一份可能過期的
// 名單」傷害大得多。寧可錯留、不可錯殺。
//
// mu 保護 renews 與 protected;record 在每次心跳的熱路徑上,
// 鎖內只做一次加法。
type guard struct {
	mu sync.Mutex
	// threshold <= 0 表示停用(永不進入自保)。
	threshold float64
	// renews 是目前統計窗內收到的心跳數,結算時歸零。
	renews int
	// protected 是否處於自我保護模式。
	protected bool
}

func newGuard(threshold float64) *guard {
	return &guard{threshold: threshold}
}

// record 記錄一次心跳(註冊與續約都算)。
func (g *guard) record() {
	g.mu.Lock()
	g.renews++
	g.mu.Unlock()
}

// isProtected 回報是否處於自我保護模式(剔除迴圈據此決定是否動手)。
func (g *guard) isProtected() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.protected
}

// evaluate 結算一個統計窗並歸零計數:
// 實際心跳數 < 期望值 × 閾值 → 進入(或維持)自保;恢復則自動退出。
// 回傳狀態是否翻轉與最新狀態,讓呼叫方記日誌。
func (g *guard) evaluate(instanceCount int, expectedPerInstance float64) (changed, protected bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	renews := g.renews
	g.renews = 0

	was := g.protected
	expected := float64(instanceCount) * expectedPerInstance
	// 沒有節點就沒有「期望心跳」可言,一律視為正常
	g.protected = g.threshold > 0 && expected > 0 &&
		float64(renews) < expected*g.threshold
	return g.protected != was, g.protected
}
