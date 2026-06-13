package wrr

import (
	"sync/atomic"
	"testing"
	"time"
)

// testCore 造一個用假時鐘、短視窗的核心,並預先放好節點。
func testCore(clk *fakeClock, addrs ...string) *core {
	c := newCore(Config{
		Buckets:       10,
		BucketWidth:   300 * time.Millisecond,
		CPUFloorMilli: 10,
		SuccessFloor:  0.1,
		Clock:         clk.now,
	})
	set := map[string]int64{}
	for _, a := range addrs {
		set[a] = 10
	}
	c.setAddrs(set)
	return c
}

func nodeByAddr(c *core, addr string) *node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[addr]
}

func ewtOf(c *core, addr string) int64 {
	return atomic.LoadInt64(&nodeByAddr(c, addr).ewt)
}

// TestCore_HighLatencyLosesWeight 是 demo 場景 3 的核心邏輯:
// 注入高延遲的節點,重算後有效權重大幅下降。
func TestCore_HighLatencyLosesWeight(t *testing.T) {
	clk := newFakeClock()
	c := testCore(clk, "fast1", "fast2", "slow")

	// 三個節點都成功;fast 約 1ms,slow 約 200ms
	for i := 0; i < 50; i++ {
		nodeByAddr(c, "fast1").record(1000, false)
		nodeByAddr(c, "fast2").record(1000, false)
		nodeByAddr(c, "slow").record(200000, false)
	}
	c.recompute()

	slowEwt := ewtOf(c, "slow")
	fastEwt := ewtOf(c, "fast1")
	if slowEwt >= fastEwt {
		t.Fatalf("高延遲節點權重應遠低於低延遲節點,slow=%d fast=%d", slowEwt, fastEwt)
	}
	// 流量占比 = slow / (fast1+fast2+slow),場景 3 要求 < 15%
	share := float64(slowEwt) / float64(slowEwt+fastEwt+ewtOf(c, "fast2"))
	if share >= 0.15 {
		t.Errorf("注入 200ms 延遲後 slow 流量占比應 < 15%%,實際 %.1f%%", share*100)
	}
}

// TestCore_RecoversAfterWindowExpires 是場景 3 的「恢復後回升」:
// 高延遲停止後,壞數據隨視窗過期,權重重算回升。
func TestCore_RecoversAfterWindowExpires(t *testing.T) {
	clk := newFakeClock()
	c := testCore(clk, "a", "b")

	for i := 0; i < 50; i++ {
		nodeByAddr(c, "a").record(1000, false)
		nodeByAddr(c, "b").record(200000, false) // b 很慢
	}
	c.recompute()
	if ewtOf(c, "b") >= ewtOf(c, "a") {
		t.Fatal("前置:慢節點 b 權重應低於 a")
	}

	// b 恢復正常延遲,但要等舊的慢數據滑出視窗(> 3 秒)
	clk.add(3100 * time.Millisecond)
	for i := 0; i < 50; i++ {
		nodeByAddr(c, "a").record(1000, false)
		nodeByAddr(c, "b").record(1000, false)
	}
	c.recompute()

	// 現在兩者延遲相同,權重應回到相近(差距 < 20%)
	a, b := ewtOf(c, "a"), ewtOf(c, "b")
	diff := float64(a-b) / float64(a)
	if diff < 0 {
		diff = -diff
	}
	if diff > 0.2 {
		t.Errorf("恢復後權重應回升至相近,a=%d b=%d 差距 %.0f%%", a, b, diff*100)
	}
}

// TestCore_FailuresLoseWeight 驗證失敗率高的節點權重下降。
func TestCore_FailuresLoseWeight(t *testing.T) {
	clk := newFakeClock()
	c := testCore(clk, "good", "bad")

	for i := 0; i < 50; i++ {
		nodeByAddr(c, "good").record(1000, false)
		// bad 一半失敗
		nodeByAddr(c, "bad").record(1000, i%2 == 0)
	}
	c.recompute()

	if ewtOf(c, "bad") >= ewtOf(c, "good") {
		t.Errorf("高失敗率節點權重應較低,bad=%d good=%d", ewtOf(c, "bad"), ewtOf(c, "good"))
	}
}

// TestCore_NeverZeroWeight 驗證權重永不歸零:全失敗的節點仍保留 ewt>=1,
// 才有機會在恢復後重新被選中、證明自己活過來了。
func TestCore_NeverZeroWeight(t *testing.T) {
	clk := newFakeClock()
	c := testCore(clk, "ok", "dead")

	for i := 0; i < 50; i++ {
		nodeByAddr(c, "ok").record(1000, false)
		nodeByAddr(c, "dead").record(1000, true) // 全失敗
	}
	c.recompute()

	if got := ewtOf(c, "dead"); got < 1 {
		t.Errorf("權重不得歸零,dead ewt=%d", got)
	}
}

// TestCore_CPUFloorPreventsExplosion 是 PLAN.md M5 設計注意的驗收:
// server 回報 CPU=0(記憶體 dao 容器的常態)時,公式不得除零/爆炸,
// 權重維持有限值。
func TestCore_CPUFloorPreventsExplosion(t *testing.T) {
	clk := newFakeClock()
	c := testCore(clk, "a", "b")

	nodeByAddr(c, "a").reportCPU(0) // CPU 趨近 0
	nodeByAddr(c, "b").reportCPU(0)
	for i := 0; i < 20; i++ {
		nodeByAddr(c, "a").record(1000, false)
		nodeByAddr(c, "b").record(1000, false)
	}
	c.recompute() // 不該 panic、不該 Inf/NaN

	for _, addr := range []string{"a", "b"} {
		ewt := ewtOf(c, addr)
		if ewt < 1 || ewt > 1<<40 {
			t.Errorf("CPU=0 時權重應為有限值,%s ewt=%d", addr, ewt)
		}
	}
}

// TestCore_NoSamplesKeepsStaticWeight 驗證沒有任何流量時維持靜態權重。
func TestCore_NoSamplesKeepsStaticWeight(t *testing.T) {
	clk := newFakeClock()
	c := testCore(clk, "a", "b")
	c.recompute() // 沒有任何 record
	if ewtOf(c, "a") != 10 || ewtOf(c, "b") != 10 {
		t.Errorf("無樣本應維持設定權重 10,得 a=%d b=%d", ewtOf(c, "a"), ewtOf(c, "b"))
	}
}

// TestCore_SetAddrsPreservesStats 驗證地址沒變的節點重新 setAddrs 後
// 統計不被洗掉(resolver 推同一批地址不該重置權重)。
func TestCore_SetAddrsPreservesStats(t *testing.T) {
	clk := newFakeClock()
	c := testCore(clk, "a", "b")
	for i := 0; i < 20; i++ {
		nodeByAddr(c, "a").record(1000, false)
	}
	before := atomic.LoadInt64(&nodeByAddr(c, "a").requests)

	// 再推一次同樣的地址集合
	c.setAddrs(map[string]int64{"a": 10, "b": 10})
	if after := atomic.LoadInt64(&nodeByAddr(c, "a").requests); after != before {
		t.Errorf("既有節點統計不該被重置,before=%d after=%d", before, after)
	}
}

// TestCore_Snapshot 驗證觀測快照欄位。
func TestCore_Snapshot(t *testing.T) {
	clk := newFakeClock()
	c := testCore(clk, "a")
	n := nodeByAddr(c, "a")
	n.reportCPU(250)
	for i := 0; i < 10; i++ {
		n.record(2000, i == 0) // 1/10 失敗,2ms
	}
	_ = c.pick()

	stats := c.snapshot()
	if len(stats) != 1 {
		t.Fatalf("應有 1 筆快照,得 %d", len(stats))
	}
	s := stats[0]
	if s.Addr != "a" || s.Requests != 10 || s.Fails != 1 || s.CPUMilli != 250 {
		t.Errorf("快照欄位不符: %+v", s)
	}
	if s.LatencyMs != 2 {
		t.Errorf("平均延遲應為 2ms,得 %v", s.LatencyMs)
	}
	if s.SuccessRate < 0.89 || s.SuccessRate > 0.91 {
		t.Errorf("成功率應約 0.9,得 %v", s.SuccessRate)
	}
}
