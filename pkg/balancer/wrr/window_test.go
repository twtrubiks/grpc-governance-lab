package wrr

import (
	"testing"
	"time"
)

// fakeClock 是可手動推進的時鐘,讓滑動視窗測試不依賴真實時間。
type fakeClock struct {
	t time.Time
}

func (c *fakeClock) now() time.Time      { return c.t }
func (c *fakeClock) add(d time.Duration) { c.t = c.t.Add(d) }

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(1700000000, 0)}
}

func TestRollingWindow_SumWithinWindow(t *testing.T) {
	clk := newFakeClock()
	w := newRollingWindow(10, 300*time.Millisecond, clk.now)

	w.Add(100)
	w.Add(200)
	sum, count := w.Value()
	if sum != 300 || count != 2 {
		t.Fatalf("同一桶內應累加,得 sum=%d count=%d,want 300/2", sum, count)
	}
}

// TestRollingWindow_ExpiresOldBuckets 驗證滑出視窗的資料被淘汰:
// 這是故障節點恢復後權重回得來的前提(舊的壞數據過期)。
func TestRollingWindow_ExpiresOldBuckets(t *testing.T) {
	clk := newFakeClock()
	w := newRollingWindow(10, 300*time.Millisecond, clk.now)

	w.Add(100)
	// 推進超過整個視窗(10 桶 × 300ms = 3 秒),舊資料應全部過期
	clk.add(3001 * time.Millisecond)
	w.Add(50)
	sum, count := w.Value()
	if sum != 50 || count != 1 {
		t.Fatalf("整個視窗過期後只剩新資料,得 sum=%d count=%d,want 50/1", sum, count)
	}
}

// TestRollingWindow_PartialExpiry 驗證只過期跨過的桶,未跨過的保留。
func TestRollingWindow_PartialExpiry(t *testing.T) {
	clk := newFakeClock()
	w := newRollingWindow(10, 300*time.Millisecond, clk.now)

	w.Add(10) // 桶 0
	clk.add(600 * time.Millisecond)
	w.Add(20) // 桶 2(跨 2 桶,桶 0 仍在視窗內)
	sum, count := w.Value()
	if sum != 30 || count != 2 {
		t.Fatalf("視窗內所有桶應累加,得 sum=%d count=%d,want 30/2", sum, count)
	}

	// 再推進到桶 0 滑出視窗(總共 3 秒以上)
	clk.add(2701 * time.Millisecond)
	sum, count = w.Value()
	if sum != 20 || count != 1 {
		t.Fatalf("桶 0 滑出後只剩桶 2,得 sum=%d count=%d,want 20/1", sum, count)
	}
}
