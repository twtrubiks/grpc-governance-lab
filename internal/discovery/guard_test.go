package discovery

import (
	"fmt"
	"testing"
	"time"
)

// registerN 註冊 n 個 account 副本並回傳它們。
func registerN(t *testing.T, r *Registry, n int) []Instance {
	t.Helper()
	out := make([]Instance, 0, n)
	for i := 0; i < n; i++ {
		target := ins("account", fmt.Sprintf("10.0.0.%d:9000", i+1))
		if err := r.Register(target); err != nil {
			t.Fatal(err)
		}
		out = append(out, target)
	}
	return out
}

// TestGuard_MassHeartbeatLoss 是 M3 驗收:10 個節點同時停心跳
// (模擬網路分區),Guard 進入自保、停止剔除,fetch 仍回完整列表。
func TestGuard_MassHeartbeatLoss(t *testing.T) {
	r := New(testConfig(0.85))
	t.Cleanup(r.Close)

	nodes := registerN(t, r, 10)
	stop := startBeating(t, r, 10*time.Millisecond, nodes)

	// 先正常心跳幾個統計窗,建立「期望心跳數」的基準
	time.Sleep(100 * time.Millisecond)
	// 模擬網路分區:所有心跳同時消失
	stop()

	// 等超過 TTL(120ms)+ 數個統計窗,正常情況早就該被剔除;
	// Guard 自保生效時必須一個都不少
	time.Sleep(400 * time.Millisecond)
	if got := count(r, "account"); got != 10 {
		t.Fatalf("自保模式下不得剔除任何節點,實際剩 %d/10", got)
	}
}

// TestGuard_SingleNodeFailureStillEvicted 是 M3 驗收:
// 只有單一節點停心跳(正常故障,其餘 9 個照常心跳)則照常剔除。
func TestGuard_SingleNodeFailureStillEvicted(t *testing.T) {
	r := New(testConfig(0.85))
	t.Cleanup(r.Close)

	nodes := registerN(t, r, 10)
	// 9 個節點持續心跳,第 10 個從此沉默
	startBeating(t, r, 10*time.Millisecond, nodes[:9])

	// 9/10 = 0.9 > 0.85,Guard 不觸發,死掉的節點照常被剔除
	waitFor(t, 2*time.Second, func() bool { return count(r, "account") == 9 },
		"單一節點故障應照常剔除,不受 Guard 影響")
	// 確認剔除的是沉默的那個
	instances, _, _ := r.Fetch("account")
	for _, got := range instances {
		if got.ID == nodes[9].ID {
			t.Fatal("被剔除的應是停心跳的節點")
		}
	}
}

// TestGuard_RecoveryExitsProtection 是 M3 驗收的「恢復後自動退出」:
// 分區恢復、心跳回來後,Guard 退出自保,剔除功能恢復正常。
func TestGuard_RecoveryExitsProtection(t *testing.T) {
	r := New(testConfig(0.85))
	t.Cleanup(r.Close)

	nodes := registerN(t, r, 10)
	stop := startBeating(t, r, 10*time.Millisecond, nodes)
	time.Sleep(100 * time.Millisecond)

	// 分區:全部心跳消失 → 進入自保
	stop()
	time.Sleep(300 * time.Millisecond)
	if got := count(r, "account"); got != 10 {
		t.Fatalf("自保期間不得剔除,實際剩 %d/10", got)
	}

	// 分區恢復:9 個節點的心跳回來了,第 10 個真的死了
	startBeating(t, r, 10*time.Millisecond, nodes[:9])
	// 心跳量回到 90%,下個統計窗 Guard 退出自保,死節點被照常剔除
	waitFor(t, 2*time.Second, func() bool { return count(r, "account") == 9 },
		"自保退出後,真正死掉的節點應被剔除")
}

// TestGuardEvaluate 直接驗證 guard 結算邏輯的邊界。
func TestGuardEvaluate(t *testing.T) {
	tests := []struct {
		name          string
		threshold     float64
		instanceCount int
		renews        int
		want          bool
	}{
		{"心跳充足不觸發", 0.85, 10, 20, false},
		{"心跳大量消失觸發自保", 0.85, 10, 5, true},
		{"恰好等於閾值不觸發", 0.85, 10, 17, false}, // 期望 20×0.85=17,actual >= 即正常
		{"沒有任何節點不觸發", 0.85, 0, 0, false},
		{"停用 Guard 永不觸發", -1, 10, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newGuard(tt.threshold)
			for i := 0; i < tt.renews; i++ {
				g.record()
			}
			// 每個統計窗期望每節點 2 次心跳
			_, protected := g.evaluate(tt.instanceCount, 2)
			if protected != tt.want {
				t.Errorf("protected = %v, want %v", protected, tt.want)
			}
		})
	}
}

// TestGuardEvaluate_WindowResets 驗證計數每窗歸零:
// 上一窗的心跳不能折抵下一窗。
func TestGuardEvaluate_WindowResets(t *testing.T) {
	g := newGuard(0.85)
	for i := 0; i < 100; i++ {
		g.record()
	}
	if _, protected := g.evaluate(10, 2); protected {
		t.Fatal("第一窗心跳充足不應觸發")
	}
	// 第二窗完全沒心跳:上一窗的 100 次不能拿來折抵
	changed, protected := g.evaluate(10, 2)
	if !changed || !protected {
		t.Errorf("第二窗心跳归零應觸發自保,changed=%v protected=%v", changed, protected)
	}
}
