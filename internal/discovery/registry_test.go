package discovery

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

// testConfig 回傳毫秒級的測試設定(PLAN.md M3 設計注意:
// 生產 30s/90s 的參數在測試裡縮到毫秒,驗收不用真等 90 秒)。
// guardThreshold < 0 表示停用自保,讓剔除類測試不受 Guard 干擾。
func testConfig(guardThreshold float64) *Config {
	return &Config{
		HeartbeatInterval: 30 * time.Millisecond,
		EvictFactor:       4, // TTL = 120ms
		EvictInterval:     10 * time.Millisecond,
		HardEvictAfter:    10 * time.Second, // 大到不干擾一般剔除測試
		PollTimeout:       100 * time.Millisecond,
		GuardWindow:       40 * time.Millisecond,
		GuardThreshold:    guardThreshold,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// waitFor 輪詢等待條件成立,逾時就 fail。比裸 time.Sleep 穩:
// 條件一成立就返回,不多等;真的不成立才吃滿 timeout。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("等待逾時(%v): %s", timeout, msg)
}

// startBeating 啟動背景 goroutine 持續為一批副本續約,
// 回傳停止函式(冪等)。goroutine 退出路徑:stop channel。
func startBeating(t *testing.T, r *Registry, interval time.Duration, instances []Instance) (stop func()) {
	t.Helper()
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				for _, ins := range instances {
					// 副本可能已被剔除,續約 404 是預期內的,測試只關心存活集合
					_ = r.Renew(ins.Service, ins.ID)
				}
			}
		}
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			close(stopCh)
			wg.Wait()
		})
	}
	t.Cleanup(stop)
	return stop
}

func ins(service, id string) Instance {
	return Instance{Service: service, ID: id, Addr: id}
}

func count(r *Registry, service string) int {
	instances, _, _ := r.Fetch(service)
	return len(instances)
}

func TestRegistry_RegisterAndFetch(t *testing.T) {
	r := New(testConfig(-1))
	t.Cleanup(r.Close)

	if err := r.Register(ins("account", "10.0.0.1:9000")); err != nil {
		t.Fatalf("註冊失敗: %v", err)
	}
	instances, version, ok := r.Fetch("account")
	if !ok || len(instances) != 1 || instances[0].Addr != "10.0.0.1:9000" {
		t.Fatalf("註冊後 fetch 應拿到該副本,實際: %v ok=%v", instances, ok)
	}
	if version <= 0 {
		t.Errorf("註冊後版本應 > 0,實際: %d", version)
	}

	if err := r.Register(Instance{Service: "account"}); !ecode.Equal(err, ecode.RequestErr) {
		t.Errorf("缺欄位註冊應回 -400,實際: %v", err)
	}
	if _, _, ok := r.Fetch("不存在的服務"); ok {
		t.Error("不存在的服務 fetch 應回 ok=false")
	}
}

// TestRegistry_EvictAfterMissedHeartbeats 是 M3 驗收:
// 停心跳超過 EvictFactor 個週期(本設定 120ms)後副本被剔除。
func TestRegistry_EvictAfterMissedHeartbeats(t *testing.T) {
	r := New(testConfig(-1))
	t.Cleanup(r.Close)

	if err := r.Register(ins("account", "a1")); err != nil {
		t.Fatal(err)
	}
	// 不續約,等過期
	waitFor(t, time.Second, func() bool { return count(r, "account") == 0 },
		"停心跳 4 個週期後副本應被剔除")
}

// TestRegistry_RenewKeepsAlive 驗證持續續約的副本活過多個 TTL。
func TestRegistry_RenewKeepsAlive(t *testing.T) {
	r := New(testConfig(-1))
	t.Cleanup(r.Close)

	target := ins("account", "a1")
	if err := r.Register(target); err != nil {
		t.Fatal(err)
	}
	startBeating(t, r, 10*time.Millisecond, []Instance{target})

	// 觀察 3 個 TTL 的時間,副本必須一直都在
	deadline := time.Now().Add(360 * time.Millisecond)
	for time.Now().Before(deadline) {
		if count(r, "account") != 1 {
			t.Fatal("持續續約中的副本不應被剔除")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestRegistry_CancelImmediate 是 M3 驗收:主動註銷立刻生效,
// 不必等 90 秒的心跳過期(kill -TERM 優雅下線的底層機制)。
func TestRegistry_CancelImmediate(t *testing.T) {
	r := New(testConfig(-1))
	t.Cleanup(r.Close)

	if err := r.Register(ins("account", "a1")); err != nil {
		t.Fatal(err)
	}
	if err := r.Cancel("account", "a1"); err != nil {
		t.Fatalf("註銷失敗: %v", err)
	}
	// 不等任何週期,立刻檢查
	if got := count(r, "account"); got != 0 {
		t.Fatalf("註銷後應立刻消失,實際還有 %d 個", got)
	}
}

func TestRegistry_RenewUnknownReturnsNotFound(t *testing.T) {
	r := New(testConfig(-1))
	t.Cleanup(r.Close)

	if err := r.Renew("account", "ghost"); !ecode.Equal(err, ecode.NothingFound) {
		t.Errorf("續約不存在的副本應回 -404,實際: %v", err)
	}
	if err := r.Cancel("account", "ghost"); !ecode.Equal(err, ecode.NothingFound) {
		t.Errorf("註銷不存在的副本應回 -404,實際: %v", err)
	}
}

// TestRegistry_PollReturnsOnChange 是 M3 驗收:
// 長輪詢在地址變化時 1 秒內返回(實際應在毫秒級)。
func TestRegistry_PollReturnsOnChange(t *testing.T) {
	r := New(testConfig(-1))
	t.Cleanup(r.Close)

	if err := r.Register(ins("account", "a1")); err != nil {
		t.Fatal(err)
	}
	_, version, _ := r.Fetch("account")

	type result struct {
		instances []Instance
		changed   bool
	}
	got := make(chan result, 1)
	go func() {
		instances, _, changed := r.Poll(context.Background(), "account", version)
		got <- result{instances, changed}
	}()

	// 讓 poll 先掛上去再觸發變化
	time.Sleep(10 * time.Millisecond)
	if err := r.Register(ins("account", "a2")); err != nil {
		t.Fatal(err)
	}

	select {
	case res := <-got:
		if !res.changed || len(res.instances) != 2 {
			t.Fatalf("poll 應帶回 2 個副本的新快照,實際: changed=%v %v", res.changed, res.instances)
		}
	case <-time.After(time.Second):
		t.Fatal("地址變化後 poll 應在 1 秒內返回")
	}
}

// TestRegistry_PollTimeout 驗證無變化時 poll 阻塞到逾時才返回。
func TestRegistry_PollTimeout(t *testing.T) {
	r := New(testConfig(-1))
	t.Cleanup(r.Close)

	if err := r.Register(ins("account", "a1")); err != nil {
		t.Fatal(err)
	}
	startBeating(t, r, 10*time.Millisecond, []Instance{ins("account", "a1")})

	_, version, _ := r.Fetch("account")
	start := time.Now()
	_, _, changed := r.Poll(context.Background(), "account", version)
	if changed {
		t.Fatal("無變化時 poll 不應回報 changed")
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("poll 應阻塞滿 PollTimeout(100ms),實際 %v 就返回了", elapsed)
	}
}

// TestRegistry_PollUnknownServiceBlocksUntilRegistered 驗證
// 「先訂閱、後上線」:訂閱還不存在的服務是合法的,服務一上線就收到通知。
func TestRegistry_PollUnknownServiceBlocksUntilRegistered(t *testing.T) {
	r := New(testConfig(-1))
	t.Cleanup(r.Close)

	got := make(chan []Instance, 1)
	go func() {
		instances, _, _ := r.Poll(context.Background(), "尚未上線", 0)
		got <- instances
	}()
	time.Sleep(10 * time.Millisecond)
	if err := r.Register(ins("尚未上線", "n1")); err != nil {
		t.Fatal(err)
	}

	select {
	case instances := <-got:
		if len(instances) != 1 {
			t.Fatalf("服務上線後訂閱者應收到 1 個副本,實際: %v", instances)
		}
	case <-time.After(time.Second):
		t.Fatal("服務上線後 poll 應立刻返回")
	}
}

// TestRegistry_HardEvict 驗證強制剔除上限:即使 Guard 自保生效
// (threshold=1 讓它必然觸發),死超過 HardEvictAfter 的副本仍被移除。
func TestRegistry_HardEvict(t *testing.T) {
	cfg := testConfig(1.0)
	cfg.HardEvictAfter = 200 * time.Millisecond
	r := New(cfg)
	t.Cleanup(r.Close)

	if err := r.Register(ins("account", "zombie")); err != nil {
		t.Fatal(err)
	}
	// 完全不心跳 → Guard 必然進入自保(actual=0 < expected),
	// 一般剔除被抑制;但超過 HardEvictAfter 後仍須被強制移除
	waitFor(t, 2*time.Second, func() bool { return count(r, "account") == 0 },
		"超過強制剔除上限的殭屍副本應被移除")
}

// serviceCount 回傳目前 services map 的條目數(含空殼),供回收測試斷言。
func serviceCount(r *Registry) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.services)
}

// TestRegistry_ReapsAbandonedEmptyService 驗證:Poll 對任何名字都會懶建條目,
// 但訂閱者離開後,沒副本也沒人訂閱的空殼應被背景剔除回收——否則 client
// 長輪詢大量不存在的服務名會讓 services map 無上限成長。
func TestRegistry_ReapsAbandonedEmptyService(t *testing.T) {
	cfg := testConfig(-1)
	cfg.PollTimeout = 2 * time.Second // 讓單次 Poll 在我們主動取消前保持阻塞
	r := New(cfg)
	t.Cleanup(r.Close)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Poll(ctx, "幽靈服務", 0) // 懶建空條目後阻塞
		close(done)
	}()
	waitFor(t, time.Second, func() bool { return serviceCount(r) == 1 },
		"Poll 不存在的服務應懶建出空條目")

	cancel() // 訂閱者離開,waiters 歸零
	<-done
	waitFor(t, time.Second, func() bool { return serviceCount(r) == 0 },
		"無副本、無訂閱者的空條目應被回收")
}

// TestRegistry_KeepsSubscribedEmptyService 驗證回收的另一半:有訂閱者
// (waiters > 0)的空服務不回收,維持「先訂閱、服務後上線」語意。
func TestRegistry_KeepsSubscribedEmptyService(t *testing.T) {
	cfg := testConfig(-1)
	cfg.PollTimeout = 2 * time.Second // 單次 Poll 在整個觀察窗內保持阻塞 → waiters 持續 > 0
	r := New(cfg)
	t.Cleanup(r.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Poll(ctx, "待上線", 0)
	waitFor(t, time.Second, func() bool { return serviceCount(r) == 1 },
		"訂閱不存在的服務應懶建出空條目")

	// 阻塞中的訂閱者讓 waiters > 0;連跨數個剔除週期,空條目不該被回收
	time.Sleep(80 * time.Millisecond)
	if n := serviceCount(r); n != 1 {
		t.Fatalf("有訂閱者的空服務不該被回收,實際條目數 %d", n)
	}
}
