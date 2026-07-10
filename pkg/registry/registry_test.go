package registry

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/twtrubiks/grpc-governance-lab/internal/discovery"
)

// serverConfig 是測試用的註冊中心設定:TTL = 30ms × 4 = 120ms,
// 長輪詢 50ms 逾時,Guard 停用(SDK 測試不關心自保)。
func serverConfig() *discovery.Config {
	return &discovery.Config{
		HeartbeatInterval: 30 * time.Millisecond,
		EvictFactor:       4,
		EvictInterval:     10 * time.Millisecond,
		HardEvictAfter:    10 * time.Second,
		PollTimeout:       50 * time.Millisecond,
		GuardThreshold:    -1,
		Logger:            discardLogger(),
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startServer 在指定位址(空字串表示隨機 port)起一個註冊中心,
// 回傳其位址與關閉函式。可在同一位址重啟,模擬註冊中心重啟。
func startServer(t *testing.T, addr string) (string, *discovery.Registry, func()) {
	t.Helper()
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("監聽 %s 失敗: %v", addr, err)
	}
	reg := discovery.New(serverConfig())
	srv := &http.Server{Handler: discovery.NewHandler(reg)}
	go func() { _ = srv.Serve(lis) }()
	var once sync.Once
	closeFn := func() {
		once.Do(func() {
			_ = srv.Close()
			reg.Close()
		})
	}
	t.Cleanup(closeFn)
	return lis.Addr().String(), reg, closeFn
}

// newSDK 建立指向測試註冊中心的 SDK client(毫秒級參數)。
func newSDK(addr string) *Client {
	return New(Config{
		Endpoint:          "http://" + addr,
		HeartbeatInterval: 20 * time.Millisecond,
		RequestTimeout:    time.Second,
		PollTimeout:       time.Second, // 必須 > server 端的 50ms
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        50 * time.Millisecond,
		Logger:            discardLogger(),
	})
}

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

func serverCount(reg *discovery.Registry, service string) int {
	instances, _, _ := reg.Fetch(service)
	return len(instances)
}

// TestRegister_HeartbeatKeepsAlive 驗證背景心跳讓副本活過多個 TTL。
func TestRegister_HeartbeatKeepsAlive(t *testing.T) {
	addr, reg, _ := startServer(t, "")
	sdk := newSDK(addr)

	r, err := sdk.Register(context.Background(), Instance{Service: "account", Addr: "10.0.0.1:9000"})
	if err != nil {
		t.Fatalf("註冊失敗: %v", err)
	}
	t.Cleanup(func() { _ = r.Deregister(context.Background()) })

	// 觀察 3 個 TTL(360ms),副本必須一直都在
	deadline := time.Now().Add(360 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := serverCount(reg, "account"); got != 1 {
			t.Fatalf("背景心跳中的副本不應消失,實際剩 %d", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestDeregister_RemovesImmediately 是 M3 驗收:優雅下線立刻移除,
// 且停掉的心跳不會把節點重新加回去。
func TestDeregister_RemovesImmediately(t *testing.T) {
	addr, reg, _ := startServer(t, "")
	sdk := newSDK(addr)

	r, err := sdk.Register(context.Background(), Instance{Service: "account", Addr: "10.0.0.1:9000"})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Deregister(context.Background()); err != nil {
		t.Fatalf("註銷失敗: %v", err)
	}
	if got := serverCount(reg, "account"); got != 0 {
		t.Fatalf("註銷後應立刻消失,實際剩 %d", got)
	}
	// 等好幾個心跳週期:若 Deregister 的停心跳順序寫反,
	// 在途心跳的 404 會觸發重新註冊,節點詐屍
	time.Sleep(100 * time.Millisecond)
	if got := serverCount(reg, "account"); got != 0 {
		t.Fatalf("註銷後心跳不得把節點加回去,實際剩 %d", got)
	}
}

// TestRegister_InitialFailureRecovered 驗證初次註冊失敗的自癒:
// 註冊中心比服務晚就緒(docker-compose 的 depends_on 只等容器啟動、
// 不等 HTTP listener ready)時,Register 照樣啟動心跳,
// 待註冊中心可達後自動補註冊,而不是永久缺席服務發現。
func TestRegister_InitialFailureRecovered(t *testing.T) {
	// 先佔一個位址再放掉,拿到「還沒有註冊中心在聽」的位址
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	sdk := newSDK(addr)
	r, err := sdk.Register(context.Background(), Instance{Service: "account", Addr: "10.0.0.1:9000"})
	if err != nil {
		t.Fatalf("暫時性註冊失敗不應回傳錯誤(心跳會補註冊),實際: %v", err)
	}
	t.Cleanup(func() { _ = r.Deregister(context.Background()) })

	// 註冊中心「後來」才在同一位址啟動
	_, reg, _ := startServer(t, addr)
	waitFor(t, time.Second, func() bool { return serverCount(reg, "account") == 1 },
		"註冊中心就緒後心跳應自動補註冊")
}

// TestRegister_RetriesWithBackoffUntilRegistered 驗證初次註冊失敗後
// 以指數退避持續重試、直到補註冊成功,而不是只補一拍就退回心跳
// 週期:心跳週期故意設得很長(10s),註冊中心等退避跑過數輪後才
// 就緒——若補註冊只嘗試一次,本測試會等不到節點出現。
func TestRegister_RetriesWithBackoffUntilRegistered(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	sdk := New(Config{
		Endpoint:          "http://" + addr,
		HeartbeatInterval: 10 * time.Second, // 退回心跳週期 = 測試逾時
		RequestTimeout:    time.Second,
		PollTimeout:       time.Second,
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        50 * time.Millisecond,
		Logger:            discardLogger(),
	})
	r, err := sdk.Register(context.Background(), Instance{Service: "account", Addr: "10.0.0.1:9000"})
	if err != nil {
		t.Fatalf("暫時性註冊失敗不應回傳錯誤: %v", err)
	}
	t.Cleanup(func() { _ = r.Deregister(context.Background()) })

	// 讓退避跑過數輪(10+20+40+50ms...)之後註冊中心才就緒
	time.Sleep(150 * time.Millisecond)
	_, reg, _ := startServer(t, addr)
	waitFor(t, 2*time.Second, func() bool { return serverCount(reg, "account") == 1 },
		"退避重試應在註冊中心就緒後補註冊,不必等到心跳週期")
}

// TestRegister_InvalidRequestFailsFast 驗證請求不合法(缺必要欄位)
// 時 Register 直接回錯誤——這種錯誤重試也不會成功,不該啟動心跳。
func TestRegister_InvalidRequestFailsFast(t *testing.T) {
	addr, _, _ := startServer(t, "")
	sdk := newSDK(addr)

	r, err := sdk.Register(context.Background(), Instance{Service: "account"}) // 缺 Addr
	if err == nil {
		t.Fatal("缺必要欄位應回錯誤,實際成功")
	}
	if r != nil {
		t.Fatalf("回錯誤時不應回傳 Registration,實際: %+v", r)
	}
}

// TestHeartbeat_ReregistersAfterEviction 驗證心跳 404 自動重新註冊:
// 註冊中心單方面忘掉節點(剔除/重啟清空)後,節點自己回來。
func TestHeartbeat_ReregistersAfterEviction(t *testing.T) {
	addr, reg, _ := startServer(t, "")
	sdk := newSDK(addr)

	r, err := sdk.Register(context.Background(), Instance{Service: "account", Addr: "10.0.0.1:9000"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Deregister(context.Background()) })

	// 模擬註冊中心單方面忘掉這個節點
	if err := reg.Cancel("account", "10.0.0.1:9000"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { return serverCount(reg, "account") == 1 },
		"下一次心跳收到 404 後應自動重新註冊")
}

// recvSnapshot 從 Watcher 收一份快照,逾時 fail。
func recvSnapshot(t *testing.T, w *Watcher, timeout time.Duration) []Instance {
	t.Helper()
	select {
	case snap, ok := <-w.C:
		if !ok {
			t.Fatal("訂閱通道被意外關閉")
		}
		return snap
	case <-time.After(timeout):
		t.Fatal("等待訂閱快照逾時")
		return nil
	}
}

// TestWatch_ReceivesMembershipChanges 是 M3 驗收:訂閱者在成員
// 變動時(上線/下線)1 秒內收到新快照。
func TestWatch_ReceivesMembershipChanges(t *testing.T) {
	addr, reg, _ := startServer(t, "")
	sdk := newSDK(addr)

	w := sdk.Watch(context.Background(), "account")
	t.Cleanup(w.Stop)

	if err := reg.Register(discovery.Instance{Service: "account", ID: "a1", Addr: "a1"}); err != nil {
		t.Fatal(err)
	}
	if snap := recvSnapshot(t, w, time.Second); len(snap) != 1 || snap[0].ID != "a1" {
		t.Fatalf("上線後應收到 1 副本快照,實際: %+v", snap)
	}

	if err := reg.Register(discovery.Instance{Service: "account", ID: "a2", Addr: "a2"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		select {
		case snap := <-w.C:
			return len(snap) == 2
		default:
			return false
		}
	}, "第二個副本上線後應收到 2 副本快照")

	if err := reg.Cancel("account", "a1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		select {
		case snap := <-w.C:
			return len(snap) == 1 && snap[0].ID == "a2"
		default:
			return false
		}
	}, "副本下線後應收到 1 副本快照")
}

// TestWatch_SurvivesServerRestart 是 demo 場景 6 的 SDK 端機制:
// 註冊中心整個掛掉再重啟(記憶體清空、版本歸零),
// 訂閱迴圈退避重連、自動恢復,不需要任何人工介入。
func TestWatch_SurvivesServerRestart(t *testing.T) {
	addr, reg, closeFn := startServer(t, "")
	sdk := newSDK(addr)

	w := sdk.Watch(context.Background(), "account")
	t.Cleanup(w.Stop)

	if err := reg.Register(discovery.Instance{Service: "account", ID: "a1", Addr: "a1"}); err != nil {
		t.Fatal(err)
	}
	if snap := recvSnapshot(t, w, time.Second); len(snap) != 1 {
		t.Fatalf("初始快照應有 1 副本,實際: %+v", snap)
	}

	// 註冊中心死亡:訂閱通道必須保持沉默(不推空、不關閉),
	// 消費者手上的最後一份快照繼續有效
	closeFn()
	select {
	case snap, ok := <-w.C:
		t.Fatalf("控制面掛掉時不該有任何推送,實際收到: %+v ok=%v", snap, ok)
	case <-time.After(150 * time.Millisecond):
	}

	// 同一位址重啟(全新記憶體),新的成員上線
	_, reg2, _ := startServer(t, addr)
	if err := reg2.Register(discovery.Instance{Service: "account", ID: "a2", Addr: "a2"}); err != nil {
		t.Fatal(err)
	}

	// 退避重連後訂閱自動恢復,收到重啟後的新快照
	waitFor(t, 3*time.Second, func() bool {
		select {
		case snap := <-w.C:
			return len(snap) == 1 && snap[0].ID == "a2"
		default:
			return false
		}
	}, "註冊中心重啟後訂閱應自動恢復並收到新快照")
}

// TestWatch_StopClosesChannel 驗證訂閱的退出路徑:Stop 後通道關閉。
func TestWatch_StopClosesChannel(t *testing.T) {
	addr, _, _ := startServer(t, "")
	sdk := newSDK(addr)

	w := sdk.Watch(context.Background(), "account")
	w.Stop()
	waitFor(t, time.Second, func() bool {
		select {
		case _, ok := <-w.C:
			return !ok
		default:
			return false
		}
	}, "Stop 後訂閱通道應被關閉")
}

// TestFetch 驗證一次性拉取。
func TestFetch(t *testing.T) {
	addr, reg, _ := startServer(t, "")
	sdk := newSDK(addr)

	if err := reg.Register(discovery.Instance{Service: "account", ID: "a1", Addr: "a1"}); err != nil {
		t.Fatal(err)
	}
	instances, err := sdk.Fetch(context.Background(), "account")
	if err != nil || len(instances) != 1 {
		t.Fatalf("fetch 應拿到 1 副本,實際: %v err=%v", instances, err)
	}
}
