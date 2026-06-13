package resolver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	accountv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/account/v1"
	"github.com/twtrubiks/grpc-governance-lab/internal/discovery"
	"github.com/twtrubiks/grpc-governance-lab/pkg/registry"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startDiscovery 起一個毫秒級參數的註冊中心,回傳位址、註冊表與冪等關閉函式。
func startDiscovery(t *testing.T) (string, *discovery.Registry, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	reg := discovery.New(&discovery.Config{
		HeartbeatInterval: 50 * time.Millisecond,
		EvictFactor:       4,
		EvictInterval:     10 * time.Millisecond,
		HardEvictAfter:    10 * time.Second,
		PollTimeout:       50 * time.Millisecond,
		GuardThreshold:    -1,
		Logger:            discardLogger(),
	})
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

// echoAccount 的 GetUser 回傳自己的監聽位址,測試據此辨認流量打到了哪個副本。
type echoAccount struct {
	accountv1.UnimplementedAccountServiceServer
	addr string
}

func (e *echoAccount) GetUser(context.Context, *accountv1.GetUserRequest) (*accountv1.GetUserResponse, error) {
	return &accountv1.GetUserResponse{User: &accountv1.User{Id: 1, Name: e.addr}}, nil
}

// startAccount 起一個真 TCP 的 account gRPC server。
func startAccount(t *testing.T) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	accountv1.RegisterAccountServiceServer(srv, &echoAccount{addr: lis.Addr().String()})
	go func() { _ = srv.Serve(lis) }()
	var once sync.Once
	stop = func() { once.Do(srv.Stop) }
	t.Cleanup(stop)
	return lis.Addr().String(), stop
}

// newSDK 指向測試註冊中心的 SDK(毫秒級參數)。
func newSDK(discoveryAddr string) *registry.Client {
	return registry.New(registry.Config{
		Endpoint:          "http://" + discoveryAddr,
		HeartbeatInterval: 20 * time.Millisecond,
		RequestTimeout:    time.Second,
		PollTimeout:       time.Second,
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        50 * time.Millisecond,
		Logger:            discardLogger(),
	})
}

// dial 用 discovery:///account 建立 round-robin 的 gRPC 連線——
// gateway 不寫任何 account 地址,地址全部來自服務發現。
func dial(t *testing.T, sdk *registry.Client) accountv1.AccountServiceClient {
	t.Helper()
	conn, err := grpc.NewClient("discovery:///account",
		grpc.WithResolvers(NewBuilder(sdk, discardLogger())),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return accountv1.NewAccountServiceClient(conn)
}

// call 呼叫一次 GetUser,回傳服務端副本的位址。
func call(client accountv1.AccountServiceClient) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.GetUser(ctx, &accountv1.GetUserRequest{Id: 1}, grpc.WaitForReady(true))
	if err != nil {
		return "", err
	}
	return resp.GetUser().GetName(), nil
}

// hitSet 連打 n 次,收集實際命中的副本位址集合。
func hitSet(t *testing.T, client accountv1.AccountServiceClient, n int) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	for i := 0; i < n; i++ {
		addr, err := call(client)
		if err != nil {
			t.Fatalf("第 %d 次呼叫失敗: %v", i+1, err)
		}
		got[addr] = true
	}
	return got
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待逾時(%v): %s", timeout, msg)
}

// register 用 SDK 註冊一個副本(帶背景心跳)。
func register(t *testing.T, sdk *registry.Client, addr string) *registry.Registration {
	t.Helper()
	r, err := sdk.Register(context.Background(), registry.Instance{Service: "account", Addr: addr})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// 測試結束時副本可能已主動註銷過,重複註銷的錯誤不影響驗證
		_ = r.Deregister(context.Background())
	})
	return r
}

// TestResolver_DiscoversNewReplica 是 demo 場景 1:
// 新副本啟動後自動接到流量,gateway 全程不改任何設定。
func TestResolver_DiscoversNewReplica(t *testing.T) {
	discoveryAddr, _, _ := startDiscovery(t)
	sdk := newSDK(discoveryAddr)

	addr1, _ := startAccount(t)
	register(t, sdk, addr1)
	client := dial(t, sdk)

	if got := hitSet(t, client, 8); !got[addr1] {
		t.Fatalf("流量應打到 %s,實際: %v", addr1, got)
	}

	// 第二個副本上線:不重建連線、不改設定,流量自動分到新副本
	addr2, _ := startAccount(t)
	register(t, sdk, addr2)
	waitFor(t, 3*time.Second, func() bool {
		got := map[string]bool{}
		for i := 0; i < 10; i++ {
			addr, err := call(client)
			if err != nil {
				return false
			}
			got[addr] = true
		}
		return got[addr1] && got[addr2]
	}, "新副本上線後應自動接到流量")
}

// TestResolver_FailoverOnReplicaDeath 是 demo 場景 2 的機制版:
// 副本死亡(TCP 斷),流量自動轉移到倖存副本,不需等註冊中心剔除——
// 連線層是「立即」的故障感知,註冊中心剔除只是最終一致的兜底。
func TestResolver_FailoverOnReplicaDeath(t *testing.T) {
	discoveryAddr, _, _ := startDiscovery(t)
	sdk := newSDK(discoveryAddr)

	addr1, _ := startAccount(t)
	addr2, stop2 := startAccount(t)
	register(t, sdk, addr1)
	register(t, sdk, addr2)
	client := dial(t, sdk)

	waitFor(t, 3*time.Second, func() bool {
		got := map[string]bool{}
		for i := 0; i < 10; i++ {
			addr, err := call(client)
			if err != nil {
				return false
			}
			got[addr] = true
		}
		return got[addr1] && got[addr2]
	}, "兩個副本都應接到流量")

	// 模擬 docker kill:TCP 直接斷,心跳也停(但註冊中心還沒剔除)
	stop2()
	waitFor(t, 3*time.Second, func() bool {
		// round_robin 只挑 READY 的連線,死副本的 subconn 進入
		// TRANSIENT_FAILURE 後流量應全部落在倖存副本
		for i := 0; i < 10; i++ {
			addr, err := call(client)
			if err != nil || addr != addr1 {
				return false
			}
		}
		return true
	}, "副本死亡後流量應全部轉移到倖存副本")
}

// TestResolver_SurvivesDiscoveryOutage 是 demo 場景 6:
// kill 註冊中心,業務流量完全不受影響(控制面/資料面分離)。
func TestResolver_SurvivesDiscoveryOutage(t *testing.T) {
	discoveryAddr, _, closeDiscovery := startDiscovery(t)
	sdk := newSDK(discoveryAddr)

	addr1, _ := startAccount(t)
	register(t, sdk, addr1)
	client := dial(t, sdk)
	if got := hitSet(t, client, 5); !got[addr1] {
		t.Fatalf("前置:流量應打到 %s,實際 %v", addr1, got)
	}

	// 控制面死亡:resolver 收不到任何更新,gRPC 用最後一次的地址繼續打
	closeDiscovery()
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := call(client); err != nil {
			t.Fatalf("註冊中心掛掉期間業務呼叫不得失敗: %v", err)
		}
	}
}

// TestResolver_IgnoresEmptySnapshot 驗證快取降級的另一半:
// 註冊中心推來空列表(例如重啟後副本還沒重新報到)時保留舊地址,
// 而不是把整個服務打掛。
func TestResolver_IgnoresEmptySnapshot(t *testing.T) {
	discoveryAddr, reg, _ := startDiscovery(t)
	sdk := newSDK(discoveryAddr)

	addr1, _ := startAccount(t)
	// 直接在 server 端註冊(不帶 SDK 心跳),才能用 Cancel 製造空快照
	if err := reg.Register(discovery.Instance{Service: "account", ID: addr1, Addr: addr1}); err != nil {
		t.Fatal(err)
	}
	client := dial(t, sdk)
	if got := hitSet(t, client, 5); !got[addr1] {
		t.Fatalf("前置:流量應打到 %s,實際 %v", addr1, got)
	}

	// 副本從註冊表消失(但進程還活著)→ 訂閱者收到空快照
	if err := reg.Cancel("account", addr1); err != nil {
		t.Fatal(err)
	}
	// 給空快照足夠的傳播時間後,呼叫必須照常成功(舊地址被保留)
	time.Sleep(200 * time.Millisecond)
	if got := hitSet(t, client, 5); !got[addr1] {
		t.Fatalf("空快照應被忽略、舊地址保留,實際 %v", got)
	}
}

// fakeClientConn 是 resolver.ClientConn 的測試替身:記錄被接受的
// UpdateState,並可指定讓下一次 UpdateState 失敗。
type fakeClientConn struct {
	mu       sync.Mutex
	states   []resolver.State
	failOnce bool
}

func (f *fakeClientConn) UpdateState(s resolver.State) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failOnce {
		f.failOnce = false
		return errors.New("暫時性失敗:連線重整中")
	}
	f.states = append(f.states, s)
	return nil
}

func (f *fakeClientConn) ReportError(error)                                    {}
func (f *fakeClientConn) NewAddress([]resolver.Address)                        {}
func (f *fakeClientConn) NewServiceConfig(string)                              {}
func (f *fakeClientConn) ParseServiceConfig(string) *serviceconfig.ParseResult { return nil }

func (f *fakeClientConn) accepted() []resolver.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]resolver.State(nil), f.states...)
}

// TestResolver_ResolveNowRetriesAfterTransientFailure 驗證:pushState 遇到
// 暫時性 UpdateState 失敗時地址不會被永久丟棄——gRPC 隨後的 ResolveNow
// 會以最後一次地址重推成功(不必傻等下次成員變動)。
func TestResolver_ResolveNowRetriesAfterTransientFailure(t *testing.T) {
	fcc := &fakeClientConn{failOnce: true}
	r := &discoveryResolver{service: "account", cc: fcc, logger: discardLogger()}

	addrs := []resolver.Address{{Addr: "127.0.0.1:9000"}}
	r.pushState(addrs) // 第一次 UpdateState 失敗,只記在 last
	if got := fcc.accepted(); len(got) != 0 {
		t.Fatalf("暫時性失敗時不該有被接受的更新,實際 %d 筆", len(got))
	}

	r.ResolveNow(resolver.ResolveNowOptions{}) // gRPC 要求重解析 → 重推最後地址
	got := fcc.accepted()
	if len(got) != 1 {
		t.Fatalf("ResolveNow 應以最後地址重推一次,實際 %d 筆", len(got))
	}
	if len(got[0].Addresses) != 1 || got[0].Addresses[0].Addr != "127.0.0.1:9000" {
		t.Fatalf("重推的地址不正確:%+v", got[0].Addresses)
	}
}
