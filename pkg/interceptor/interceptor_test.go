package interceptor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	accountv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/account/v1"
	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

// fakeAccount 是測試用的 AccountService:行為由注入的 handler 決定,
// 每個測試案例各自定義 server 端要怎麼回應。
type fakeAccount struct {
	accountv1.UnimplementedAccountServiceServer
	handler func(ctx context.Context, req *accountv1.GetUserRequest) (*accountv1.GetUserResponse, error)
}

func (f *fakeAccount) GetUser(ctx context.Context, req *accountv1.GetUserRequest) (*accountv1.GetUserResponse, error) {
	return f.handler(ctx, req)
}

// syncBuffer 讓測試 goroutine 安全讀取 server goroutine 寫入的日誌。
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newTestClient 起一個掛滿 server 攔截器鏈的 bufconn gRPC server,
// 回傳掛了 client 攔截器的 AccountService client。
// 全程走真實的 gRPC wire format,不是進程內函式呼叫。
func newTestClient(t *testing.T, cfg *ClientConfig, logW io.Writer,
	h func(ctx context.Context, req *accountv1.GetUserRequest) (*accountv1.GetUserResponse, error),
) accountv1.AccountServiceClient {
	t.Helper()
	if logW == nil {
		logW = io.Discard
	}
	logger := slog.New(slog.NewJSONHandler(logW, nil))

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(ChainUnaryServer(logger), ServerKeepalive(nil))
	accountv1.RegisterAccountServiceServer(srv, &fakeAccount{handler: h})
	go func() {
		// Serve 在 Stop 後回傳,錯誤對測試無意義
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithUnaryInterceptor(UnaryClient(cfg)),
	)
	if err != nil {
		t.Fatalf("建立 client 連線失敗: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return accountv1.NewAccountServiceClient(conn)
}

// TestRecovery_PanicToServerErr 是 M2 驗收:server handler panic 時,
// client 收到的是業務碼 -500,連線不中斷、後續請求照常服務。
func TestRecovery_PanicToServerErr(t *testing.T) {
	shouldPanic := true
	client := newTestClient(t, nil, nil, func(_ context.Context, _ *accountv1.GetUserRequest) (*accountv1.GetUserResponse, error) {
		if shouldPanic {
			panic("dao 越界了")
		}
		return &accountv1.GetUserResponse{User: &accountv1.User{Id: 1}}, nil
	})

	_, err := client.GetUser(context.Background(), &accountv1.GetUserRequest{Id: 1})
	if !ecode.Equal(err, ecode.ServerErr) {
		t.Fatalf("panic 後 client 應收到 ServerErr(-500),實際: %v", err)
	}

	// panic 不得拖垮 server:下一個請求要正常成功
	shouldPanic = false
	if _, err := client.GetUser(context.Background(), &accountv1.GetUserRequest{Id: 1}); err != nil {
		t.Fatalf("panic 後 server 應繼續服務,實際第二個請求失敗: %v", err)
	}
}

// TestEcode_AcrossNetwork 驗證業務碼跨真實 gRPC 邊界透傳:
// server 回 wrap 過的 -404,client 還原後 code 與 message 都不失真。
func TestEcode_AcrossNetwork(t *testing.T) {
	client := newTestClient(t, nil, nil, func(_ context.Context, _ *accountv1.GetUserRequest) (*accountv1.GetUserResponse, error) {
		return nil, fmt.Errorf("service 層: %w", ecode.NothingFound)
	})

	_, err := client.GetUser(context.Background(), &accountv1.GetUserRequest{Id: 404})
	if !ecode.Equal(err, ecode.NothingFound) {
		t.Fatalf("應還原為 -404,實際: %v", err)
	}
	var c ecode.Code
	if !errors.As(err, &c) {
		t.Fatalf("client 收到的錯誤應是 ecode.Code,實際型別: %T", err)
	}
	if c.Message() != ecode.NothingFound.Message() {
		t.Errorf("message 跨網路後失真: %q, want %q", c.Message(), ecode.NothingFound.Message())
	}
}

// TestUnaryClient_TimeoutDecrement 是 M2 驗收:上游(模擬 gateway)設
// 500ms deadline,下游(account)收到的 ctx 剩餘時間必須 < 500ms,
// 即使 client 自己設定的全域超時(2s)更寬鬆。
func TestUnaryClient_TimeoutDecrement(t *testing.T) {
	var (
		gotDeadline bool
		remaining   time.Duration
	)
	client := newTestClient(t, &ClientConfig{Timeout: 2 * time.Second}, nil,
		func(ctx context.Context, _ *accountv1.GetUserRequest) (*accountv1.GetUserResponse, error) {
			var d time.Time
			d, gotDeadline = ctx.Deadline()
			remaining = time.Until(d)
			return &accountv1.GetUserResponse{}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := client.GetUser(ctx, &accountv1.GetUserRequest{Id: 1}); err != nil {
		t.Fatalf("呼叫失敗: %v", err)
	}
	if !gotDeadline {
		t.Fatal("server 端應收到 deadline,實際沒有")
	}
	if remaining <= 0 || remaining >= 500*time.Millisecond {
		t.Errorf("server 端剩餘時間應在 (0, 500ms) 內,實際: %v", remaining)
	}
}

// TestUnaryClient_PerMethodOverride 是 M2 驗收:per-method 超時(100ms)
// 覆蓋全域設定(5s)生效。
func TestUnaryClient_PerMethodOverride(t *testing.T) {
	var remaining time.Duration
	cfg := &ClientConfig{
		Timeout: 5 * time.Second,
		Method: map[string]*MethodConfig{
			accountv1.AccountService_GetUser_FullMethodName: {Timeout: 100 * time.Millisecond},
		},
	}
	client := newTestClient(t, cfg, nil,
		func(ctx context.Context, _ *accountv1.GetUserRequest) (*accountv1.GetUserResponse, error) {
			if d, ok := ctx.Deadline(); ok {
				remaining = time.Until(d)
			}
			return &accountv1.GetUserResponse{}, nil
		})

	if _, err := client.GetUser(context.Background(), &accountv1.GetUserRequest{Id: 1}); err != nil {
		t.Fatalf("呼叫失敗: %v", err)
	}
	if remaining <= 0 || remaining > 100*time.Millisecond {
		t.Errorf("per-method 100ms 應覆蓋全域 5s,server 端剩餘時間實際: %v", remaining)
	}
}

// TestUnaryClient_MetadataWhitelist 驗證 metadata 白名單透傳:
// 名單內的 key 原樣到達下游,名單外的不外洩。
func TestUnaryClient_MetadataWhitelist(t *testing.T) {
	var gotUserID, gotSecret []string
	cfg := &ClientConfig{PropagateKeys: []string{"x-md-user-id"}}
	client := newTestClient(t, cfg, nil,
		func(ctx context.Context, _ *accountv1.GetUserRequest) (*accountv1.GetUserResponse, error) {
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				gotUserID = md.Get("x-md-user-id")
				gotSecret = md.Get("x-internal-secret")
			}
			return &accountv1.GetUserResponse{}, nil
		})

	// 模擬本進程是中繼:上游帶著 metadata 進來(incoming),要接力給下游
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-md-user-id", "42",
		"x-internal-secret", "不該外洩",
	))
	if _, err := client.GetUser(ctx, &accountv1.GetUserRequest{Id: 1}); err != nil {
		t.Fatalf("呼叫失敗: %v", err)
	}
	if len(gotUserID) != 1 || gotUserID[0] != "42" {
		t.Errorf("白名單 key 應透傳到下游,實際: %v", gotUserID)
	}
	if len(gotSecret) != 0 {
		t.Errorf("白名單外的 key 不得透傳,實際洩漏: %v", gotSecret)
	}
}

// TestServerAccessLog_Fields 驗證 access log 一行含方法名與業務碼。
func TestServerAccessLog_Fields(t *testing.T) {
	buf := &syncBuffer{}
	client := newTestClient(t, nil, buf, func(_ context.Context, _ *accountv1.GetUserRequest) (*accountv1.GetUserResponse, error) {
		return nil, ecode.NothingFound
	})

	_, _ = client.GetUser(context.Background(), &accountv1.GetUserRequest{Id: 1}) // 錯誤本身不是受測點
	logLine := buf.String()
	for _, want := range []string{accountv1.AccountService_GetUser_FullMethodName, `"code":-404`} {
		if !strings.Contains(logLine, want) {
			t.Errorf("access log 應包含 %q,實際: %s", want, logLine)
		}
	}
}

// TestServerKeepalive_Defaults 驗證 nil 設定與零值欄位會補預設,不會 panic。
func TestServerKeepalive_Defaults(t *testing.T) {
	if opt := ServerKeepalive(nil); opt == nil {
		t.Error("nil 設定應回傳可用的 ServerOption")
	}
	if opt := ServerKeepalive(&KeepaliveConfig{IdleTimeout: time.Second}); opt == nil {
		t.Error("部分欄位設定應回傳可用的 ServerOption")
	}
}
