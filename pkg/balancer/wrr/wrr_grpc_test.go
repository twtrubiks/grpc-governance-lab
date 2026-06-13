package wrr

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	accountv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/account/v1"
)

func init() {
	// 關掉 gRPC 內部日誌,測試輸出乾淨
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(nopWriter{}, nopWriter{}, nopWriter{}))
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// wrrTestServer 是可注入延遲、CPU 回報與錯誤的 account server。
type wrrTestServer struct {
	accountv1.UnimplementedAccountServiceServer
	addr     string
	delay    atomic.Int64 // 注入延遲(毫秒)
	cpuMilli atomic.Int64 // 經 trailer 回報的 CPU
	failCode atomic.Int32 // 非 0 時回對應 grpc code 錯誤
}

func (s *wrrTestServer) GetUser(ctx context.Context, _ *accountv1.GetUserRequest) (*accountv1.GetUserResponse, error) {
	if cpu := s.cpuMilli.Load(); cpu > 0 {
		_ = grpc.SetTrailer(ctx, metadata.Pairs(CPUTrailerKey, strconv.FormatInt(cpu, 10)))
	}
	if d := s.delay.Load(); d > 0 {
		time.Sleep(time.Duration(d) * time.Millisecond)
	}
	if code := s.failCode.Load(); code != 0 {
		return nil, status.Error(codes.Code(code), "injected")
	}
	return &accountv1.GetUserResponse{User: &accountv1.User{Id: 1, Name: s.addr}}, nil
}

// startWRRServer 起一個真 TCP 的 account server。
func startWRRServer(t *testing.T) *wrrTestServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &wrrTestServer{addr: lis.Addr().String()}
	srv := grpc.NewServer()
	accountv1.RegisterAccountServiceServer(srv, s)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return s
}

// dialWRR 用 manual resolver 餵入給定地址,啟用 wrr balancer。
func dialWRR(t *testing.T, addrs []string) accountv1.AccountServiceClient {
	t.Helper()
	r := manual.NewBuilderWithScheme("wrrtest")
	state := resolver.State{}
	for _, a := range addrs {
		state.Addresses = append(state.Addresses, resolver.Address{Addr: a})
	}
	r.InitialState(state)

	conn, err := grpc.NewClient(r.Scheme()+":///account",
		grpc.WithResolvers(r),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(ServiceConfig),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return accountv1.NewAccountServiceClient(conn)
}

func callOnce(client accountv1.AccountServiceClient) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.GetUser(ctx, &accountv1.GetUserRequest{Id: 1}, grpc.WaitForReady(true))
	if err != nil {
		return "", err
	}
	return resp.GetUser().GetName(), nil
}

func waitForG(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待逾時(%v): %s", timeout, msg)
}

// statByAddr 從 Stats() 取指定位址的快照。
func statByAddr(addr string) (Stat, bool) {
	for _, s := range Stats() {
		if s.Addr == addr {
			return s, true
		}
	}
	return Stat{}, false
}

// TestGRPC_DistributesAcrossReplicas 驗證透過真 gRPC 流量分散到所有副本。
func TestGRPC_DistributesAcrossReplicas(t *testing.T) {
	s1, s2, s3 := startWRRServer(t), startWRRServer(t), startWRRServer(t)
	client := dialWRR(t, []string{s1.addr, s2.addr, s3.addr})

	waitForG(t, 3*time.Second, func() bool {
		got := map[string]bool{}
		for i := 0; i < 30; i++ {
			addr, err := callOnce(client)
			if err != nil {
				return false
			}
			got[addr] = true
		}
		return got[s1.addr] && got[s2.addr] && got[s3.addr]
	}, "三個副本都應接到流量")
}

// TestGRPC_FailoverOnDeath 是 demo 場景 2:副本死亡(TCP 斷),
// 流量自動轉移到倖存副本,呼叫不報錯。
func TestGRPC_FailoverOnDeath(t *testing.T) {
	s1 := startWRRServer(t)
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	dyingAddr := lis.Addr().String()
	dying := grpc.NewServer()
	accountv1.RegisterAccountServiceServer(dying, &wrrTestServer{addr: dyingAddr})
	go func() { _ = dying.Serve(lis) }()

	client := dialWRR(t, []string{s1.addr, dyingAddr})
	waitForG(t, 3*time.Second, func() bool {
		got := map[string]bool{}
		for i := 0; i < 20; i++ {
			addr, _ := callOnce(client)
			got[addr] = true
		}
		return got[s1.addr] && got[dyingAddr]
	}, "兩副本都應先接到流量")

	dying.Stop() // docker kill
	waitForG(t, 3*time.Second, func() bool {
		for i := 0; i < 20; i++ {
			addr, err := callOnce(client)
			if err != nil || addr != s1.addr {
				return false
			}
		}
		return true
	}, "副本死亡後流量應全部轉移到倖存副本且不報錯")
}

// TestGRPC_LatencyShiftsTrafficAndRecovers 是 demo 場景 3 的端到端版:
// 對某副本注入延遲,它的有效權重下降、流量占比降到低位;延遲移除後回升。
func TestGRPC_LatencyShiftsTrafficAndRecovers(t *testing.T) {
	// 用毫秒級視窗與重算週期,測試不必等真實的 3 秒;測完還原預設
	RegisterWithConfig(Config{
		Buckets:        10,
		BucketWidth:    50 * time.Millisecond, // 視窗 500ms
		RecalcInterval: 50 * time.Millisecond,
	})
	t.Cleanup(func() { RegisterWithConfig(Config{}) }) // 還原預設參數

	s1, s2, s3 := startWRRServer(t), startWRRServer(t), startWRRServer(t)
	client := dialWRR(t, []string{s1.addr, s2.addr, s3.addr})

	// 背景持續打流量
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = callOnce(client)
			}
		}
	}()
	t.Cleanup(func() { close(stop); wg.Wait() })

	// 等三副本都進入流量
	waitForG(t, 3*time.Second, func() bool {
		_, ok1 := statByAddr(s1.addr)
		_, ok2 := statByAddr(s2.addr)
		_, ok3 := statByAddr(s3.addr)
		return ok1 && ok2 && ok3
	}, "三副本都應出現在 Stats")

	// 對 s3 注入 80ms 延遲,等權重重算
	s3.delay.Store(80)
	waitForG(t, 5*time.Second, func() bool {
		s3stat, ok := statByAddr(s3.addr)
		if !ok {
			return false
		}
		s1stat, _ := statByAddr(s1.addr)
		// 慢副本有效權重應明顯低於快副本
		return s3stat.EffectiveWeight*2 < s1stat.EffectiveWeight
	}, "注入延遲後慢副本有效權重應顯著下降")

	// 移除延遲,等舊的慢數據滑出視窗後權重回升
	s3.delay.Store(0)
	waitForG(t, 5*time.Second, func() bool {
		s3stat, _ := statByAddr(s3.addr)
		s1stat, _ := statByAddr(s1.addr)
		// 回升到與快副本相近(差距 < 50%)
		if s1stat.EffectiveWeight == 0 {
			return false
		}
		ratio := float64(s3stat.EffectiveWeight) / float64(s1stat.EffectiveWeight)
		return ratio > 0.5
	}, "延遲移除後慢副本權重應回升")
}

func TestIsNodeFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil 不算故障", nil, false},
		{"Unavailable 算節點故障", status.Error(codes.Unavailable, ""), true},
		{"DeadlineExceeded 算節點故障", status.Error(codes.DeadlineExceeded, ""), true},
		{"ResourceExhausted 算節點故障", status.Error(codes.ResourceExhausted, ""), true},
		{"NotFound 是業務錯誤不算故障", status.Error(codes.NotFound, ""), false},
		{"InvalidArgument 是業務錯誤不算故障", status.Error(codes.InvalidArgument, ""), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNodeFailure(tt.err); got != tt.want {
				t.Errorf("isNodeFailure() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMilliFromTrailer(t *testing.T) {
	if _, ok := milliFromTrailer(nil, CPUTrailerKey); ok {
		t.Error("nil trailer 應回 ok=false")
	}
	if _, ok := milliFromTrailer(metadata.MD{}, CPUTrailerKey); ok {
		t.Error("無對應 key 應回 ok=false")
	}
	md := metadata.Pairs(CPUTrailerKey, "250", SuccessTrailerKey, "990")
	if milli, ok := milliFromTrailer(md, CPUTrailerKey); !ok || milli != 250 {
		t.Errorf("CPU 應解出 250,得 %d ok=%v", milli, ok)
	}
	if milli, ok := milliFromTrailer(md, SuccessTrailerKey); !ok || milli != 990 {
		t.Errorf("成功率應解出 990,得 %d ok=%v", milli, ok)
	}
	if _, ok := milliFromTrailer(metadata.Pairs(CPUTrailerKey, "abc"), CPUTrailerKey); ok {
		t.Error("非數字應回 ok=false")
	}
}
