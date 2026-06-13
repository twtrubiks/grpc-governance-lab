// demo 是 account 與 relation 共用的業務 binary,用 -role 切換角色。
//
// 它把 pkg/ 的治理元件串起來:掛 server 攔截器鏈(recovery / access log /
// ecode / 負載回報)、註冊到 discovery 並背景心跳、提供標準 gRPC health
// check,並開一個 admin 埠供 demo 注入故障(延遲 / 錯誤 / CPU)。
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	accountv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/account/v1"
	relationv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/relation/v1"
	"github.com/twtrubiks/grpc-governance-lab/internal/account"
	"github.com/twtrubiks/grpc-governance-lab/internal/relation"
	"github.com/twtrubiks/grpc-governance-lab/pkg/balancer/wrr"
	"github.com/twtrubiks/grpc-governance-lab/pkg/interceptor"
	"github.com/twtrubiks/grpc-governance-lab/pkg/registry"
)

func main() {
	var (
		role         = flag.String("role", "account", "服務角色:account 或 relation")
		grpcAddr     = flag.String("addr", ":9000", "gRPC 監聽位址")
		advertise    = flag.String("advertise", "", "註冊到 discovery 的對外位址(空則用 addr;容器內需設成可被 gateway 連到的 host:port)")
		adminAddr    = flag.String("admin", ":9090", "chaos admin HTTP 位址")
		discoveryURL = flag.String("discovery", "http://127.0.0.1:7171", "註冊中心位址")
		serviceName  = flag.String("name", "", "註冊的服務名(空則用 role)")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("role", *role, "addr", *grpcAddr)
	if *serviceName == "" {
		*serviceName = *role
	}
	if *advertise == "" {
		*advertise = *grpcAddr
	}

	ch := &chaos{}
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			interceptor.ServerEcode(),                   // 最外:離開前轉 grpc status
			interceptor.ServerAccessLog(logger),         // 記錄每請求
			wrr.ReportLoadInterceptor(ch.cpuReporter()), // 回報 CPU 給 client balancer
			ch.interceptor(),                            // demo 故障注入(延遲/錯誤)
			interceptor.Recovery(logger),                // 最內:就地接住 panic
		),
		interceptor.ServerKeepalive(nil),
	)

	switch *role {
	case "account":
		accountv1.RegisterAccountServiceServer(srv, account.NewService())
	case "relation":
		relationv1.RegisterRelationServiceServer(srv, relation.NewService())
	default:
		logger.Error("未知角色,只接受 account / relation", "role", *role)
		os.Exit(1)
	}

	// 標準 gRPC health check,供 K8s readiness probe 與 grpc_health_probe 用
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hs)

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		logger.Error("監聽失敗", "error", err)
		os.Exit(1)
	}

	// admin 埠(chaos 注入)
	adminSrv := &http.Server{Addr: *adminAddr, Handler: ch.adminHandler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Warn("admin server 退出", "error", err)
		}
	}()

	grpcErr := make(chan error, 1)
	go func() { grpcErr <- srv.Serve(lis) }()
	logger.Info("demo 服務啟動", "service", *serviceName, "advertise", *advertise, "admin", *adminAddr)

	// 註冊到 discovery,啟動背景心跳
	sdk := registry.New(registry.Config{Endpoint: *discoveryURL, Logger: logger})
	reg, err := sdk.Register(context.Background(), registry.Instance{
		Service: *serviceName,
		ID:      *advertise,
		Addr:    *advertise,
	})
	if err != nil {
		// 註冊失敗不致命:心跳會在 discovery 恢復後自動補註冊;先把服務跑起來
		logger.Warn("初次註冊失敗,將由心跳重試", "error", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		logger.Info("收到退出訊號,優雅下線中")
	case err := <-grpcErr:
		logger.Error("gRPC server 異常退出", "error", err)
	}

	// 優雅下線:先向 discovery 註銷(SDK 內部會先停心跳再 cancel,
	// 不會詐屍),讓 gateway 立刻把本節點移出,再停 gRPC
	if reg != nil {
		deregCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := reg.Deregister(deregCtx); err != nil {
			logger.Warn("註銷失敗", "error", err)
		}
		cancel()
	}
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	srv.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_ = adminSrv.Shutdown(shutdownCtx)
	cancel()
}
