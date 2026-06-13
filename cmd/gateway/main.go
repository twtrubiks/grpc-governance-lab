// gateway 是 HTTP→gRPC 網關:對外提供聚合 API 與 /debug/backends,
// 對內用自訂 resolver(接 discovery)+ 動態 WRR balancer 呼叫業務服務,
// 全程不寫死任何業務服務地址。
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	accountv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/account/v1"
	relationv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/relation/v1"
	"github.com/twtrubiks/grpc-governance-lab/internal/gateway"
	"github.com/twtrubiks/grpc-governance-lab/pkg/balancer/wrr"
	"github.com/twtrubiks/grpc-governance-lab/pkg/interceptor"
	"github.com/twtrubiks/grpc-governance-lab/pkg/registry"
	gresolver "github.com/twtrubiks/grpc-governance-lab/pkg/resolver"
)

func main() {
	var (
		httpAddr     = flag.String("addr", ":8080", "HTTP 監聽位址")
		discoveryURL = flag.String("discovery", "http://127.0.0.1:7171", "註冊中心位址")
		accountSvc   = flag.String("account", "account", "account 服務名")
		relationSvc  = flag.String("relation", "relation", "relation 服務名")
		aggTimeout   = flag.Duration("timeout", 200*time.Millisecond, "聚合超時")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "gateway")

	sdk := registry.New(registry.Config{Endpoint: *discoveryURL, Logger: logger})
	resolverBuilder := gresolver.NewBuilder(sdk, logger)

	// client 攔截器:per-call 250ms 超時、metadata 白名單透傳、ecode 還原
	clientCfg := &interceptor.ClientConfig{
		Timeout:       250 * time.Millisecond,
		PropagateKeys: []string{"x-md-user-id"},
	}

	accountConn, err := dialService(*accountSvc, resolverBuilder, clientCfg)
	if err != nil {
		logger.Error("建立 account 連線失敗", "error", err)
		os.Exit(1)
	}
	defer func() { _ = accountConn.Close() }()

	relationConn, err := dialService(*relationSvc, resolverBuilder, clientCfg)
	if err != nil {
		logger.Error("建立 relation 連線失敗", "error", err)
		os.Exit(1)
	}
	defer func() { _ = relationConn.Close() }()

	agg := gateway.NewAggregator(
		accountv1.NewAccountServiceClient(accountConn),
		relationv1.NewRelationServiceClient(relationConn),
		*aggTimeout, logger,
	)

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.GET("/healthz", func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	gateway.RegisterRoutes(engine, agg, wrr.Stats)

	srv := &http.Server{Addr: *httpAddr, Handler: engine, ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	logger.Info("gateway 啟動", "addr", *httpAddr, "discovery", *discoveryURL)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		logger.Info("收到退出訊號,關閉中")
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server 異常退出", "error", err)
			os.Exit(1)
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// dialService 用 discovery:///<service> 建立啟用 WRR balancer 的 gRPC 連線。
func dialService(service string, rb *gresolver.Builder, clientCfg *interceptor.ClientConfig) (*grpc.ClientConn, error) {
	return grpc.NewClient("discovery:///"+service,
		grpc.WithResolvers(rb),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(wrr.ServiceConfig),
		grpc.WithUnaryInterceptor(interceptor.UnaryClient(clientCfg)),
	)
}
