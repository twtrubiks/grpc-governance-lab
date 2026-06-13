// 註冊中心進程入口:組裝 internal/discovery,設定經 flag 注入。
// 儲存是純記憶體(教學用,刻意不上 etcd),重啟即清空——
// SDK 的心跳會在重啟後自動重新註冊(pkg/registry)。
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

	"github.com/twtrubiks/grpc-governance-lab/internal/discovery"
)

func main() {
	var (
		addr           = flag.String("addr", ":7171", "HTTP 監聽位址")
		heartbeat      = flag.Duration("heartbeat", 30*time.Second, "期望的心跳週期")
		evictFactor    = flag.Int("evict-factor", 3, "漏幾次心跳剔除")
		evictInterval  = flag.Duration("evict-interval", 10*time.Second, "剔除掃描週期")
		hardEvict      = flag.Duration("hard-evict", time.Hour, "強制剔除上限(自保模式也守不住這條線)")
		pollTimeout    = flag.Duration("poll-timeout", 30*time.Second, "長輪詢最長等待")
		guardWindow    = flag.Duration("guard-window", time.Minute, "Guard 統計窗")
		guardThreshold = flag.Float64("guard-threshold", 0.85, "Guard 自保閾值,<=0 停用")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	reg := discovery.New(&discovery.Config{
		HeartbeatInterval: *heartbeat,
		EvictFactor:       *evictFactor,
		EvictInterval:     *evictInterval,
		HardEvictAfter:    *hardEvict,
		PollTimeout:       *pollTimeout,
		GuardWindow:       *guardWindow,
		GuardThreshold:    *guardThreshold,
		Logger:            logger,
	})

	srv := &http.Server{
		Addr:    *addr,
		Handler: discovery.NewHandler(reg),
		// 長輪詢會掛著連線,讀逾時要大於 poll-timeout
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	logger.Info("discovery 啟動", "addr", *addr,
		"heartbeat", heartbeat.String(), "guard_threshold", *guardThreshold)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		logger.Info("收到退出訊號,優雅關閉中")
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server 異常退出", "error", err)
			os.Exit(1)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("關閉 HTTP server 失敗", "error", err)
	}
	reg.Close()
}
