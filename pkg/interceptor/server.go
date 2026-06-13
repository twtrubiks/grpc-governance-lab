package interceptor

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

// ChainUnaryServer 組裝建議的 server 攔截器鏈,順序(由外而內):
//
//	ServerEcode → ServerAccessLog → Recovery
//
// 為什麼是這個順序:Recovery 最貼近 handler,panic 一發生就地轉成
// 業務錯誤,外層的 access log 才記得到這筆請求;ServerEcode 在最外層,
// 錯誤離開 server 前的最後一刻才轉成 grpc status,中間所有攔截器
// 看到的都還是好處理的 ecode。
func ChainUnaryServer(logger *slog.Logger) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(
		ServerEcode(),
		ServerAccessLog(logger),
		Recovery(logger),
	)
}

// Recovery 把 handler 的 panic 轉成 ecode.ServerErr 並記錄堆疊,
// client 收到的是業務碼 -500 而非連線中斷;server 進程不退出。
func Recovery(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorContext(ctx, "panic recovered",
					"method", info.FullMethod,
					"panic", r,
					"stack", string(debug.Stack()),
				)
				err = ecode.ServerErr
			}
		}()
		return handler(ctx, req)
	}
}

// ServerAccessLog 每請求記一行結構化日誌:方法、對端、耗時、業務碼。
// 放在 Recovery 外層,panic 被轉成錯誤後一樣會被記到。
func ServerAccessLog(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)

		peerAddr := ""
		if p, ok := peer.FromContext(ctx); ok {
			peerAddr = p.Addr.String()
		}
		attrs := []any{
			"method", info.FullMethod,
			"peer", peerAddr,
			"code", ecode.FromError(err).Code(),
			"duration_ms", float64(time.Since(start).Microseconds()) / 1000,
		}
		if err != nil {
			attrs = append(attrs, "error", err.Error())
			logger.ErrorContext(ctx, "access", attrs...)
		} else {
			logger.InfoContext(ctx, "access", attrs...)
		}
		return resp, err
	}
}

// ServerEcode 在錯誤離開 server 前把業務錯誤轉成帶 details 的 grpc status,
// 讓業務碼跨網路透傳(details 載體見 pkg/ecode)。必須放在鏈的最外層。
func ServerEcode() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			return resp, ecode.ToStatus(err).Err()
		}
		return resp, nil
	}
}
