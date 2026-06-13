package wrr

import (
	"context"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ReportLoadInterceptor 是 server 端攔截器,在每個回應的 trailer 裡夾帶
// 本節點的負載(CPU 使用率千分比),讓 client 端的 wrr balancer 據此調權。
//
// 這是 wrr「負載回報協議」的 producer 端,對應 client 端 balancer 在
// Done 回呼讀 CPUTrailerKey 的 consumer 端。
//
// cpuMilli 回傳當下 CPU 使用率(0~1000);傳 nil 則不回報(client 端
// 會以下限值代入,CPU 維度等同停用)。
func ReportLoadInterceptor(cpuMilli func() int64) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if cpuMilli != nil {
			// 在呼叫 handler 前設定 trailer:即使 handler 回錯誤,
			// 負載資訊一樣會隨 trailer 送回 client
			_ = grpc.SetTrailer(ctx, metadata.Pairs(CPUTrailerKey, strconv.FormatInt(cpuMilli(), 10)))
		}
		return handler(ctx, req)
	}
}
