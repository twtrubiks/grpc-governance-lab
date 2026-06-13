package interceptor

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

// ClientConfig 是 client 攔截器的設定。
type ClientConfig struct {
	// Timeout 全域預設超時;<= 0 表示不主動加 deadline
	// (但仍會尊重並沿用上游 context 既有的 deadline)。
	Timeout time.Duration
	// PropagateKeys 是 metadata 透傳白名單:incoming metadata 中
	// 命中的 key 會原樣複製到 outgoing,沿呼叫鏈一路傳下去
	// (例如 user-id);白名單外的 key 一律不透傳,避免內部標頭外洩。
	PropagateKeys []string
	// Method 是 per-method 覆蓋,key 用完整方法名,
	// 例如 "/account.v1.AccountService/GetUser"。
	Method map[string]*MethodConfig
}

// MethodConfig 是單一方法的覆蓋設定。
//
// NOTE(取捨):熔斷器插槽預留在這一層——成熟框架的 breaker 正是按 method
// 隔離。backlog 接入 SRE breaker 時,在此加 Breaker 欄位、
// 在 UnaryClient 的 invoke 前後掛 Allow/Done 即可,不需要動架構。
type MethodConfig struct {
	// Timeout 覆蓋全域超時;<= 0 表示沿用全域值。
	Timeout time.Duration
}

// UnaryClient 回傳 client 攔截器,做三件事:
//
//  1. 超時遞減:本次呼叫的 deadline = min(上游剩餘時間, 設定超時),
//     確保下游永遠不會比上游活得久(上游都放棄了,下游做完也是白做)
//  2. metadata 白名單透傳:incoming → outgoing 原樣複製
//  3. status → ecode 還原:呼叫失敗時回傳業務碼,
//     業務層直接 ecode.Equal(err, ecode.NothingFound) 分支
func UnaryClient(cfg *ClientConfig) grpc.UnaryClientInterceptor {
	if cfg == nil {
		cfg = &ClientConfig{}
	}
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// per-method 超時覆蓋全域設定
		timeout := cfg.Timeout
		if mc := cfg.Method[method]; mc != nil && mc.Timeout > 0 {
			timeout = mc.Timeout
		}
		// 超時遞減:上游剩餘時間更短就用上游的,deadline 只會收緊不會放寬
		if deadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(deadline); timeout <= 0 || remaining < timeout {
				timeout = remaining
			}
		}
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		// 白名單 metadata 透傳:這個進程是中繼(server 收到後再呼叫下游)時,
		// 把上游帶來的身分資訊接力給下游
		if in, ok := metadata.FromIncomingContext(ctx); ok {
			for _, key := range cfg.PropagateKeys {
				for _, val := range in.Get(key) {
					ctx = metadata.AppendToOutgoingContext(ctx, key, val)
				}
			}
		}

		if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
			// 還原成業務碼:details 裡有 ecode 就原樣還原,
			// 否則(連線層錯誤、對方逾時)按 grpc code 粗映射
			return ecode.FromError(err)
		}
		return nil
	}
}
