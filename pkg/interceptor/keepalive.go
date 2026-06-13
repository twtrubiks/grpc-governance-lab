package interceptor

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// KeepaliveConfig 是 server 端連線生命週期的五個參數(PLAN.md M2)。
// 零值欄位採用 DefaultKeepalive 的保守預設;所有欄位可經設定注入。
type KeepaliveConfig struct {
	// IdleTimeout 連線閒置多久後送 GOAWAY 要求 client 重連
	// (對應 grpc keepalive.MaxConnectionIdle)。
	IdleTimeout time.Duration
	// MaxLifeTime 連線最長存活時間,到期送 GOAWAY——配合服務發現,
	// 強迫長連線定期重建,新副本才接得到老 client 的流量
	// (對應 MaxConnectionAge)。
	MaxLifeTime time.Duration
	// ForceCloseWait 送出 GOAWAY 後等多久強制斷線,留時間給在途請求
	// (對應 MaxConnectionAgeGrace)。
	ForceCloseWait time.Duration
	// KeepaliveInterval 連線無活動多久後主動 ping 探活(對應 Time)。
	KeepaliveInterval time.Duration
	// KeepaliveTimeout ping 出去多久沒回應就視為斷線(對應 Timeout)。
	KeepaliveTimeout time.Duration
}

// DefaultKeepalive 回傳生產預設值:閒置 60s 踢、最長活 2h、
// 寬限 20s、60s ping 一次、20s 沒 pong 斷線。
func DefaultKeepalive() *KeepaliveConfig {
	return &KeepaliveConfig{
		IdleTimeout:       time.Minute,
		MaxLifeTime:       2 * time.Hour,
		ForceCloseWait:    20 * time.Second,
		KeepaliveInterval: time.Minute,
		KeepaliveTimeout:  20 * time.Second,
	}
}

// ServerKeepalive 把 KeepaliveConfig 轉成 grpc.ServerOption;
// cfg 為 nil 或欄位為零值時補上 DefaultKeepalive 的預設。
func ServerKeepalive(cfg *KeepaliveConfig) grpc.ServerOption {
	def := DefaultKeepalive()
	if cfg == nil {
		cfg = def
	}
	pick := func(v, fallback time.Duration) time.Duration {
		if v > 0 {
			return v
		}
		return fallback
	}
	return grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle:     pick(cfg.IdleTimeout, def.IdleTimeout),
		MaxConnectionAge:      pick(cfg.MaxLifeTime, def.MaxLifeTime),
		MaxConnectionAgeGrace: pick(cfg.ForceCloseWait, def.ForceCloseWait),
		Time:                  pick(cfg.KeepaliveInterval, def.KeepaliveInterval),
		Timeout:               pick(cfg.KeepaliveTimeout, def.KeepaliveTimeout),
	})
}
