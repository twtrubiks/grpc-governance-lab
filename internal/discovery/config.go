package discovery

import (
	"log/slog"
	"time"
)

// Config 是註冊中心的全部時間參數與閾值。
// 零值欄位採用生產預設;測試一律注入毫秒級參數,
// 「停心跳 90 秒後剔除」這類驗收才不用真等 90 秒。
type Config struct {
	// HeartbeatInterval 期望的心跳週期,預設 30s。
	// 剔除門檻與 Guard 期望心跳數都從它推導。
	HeartbeatInterval time.Duration
	// EvictFactor 漏幾次心跳才剔除,預設 3(即 90s)。
	// 取 3 不取 1:單次心跳丟包(GC 停頓、網路抖動)不該判死刑。
	EvictFactor int
	// EvictInterval 剔除掃描週期,預設 10s。
	EvictInterval time.Duration
	// HardEvictAfter 強制剔除上限,預設 1h:
	// 即使 Guard 自保模式生效,死了這麼久的節點也一定移除,
	// 避免極端情況下殭屍節點永久殘留。
	HardEvictAfter time.Duration
	// PollTimeout 長輪詢無變化時的最長等待,預設 30s。
	PollTimeout time.Duration
	// GuardWindow Guard 統計窗長度,預設 1 分鐘。
	GuardWindow time.Duration
	// GuardThreshold 自保閾值,預設 0.85:
	// 一個統計窗內實際心跳數 < 期望值 × 閾值,即判定為網路分區、
	// 進入自保模式停止剔除。<= 0 表示停用 Guard(部分測試用)。
	GuardThreshold float64
	// Logger 結構化日誌;nil 用 slog.Default()。
	Logger *slog.Logger
}

// withDefaults 回傳補上生產預設值的副本,不修改原設定。
func (c *Config) withDefaults() Config {
	out := Config{}
	if c != nil {
		out = *c
	}
	if out.HeartbeatInterval <= 0 {
		out.HeartbeatInterval = 30 * time.Second
	}
	if out.EvictFactor <= 0 {
		out.EvictFactor = 3
	}
	if out.EvictInterval <= 0 {
		out.EvictInterval = 10 * time.Second
	}
	if out.HardEvictAfter <= 0 {
		out.HardEvictAfter = time.Hour
	}
	if out.PollTimeout <= 0 {
		out.PollTimeout = 30 * time.Second
	}
	if out.GuardWindow <= 0 {
		out.GuardWindow = time.Minute
	}
	// GuardThreshold 零值(未設定)補預設;負值視為「明確停用」保留原樣
	if out.GuardThreshold == 0 {
		out.GuardThreshold = 0.85
	}
	if out.Logger == nil {
		out.Logger = slog.Default()
	}
	return out
}
