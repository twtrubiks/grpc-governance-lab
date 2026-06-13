package registry

import (
	"context"
	"errors"
	"time"
)

// Watcher 訂閱一個服務的副本變化。
//
// C 在每次成員變動時收到一份完整快照;只保留最新值——消費者來不及
// 收的中間狀態會被丟棄(訂閱者只關心「現在有誰」,不關心歷史)。
// 僅支援單一消費者。ctx 取消後 C 會被關閉。
type Watcher struct {
	// C 副本快照通道;關閉表示訂閱已結束。
	C <-chan []Instance
	// Stop 結束訂閱(冪等);也可直接取消傳給 Watch 的 ctx。
	Stop context.CancelFunc
}

// Watch 啟動背景長輪詢訂閱。discovery 不可用時指數退避重連、
// 不關閉通道也不推送任何東西——消費者手上的最後一份快照繼續有效,
// 這是「控制面掛掉不影響資料面」的 SDK 端基礎(M4 resolver 快取降級)。
func (c *Client) Watch(ctx context.Context, service string) *Watcher {
	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan []Instance, 1)
	go c.watchLoop(ctx, service, ch)
	return &Watcher{C: ch, Stop: cancel}
}

func (c *Client) watchLoop(ctx context.Context, service string, ch chan []Instance) {
	defer close(ch)
	var version int64
	backoff := c.cfg.BackoffBase
	for {
		if ctx.Err() != nil {
			return
		}
		instances, newVersion, err := c.poll(ctx, service, version)
		switch {
		case err == nil:
			backoff = c.cfg.BackoffBase
			version = newVersion
			// 只保留最新快照:通道滿了就丟掉舊的再放新的
			select {
			case ch <- instances:
			default:
				select {
				case <-ch:
				default:
				}
				ch <- instances
			}
		case errors.Is(err, errNotModified):
			// 長輪詢逾時無變化,立刻再掛一輪
			backoff = c.cfg.BackoffBase
		default:
			if ctx.Err() != nil {
				return
			}
			c.cfg.Logger.Warn("poll 失敗,退避後重試",
				"service", service, "backoff", backoff.String(), "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > c.cfg.BackoffMax {
				backoff = c.cfg.BackoffMax
			}
		}
	}
}
