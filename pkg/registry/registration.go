package registry

import (
	"context"
	"sync"
	"time"

	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

// Register 註冊一個副本並啟動背景心跳 goroutine。
// 服務關閉時必須呼叫 Registration.Deregister 優雅下線,
// 註冊中心才會立刻(而非等心跳過期)摘掉本節點。
func (c *Client) Register(ctx context.Context, ins Instance) (*Registration, error) {
	if ins.ID == "" {
		ins.ID = ins.Addr
	}
	if err := c.register(ctx, ins); err != nil {
		return nil, err
	}
	r := &Registration{
		c:    c,
		ins:  ins,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go r.heartbeatLoop()
	return r, nil
}

// Registration 代表一次成功的註冊與其背景心跳。
type Registration struct {
	c   *Client
	ins Instance

	// stop 關閉時心跳迴圈退出(goroutine 的明確退出路徑),
	// done 在迴圈真正結束後關閉,Deregister 據此確認不會再有在途心跳。
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// heartbeatLoop 週期性續約;發現自己不在註冊表上(被剔除、或
// 註冊中心重啟清空)就自動重新註冊。其餘錯誤等下個週期再試——
// 心跳本身就是重試機制,不必再疊一層。
func (r *Registration) heartbeatLoop() {
	defer close(r.done)
	ticker := time.NewTicker(r.c.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			r.beat()
		}
	}
}

// beat 送一次心跳,404 時重新註冊。
func (r *Registration) beat() {
	ctx, cancel := context.WithTimeout(context.Background(), r.c.cfg.RequestTimeout)
	defer cancel()
	err := r.c.renew(ctx, r.ins)
	switch {
	case err == nil:
	case ecode.Equal(err, ecode.NothingFound):
		// 註冊中心不認得我:可能已被剔除,或註冊中心重啟記憶體清空,
		// 重新註冊把自己加回去
		if rerr := r.c.register(ctx, r.ins); rerr != nil {
			r.c.cfg.Logger.Warn("重新註冊失敗,下個心跳週期再試",
				"service", r.ins.Service, "id", r.ins.ID, "error", rerr)
		} else {
			r.c.cfg.Logger.Info("心跳發現未註冊,已重新註冊",
				"service", r.ins.Service, "id", r.ins.ID)
		}
	default:
		r.c.cfg.Logger.Warn("心跳失敗,下個週期再試",
			"service", r.ins.Service, "id", r.ins.ID, "error", err)
	}
}

// Deregister 優雅下線:先停心跳、確認迴圈退出,再向註冊中心註銷。
//
// 順序不能反——若先註銷再停心跳,在途心跳會收到 404,
// 而 404 的語意是「自動重新註冊」,剛下線的節點就被自己加回去了。
func (r *Registration) Deregister(ctx context.Context) error {
	r.stopOnce.Do(func() { close(r.stop) })
	<-r.done
	return r.c.cancel(ctx, r.ins)
}
