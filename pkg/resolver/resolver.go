package resolver

import (
	"context"
	"log/slog"
	"sync"

	"google.golang.org/grpc/resolver"

	"github.com/twtrubiks/grpc-governance-lab/pkg/registry"
)

// Scheme 是本 resolver 的 target scheme:discovery:///<service>。
const Scheme = "discovery"

// Builder 實作 grpc resolver.Builder,把 discovery:///<service> 解析成
// 註冊中心上該服務的副本地址,並持續訂閱變化。
//
// 用 grpc.WithResolvers(builder) 注入單一連線,不汙染全域註冊表
// (全域 resolver.Register 會讓測試之間互相干擾)。
type Builder struct {
	client *registry.Client
	logger *slog.Logger
}

// NewBuilder 建立 resolver builder;logger 為 nil 時用 slog.Default()。
func NewBuilder(client *registry.Client, logger *slog.Logger) *Builder {
	if logger == nil {
		logger = slog.Default()
	}
	return &Builder{client: client, logger: logger}
}

// Scheme 實作 resolver.Builder。
func (b *Builder) Scheme() string { return Scheme }

// Build 實作 resolver.Builder:對 target 的服務啟動訂閱,
// 地址變化時推給 gRPC 連線。
func (b *Builder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	service := target.Endpoint()
	ctx, cancel := context.WithCancel(context.Background())
	r := &discoveryResolver{
		service: service,
		cc:      cc,
		cancel:  cancel,
		logger:  b.logger,
		done:    make(chan struct{}),
	}
	go r.watch(b.client.Watch(ctx, service))
	return r, nil
}

// discoveryResolver 把 registry 訂閱橋接到 grpc 的地址更新。
type discoveryResolver struct {
	service string
	cc      resolver.ClientConn
	cancel  context.CancelFunc
	logger  *slog.Logger
	done    chan struct{}

	// mu 序列化所有 UpdateState 呼叫(watch goroutine 與 gRPC 觸發的
	// ResolveNow 可能併發),並保護 last。
	mu sync.Mutex
	// last 是最後一次嘗試推給 gRPC 的地址。UpdateState 偶發失敗(通常是
	// 連線正在重整)時,gRPC 隨後會呼叫 ResolveNow 要求重解析,屆時用它
	// 重推一次,避免暫時性失敗讓本次更新被「永久丟棄」到下次成員變動。
	last []resolver.Address
}

// watch 消費訂閱快照並推給 gRPC。退出路徑:Close() 取消 ctx 後
// Watcher 會關閉通道,range 結束。
//
// 快取降級(demo 場景 6 的 client 端機制):
//   - 註冊中心不可用:SDK 的 Watcher 保持沉默,這裡收不到任何東西,
//     gRPC 繼續用最後一次的地址——控制面掛掉不影響資料面
//   - 空快照一律忽略:可能是註冊中心重啟後副本還沒重新報到,
//     把空列表推給 gRPC 等於把整個服務打掛;保留舊地址,
//     真死掉的副本由連線層(TCP 斷線)兜底
func (r *discoveryResolver) watch(w *registry.Watcher) {
	defer close(r.done)
	for snapshot := range w.C {
		if len(snapshot) == 0 {
			// NOTE(取捨):代價是「服務真的縮容到 0」時舊地址會殘留,
			// 但呼叫會在連線層立刻失敗,語意上與空列表相同
			r.logger.Warn("忽略空的副本快照,保留現有地址", "service", r.service)
			continue
		}
		addrs := make([]resolver.Address, 0, len(snapshot))
		for _, ins := range snapshot {
			addrs = append(addrs, resolver.Address{Addr: ins.Addr})
		}
		r.pushState(addrs)
	}
}

// pushState 把地址推給 gRPC,並記下作為 ResolveNow 重推的依據。
func (r *discoveryResolver) pushState(addrs []resolver.Address) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.last = addrs
	if err := r.cc.UpdateState(resolver.State{Addresses: addrs}); err != nil {
		// gRPC 拒絕本次更新(通常是連線正在重整),保留在 last,
		// 等 gRPC 下次 ResolveNow 時重推,不必傻等下次成員變動
		r.logger.Warn("UpdateState 失敗,將於 ResolveNow 時以最後地址重推",
			"service", r.service, "error", err)
	}
}

// ResolveNow 實作 resolver.Resolver。長輪詢訂閱常駐、變化即推,沒有「主動
// 重新查詢」的動作;但 gRPC 在連線出狀況時會呼叫這裡,我們順勢用最後一次
// 的地址重推一次——補上 pushState 偶發失敗時被丟棄的更新。
func (r *discoveryResolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.last) == 0 {
		return
	}
	if err := r.cc.UpdateState(resolver.State{Addresses: r.last}); err != nil {
		r.logger.Warn("ResolveNow 重推地址失敗", "service", r.service, "error", err)
	}
}

// Close 實作 resolver.Resolver:結束訂閱並等 watch goroutine 退出。
func (r *discoveryResolver) Close() {
	r.cancel()
	<-r.done
}
