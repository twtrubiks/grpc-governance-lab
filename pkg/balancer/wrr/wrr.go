package wrr

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Name 是本 balancer 在 gRPC service config 註冊的名稱。
// 使用:grpc.WithDefaultServiceConfig(wrr.ServiceConfig)。
const Name = "governance_wrr"

// CPUTrailerKey 是 server 經 gRPC trailer 回報 CPU 使用率(千分比 0~1000)
// 的 metadata key;client 端 balancer 讀它參與權重重算。
const CPUTrailerKey = "cpu_usage"

// SuccessTrailerKey 是 server 經 trailer 回報「自評成功率」(千分比 0~1000)
// 的 metadata key。它與 client 端統計的成功率互補:client 看到的是
// 「這條連線」的成敗,server 自評的是「它整體」的健康度(可納入下游依賴、
// 佇列深度等 client 看不到的訊號)。server 不回報時 client 端假設 1.0。
const SuccessTrailerKey = "success_ratio"

// ServiceConfig 是啟用本 balancer 的 gRPC service config JSON,
// 傳給 grpc.WithDefaultServiceConfig。
const ServiceConfig = `{"loadBalancingConfig":[{"` + Name + `":{}}]}`

func init() {
	// 全域註冊;per-ClientConn 狀態(統計、recalc goroutine)在 Build 建立,
	// 互不干擾。
	balancer.Register(&builder{})
}

// RegisterWithConfig 用自訂參數重新註冊本 balancer(覆蓋 init 的預設)。
// 必須在建立任何使用本 balancer 的 ClientConn「之前」呼叫;
// 部署時可藉此調整滑動視窗大小與重算週期。
func RegisterWithConfig(cfg Config) {
	balancer.Register(&builder{cfg: cfg})
}

// builder 實作 balancer.Builder,每條 ClientConn 給一個獨立的 core。
type builder struct {
	cfg Config
}

// Name 實作 balancer.Builder。
func (b *builder) Name() string { return Name }

// Build 為一條 ClientConn 建立 balancer:
// 起一個 core(節點統計 + 權重重算)與背景 recalc goroutine,
// SubConn 生命週期交給 gRPC 官方的 base balancer 管理,
// pick 邏輯委派給 core。
func (b *builder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	co := newCore(b.cfg)
	co.service = opts.Target.Endpoint()

	ctx, cancel := context.WithCancel(context.Background())
	go co.recalcLoop(ctx)
	registerCore(co)

	inner := base.NewBalancerBuilder(Name, &pickerBuilder{core: co}, base.Config{HealthCheck: true}).
		Build(cc, opts)
	return &wrapped{Balancer: inner, core: co, cancel: cancel}
}

// wrapped 在 base balancer 外層補上 core 的生命週期收尾。
type wrapped struct {
	balancer.Balancer
	core   *core
	cancel context.CancelFunc
	once   sync.Once
}

// Close 停止 recalc goroutine、從 Stats 登記表移除,再關閉內層 balancer。
func (w *wrapped) Close() {
	w.once.Do(func() {
		w.cancel()
		unregisterCore(w.core)
	})
	w.Balancer.Close()
}

// pickerBuilder 在每次 ready SubConn 集合變化時,把集合對齊到 core
// 並產生一個委派 core 的 picker。
type pickerBuilder struct {
	core *core
}

// Build 實作 base.PickerBuilder。
func (pb *pickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	addrs := make(map[string]int64, len(info.ReadySCs))
	scByAddr := make(map[string]balancer.SubConn, len(info.ReadySCs))
	for sc, scInfo := range info.ReadySCs {
		addr := scInfo.Address.Addr
		// 只把 READY 的副本納入,死掉的副本(TRANSIENT_FAILURE)
		// 不在 ReadySCs 裡——故障副本自然不會被 pick(場景 2 轉移)
		addrs[addr] = DefaultWeight
		scByAddr[addr] = sc
	}
	pb.core.setAddrs(addrs)
	return &grpcPicker{core: pb.core, scByAddr: scByAddr}
}

// grpcPicker 把 core 選出的 node 對映到 gRPC SubConn,並在 RPC 完成時
// 經 Done 回呼餵回統計(延遲、成敗、trailer CPU)。
type grpcPicker struct {
	core     *core
	scByAddr map[string]balancer.SubConn
}

// Pick 實作 balancer.Picker。
func (p *grpcPicker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	n := p.core.pick()
	if n == nil {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	sc, ok := p.scByAddr[n.addr]
	if !ok {
		// 選中的 node 不在當前 ready 集合(集合剛變動的瞬間),
		// 回 ErrNoSubConnAvailable 讓 gRPC 重試 pick
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	start := time.Now()
	done := func(di balancer.DoneInfo) {
		n.record(time.Since(start).Microseconds(), isNodeFailure(di.Err))
		if milli, ok := milliFromTrailer(di.Trailer, CPUTrailerKey); ok {
			n.reportCPU(milli)
		}
		if milli, ok := milliFromTrailer(di.Trailer, SuccessTrailerKey); ok {
			n.reportServerSuccess(float64(milli) / 1000)
		}
	}
	return balancer.PickResult{SubConn: sc, Done: done}, nil
}

// isNodeFailure 判斷一次 RPC 結果是否代表「節點不健康」。
//
// NOTE(取捨):只有傳輸層/過載訊號(連不上、逾時、資源耗盡)算節點故障;
// 業務錯誤(-404 NotFound、-400 等)代表節點正常工作、只是這筆請求有業務
// 問題,不該因此降低它的權重——否則「查無資料」這種正常結果會誤殺健康節點。
func isNodeFailure(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// milliFromTrailer 從 trailer 解出指定 key 的千分比整數值。
func milliFromTrailer(md metadata.MD, key string) (int64, bool) {
	if md == nil {
		return 0, false
	}
	vals := md.Get(key)
	if len(vals) == 0 {
		return 0, false
	}
	milli, err := strconv.ParseInt(vals[0], 10, 64)
	if err != nil {
		return 0, false
	}
	return milli, true
}

// 所有活著的 core 的登記表,供 Stats() 聚合輸出 /debug/backends。
var (
	coresMu sync.Mutex
	cores   = make(map[*core]struct{})
)

func registerCore(c *core) {
	coresMu.Lock()
	cores[c] = struct{}{}
	coresMu.Unlock()
}

func unregisterCore(c *core) {
	coresMu.Lock()
	delete(cores, c)
	coresMu.Unlock()
}

// Stats 回傳目前所有 ClientConn 上各後端節點的即時快照,
// 供 gateway 的 /debug/backends 端點輸出(demo 場景 3、5、6 的觀測面)。
// 結果按 (service, addr) 排序,輸出穩定。
func Stats() []Stat {
	coresMu.Lock()
	cs := make([]*core, 0, len(cores))
	for c := range cores {
		cs = append(cs, c)
	}
	coresMu.Unlock()

	var out []Stat
	for _, c := range cs {
		out = append(out, c.snapshot()...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Service != out[j].Service {
			return out[i].Service < out[j].Service
		}
		return out[i].Addr < out[j].Addr
	})
	return out
}
