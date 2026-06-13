package discovery

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

// Instance 是註冊中心裡的一個服務副本,也是 HTTP API 的 JSON 形狀。
type Instance struct {
	// Service 服務名,例如 "account"。
	Service string `json:"service"`
	// ID 副本唯一識別(同一服務內唯一),慣例上用 addr。
	ID string `json:"id"`
	// Addr 副本的 gRPC 位址,host:port。
	Addr string `json:"addr"`
	// Metadata 附加資訊(版本、機房等),原樣透傳給訂閱者。
	Metadata map[string]string `json:"metadata,omitempty"`
}

// entry 是 Instance 加上心跳簿記,只存在於 Registry 內部。
type entry struct {
	ins       Instance
	lastRenew time.Time
}

// service 是單一服務的副本集合與變更廣播。
type service struct {
	instances map[string]*entry
	// version 單調遞增,任何成員變動(註冊/註銷/剔除)+1;
	// 心跳續約不算變動。長輪詢用它判斷「自上次之後有沒有變化」。
	version int64
	// update 在每次成員變動時被 close 並換新,長輪詢據此被喚醒。
	update chan struct{}
	// waiters 是目前阻塞在這個服務上的長輪詢數。Poll 進出時增減。
	// 用途:背景剔除迴圈只回收「沒有副本、也沒有人在訂閱」的空條目——
	// 避免 client 長輪詢大量不存在的服務名把 services map 撐爆;
	// 有訂閱者(waiters > 0)的空服務不回收,維持「可先訂閱、服務後上線」語意。
	waiters int
}

// bumpLocked 標記一次成員變動並喚醒所有長輪詢;呼叫方須持 Registry.mu 寫鎖。
// 版本用「時間戳為底、保證單調遞增」而非從 1 數起:註冊中心重啟後
// 記憶體清空、計數器歸零,若新舊版本號撞在同一個小整數,
// 訂閱者會以為沒變化而永久阻塞;時間戳讓重啟前後的版本幾乎必然不同。
func (s *service) bumpLocked() {
	v := time.Now().UnixNano()
	if v <= s.version {
		v = s.version + 1
	}
	s.version = v
	close(s.update)
	s.update = make(chan struct{})
}

// snapshotLocked 回傳排序後的副本快照;呼叫方至少持讀鎖。
func (s *service) snapshotLocked() []Instance {
	out := make([]Instance, 0, len(s.instances))
	for _, e := range s.instances {
		out = append(out, e.ins)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Registry 是純記憶體的服務註冊表。
//
// mu 保護 services 及其內部所有欄位;Fetch/Poll 讀路徑持 RLock,
// 註冊/註銷/剔除與長輪詢的懶建服務持寫鎖。鎖內只做 map 操作不做 IO。
type Registry struct {
	cfg Config

	mu       sync.RWMutex
	services map[string]*service

	guard *guard

	stop     chan struct{}
	loopDone chan struct{}
}

// New 建立註冊表並啟動背景的剔除與 Guard 統計迴圈,
// 用畢必須呼叫 Close 結束背景 goroutine。
func New(cfg *Config) *Registry {
	r := &Registry{
		cfg:      cfg.withDefaults(),
		services: make(map[string]*service),
		stop:     make(chan struct{}),
		loopDone: make(chan struct{}),
	}
	r.guard = newGuard(r.cfg.GuardThreshold)
	go r.loop()
	return r
}

// Close 停止背景迴圈並等待其退出;冪等性由呼叫方保證(只呼叫一次)。
func (r *Registry) Close() {
	close(r.stop)
	<-r.loopDone
}

// loop 是唯一的背景 goroutine:定期剔除過期副本、定期結算 Guard 統計窗。
// 退出路徑:Close() 關閉 stop。
func (r *Registry) loop() {
	defer close(r.loopDone)
	evictTicker := time.NewTicker(r.cfg.EvictInterval)
	defer evictTicker.Stop()
	guardTicker := time.NewTicker(r.cfg.GuardWindow)
	defer guardTicker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-evictTicker.C:
			r.evictExpired()
		case <-guardTicker.C:
			r.evaluateGuard()
		}
	}
}

// serviceLocked 取出或懶建服務條目;呼叫方須持寫鎖。
// 懶建讓「訂閱還不存在的服務」成為合法操作(等它上線即收到通知)。
func (r *Registry) serviceLocked(name string) *service {
	s, ok := r.services[name]
	if !ok {
		s = &service{
			instances: make(map[string]*entry),
			update:    make(chan struct{}),
		}
		r.services[name] = s
	}
	return s
}

// Register 註冊(或重新註冊)一個副本,並視為一次心跳。
func (r *Registry) Register(ins Instance) error {
	if ins.Service == "" || ins.ID == "" || ins.Addr == "" {
		return fmt.Errorf("register 缺少必要欄位 service/id/addr: %w", ecode.RequestErr)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.serviceLocked(ins.Service)
	prev, exists := s.instances[ins.ID]
	s.instances[ins.ID] = &entry{ins: ins, lastRenew: time.Now()}
	// 心跳失敗後的重新註冊(內容沒變)不必吵醒訂閱者
	if !exists || prev.ins.Addr != ins.Addr {
		s.bumpLocked()
	}
	r.guard.record()
	return nil
}

// Renew 心跳續約;副本不存在回 ecode.NothingFound,
// SDK 收到後應重新註冊(節點可能已被剔除或註冊中心重啟過)。
func (r *Registry) Renew(serviceName, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.services[serviceName]
	if !ok {
		return fmt.Errorf("renew: 服務 %s 不存在: %w", serviceName, ecode.NothingFound)
	}
	e, ok := s.instances[id]
	if !ok {
		return fmt.Errorf("renew: 副本 %s/%s 不存在: %w", serviceName, id, ecode.NothingFound)
	}
	e.lastRenew = time.Now()
	r.guard.record()
	return nil
}

// Cancel 主動註銷(優雅下線)。明確的下線訊號不受 Guard 自保影響:
// 自保防的是「心跳消失」這種曖昧訊號,不是服務自己說再見。
func (r *Registry) Cancel(serviceName, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.services[serviceName]
	if !ok {
		return fmt.Errorf("cancel: 服務 %s 不存在: %w", serviceName, ecode.NothingFound)
	}
	if _, ok := s.instances[id]; !ok {
		return fmt.Errorf("cancel: 副本 %s/%s 不存在: %w", serviceName, id, ecode.NothingFound)
	}
	delete(s.instances, id)
	s.bumpLocked()
	return nil
}

// Fetch 回傳服務目前的副本快照與版本;服務不存在時 ok 為 false。
func (r *Registry) Fetch(serviceName string) (instances []Instance, version int64, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, exists := r.services[serviceName]
	if !exists {
		return nil, 0, false
	}
	return s.snapshotLocked(), s.version, true
}

// Poll 長輪詢:版本與 sinceVersion 不一致就立刻返回,否則阻塞到
// 下一次成員變動、PollTimeout 或 ctx 取消(後兩者 changed 為 false)。
// 「不一致」而非「更新」:訂閱者帶著重啟前的舊版本號回來時
// (server 版本反而比較小),也要立刻給它當前快照重新對齊。
func (r *Registry) Poll(ctx context.Context, serviceName string, sinceVersion int64) (instances []Instance, version int64, changed bool) {
	timeout := time.NewTimer(r.cfg.PollTimeout)
	defer timeout.Stop()
	for {
		r.mu.Lock()
		s := r.serviceLocked(serviceName)
		if s.version != sinceVersion {
			snap, v := s.snapshotLocked(), s.version
			r.mu.Unlock()
			return snap, v, true
		}
		update := s.update
		// 標記為訂閱中:在我們阻塞期間,剔除迴圈不得把這個(可能為空的)
		// 服務條目當成「沒人要的空殼」回收掉。
		s.waiters++
		r.mu.Unlock()

		select {
		case <-update:
			// 有變動,回到迴圈頭重讀快照
			r.releaseWaiter(serviceName)
		case <-timeout.C:
			r.releaseWaiter(serviceName)
			return nil, sinceVersion, false
		case <-ctx.Done():
			r.releaseWaiter(serviceName)
			return nil, sinceVersion, false
		}
	}
}

// releaseWaiter 在一次長輪詢等待結束後遞減 waiters。
// 服務條目可能已被回收(理論上不會,因為 waiters > 0 時不回收),故防呆檢查。
func (r *Registry) releaseWaiter(serviceName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.services[serviceName]; ok && s.waiters > 0 {
		s.waiters--
	}
}

// evictExpired 剔除心跳過期的副本。兩道門檻:
//   - 一般剔除(漏 EvictFactor 次心跳):受 Guard 自保抑制
//   - 強制剔除(HardEvictAfter):無視自保,殭屍節點不得永久殘留
func (r *Registry) evictExpired() {
	ttl := r.cfg.HeartbeatInterval * time.Duration(r.cfg.EvictFactor)
	protected := r.guard.isProtected()
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()
	for name, s := range r.services {
		changed := false
		for id, e := range s.instances {
			idle := now.Sub(e.lastRenew)
			hardExpired := idle > r.cfg.HardEvictAfter
			expired := idle > ttl && !protected
			if hardExpired || expired {
				delete(s.instances, id)
				changed = true
				r.cfg.Logger.Warn("剔除過期副本",
					"service", name, "id", id,
					"idle", idle.String(), "hard", hardExpired)
			}
		}
		if changed {
			s.bumpLocked()
		}
		// 回收沒副本、也沒人訂閱的空條目:Poll 對任何名字都會懶建條目,
		// 不回收的話,client 長輪詢大量不存在的服務名會讓 services map 無上限成長。
		// 有訂閱者(waiters > 0)時保留,維持「先訂閱、服務後上線」語意。
		if len(s.instances) == 0 && s.waiters == 0 {
			delete(r.services, name)
		}
	}
}

// evaluateGuard 結算一個 Guard 統計窗:
// 期望心跳數 = 目前副本總數 × (統計窗 / 心跳週期)。
func (r *Registry) evaluateGuard() {
	r.mu.RLock()
	count := 0
	for _, s := range r.services {
		count += len(s.instances)
	}
	r.mu.RUnlock()

	perInstance := float64(r.cfg.GuardWindow) / float64(r.cfg.HeartbeatInterval)
	changed, protected := r.guard.evaluate(count, perInstance)
	if changed {
		if protected {
			r.cfg.Logger.Warn("Guard 進入自我保護:心跳大量消失,疑似網路分區,停止剔除",
				"instances", count)
		} else {
			r.cfg.Logger.Info("Guard 退出自我保護,恢復正常剔除")
		}
	}
}
