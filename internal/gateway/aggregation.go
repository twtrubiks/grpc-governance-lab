package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	accountv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/account/v1"
	relationv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/relation/v1"
	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

// DegradedFollowerCount 是 relation 服務降級時粉絲數的回退值。
// 用 -1 而非 0:讓前端能區分「真的 0 個粉絲」與「粉絲數暫時查不到」。
const DegradedFollowerCount = -1

// Profile 是聚合後的使用者檔案。
type Profile struct {
	ID            int64  `json:"id"`
	Name          string `json:"name"`
	FollowerCount int64  `json:"follower_count"`
	// Degraded 為 true 表示 relation 服務失敗、粉絲數是回退值。
	Degraded bool `json:"degraded"`
}

// Aggregator 用 errgroup 併發呼叫 account 與 relation 聚合使用者檔案。
type Aggregator struct {
	account  accountv1.AccountServiceClient
	relation relationv1.RelationServiceClient
	timeout  time.Duration
	logger   *slog.Logger
}

// NewAggregator 建立聚合器;timeout <= 0 時用 200ms 預設。
func NewAggregator(account accountv1.AccountServiceClient, relation relationv1.RelationServiceClient, timeout time.Duration, logger *slog.Logger) *Aggregator {
	if timeout <= 0 {
		timeout = 200 * time.Millisecond
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Aggregator{account: account, relation: relation, timeout: timeout, logger: logger}
}

// GetProfile 併發聚合 account(主資料)與 relation(可降級)。
//
// 取捨——兩個服務的失敗語意不同:
//   - account 是主資料,查不到使用者整個請求就沒有意義 → 失敗即回錯誤碼,
//     並透過 errgroup 的 ctx 取消「取消掉還在跑的 relation 呼叫」,不浪費資源
//   - relation 是次要資料,它掛掉不該拖垮整個檔案 → 失敗時降級
//     (粉絲數回 -1、Degraded=true),HTTP 仍回 200
//
// errgroup 的 ctx 在「第一個回傳 error 的 goroutine」後被取消;
// 因為 relation 的 goroutine 永不回傳 error(降級而非報錯),
// 只有 account 失敗會觸發取消,正是我們要的語意。
func (a *Aggregator) GetProfile(ctx context.Context, id int64) (*Profile, error) {
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	var user *accountv1.User
	g.Go(func() error {
		resp, err := a.account.GetUser(ctx, &accountv1.GetUserRequest{Id: id})
		if err != nil {
			return err // account 失敗 → 取消 relation、整個請求失敗
		}
		user = resp.GetUser()
		return nil
	})

	followerCount := int64(DegradedFollowerCount)
	degraded := true
	g.Go(func() error {
		resp, err := a.relation.GetFollowerCount(ctx, &relationv1.GetFollowerCountRequest{UserId: id})
		if err != nil {
			// 降級:吞掉錯誤、保留回退值,不讓它觸發 errgroup 取消
			a.logger.WarnContext(ctx, "relation 降級", "id", id, "error", err)
			return nil
		}
		followerCount = resp.GetCount()
		degraded = false
		return nil
	})

	if err := g.Wait(); err != nil {
		// 只有 account 的錯誤會走到這;原樣回業務碼讓 HTTP 層透傳
		return nil, err
	}
	// account 回了成功卻沒帶 user:上游契約被破壞(不是「查無使用者」,
	// 那會是 -404 走上面的錯誤路徑)。當成伺服器錯誤,別默默回 id=0 的空檔案。
	if user == nil {
		return nil, fmt.Errorf("account 回應成功但缺少 user(id=%d): %w", id, ecode.ServerErr)
	}
	return &Profile{
		ID:            user.GetId(),
		Name:          user.GetName(),
		FollowerCount: followerCount,
		Degraded:      degraded,
	}, nil
}
