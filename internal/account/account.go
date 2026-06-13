package account

import (
	"context"
	"fmt"
	"sync"

	accountv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/account/v1"
	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

// User 是記憶體 dao 裡的使用者。
type User struct {
	ID   int64
	Name string
}

// Service 是 account 業務,實作 accountv1.AccountServiceServer。
//
// 分層刻意極簡(handler/service/dao 合在一個小型 struct):業務不是重點,
// 它存在是為了證明 pkg/ 的治理元件真的能用。mu 保護 users。
type Service struct {
	accountv1.UnimplementedAccountServiceServer

	mu    sync.RWMutex
	users map[int64]User
}

// NewService 建立 account 服務並塞入一批假資料。
func NewService() *Service {
	s := &Service{users: make(map[int64]User)}
	// demo 假資料:id 1~50 都查得到,其餘回 -404
	for i := int64(1); i <= 50; i++ {
		s.users[i] = User{ID: i, Name: fmt.Sprintf("user-%d", i)}
	}
	return s
}

// GetUser 依 ID 查使用者;不存在時回 ecode.NothingFound(-404),
// 經 server 攔截器轉成帶 details 的 grpc status 跨網路透傳。
func (s *Service) GetUser(_ context.Context, req *accountv1.GetUserRequest) (*accountv1.GetUserResponse, error) {
	if req.GetId() <= 0 {
		return nil, fmt.Errorf("id 必須為正: %w", ecode.RequestErr)
	}
	s.mu.RLock()
	u, ok := s.users[req.GetId()]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("使用者 %d 不存在: %w", req.GetId(), ecode.NothingFound)
	}
	return &accountv1.GetUserResponse{
		User: &accountv1.User{Id: u.ID, Name: u.Name},
	}, nil
}
