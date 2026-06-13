package relation

import (
	"context"
	"fmt"
	"sync"

	relationv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/relation/v1"
	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

// Service 是 relation 業務,實作 relationv1.RelationServiceServer。
// mu 保護 followers。
type Service struct {
	relationv1.UnimplementedRelationServiceServer

	mu        sync.RWMutex
	followers map[int64]int64
}

// NewService 建立 relation 服務並塞入假資料(粉絲數 = id × 100)。
func NewService() *Service {
	s := &Service{followers: make(map[int64]int64)}
	for i := int64(1); i <= 50; i++ {
		s.followers[i] = i * 100
	}
	return s
}

// GetFollowerCount 查粉絲數;不存在的使用者回 0(而非錯誤)——
// 「沒有粉絲」是合法狀態,不是查無資料。
func (s *Service) GetFollowerCount(_ context.Context, req *relationv1.GetFollowerCountRequest) (*relationv1.GetFollowerCountResponse, error) {
	if req.GetUserId() <= 0 {
		return nil, fmt.Errorf("user_id 必須為正: %w", ecode.RequestErr)
	}
	s.mu.RLock()
	count := s.followers[req.GetUserId()]
	s.mu.RUnlock()
	return &relationv1.GetFollowerCountResponse{Count: count}, nil
}
