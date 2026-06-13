package relation

import (
	"context"
	"testing"

	relationv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/relation/v1"
	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

func TestService_GetFollowerCount(t *testing.T) {
	s := NewService()
	tests := []struct {
		name      string
		userID    int64
		wantCount int64
		wantCode  ecode.Code
	}{
		{"存在的使用者", 1, 100, ecode.OK},
		{"另一個使用者", 5, 500, ecode.OK},
		{"不存在的使用者回 0 而非錯誤", 9999, 0, ecode.OK},
		{"user_id 為 0 回 -400", 0, 0, ecode.RequestErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := s.GetFollowerCount(context.Background(), &relationv1.GetFollowerCountRequest{UserId: tt.userID})
			if !ecode.Equal(err, tt.wantCode) {
				t.Fatalf("錯誤碼 = %v, want %v", ecode.Cause(err), tt.wantCode)
			}
			if tt.wantCode == ecode.OK && resp.GetCount() != tt.wantCount {
				t.Errorf("count = %d, want %d", resp.GetCount(), tt.wantCount)
			}
		})
	}
}
