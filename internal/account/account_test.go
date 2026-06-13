package account

import (
	"context"
	"testing"

	accountv1 "github.com/twtrubiks/grpc-governance-lab/api/proto/account/v1"
	"github.com/twtrubiks/grpc-governance-lab/pkg/ecode"
)

func TestService_GetUser(t *testing.T) {
	s := NewService()
	tests := []struct {
		name     string
		id       int64
		wantName string
		wantCode ecode.Code
	}{
		{"存在的使用者", 1, "user-1", ecode.OK},
		{"另一個存在的使用者", 50, "user-50", ecode.OK},
		{"不存在的使用者回 -404", 9999, "", ecode.NothingFound},
		{"id 為 0 回 -400", 0, "", ecode.RequestErr},
		{"id 為負回 -400", -1, "", ecode.RequestErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := s.GetUser(context.Background(), &accountv1.GetUserRequest{Id: tt.id})
			if !ecode.Equal(err, tt.wantCode) {
				t.Fatalf("錯誤碼 = %v, want %v", ecode.Cause(err), tt.wantCode)
			}
			if tt.wantCode == ecode.OK && resp.GetUser().GetName() != tt.wantName {
				t.Errorf("name = %q, want %q", resp.GetUser().GetName(), tt.wantName)
			}
		})
	}
}
