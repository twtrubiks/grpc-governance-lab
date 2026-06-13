package ecode

import (
	"errors"
	"fmt"
	"testing"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestStatus_RoundTrip 是 M1 的驗收測試(PLAN.md §4 M1):
// error 經過「ecode → status → 網路序列化 → status → ecode」
// 完整往返後,code 與 message 必須一字不差。
func TestStatus_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Code
	}{
		{"業務碼 -404", NothingFound, NothingFound},
		{"被 %w 包裹的業務碼", fmt.Errorf("dao 層: %w", Deadline), Deadline},
		{"未知錯誤兜底為 -500", errors.New("沒人認領的錯"), ServerErr},
		{"未註冊進 grpc code 映射表的業務碼", New(10001, "餘額不足"), Code{code: 10001, message: "餘額不足"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := ToStatus(tt.err)

			// 模擬跨網路:status 的 proto 形式序列化成 bytes 再還原,
			// 證明 details 夾帶的業務碼經得起真實的 wire format
			raw, err := proto.Marshal(st.Proto())
			if err != nil {
				t.Fatalf("序列化 status 失敗: %v", err)
			}
			var pb spb.Status
			if err := proto.Unmarshal(raw, &pb); err != nil {
				t.Fatalf("反序列化 status 失敗: %v", err)
			}

			got := FromStatus(status.FromProto(&pb))
			if got.Code() != tt.want.Code() {
				t.Errorf("往返後 code = %d, want %d", got.Code(), tt.want.Code())
			}
			if got.Message() != tt.want.Message() {
				t.Errorf("往返後 message = %q, want %q", got.Message(), tt.want.Message())
			}
		})
	}
}

func TestFromError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Code
	}{
		{"nil 返回 OK", nil, OK},
		{"錯誤鏈上已帶 Code 直接反解", fmt.Errorf("同進程: %w", AccessDenied), AccessDenied},
		{"不帶 details 的 grpc NotFound 粗映射", status.Error(codes.NotFound, "找不到"), NothingFound},
		{"grpc DeadlineExceeded 粗映射(client 端逾時)", status.Error(codes.DeadlineExceeded, "逾時"), Deadline},
		{"grpc Unavailable 粗映射(連線層故障)", status.Error(codes.Unavailable, "連不上"), ServiceUnavailable},
		{"非 grpc 的普通 error 兜底", errors.New("misc"), ServerErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromError(tt.err); got.Code() != tt.want.Code() {
				t.Errorf("FromError() code = %d, want %d", got.Code(), tt.want.Code())
			}
		})
	}
}

// TestToStatus_GRPCCodeMapping 驗證外層 grpc code 的粗粒度映射:
// 給不認識本框架的客戶端/中間件看的語意要大致正確。
func TestToStatus_GRPCCodeMapping(t *testing.T) {
	tests := []struct {
		name string
		in   Code
		want codes.Code
	}{
		{"-400 → InvalidArgument", RequestErr, codes.InvalidArgument},
		{"-401 → Unauthenticated", Unauthorized, codes.Unauthenticated},
		{"-403 → PermissionDenied", AccessDenied, codes.PermissionDenied},
		{"-404 → NotFound", NothingFound, codes.NotFound},
		{"-498 → Canceled", Canceled, codes.Canceled},
		{"-500 → Internal", ServerErr, codes.Internal},
		{"-503 → Unavailable", ServiceUnavailable, codes.Unavailable},
		{"-504 → DeadlineExceeded", Deadline, codes.DeadlineExceeded},
		{"-509 → ResourceExhausted", LimitExceed, codes.ResourceExhausted},
		{"未映射的業務碼 → Unknown", Code{code: 10002, message: "x"}, codes.Unknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToStatus(tt.in).Code(); got != tt.want {
				t.Errorf("grpc code = %v, want %v", got, tt.want)
			}
		})
	}
}
