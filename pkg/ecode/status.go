package ecode

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ecodev1 "github.com/twtrubiks/grpc-governance-lab/api/proto/ecode/v1"
)

// ToStatus 把 error 轉成可跨 gRPC 邊界的 *status.Status,server 端攔截器使用。
//
// 業務碼以 ecodev1.Error 夾進 status details 原樣傳遞;外層的 grpc code
// 只是給不認識本框架的客戶端(或中間件)看的粗粒度映射,
// client 端還原時一律以 details 裡的業務碼為準。
func ToStatus(err error) *status.Status {
	c := Cause(err)
	st := status.New(toGRPCCode(c), c.Message())
	detail := &ecodev1.Error{Code: int32(c.Code()), Message: c.Message()}
	stWithDetail, attachErr := st.WithDetails(detail)
	if attachErr != nil {
		// WithDetails 只在 grpc code 為 OK 時失敗(OK 不允許帶錯誤詳情)。
		// 此時退回不帶 details 的 status,client 端會走 grpc code 粗映射。
		return st
	}
	return stWithDetail
}

// FromError 把 gRPC client 收到的 error 還原成業務錯誤碼,client 端攔截器使用:
//   - nil → OK
//   - 錯誤鏈上已帶 Code(同進程呼叫或上層已轉換)→ 直接反解
//   - gRPC status → FromStatus(優先讀 details,否則粗映射)
//   - 其他 → ServerErr
func FromError(err error) Code {
	if err == nil {
		return OK
	}
	var c Code
	if errors.As(err, &c) {
		return c
	}
	st, ok := status.FromError(err)
	if !ok {
		return ServerErr
	}
	return FromStatus(st)
}

// FromStatus 從 *status.Status 還原業務錯誤碼。
// details 裡找得到 ecodev1.Error 就用它(權威來源,code/message 跨網路不失真);
// 找不到表示對方不是本框架的服務,按 grpc code 粗粒度映射。
func FromStatus(st *status.Status) Code {
	for _, d := range st.Details() {
		if e, ok := d.(*ecodev1.Error); ok {
			// 不查本地註冊表:對方服務的業務碼本地未必註冊過,
			// message 隨網路攜帶,直接重建值即可。
			return Code{code: int(e.Code), message: e.Message}
		}
	}
	return fromGRPCCode(st.Code(), st.Message())
}

// toGRPCCode 把業務碼映射成 grpc code。對應不上的業務碼一律 Unknown,
// 反正權威資訊在 details,這層只求語意大致正確。
func toGRPCCode(c Code) codes.Code {
	switch c.code {
	case OK.code:
		return codes.OK
	case RequestErr.code:
		return codes.InvalidArgument
	case Unauthorized.code:
		return codes.Unauthenticated
	case AccessDenied.code:
		return codes.PermissionDenied
	case NothingFound.code:
		return codes.NotFound
	case Canceled.code:
		return codes.Canceled
	case ServerErr.code:
		return codes.Internal
	case ServiceUnavailable.code:
		return codes.Unavailable
	case Deadline.code:
		return codes.DeadlineExceeded
	case LimitExceed.code:
		return codes.ResourceExhausted
	default:
		return codes.Unknown
	}
}

// fromGRPCCode 是 toGRPCCode 的反向兜底:只有在對方不帶 ecode details
// (非本框架服務、或 grpc 框架自己產生的錯誤,如 client 端 deadline)時才會用到。
func fromGRPCCode(code codes.Code, _ string) Code {
	switch code {
	case codes.OK:
		return OK
	case codes.InvalidArgument:
		return RequestErr
	case codes.Unauthenticated:
		return Unauthorized
	case codes.PermissionDenied:
		return AccessDenied
	case codes.NotFound:
		return NothingFound
	case codes.Canceled:
		return Canceled
	case codes.Unavailable:
		return ServiceUnavailable
	case codes.DeadlineExceeded:
		return Deadline
	case codes.ResourceExhausted:
		return LimitExceed
	default:
		return ServerErr
	}
}
