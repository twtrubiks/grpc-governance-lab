// Package ecode 提供全站統一的業務錯誤碼(里程碑 M1)。
//
// 職責:
//   - Code 型別與全域註冊表:New() 註冊錯誤碼與訊息,重複註冊即 panic
//     (程式設計錯誤,fail-fast,這是 pkg/ 下唯一允許的 panic)
//   - Cause(err):從任意 error(含 wrap 過的)反解出 Code
//   - 與 google.golang.org/grpc/status 互轉,用 status.Details
//     夾帶 proto,讓 -404 穿過 gRPC 邊界後仍是 -404 而非 codes.Unknown
//
// 驗收標準見 PLAN.md M1:error 經
// ecode → status → 序列化 → status → ecode 往返後 code/message 不變。
//
// 設計參考(只讀思路、不抄代碼,clean-room 聲明見 PLAN.md §7):
// go-kratos v2 errors 套件。
package ecode
