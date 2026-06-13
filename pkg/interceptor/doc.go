// Package interceptor 提供 gRPC server/client 攔截器鏈(里程碑 M2)。
//
// server 端:
//   - recovery:panic 轉 ecode.ServerErr,記錄堆疊,連線不中斷
//   - access log:每請求一行結構化日誌(caller、method、耗時、code)
//   - ecode → grpc status 轉換
//
// client 端:
//   - grpc status → ecode 還原
//   - 超時遞減:deadline 沿呼叫鏈傳遞並取最小值,避免下游比上游活得久
//   - metadata 白名單透傳(user-id 等)
//   - per-method 設定覆蓋:各方法可獨立指定超時
//
// 設計注意(PLAN.md M2):
//   - client 鏈要留好熔斷器插槽(backlog 做 SRE breaker 時不動架構)
//   - 刻意不做自動 retry:重試牽涉冪等性,屬業務層職責,
//     決策理由詳見 docs/02-interceptor.md
package interceptor
