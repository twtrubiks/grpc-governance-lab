// Package resolver 實作 gRPC 的服務發現解析器(里程碑 M4)。
//
// 職責:
//   - 實作 google.golang.org/grpc/resolver 的 Builder/Resolver 介面,
//     target 格式 discovery:///account
//   - 背景訂閱 pkg/registry,地址變化時 UpdateState 通知 gRPC 重建連線
//   - 快取降級:discovery 不可用(poll 失敗)時,保留最後一次成功的
//     地址列表繼續服務,背景指數退避重連——註冊中心是控制面,
//     掛掉不得影響資料面(demo 場景 6 的核心)
//
// NOTE(取捨):業界還有 subset 一致性雜湊子集選取與 color/zone 染色路由,
// 本專案不實作,原理與適用規模寫在 docs/04-resolver.md。
package resolver
