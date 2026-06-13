// Package discovery 是迷你註冊中心的核心(里程碑 M3)。
//
// 職責:
//   - Registry:純記憶體的服務註冊表(註冊/心跳/註銷/查詢/長輪詢),
//     心跳逾時剔除(預設漏 3 次,90 秒),另設強制剔除上限(預設 1 小時)
//   - Guard 自我保護:每個統計窗比較實際心跳數與期望值,
//     低於閾值(預設 85%)即判定為網路分區而非服務全滅,
//     停止剔除任何節點,恢復後自動退出(類 Eureka self-preservation)
//   - HTTP API:POST /register、/renew、/cancel;GET /fetch、/poll(長輪詢)
//
// 所有時間參數一律經 Config 注入、不寫死:生產預設 30s/90s,
// 單元測試用毫秒級參數(PLAN.md M3 設計注意)。
//
// cmd/discovery 是組裝這個套件的進程入口;client 端 SDK 在 pkg/registry。
//
// 設計參考(只讀思路、不抄代碼,clean-room 聲明見 PLAN.md §7):
// go-kratos registry 介面、Netflix Eureka self-preservation 文件。
package discovery
