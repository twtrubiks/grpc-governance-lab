// Package registry 是註冊中心的 client SDK(里程碑 M3)。
//
// 職責(對手是 cmd/discovery 那個 server):
//   - Register:服務啟動時註冊自己(appid、addr、metadata)
//   - 背景心跳 goroutine:預設每 30 秒 renew 一次,失敗自動重新註冊
//   - 優雅退出:收到關閉訊號時「先停心跳、再 cancel 註冊」,
//     讓註冊中心立刻(而非 90 秒後)摘掉本節點;順序不能反,
//     否則在途心跳的 404 會觸發自動重新註冊,把節點加回去
//   - 訂閱:長輪詢 poll,地址變化時通知訂閱者(給 pkg/resolver 用)
//
// 設計注意(PLAN.md M3):所有時間參數(心跳週期、poll 超時)
// 一律經 config 注入,不寫死——測試用毫秒級,30s 只是生產預設值。
//
// 併發規範:所有 goroutine 必須有明確退出路徑(ctx cancel),
// 見 docs/CODE_QUALITY.md §4。
package registry
