// Package gateway 是 HTTP 網關的聚合邏輯(里程碑 M6)。
//
// 職責:
//   - GET /user/profile?id=:用 errgroup 併發呼叫 account + relation,
//     200ms 超時,任一失敗即取消其餘;relation 失敗時降級
//     (粉絲數回 -1,不報錯)——這是 BFF/聚合層的日常模式
//   - GET /debug/backends:輸出各後端節點即時 QPS、權重、成功率
//     (資料來自 pkg/balancer/wrr 的觀測面,demo 場景 3、5、6 靠它)
//   - 錯誤碼透傳:下游 ecode 原樣轉成 {"code": ..., "message": ...}
package gateway
