// Package wrr 實作動態加權輪詢負載均衡器(里程碑 M5,本專案最難的部分)。
//
// 職責:
//   - 加權輪詢 Picker(Nginx smooth WRR:cwt += ewt,選最大者再減總權重)
//   - 每節點滑動視窗(10 bucket x 300ms)統計成功率與延遲
//   - server 透過 gRPC trailer(key "cpu_usage")回報 CPU,
//     client 端定時重算權重:
//     score = sqrt(client成功率 x server成功率^2 x 1e9 / (延遲 x CPU))
//   - 觀測面:per-node picks/成功/延遲計數,供 gateway 的
//     /debug/backends 端點輸出(demo 場景 3、5、6 全靠它)
//
// 併發規範(CODE_QUALITY.md §4):Pick() 被每個請求併發呼叫,
// 權重表多讀少寫——atomic + RWMutex,鎖內不做計算;
// 驗收含 8 goroutine 併發 benchmark > 100 萬 ops/s,-race 必過。
//
// NOTE(取捨):demo 容器內 CPU 趨近 0,公式除以 CPU 會除零或爆炸——
// CPU 與成功率都要 clamp 下限(成功率下限 0.1,避免冷啟動節點被歸零),
// demo 場景 3 實際由「延遲」維度主導,詳見 docs/05-balancer.md。
package wrr
