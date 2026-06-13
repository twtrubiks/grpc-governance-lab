# 02 — pkg/interceptor 攔截器鏈設計

> 對應里程碑 M2。引用外部實作只帶「來源 + 文字描述」(PLAN.md §7);
> 設計思路對照 go-kratos 的 middleware 與 transport/grpc。

## 1. 要解決的問題(不做會怎樣)

跨服務呼叫有一堆「每個服務都要做、但跟業務無關」的事:

- **panic 隔離**:handler 一個越界,整條 TCP 連線斷掉,client 看到的是
  莫名其妙的 `Unavailable`,還可能雪崩重試。
- **超時管理**:gateway 給整個請求 500ms 預算,account 不知情、自己又等
  了 2s——上游早就放棄了,下游還在燒資源做白工。
- **身分透傳**:user-id 在 gateway 解出來,內層服務各自重新解一遍,
  或者乾脆拿不到。
- **觀測**:沒有統一 access log,排障只能在各服務各自的 print 裡撈。

這些事寫進業務 handler 是災難(每個方法重複一遍),正確位置是
gRPC interceptor——對業務透明、全站行為一致。

## 2. 業界常見做法

- keepalive 五參數的生產預設:閒置 60s、最長存活 2h、寬限 20s、
  60s ping、20s 逾時。本專案的 `DefaultKeepalive` 採同一組值。
- server 端常見做法是用一個大攔截器:從 ctx 取上游 deadline、
  與本服務設定的 Timeout **取最小值**後重新 `WithTimeout`
  (常還預留一點緩衝),接著從 metadata 萃取 caller / color / remote_ip。
  注意:成熟框架**在 server 端也做超時遞減**。
- client 設定同時是全域與 per-method(`Method map[string]*ClientConfig`),
  且內嵌 `Breaker` 設定。
- 熔斷器按 method 隔離:invoke 前 `Allow()`,defer 裡按結果回報;
  超時遞減 `Timeout.Shrink(ctx)`;白名單 metadata(color/ip/port)
  塞進 outgoing。
- recovery:server/client 兩端都掛,轉 `ecode.ServerErr` 並印堆疊。

## 3. 我的設計與取捨

| 決策 | 成熟框架 | 本專案 | 理由 |
|---|---|---|---|
| 鏈的組織 | 一個大 `handle()` 做完所有事 | 拆成 Recovery / AccessLog / ServerEcode 三個可獨立使用的攔截器,`ChainUnaryServer` 給建議組合 | 單一職責,測試與講解都容易;gRPC 官方 `ChainUnaryInterceptor` 已解決鏈接問題 |
| 超時遞減位置 | client、server 兩端都做 | 只在 client 端做 | gRPC 本身會把 deadline 編進 `grpc-timeout` header 傳給下游,server 端 ctx 天然帶上游 deadline;成熟框架 server 端再 shrink 是為了「本服務設定的 Timeout 上限」,demo 不需要這層;若未來要加,位置在 Recovery 內層加一個攔截器即可 |
| 20ms 緩衝 | 遞減時預留 20ms 給網路傳輸 | 不做 | 教學專案,少一個魔法數字;文檔記下這個巧思 |
| 熔斷 | 按 method 隔離,嵌在 client 鏈 | 不做,但 `MethodConfig` 預留插槽 | PLAN.md backlog;接入時在 invoke 前後掛 Allow/Done,不動架構 |
| 自動 retry | 沒有 | 也沒有(刻意) | 見下節 |
| 限流(rate limiting) | server 端入站攔截器 + 自適應演算法 | 不做 | 見「本版未做的治理項目」一節 |
| 傳輸安全 / 認證 | TLS/mTLS + per-RPC 憑證 | 不做,僅 insecure | 同上;demo 全在本機可信網路內 |
| metrics / 可觀測性 | Prometheus 指標 + 攔截器埋點 | 只有 `/debug/backends` 與 access log | 同上;指標化等同追蹤,延後到標準工具 |
| trace/color 染色 | 有 | 無 | 追蹤屬 OpenTelemetry 範疇,染色需要多環境場景(docs/04 會講原理) |

### 為什麼不做自動 retry(設計決策,面試高頻)

成熟框架的 client 鏈裡**通常沒有 retry,這不是偷懶而是立場**:

1. **冪等性是業務知識**:`GetUser` 重試安全,`Transfer` 重試就是
   重複轉帳。攔截器層不知道哪個方法冪等,做了就是埋雷。
2. **重試風暴**:鏈路上每層都自作主張重試 3 次,故障時放大倍數是
   指數級(3 層就是 27 倍流量),正是雪崩的標準成因。
3. 真要做,正確位置是業務層用顯式的重試包裝(知道冪等性),或
   gRPC 官方的 service config retry policy(宣告式、有 hedging 與
   退避),而不是自己在攔截器裡寫 for 迴圈。

### 本版未做的治理項目:限流 / 傳輸安全 / metrics(範圍外宣告)

熔斷有「`MethodConfig` 預留插槽 + 文件」的待遇,以下三項則明確劃出範圍外。
比照熔斷,這裡交代「會擺在哪、為什麼此版省略、未來接點」,免得讀者誤以為
忘了做:

- **限流(rate limiting)**:位置在 **server 端入站攔截器**(`ServerEcode`
  外層、最先擋下過量請求),超出閾值直接回 `ecode.LimitExceed (-509)`。
  此版省略是因為要演示「限流真的有意義」需要壓測到過載,demo 場景沒這個劇本。
  **注意:`ecode.LimitExceed` 其實已經接好** ecode↔gRPC 的雙向翻譯表
  (`pkg/ecode/status.go`),只是目前沒有限流器去觸發它——之後補一個
  令牌桶或自適應限流器(對照 go-kratos `aegis/ratelimit` 的 BBR)即可發出。
- **傳輸安全 / 認證(TLS、mTLS、per-RPC 憑證)**:位置在**連線建立層**
  (`grpc.Creds` / `grpc.WithTransportCredentials`)與一個 **server 端認證
  攔截器**(驗 token、把身分塞進 ctx)。此版全用 `insecure`,因為 demo 跑在
  本機可信的 docker network 內;要上生產時把 insecure 換成
  `credentials.NewTLS(...)`、認證攔截器接在 `Recovery` 內層即可。
  (本專案已做的 metadata 白名單透傳是「身分傳遞」,不是「身分驗證」,兩者
  不要混淆。)
- **metrics / 可觀測性**:位置在一個 **server/client 雙端攔截器**統計 QPS/
  延遲/錯誤,輸出走 Prometheus `/metrics` 端點。此版只有 balancer 的
  `/debug/backends` 與 access log;指標化在性質上等同分散式追蹤——屬於標準
  工具(OpenTelemetry / Prometheus)的範疇,刻意不自寫,避免重造輪子。

這三項與熔斷、retry、染色一樣,都是「知道在哪、為何不做」的刻意取捨,
而非疏漏。

### 鏈的順序為什麼是 ServerEcode → AccessLog → Recovery

錯誤從 handler 往外冒的路徑是 Recovery → AccessLog → ServerEcode:

- Recovery 最內層:panic 一出 handler 就被轉成 `ecode.ServerErr`,
  外面兩層看到的是普通錯誤,**AccessLog 才記得到這筆請求**。
- ServerEcode 最外層:中間所有攔截器處理的都是好比較的 ecode,
  離開 server 前的最後一刻才轉成 grpc status。

## 4. 踩過的坑

- **`AppendToOutgoingContext` vs 整包覆蓋**:透傳 metadata 時若用
  `NewOutgoingContext` 整包覆蓋,會把使用者已經自己塞進 outgoing 的
  key 洗掉;用 Append 語意才是「接力,不破壞」。
- **超時遞減的邊界**:設定超時 ≤ 0(未設定)時也要尊重上游 deadline,
  否則「上游有預算、本層設定空白」會變成完全不限時。
  實作上「timeout <= 0 或上游更短就用上游」一條式子蓋掉兩種情況。
- **named return 才能在 defer 裡改錯誤**:Recovery 的 recover 在
  defer 中發生,必須用 named return value 把 err 換成 ServerErr,
  不然 panic 恢復後回傳的還是 nil。

## 5. 驗證方式

全部走 bufconn 上的**真實 gRPC wire format**(不是進程內函式呼叫),
測試見 `pkg/interceptor/interceptor_test.go`,覆蓋率 100%:

| 驗收(PLAN.md M2) | 測試 |
|---|---|
| panic 時 client 收 ecode.ServerErr 而非連線中斷 | `TestRecovery_PanicToServerErr`(並驗證下一個請求照常成功) |
| 上游 500ms deadline,下游剩餘 < 500ms | `TestUnaryClient_TimeoutDecrement`(client 全域 2s 故意比上游寬) |
| per-method 超時覆蓋全域 | `TestUnaryClient_PerMethodOverride`(方法 100ms 蓋掉全域 5s) |
| 業務碼跨網路不失真 | `TestEcode_AcrossNetwork`(wrap 過的 -404,code 與 message 都驗) |
| metadata 白名單 | `TestUnaryClient_MetadataWhitelist`(名單內到達、名單外不洩漏) |
| access log 結構化欄位 | `TestServerAccessLog_Fields` |

跑法:`go test -race ./pkg/interceptor/`。
