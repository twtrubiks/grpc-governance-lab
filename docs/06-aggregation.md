# 06 — gateway 併發聚合與 demo 整合設計

> 對應里程碑 M6。引用外部實作只帶「來源 + 文字描述」(PLAN.md §7)。

## 1. 要解決的問題(不做會怎樣)

一個「使用者檔案」頁要同時拿 account(基本資料)與 relation(粉絲數)。
天真做法是序列呼叫:

```
user := account.GetUser(id)        // 50ms
count := relation.GetFollowerCount(id) // 50ms
// 總延遲 100ms,且 relation 掛掉整頁就壞了
```

兩個問題:
1. **延遲疊加**:兩個本可並行的呼叫被串成 100ms。
2. **可用性耦合**:relation 是次要資料,它掛掉不該讓整個檔案 503——
   粉絲數暫時顯示不出來可以接受,使用者名字查不到才是真的壞。

要的是:**併發呼叫**(延遲取 max 而非 sum)+ **差異化失敗處理**
(主資料失敗即失敗、次要資料失敗則降級)。

## 2. 業界常見做法

- BFF/「接口層」的典型職責就是把多個後端服務的結果聚合成一個前端要的
  形狀,常大量使用 errgroup 之類的併發原語。
- `errgroup.WithContext` 衍生一個可取消的 group;**第一個回傳 non-nil
  error 的 goroutine 會 cancel 整個 group 的 ctx**,其餘 goroutine 收到
  取消訊號即可提早收手。官方 `golang.org/x/sync/errgroup` 即此語意,
  還多了 `SetLimit`(限制併發數)。

## 3. 我的設計與取捨

直接用官方 `golang.org/x/sync/errgroup`(不重造)。核心在
`internal/gateway/aggregation.go` 的 `GetProfile`:

```
g, ctx := errgroup.WithContext(ctx)   // 帶 200ms 超時
g.Go(取 account)   // 失敗 → return err → 取消 group
g.Go(取 relation)  // 失敗 → 記 log、return nil → 不取消、保留降級值
g.Wait()           // 只有 account 的錯誤會浮上來
```

**關鍵設計:用「要不要 return error」來編碼失敗語意。**

| 服務 | 失敗時 | 為什麼 |
|---|---|---|
| account(主資料) | `return err` | 觸發 errgroup 取消,**順手把還在跑的 relation 呼叫也取消掉**,不浪費資源;錯誤碼往上傳給 HTTP 層透傳 |
| relation(次要) | 吞掉錯誤、`return nil`,粉絲數保留回退值 -1 | 它的失敗不該汙染 group 的 ctx;HTTP 仍回 200 + `degraded:true` |

回退值用 **-1 而非 0**:讓前端能區分「真的 0 個粉絲」與「暫時查不到」。

**超時**:聚合層自己包一層 200ms `context.WithTimeout`;這個 deadline
經 M2 的 client 攔截器「超時遞減」一路傳給下游,下游不會比 gateway
活得久。relation 若 200ms 沒回,它的 goroutine 拿到 DeadlineExceeded、
走降級分支,不拖累 account。

**錯誤碼透傳(demo 場景 4)的完整路徑**:
```
account dao: return -404 (ecode)
  → account server 攔截器:ecode → grpc status(夾 details)
  → 跨網路
  → gateway client 攔截器:status → ecode 還原
  → aggregation: errgroup 把它當 account 失敗往上傳
  → HTTP 層 writeEcode: ecode → {"code":-404,"message":...} + HTTP 404
```
最內層的 -404,穿過 gRPC 與 gateway 後,HTTP body 仍是 `{"code":-404}`。

### 觀測面:/debug/backends

`GET /debug/backends` 直接輸出 `wrr.Stats()`——各後端節點的有效權重、
累計 picks、視窗成功率與平均延遲。demo 場景 3/5/6 全靠它「看得見
流量分布」:注入延遲時眼看著某節點 effective_weight 掉下去、picks
占比縮小;移除後回升。

### 現代化補強:gRPC health check

demo 服務註冊標準 `grpc_health_v1`(約 20 行),回 SERVING。
早期框架沒有(K8s 生態尚未成熟),這是現代化補強:
docker-compose 用 `grpc-health-probe` 做 healthcheck,gateway
`depends_on: service_healthy` 等業務服務就緒才啟動。優雅下線時先把
狀態切 NOT_SERVING 再 GracefulStop。

### CPU 負載回報的閉環

server 端 `wrr.ReportLoadInterceptor` 把 CPU 千分比塞進 trailer →
client 端 balancer 在 Done 回呼讀出 → 參與權重重算。demo 服務的
CPU 由 chaos admin 埠注入(記憶體 dao 真實 CPU≈0,場景 3 由延遲主導,
見 docs/05),但這條 producer→consumer 的協議是完整通的。

## 4. 踩過的坑

- **errgroup 的取消是「第一個 error」觸發**:若 relation 也 return
  error,account 還沒回來時 relation 的失敗會把 account 的呼叫取消掉,
  變成「次要服務拖垮主服務」——正好相反。所以 relation 分支**永遠
  return nil**,把失敗關在自己的 goroutine 裡。
- **distroless 沒有 shell**:compose healthcheck 不能用 `curl`/`wget`,
  必須放一個探測 binary 進映像(grpc-health-probe)。
- **advertise 位址**:容器內服務監聽 `:9000`,但註冊到 discovery 的
  位址必須是 gateway 連得到的 `account1:9000`(docker DNS 名),
  不能是 `:9000`——否則 gateway 解析到的地址連不上。

## 5. 驗證方式

| 驗收(PLAN.md M6 / 場景) | 驗證 |
|---|---|
| 聚合 API 在 relation 全掛時仍回 200 + 降級 | 單元 `TestAggregator_RelationDegrades`;現場 kill relation → `degraded:true, follower_count:-1, HTTP 200` |
| account 失敗錯誤碼透傳(場景 4) | 單元 `TestAggregator_AccountFailsPropagates`、`TestHTTP_ProfileAndErrors`;現場 `id=9999` → HTTP 404 + body `code:-404` |
| 正常併發聚合 | `TestAggregator_BothSucceed` |
| 新副本自動接流量(場景 1) | 現場:3 副本註冊、/debug/backends 三者都有 picks |
| kill 副本流量轉移、錯誤率不飆(場景 2) | 現場壓測 kill 後 100/100 成功;e2e `TestGRPC_FailoverOnDeath` |
| 注入延遲占比下降、移除回升(場景 3) | 現場:200ms 延遲 → 慢節點占比 4.3%(<15%)、ewt 70 vs 5500;移除後回到 ~3600 三者相近;`TestGRPC_LatencyShiftsTrafficAndRecovers` |
| Guard 自保(場景 5) | `TestGuard_MassHeartbeatLoss` 等(docs/03);chaos `scenario5-partition.sh` |
| kill 註冊中心業務無感(場景 6) | `TestResolver_SurvivesDiscoveryOutage`、`TestWatch_SurvivesServerRestart`;chaos `scenario6-kill-discovery.sh` |
| grpc_health_probe 回 SERVING | demo 註冊 grpc_health_v1;compose healthcheck 綠 |

跑法:`go test -race ./internal/... ./pkg/...`;整套 demo
`docker compose up --build`,劇本見 README。
