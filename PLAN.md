# grpc-governance-lab 專案規劃書

> 參考 go-kratos 等開源服務治理框架的架構思想,
> 用現代 Go 工具鏈親手實作 gRPC 服務治理層:服務發現、動態負載均衡、
> 攔截器鏈、錯誤碼透傳、併發聚合。
>
> 定位:**學習載體 + 可現場 demo 的作品集**,不是生產框架。

---

## 1. 專案目標

做完之後,`docker-compose up` 可以演示六件事:

| # | 演示場景 | 證明的能力 |
|---|---------|-----------|
| 1 | 新副本啟動後幾秒內自動接到流量,全程不改設定 | 服務註冊與發現 |
| 2 | `docker kill` 一個副本,壓測錯誤率不飆升,流量自動轉移 | 故障轉移、心跳剔除 |
| 3 | 對某副本注入延遲,它的流量占比自動從 33% 降到 ~10%,恢復後回升 | 動態加權負載均衡 |
| 4 | 最內層服務回 `-404`,穿過 gRPC 與 gateway 後 HTTP 回應仍是 `{"code":-404}` | 錯誤碼跨服務透傳 |
| 5 | 模擬網路分區讓大量心跳同時消失,註冊中心進入自我保護、拒絕剔除,服務照常互打 | Guard 自我保護(類 Eureka self-preservation) |
| 6 | `docker kill` 註冊中心本身,業務流量完全不受影響;重啟後訂閱自動恢復 | 控制面/資料面分離(resolver 快取降級) |

場景 3、5、6 都依賴「看得見流量分布」:gateway 暴露 `/debug/backends`
(各後端節點的即時 QPS、權重、成功率),demo 全程開著它觀察。

加上一個聚合 API 展示併發:gateway 用 errgroup 同時呼叫多個服務,
任一失敗即取消其餘,超時自動降級。

**刻意不做的事**(已有更好的現成品,避免重複造輪):
- 熔斷器 → 之後可選配 sony/gobreaker 或自寫(獨立 side quest)
- 分散式追蹤 → 直接用 OpenTelemetry
- HTTP 框架 → 直接用 gin
- 設定中心、訊息隊列 → 不在本期範圍

---

## 2. 整體架構

```
                       ┌───────────────────┐
                       │  discovery        │  迷你註冊中心 (HTTP, ~500 行)
                       │  註冊/心跳/訂閱     │
                       └──┬─────────────▲──┘
   gateway 訂閱地址變化     │             │  account / relation
   (poll, 只讀)            ▼             │  各自註冊 + 心跳續約
   HTTP            ┌─────────────────┐  │
 ┌──────┐   HTTP   │ gateway         │  │
 │client│ ───────▶ │  - HTTP→gRPC 網關│  │
 └──────┘          │  - errgroup 聚合 │  │
                   │  - 自訂 resolver │  │
                   │  - 動態 WRR      │  │
                   └──┬──────────┬───┘   │
                      │ gRPC     │ gRPC  │
              ┌───────▼──┐   ┌───▼─────┐ │
              │ account  │   │ relation│ │  demo 業務服務
              │ ×3 副本   │   │ ×1      │ │  (共用 binary,切換角色)
              └────┬─────┘   └────┬────┘ │
                   └──────────────┴──────┘
```

呼叫鏈:`HTTP client → gateway → (併發) account + relation`

註:對 discovery 而言,**gateway 只「訂閱」(poll 讀地址)、不註冊**;會 `register` +
心跳續約的是 account / relation。業務 RPC(gateway→account/relation)走直連、不經
discovery——這就是控制面/資料面分離,也是 demo 場景 6 的基礎。

---

## 3. Repo 結構

```
grpc-governance-lab/
├── PLAN.md                  # 本文件
├── README.md                # 門面:專案介紹 + demo GIF + 快速開始
├── go.mod
├── Makefile                 # proto 生成、build、test、lint
├── .github/workflows/ci.yml # CI:go test -race + go vet + lint(M1 就建)
├── docker-compose.yml       # discovery + account×3 + relation + gateway
├── api/
│   └── proto/
│       ├── account/v1/account.proto
│       ├── relation/v1/relation.proto
│       └── ecode/v1/ecode.proto        # 錯誤碼 details 的 proto 定義
├── pkg/                     # 治理層(本專案的核心價值)
│   ├── ecode/               # 錯誤碼:Code 型別、註冊表、grpc status 互轉
│   ├── registry/            # 註冊中心 client SDK:註冊、心跳、訂閱
│   ├── resolver/            # 自訂 gRPC resolver,接 registry
│   ├── balancer/wrr/        # 動態加權輪詢 + 滑動視窗統計
│   └── interceptor/         # server/client 攔截器:日誌、超時遞減、
│                            #   metadata 透傳、ecode↔status 轉換、recovery
├── cmd/
│   ├── discovery/           # 註冊中心 server
│   ├── demo/                # account/relation 共用 binary(flag 切角色)
│   └── gateway/             # HTTP 網關 (gin)
├── internal/
│   ├── account/             # account 業務:handler / service / dao(記憶體)
│   ├── relation/            # relation 業務
│   └── gateway/             # 聚合邏輯(errgroup 扇出在這)
├── deploy/
│   └── chaos/               # demo 用腳本:kill 副本、注入延遲、壓測
└── docs/                    # 設計文檔(見 §6)
```

---

## 4. 模組規劃與驗收標準

每個模組都標注對照的 go-kratos 開源實作,實作時逐一對照其設計思路。

### M1 — `pkg/ecode` 錯誤碼(預估 1 天)

| 項目 | 內容 |
|---|---|
| 功能 | `Code` 型別、`New()` 全域註冊(重複即 panic)、`Cause(err)` 反解、與 `grpc/status` 互轉(用 `status.Details` 夾帶 proto);同時建好 repo 地基:go.mod、Makefile、CI(GitHub Actions:`go test -race ./...` + `go vet` + lint,README 掛 badge) |
| 參考 | go-kratos `errors` 套件(`errors.go`、`types.go`) |
| 驗收 | 單元測試:error 經過 `ecode → status → 網路序列化 → status → ecode` 往返後 code/message 不變;CI 在 GitHub 上跑綠 |

### M2 — `pkg/interceptor` 攔截器鏈(預估 2 天)

| 項目 | 內容 |
|---|---|
| 功能 | server:recovery(panic→ecode)、access log(一行結構化日誌)、ecode→status、keepalive 五參數設定(IdleTimeout、MaxLifeTime、ForceCloseWait、KeepAliveInterval、KeepAliveTimeout)。client:status→ecode、超時遞減(deadline 沿呼叫鏈傳遞並扣減)、metadata 透傳(user-id 等白名單 key)、**per-method 設定覆蓋**(各方法可獨立指定超時,config 結構預留熔斷欄位) |
| 參考 | go-kratos `middleware`(recovery、logging、metadata)與 `transport/grpc` 的 server/client 攔截器組裝 |
| 驗收 | 先用寫死地址跑通 gateway→account 全鏈路;測試:gateway 設 500ms deadline,account 收到的 ctx 剩餘時間 < 500ms;account panic 時 client 收到 ecode.ServerErr 而非連線中斷;per-method 超時覆蓋全域設定生效 |
| 設計注意 | client 攔截器鏈要留好熔斷器插槽(熔斷器按 method 隔離,可參考 go-kratos aegis 的 SRE breaker),backlog 做熔斷時不需動架構;刻意不做 retry(重試牽涉冪等性,屬業務層職責,此決策寫入 docs/02) |

### M3 — `cmd/discovery` + `pkg/registry` 迷你註冊中心(預估 4-5 天)

| 項目 | 內容 |
|---|---|
| 功能 | server:`POST /register`、`POST /renew`(心跳)、`POST /cancel`、`GET /fetch`、`GET /poll`(長輪詢訂閱,30 秒超時)。心跳週期 30 秒,**漏 3 次(90 秒)才剔除**,另設 1 小時強制剔除上限。**Guard 自我保護**:每分鐘比較實際收到的心跳數與預期值(節點數 × 每分鐘心跳次數),若低於 85% 即進入自保模式、停止剔除任何節點,恢復後自動退出。SDK:背景心跳 goroutine、心跳失敗自動重新註冊、優雅退出(先停心跳再 cancel,避免在途心跳 404 觸發重新註冊而詐屍)、訂閱迴圈 |
| 參考 | go-kratos `registry` 介面與 contrib 的 eureka registry;Guard 自我保護對照 Netflix Eureka self-preservation |
| 驗收 | 測試:註冊→fetch 拿得到;停心跳 90 秒後 fetch 拿不到;poll 在地址變化時 1 秒內返回;kill -TERM 服務時,註冊中心立刻(而非 90 秒後)移除該節點。Guard 測試:10 個節點同時停心跳(模擬網路分區),註冊中心進入自保、fetch 仍回完整列表;單一節點停心跳(正常故障)則照常剔除 |
| 設計注意 | **所有時間參數(心跳週期、剔除倍數、poll 超時、Guard 統計窗)一律經 config 注入,不寫死**——否則「停心跳 90 秒後剔除」這類測試要真等 90 秒;單元測試全部用毫秒級參數,90s/30s 只是生產預設值 |

### M4 — `pkg/resolver` 接上服務發現(預估 1 天)

| 項目 | 內容 |
|---|---|
| 功能 | 實作 `resolver.Builder`/`resolver.Resolver`,target 格式 `discovery:///account`,背景訂閱 registry,地址變化時 `UpdateState`;**快取降級**:discovery 不可用(poll 失敗)時保留最後一次成功的地址列表繼續服務,背景指數退避重連——註冊中心是控制面,掛掉不得影響資料面 |
| 參考 | go-kratos `transport/grpc/resolver/discovery`(由 registry.Discovery 建 resolver、本地快取與節點輪轉) |
| 驗收 | gateway 不寫任何 account 地址;起第 4 個副本後新副本自動接流量;kill 副本後流量轉移、壓測錯誤率不飆升(demo 場景 1、2 在此打通);kill 註冊中心後壓測 60 秒零錯誤,重啟註冊中心後訂閱自動恢復(demo 場景 6) |
| 不實作但寫入文檔 | 業界常見的 subset 一致性雜湊子集選取(百萬級客戶端時避免全量訂閱)與 zone/color 染色路由(灰度發佈)——demo 環境展示不出效果,在 docs/04 留一節說明其原理與適用場景 |

### M5 — `pkg/balancer/wrr` 動態加權負載均衡(預估 2-3 天,最難)

| 項目 | 內容 |
|---|---|
| 功能 | 加權輪詢 Picker;每節點滑動視窗(10 bucket × 300ms)統計成功率與延遲;server 透過 trailer metadata(key `cpu_usage`)回報 CPU;定時依「成功率/延遲/CPU」重算權重。`Pick()` 高併發安全(atomic + RWMutex,鎖內不做計算)。**觀測面**:balancer 內建 per-node picks/成功/延遲計數,gateway 暴露 `GET /debug/backends` 回各節點即時 QPS、權重、成功率(場景 3、5、6 的演示全靠它) |
| 參考 | go-kratos `selector/wrr` 動態加權選擇器;CPU/延遲感知的權重公式 `score = sqrt(cs × ss² × 1e9 / (lag × cpu))` **直接改寫自 go-kratos(原 warden)的 WRR 計分實作**(Apache-2.0,歸屬見 NOTICE),其思路受 EWMA 平滑與 P2C 選擇啟發 |
| 驗收 | `go test -race` 全過;benchmark:Pick() 在 8 goroutine 併發下 > 100 萬 ops/s;demo 場景 3:注入 200ms 延遲後該節點流量占比 < 15%,移除後 30 秒內回升;`/debug/backends` 數字與壓測工具統計一致 |
| 設計注意 | demo 服務是記憶體 dao,容器內 CPU 趨近 0,**權重公式除以 CPU 會除零或爆炸**——CPU 與成功率都要 clamp 下限(成功率下限取 0.1 同理),並接受場景 3 實際由「延遲」主導權重變化;文檔註明:CPU 維度要在真實負載下才有戲份 |

### M6 — demo 業務 + 併發聚合 + docker-compose(預估 2-3 天)

| 項目 | 內容 |
|---|---|
| 功能 | account(GetUser)、relation(GetFollowerCount)業務,記憶體 dao 即可;gateway `GET /user/profile?id=` 用 errgroup 併發聚合兩服務,200ms 超時、relation 失敗時降級(粉絲數回 -1 不報錯);標準 gRPC health check(`grpc_health_v1`,約 20 行,供 K8s readiness probe 用,屬現代化補強);chaos 腳本 + README demo 劇本(場景 5 網路分區用 `docker network disconnect` 或 SIGSTOP 模擬,場景 6 直接 kill discovery 容器) |
| 參考 | 一般 BFF/聚合層的扇出模式、`golang.org/x/sync/errgroup`(官方) |
| 驗收 | 六個 demo 場景全部可照 README 腳本重現;聚合 API 在 relation 全掛時仍回 200 + 降級資料;`grpc_health_probe` 對各服務回 SERVING |

**總時程:約 13-16 個工作天**(業餘時間約 4 週;含 CI、觀測端點、
快取降級等本輪補強,各自半天以內)。
每個里程碑結束時:測試全綠、能獨立展示、補完該模組的設計文檔。

---

## 5. 技術選型

| 類別 | 選擇 | 理由 |
|---|---|---|
| Go | 1.22+,go modules | 現代工具鏈 |
| gRPC | google.golang.org/grpc 最新版 | 主角 |
| proto 生成 | buf(或 protoc + Makefile) | buf lint 順便學 |
| HTTP | gin | 不自寫 HTTP 框架 |
| 併發 | golang.org/x/sync/errgroup(官方已有 SetLimit) | 不重造 |
| 日誌 | log/slog(標準庫) | 夠用且零依賴 |
| 註冊中心儲存 | 純記憶體 map + RWMutex | 教學用,刻意不上 etcd |
| 測試 | 標準 testing + testcontainers 不需要 | 全部可單機測 |

---

## 6. 文檔規劃

文檔和程式碼同等重要——這個 repo 的價值一半在「講清楚為什麼」。

```
README.md                      # 門面(最後寫,M6 完成後)
│   ├── 30 秒介紹:這是什麼、解決什麼問題
│   ├── demo GIF / asciinema:kill 副本流量轉移的畫面
│   ├── 快速開始:docker-compose up + 四個場景的操作劇本
│   ├── 架構圖
│   └── 與 go-kratos 的對照表(每個 pkg 對應它哪個套件)
└── docs/
    ├── 01-ecode.md            # 每模組一篇設計文檔,固定格式:
    ├── 02-interceptor.md      #   1. 要解決的問題(不做會怎樣)
    ├── 03-discovery.md        #   2. 業界/go-kratos 怎麼做(引公開實作)
    ├── 04-resolver.md         #   3. 我的設計與取捨(哪裡簡化了、為什麼)
    ├── 05-balancer.md         #   4. 踩過的坑(競爭、邊界條件)
    └── 06-aggregation.md      #   5. 驗證方式(測試怎麼證明它對)
```

幾篇文檔須額外涵蓋的「設計決策」段落(面試高頻考點):
- **02-interceptor**:為什麼不做自動 retry(重試牽涉冪等性,
  屬業務層職責);熔斷器插槽的預留設計
- **03-discovery**:Guard 自我保護的推導——為什麼「心跳大量消失」更可能是
  網路分區而非服務全滅,85% 閾值的取捨,對照 Eureka self-preservation
- **04-resolver**:subset 一致性雜湊與 color/zone 染色路由的原理、
  適用規模、為什麼本專案不實作;控制面/資料面分離——為什麼註冊中心
  掛掉業務不受影響(快取降級)
- **05-balancer**:兩層故障感知——TCP 斷線是 gRPC 連線層「立即」感知的,
  註冊中心剔除只是 90 秒「最終一致」的兜底;這是場景 2 錯誤率不飆升的
  真正原因,兩層各管什麼要講清楚

寫作原則:
- 每篇設計文檔在該里程碑完成時一起寫,不留到最後(會忘)
- 文檔裡引用 go-kratos 等開源實作時帶套件路徑與描述,可回查
- README 的 demo 劇本必須「複製貼上就能跑」,每條指令都實測過

---

## 7. Clean-room 聲明(公開 repo 前必讀)

本專案為**獨立重新實作(clean-room style)**,只參考公開資訊。要讓這個
repo 能安全地公開當作品集,全程遵守:

1. **只讀思路,不抄代碼**——任何一行程式碼(包括註解、變數命名風格)都
   不從任何非公開授權來源複製貼上;一律理解演算法後自己重寫,
   API 用法以 grpc-go 官方文件與 examples 為準。
2. 文檔中引用外部實作僅限「來源 + 文字描述」,作為閱讀筆記,
   不貼非公開授權的原始碼片段。
3. README 加 disclaimer:本專案為獨立重新實作(clean-room style),
   設計思想參考公開資訊(go-kratos 開源版、Dapper/SRE 論文、
   Eureka 文件)。
4. 對照學習一律引用**正式開源的 go-kratos**(Apache-2.0)等公開專案,
   引用它們完全沒有法律問題。

## 8. 風險與對策

| 風險 | 對策 |
|---|---|
| M5 balancer 併發設計卡關 | 先實作「無統計的靜態 WRR」拿到可跑版本,滑動視窗統計做為第二刀 |
| grpc-go API 版本演進快、舊文章常過時 | 只參考開源框架的「設計思路」,API 用法以官方 examples/features 為準(resolver、balancer 官方都有範例) |
| 範圍蔓延(想加熔斷、追蹤、設定中心…) | 一律記入 PLAN.md 末尾的 backlog,M6 之前不動工 |

## Backlog(本期不做)

- [ ] SRE 自適應熔斷器(對照 go-kratos aegis 的 `circuitbreaker/sre`;
      M2 已在 client 攔截器鏈預留插槽,接入時不需動架構)
- [ ] OpenTelemetry 全鏈路追蹤接入
- [ ] P2C(Power of Two Choices)balancer,與 WRR 壓測對比
- [ ] 註冊中心多副本與資料同步(同 zone 全節點複製、跨 zone 隨機單點複製)
- [ ] subset 子集選取與 color/zone 染色路由(原理先寫入 docs/04,
      若要實作需先有多環境 demo 場景)
