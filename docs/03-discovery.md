# 03 — 迷你註冊中心與 client SDK 設計

> 對應里程碑 M3。server 在 `internal/discovery` + `cmd/discovery`,
> SDK 在 `pkg/registry`。引用外部實作只帶「來源 + 文字描述」(PLAN.md §7);
> Guard 自我保護對照 Netflix Eureka self-preservation。

## 1. 要解決的問題(不做會怎樣)

gateway 要呼叫 account 的 3 個副本,地址寫在哪?

- **寫死在設定檔**:擴容第 4 個副本要改設定 + 重啟 gateway;
  副本換 IP(容器重啟是常態)就是線上事故。
- **掛個 LB(nginx/k8s service)**:能用,但 L4 LB 看不懂 gRPC——
  長連線會把流量黏死在建連時選中的副本上,且無法做 M5 要的
  「按延遲/成功率動態調權」,client-side LB 必須拿到**全部後端地址**。

所以需要註冊中心:副本啟動時自己報到、定期心跳證明活著、
訂閱者在成員變化時收到推播。同時它必須是**控制面**——
它自己掛掉不能影響已經建立的呼叫(demo 場景 6)。

## 2. 業界常見做法

- 剔除雙門檻:90 秒(漏 3 次 30 秒心跳)與 3600 秒天花板;剔除迴圈
  先問 Guard,自保模式下只有超過天花板的節點才會被動手。
- 自保閾值 0.85;期望心跳數按「每節點每分鐘 2 次」**增量維護**
  (註冊 +2、註銷 -2),實際心跳數每分鐘結算一次;實際 < 期望 × 0.85
  即判定自保,只記 log 不剔除。
- 長輪詢:client 帶版本號 poll,server hold 住直到變化或逾時
  (Eureka 用 30 秒全量/增量拉取,長輪詢延遲更低)。
- 多機房同步:同 zone 全節點複製、跨 zone 單點複製——本專案不做
  (單機 demo 展示不出來),記入 backlog。

## 3. 我的設計與取捨

| 決策 | 成熟框架 | 本專案 | 理由 |
|---|---|---|---|
| Guard 期望值 | 註冊/註銷時增量維護(+2/-2) | 統計窗結束時用「當下節點數 × 窗長/心跳週期」重算 | 少一份要和註冊表強一致的狀態;代價是節點在窗中途註冊會虛增期望值,屬可接受誤差(增量維護其實也有時間粒度誤差) |
| 版本號 | 時間戳(latest_timestamp) | 時間戳為底、保證單調遞增 | 重啟後記憶體清空,計數器會撞號導致訂閱者永久阻塞;poll 對「版本不一致」(而非僅「更新」)即返回,重啟後訂閱自動對齊 |
| 儲存 | 記憶體 + 多機房複製 | 純記憶體單點 | 教學用;SDK 的 404 自動重註冊 + resolver 快取降級,讓單點重啟不是事故 |
| API | HTTP + 自家框架 | 標準庫 net/http(Go 1.22 pattern routing) | 零依賴 |
| 錯誤形狀 | 自家 ecode | HTTP 狀態 + body 帶 pkg/ecode 業務碼 | SDK 用 `ecode.Equal(err, NothingFound)` 判斷要不要重新註冊,跨 HTTP 邊界語意不丟 |

### Guard 自我保護的推導(面試高頻)

**為什麼「心跳大量消失」不能直接剔除?**
單一節點心跳消失,大概率它真的死了;但 10 個節點的心跳**同時**
消失,貝氏推斷下更可能的解釋是:節點都活著,是註冊中心自己被
網路分區隔離了(一個事件 vs 十個獨立事件同時發生)。
此時若照常剔除,訂閱者會拿到空列表、流量瞬間歸零——
**註冊中心親手製造了它本該防止的全站故障**。
寧可錯留(訂閱者拿到可能過期的名單,呼叫失敗由 client 端
連線層兜底),不可錯殺。這就是 Eureka self-preservation 的邏輯。

**0.85 的取捨**:閾值太高(如 0.99)→ 日常抖動(GC、丟包)頻繁
誤觸自保,剔除功能形同虛設;太低(如 0.5)→ 真分區時要死掉一半
心跳才觸發,保護來得太晚。0.85 容忍 15% 抖動,等價於
「10 節點掛 1 個不觸發、掛 2 個觸發」——單機故障照常剔除,
成片消失才判定分區。

**兩個配套設計**:
- 主動 `cancel` 不受自保影響——「我要下線」是明確訊號,
  自保防的是「心跳消失」這種曖昧訊號。
- 強制剔除天花板(1h)——自保判斷錯了(真的全死了)時,
  殭屍節點不至於永久殘留。

### 為什麼 SDK 的 Deregister 要「先停心跳、再註銷」

心跳迴圈對 404 的反應是**自動重新註冊**(為了在註冊中心重啟後
自癒)。若先註銷再停心跳,在途心跳收到 404 就會把剛下線的節點
加回去(詐屍)。順序倒過來:確認心跳 goroutine 完全退出,
再發 cancel。`TestDeregister_RemovesImmediately` 專門驗證這個。

### 為什麼初次註冊失敗不能直接回錯誤

心跳的「404 → 補註冊」只在心跳 goroutine 啟動後才存在;若
`Register` 在初次失敗時直接回 `nil, err`,呼叫方多半只能 log 之後
繼續跑,心跳沒啟動、沒有人補註冊,服務就**永久缺席服務發現**。
這個競態真的踩得到:docker-compose 的 `depends_on` 只等容器啟動、
不等 HTTP listener ready,demo 可能搶在 discovery 開始監聽前送出
註冊請求。所以 `Register` 對暫時性失敗照樣回傳 Registration 並
啟動心跳,心跳迴圈以指數退避(BackoffBase 起跳、BackoffMax 封頂,
與 watcher 同一套節奏)重試到補註冊成功,才切回正常心跳週期——
只補一拍不夠,註冊中心晚於退避起點才就緒的話,服務仍會缺席
服務發現整整一個心跳週期(預設 30 秒)。只有 RequestErr
(請求不合法,重試也不會成功)才回錯誤。

## 4. 踩過的坑

- **長輪詢喚醒的不一定是你等的事件**:HTTP poll 測試裡,第一次
  304 poll 燒掉 100ms,被觀察的副本(無心跳)TTL 到期被剔除,
  喚醒第二次 poll 的是「剔除」而非測試預期的「註冊」。
  教訓:測試裡所有「應該活著」的副本都要真的給它心跳。
- **t.Cleanup 與手動關閉重複執行**:關閉函式必須冪等
  (sync.Once),否則 `close(closed channel)` panic。
- **Guard 與剔除的時序競爭**:TTL 到期與統計窗結算誰先發生不
  確定。參數要保證「分區發生後,自保判定先於 TTL 到期」:
  統計窗(40ms)< TTL(120ms),生產值同理(1min < 90s)。
- **`http.Client.Timeout` 不能設**:它對所有請求一視同仁,
  會殺掉本該掛 30 秒的長輪詢;逾時一律由各呼叫的 ctx 控制。

## 5. 驗證方式

毫秒級參數(心跳 30ms、TTL 120ms、統計窗 40ms),
`waitFor` 輪詢斷言取代裸 sleep。

| 驗收(PLAN.md M3) | 測試 |
|---|---|
| 註冊 → fetch 拿得到 | `TestRegistry_RegisterAndFetch`、`TestHTTP_RegisterFetchCancel` |
| 停心跳 N 個週期後 fetch 拿不到 | `TestRegistry_EvictAfterMissedHeartbeats` |
| poll 在地址變化時 1 秒內返回 | `TestRegistry_PollReturnsOnChange`、`TestHTTP_PollLongPolling`、`TestWatch_ReceivesMembershipChanges` |
| 優雅下線立刻移除(而非 90 秒後) | `TestRegistry_CancelImmediate`、`TestDeregister_RemovesImmediately`(含「不詐屍」斷言) |
| 10 節點同停心跳 → 自保、fetch 仍完整 | `TestGuard_MassHeartbeatLoss` |
| 單節點停心跳 → 照常剔除 | `TestGuard_SingleNodeFailureStillEvicted` |
| 自保恢復後自動退出 | `TestGuard_RecoveryExitsProtection` |
| 強制剔除天花板 | `TestRegistry_HardEvict` |
| 心跳 404 自動重新註冊 | `TestHeartbeat_ReregistersAfterEviction` |
| 初次註冊失敗 → 註冊中心就緒後補註冊 | `TestRegister_InitialFailureRecovered`(不合法請求則 fail fast:`TestRegister_InvalidRequestFailsFast`) |
| 補註冊以退避重試直到成功,不等心跳週期 | `TestRegister_RetriesWithBackoffUntilRegistered` |
| 註冊中心重啟後訂閱自動恢復 | `TestWatch_SurvivesServerRestart`(同位址重啟、版本重置) |

跑法:`go test -race ./internal/discovery/ ./pkg/registry/`;
覆蓋率 discovery 90%、registry SDK 81%。
