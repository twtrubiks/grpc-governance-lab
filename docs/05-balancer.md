# 05 — pkg/balancer/wrr 動態加權負載均衡設計

> 對應里程碑 M5(本專案最難的部分)。設計思路對照 go-kratos 的 selector/wrr。
> **權重計分公式與冷啟動 clamp、smooth-WRR picker 直接改寫自 go-kratos
> (原 warden)的 WRR 實作**(Apache-2.0,歸屬見專案根目錄 NOTICE);其周邊
> 程式碼(滑動視窗、觀測面、與 gRPC base balancer 整合、測試)為獨立實作。
> 公式背後的負載感知思路另受 EWMA 平滑與 P2C 選擇啟發。

## 1. 要解決的問題(不做會怎樣)

M4 把 3 個 account 副本的地址都餵給了 gRPC。預設的 round_robin
雨露均霑——但副本不是生而平等:

- 其中一台正在 GC、磁碟慢、或被鄰居容器吵到,延遲飆到 200ms;
  round_robin 照樣丟 1/3 流量給它,**整體 P99 被一台拖垮**。
- 一台剛啟動、快取還沒熱;一台規格就是比較差。

需要的是:**按即時表現動態調整每台的流量占比**——快的多給、
慢的少給,且故障恢復後自動回升。這就是動態加權輪詢(dynamic WRR)。

## 2. 業界常見做法

- 每個 subConn 維護 err/latency 的滑動視窗(10 桶各 1 秒)、server
  回報的 cpu 與 success、以及 ewt(有效權重)/cwt(當前權重)/score。
- Pick 用 Nginx smooth WRR:每個節點 `cwt += ewt`、選 cwt 最大者、
  再 `cwt -= 總權重`。全程持寫鎖。
- **權重公式**:`score = sqrt(cs · ss² · 1e9 / (lag · cpu))`,
  其中 cs 是 client 端成功率、ss 是 server 自評成功率、
  lag 平均延遲、cpu 是 server 經 trailer 回報的使用率。
- 統計在 Done 回呼裡更新,且用 CAS 把「重算」節流到每秒最多一次。
- `count < 2` 時直接 return,避免把所有節點 ewt 打成 0。
- server 端 cpu 由系統指標採集(讀 cgroup/proc)。
- subset 與 color 染色(原理見 docs/04)。

## 3. 我的設計與取捨

### 併發:為什麼能 RLock(常見實作是 Lock)

CODE_QUALITY §4 的目標是「Pick 只讀持 RLock,鎖內不做計算」。
smooth WRR 必須改 cwt,看似一定要寫鎖。我的解法:
**把 ewt/cwt 設成 atomic**,RWMutex 只保護「節點切片本身」
(增減節點時 Lock,Pick 時 RLock)。Pick 迴圈裡對 cwt 用
`atomic.AddInt64`,其回傳值就是本 goroutine 視角的新 cwt,
據此選最大者——無撕裂、無資料競爭。

代價:高併發下多個 Pick 同時改 cwt,smooth 的「平滑」會有微小
擾動,但長期命中比例仍精確正比於 ewt(`TestPicker_Distribution`
單執行緒驗精確比例,`TestPicker_ConcurrentRaceFree` + benchmark
驗併發安全與吞吐)。benchmark:8 goroutine 下 **~1260 萬 ops/s**
(要求 100 萬)。

### 權重重算:沿用 go-kratos 的公式,但拆成可獨立測試的核心

計分公式直接改寫自 go-kratos 的 WRR 實作(Apache-2.0,見 NOTICE),
連冷啟動 clamp 的取值都一致:`score = sqrt(cs · ss² · 1e9 / (lag · cpu))`。
直覺——成功率越高、延遲與 CPU 越低,分數越高、流量越多;
**ss 取平方**讓 server 自評的健康度比 client 局部觀測更有話語權。
(這段是本專案少數「照搬而非重寫」的程式碼,原因是公式本身就是重點、
照抄才能忠實呈現原框架的取捨;真正的學習價值在下面的「拆成可獨立測試的核心」。)

關鍵差異:
- **重算與 gRPC 解耦**。常見實作把重算塞在 Done 回呼裡用 CAS 節流;
  我抽出獨立的 `core`(節點 + picker + recompute),由背景 ticker
  週期呼叫。好處:`core` 不依賴 gRPC,用注入的假時鐘就能精確測
  「注入延遲 → 權重下降 → 視窗過期 → 回升」整段動態,不必跑真網路。
- **gRPC 整合用官方 `base` balancer** 管 SubConn 生命週期,
  我只實作 PickerBuilder + Picker,把 pick 委派給 core、
  在 Done 回呼把延遲/成敗/trailer CPU 餵回 core。每條 ClientConn
  一個獨立 core 與 recalc goroutine(在 Builder.Build 建立、
  Close 收尾),互不干擾。

### clamp:demo 環境的現實

PLAN.md M5 設計注意點名的坑:**demo 是記憶體 dao,容器 CPU 趨近 0,
公式除以 cpu 會除零或爆炸**。對策(對應 `core.go` 的 clamp):

| 維度 | 下限 | 為什麼 |
|---|---|---|
| cpu | 10‰(1%) | 防除零;也意味著 **demo 場景 3 實際由「延遲」維度主導**,CPU 維度要在真實負載下才有戲份 |
| 成功率 cs/ss | 0.1 | 冷啟動節點樣本少、偶發失敗不該被權重歸零 |
| 冷啟動 | 樣本 ≤5 且 cs≤0.2 時拉到 0.2 | 新節點別因運氣差被一棒打死 |
| ewt | 最低 1 | **永不歸零**:再爛的節點也留一線流量,它恢復了才有機會被選中、用新樣本證明自己 |

另一個現實:`isNodeFailure` **只把傳輸層/過載訊號(Unavailable、
DeadlineExceeded、ResourceExhausted)算節點故障**。業務錯誤
(-404 NotFound 等)代表節點正常工作、只是這筆查無資料——
不能因此降它的權重,否則「查無使用者」這種正常結果會誤殺健康節點。
(常見實作靠「業務碼都映射成 Unknown」來區分,但本專案 ecode 把 -404
映射成 NotFound,所以改用「明確的故障碼白名單」來判定。)

### 兩層故障感知(面試高頻,也是場景 2 的真正原因)

| 層 | 感知速度 | 機制 | 管什麼 |
|---|---|---|---|
| 連線層 | **立即**(毫秒) | TCP 斷 → gRPC subconn 進 TRANSIENT_FAILURE,不在 ReadySCs 裡 | 副本「死了」 |
| balancer | 秒級 | 滑動視窗 + 權重重算 | 副本「變慢/變差但還活著」 |
| 註冊中心 | 90 秒最終一致 | 心跳過期剔除 | 兜底,把死節點從名單徹底移除 |

**kill 副本時錯誤率不飆升(場景 2),靠的是第一層**——gRPC 連線層
立刻把死節點移出 ReadySCs,balancer 自然不會 pick 它,根本不必等
註冊中心那 90 秒。balancer 的權重調整(第二層)處理的是另一種情況:
節點沒死、只是劣化。三層各司其職,別混為一談。

## 4. 踩過的坑

- **業務錯誤誤判為故障**:見上,ecode 映射改了 grpc code 語意,
  沿用「非 Unknown 即故障」的舊判準會把 -404 當失敗。改白名單。
- **無樣本節點被邊緣化**:重算時若把沒流量的節點 score 當 0,
  它永遠分不到流量、永遠沒樣本。對策:無樣本給「平均分」,
  公平參與下一輪(成熟框架同此思路)。
- **全場無樣本不能動權重**:啟動瞬間誰都沒資料,若照算會把大家
  打成 1。對策:`scored == 0` 直接 return,維持靜態設定權重。
- **滑動視窗閒置後吃掉新樣本**:advance 把跨桶數 clamp 到總桶數
  後,若 lastStart 只前進一個視窗長(而非對齊到 now),節點閒置
  超過兩個視窗(>6 秒沒流量)後,追上 now 之前的每次 advance 都
  會再整窗全清——連上一筆剛寫入的新鮮樣本一起清掉,低流量節點
  的統計持續失真。對策:整窗作廢時 lastStart 直接對齊到 now
  (歷史全部作廢,游標沒有理由留在過去)。
  `TestRollingWindow_IdleBeyondWindow` 驗證。
- **測試不能等真實 3 秒視窗**:`core` 的時鐘可注入,滑動視窗與
  重算測試用假時鐘瞬間推進;e2e 則用 `RegisterWithConfig` 把視窗
  與重算週期縮到 50ms。

## 5. 驗證方式

| 驗收(PLAN.md M5 / 場景) | 測試 |
|---|---|
| Pick 命中比例正比權重、平滑 | `TestPicker_DistributionMatchesWeights`、`TestPicker_Smoothness` |
| Pick `-race` 全過、>100 萬 ops/s | `TestPicker_ConcurrentRaceFree`、`BenchmarkPicker_Pick`(~1260 萬) |
| 注入延遲 → 占比 < 15%(場景 3) | `TestCore_HighLatencyLosesWeight`(注入 200ms → 占比 < 15%) |
| 恢復後回升(場景 3) | `TestCore_RecoversAfterWindowExpires`、e2e `TestGRPC_LatencyShiftsTrafficAndRecovers` |
| CPU=0 不爆炸 | `TestCore_CPUFloorPreventsExplosion` |
| 權重永不歸零 | `TestCore_NeverZeroWeight` |
| 副本死亡流量轉移(場景 2) | e2e `TestGRPC_FailoverOnDeath` |
| /debug/backends 數字正確 | `TestCore_Snapshot`、e2e 透過 `Stats()` 斷言 |

跑法:`go test -race ./pkg/balancer/wrr/`,覆蓋率 94%。
場景 3、5、6 的「看得見流量分布」由 `Stats()` → gateway
`/debug/backends`(M6 接入)輸出。
