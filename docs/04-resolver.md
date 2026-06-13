# 04 — pkg/resolver 設計:服務發現接入 gRPC

> 對應里程碑 M4。引用外部實作只帶「來源 + 文字描述」(PLAN.md §7);
> 設計思路對照 go-kratos 的 transport/grpc/resolver/discovery。

## 1. 要解決的問題(不做會怎樣)

M3 做完,地址在註冊中心裡;但 gRPC 連線不會自己去看。
不接 resolver,gateway 還是得寫死 `account` 的地址列表,
服務發現等於白做。gRPC official 的擴充點就是 `resolver.Builder`:
target 寫 `discovery:///account`,地址哪來、何時更新,全由 resolver 決定。

## 2. 業界常見做法

- 實作 grpc 的 Builder/Resolver 介面,背景 goroutine 消費服務發現
  SDK 的事件,轉成 `newAddress` 推給 gRPC。
- SDK 端維護本地快取:poll 失敗不清空、沿用上一次結果,並指數退避重連。
- subset 子集選取(見 §3 末)。

## 3. 我的設計與取捨

結構:`pkg/registry.Watcher`(訂閱、退避、保留最新快照)→
`pkg/resolver`(橋接到 `cc.UpdateState`)。職責切開後 resolver
本體不到 100 行。

| 決策 | 說明 |
|---|---|
| `grpc.WithResolvers` 注入而非全域 `resolver.Register` | 全域註冊表會讓測試互相干擾,也藏起依賴關係;per-conn 注入讓「這條連線用哪個註冊中心」一目了然 |
| `ResolveNow` 是空操作 | 長輪詢訂閱常駐、變化即推;「主動再解析」沒有對應動作 |
| 空快照一律忽略 | 見下節 |

### 控制面/資料面分離:為什麼註冊中心掛掉業務無感(場景 6)

註冊中心是**控制面**:它只負責「告訴 client 地址」;真正的 RPC
流量(資料面)走 client ↔ server 的直連 TCP,**不經過註冊中心**。
所以它掛掉時:

1. 已建立的連線完全不受影響——gRPC 拿的是地址快照,不是代理。
2. Watcher poll 失敗 → 指數退避重連、**通道保持沉默**(不推空、
   不關閉),resolver 收不到更新 = gRPC 沿用最後一次地址。
3. 註冊中心重啟後:server 端心跳 404 自動重新註冊(M3),
   client 端 poll 因版本不一致立刻拿到新快照,訂閱自動恢復。

降級的另一半是**空快照防禦**:重啟後的瞬間,副本可能還沒重新
報到,fetch/poll 會回空列表。把空列表推給 gRPC 等於親手把服務
打掛。所以 resolver 忽略空快照、保留舊地址。
NOTE(取捨):服務真的縮容到 0 時舊地址會殘留,但此時呼叫在
連線層立刻失敗,行為與空列表等價,沒有額外傷害。

### 兩層故障感知(這也是 M5/場景 2 的理論基礎)

- **連線層(立即)**:副本死掉,TCP 斷線,gRPC subconn 進入
  TRANSIENT_FAILURE,round_robin/wrr 只挑 READY 的連線——
  毫秒級把流量繞開死節點。
- **註冊中心(最終一致)**:90 秒心跳過期後從名單移除,
  讓 client 不再為死節點保留 subconn。

kill 副本時錯誤率不飆升,**靠的是第一層**;註冊中心的剔除只是
慢速兜底。分清這兩層,才解釋得了「為什麼註冊中心的 90 秒剔除
延遲不影響可用性」。

### 不實作但要懂:subset 與染色路由

**subset 子集選取**:百萬級 client 全量訂閱所有後端,會造成
O(client × backend) 的連線數與訂閱風暴。常見做法是按 clientID
一致性雜湊選出固定大小的子集(先按 hostname 排序、用 clientID
指紋決定 shuffle 種子與起點),每個 client 只連其中 N 台,
且子集分布均勻、節點增減時擾動小。
**適用規模**:client 數 × 後端數大到連線數成為瓶頸時;
本專案單 gateway 對 3 副本,做了也展示不出任何效果。

**color/zone 染色路由**:請求帶 color 標籤(經 M2 的 metadata
透傳鏈路一路下傳),resolver/balancer 優先挑同色節點——
這就是灰度發佈(把 1% 流量染成 canary 色)與同機房就近路由的
底層機制。**需要多環境/多版本場景才有意義**,單機 demo 不做;
metadata 透傳的基礎設施(M2 白名單)已經留好。

## 4. 踩過的坑

- **`UpdateState` 會回錯誤**:連線關閉中時更新被拒。吞掉它是
  對的(下次快照再推),但要記 log——lint(errcheck)也不放過。
- **round_robin 不是預設**:gRPC 預設 pick_first,多副本測試
  必須顯式 `loadBalancingConfig`,不然流量全黏在第一個副本上。
- **測試裡的 WaitForReady**:resolver 推第一批地址前,呼叫會
  因「無可用地址」立刻失敗;`grpc.WaitForReady(true)` 讓呼叫
  等到連線就緒,測試不必猜「resolver 多久生效」。

## 5. 驗證方式

e2e:真 TCP 上的 discovery server + 多個真 gRPC account 副本
(GetUser 回自己的位址,流量去向可斷言),client 只寫
`discovery:///account`。

| 驗收(PLAN.md M4 / demo 場景) | 測試 |
|---|---|
| gateway 不寫地址,新副本自動接流量(場景 1) | `TestResolver_DiscoversNewReplica` |
| kill 副本流量轉移、錯誤率不飆升(場景 2) | `TestResolver_FailoverOnReplicaDeath`(殺 TCP、心跳未剔除期間即轉移) |
| kill 註冊中心,業務零失敗(場景 6) | `TestResolver_SurvivesDiscoveryOutage`(掛掉期間連續呼叫全成功) |
| 空快照不打掛服務 | `TestResolver_IgnoresEmptySnapshot` |

跑法:`go test -race ./pkg/resolver/`,覆蓋率 90%。
場景 6 的「重啟後訂閱自動恢復」由 SDK 層
`TestWatch_SurvivesServerRestart` 驗證(docs/03 §5)。
