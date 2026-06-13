# 01 — pkg/ecode 錯誤碼設計

> 對應里程碑 M1。引用 go-kratos 等開源實作一律只帶「來源 + 文字描述」,
> 不貼非公開授權的原始碼(PLAN.md §7 Clean-room 聲明)。

## 1. 要解決的問題(不做會怎樣)

微服務鏈路 `gateway → account → dao` 中,最內層回了一個業務錯誤
「-404 資源不存在」。如果什麼都不做:

- gRPC 預設只認 `codes.Code`(0~16 的傳輸層錯誤碼),業務錯誤從
  handler return 出去後會被壓扁成 `codes.Unknown`,**「找不到使用者」
  和「資料庫炸了」在 client 端看起來一模一樣**,沒辦法分支處理
  (前者該回 404,後者該告警)。
- 每跨一個服務就要人肉轉一次錯誤,三個服務三套 if/else,改一個碼動三處。
- 錯誤碼沒有全域註冊表,兩個團隊各自用 10001 表達不同意思,撞號後
  排查只能靠通靈。

目標:**錯誤碼像 metadata 一樣跨服務透傳**——最內層回 -404,穿過
任意層 gRPC 邊界與 gateway 後,HTTP 回應仍是 `{"code": -404}`
(demo 場景 4)。

## 2. 業界常見做法

- 全域註冊表:`New()` 向全域 map 註冊錯誤碼,重複註冊直接 panic;
  `Cause()` 從任意 error 反解出錯誤碼介面。成熟框架的 `Code` 多半是
  `int` 的型別別名,訊息存在獨立的全域 map,可由設定中心熱更新。
- 業務碼與 grpc code 的雙向粗粒度映射,並把業務碼塞進
  `status.WithDetails` 跨網路攜帶,client 端優先從 details 還原。
- 關鍵設計:**外層 grpc code 只是粗映射,details 裡的 proto 才是
  權威來源**;有些框架甚至刻意把外層定為 `codes.Unknown`,
  以免中間件誤解語意。

## 3. 我的設計與取捨

| 決策 | 成熟框架 | 本專案 | 理由 |
|---|---|---|---|
| Code 型別 | `int` 別名 + 全域訊息 map | `struct{code, message}` 值型別 | 訊息隨值攜帶,跨網路重建時不依賴本地註冊表;也不需要「設定中心熱更新訊息」的能力 |
| 反解 | 自訂 `Causer` 介面鏈 | 標準庫 `errors.As` | 早期還沒有 errors wrap 慣例,現在有,不重造 |
| 外層 grpc code | 多數情況 `Unknown` | 盡量映射語意相近的 code(-404→NotFound) | 對不認識本框架的中間件/客戶端友善;權威資訊仍在 details,粗映射錯了也不影響還原 |
| details 載體 | 專用 ecode proto | `api/proto/ecode/v1.Error`(code + message 兩欄) | 最小夠用;預留欄位編號可加 metadata map |
| 訊息國際化、錯誤詳情列表 | 有 | 不做 | 教學專案,範圍控制 |

比較語意:`Equal()` **只比 code 數值、不比 message**——跨網路傳回的
Code 是用 wire 上的資料重建的,不經過本地註冊表,message 可能因
雙方版本不同而不同;code 數值才是服務間的契約。

## 4. 踩過的坑

- **`status.WithDetails` 對 `codes.OK` 會回錯誤**(OK 不允許帶
  error details)。`ToStatus` 對這條路徑做了 fallback:退回不帶
  details 的 status。正常流程 err == nil 根本不會走到轉換,
  這個分支只是防禦。
- **errcheck 對 table-driven panic 測試**:`New()` 的回傳值在
  「重複註冊必須 panic」測試裡用不到,要顯式 `_ =` 否則 lint 不過。
- 重建自網路的 `Code{-404, "舊版訊息"}` 與本地 `NothingFound` 用
  `==` 比較會因 message 不同而不等——這就是 `Equal()` 存在的原因,
  業務代碼一律用 `ecode.Equal(err, ecode.NothingFound)`,
  不要直接 `==`。

## 5. 驗證方式

| 驗收(PLAN.md M1) | 測試 |
|---|---|
| ecode → status → 序列化 → status → ecode 往返不失真 | `TestStatus_RoundTrip`:真的把 status 的 proto 形式 `proto.Marshal` 成 bytes 再還原,含「未進映射表的業務碼 10001」案例 |
| 重複註冊 fail-fast | `TestNew_DuplicatePanic` |
| wrap 鏈反解 | `TestCause`(裸碼 / 一層 / 兩層 / 未知錯誤兜底) |
| 非本框架錯誤的兜底 | `TestFromError`(不帶 details 的 grpc 錯誤按 code 粗映射;client 端逾時 → -504) |
| 粗映射語意 | `TestToStatus_GRPCCodeMapping` 全表枚舉 |

跑法:`go test -race ./pkg/ecode/`,CI 每次 push 驗證。
