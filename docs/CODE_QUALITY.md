# 程式碼品質規範

本專案是作品集,程式碼品質本身就是展示品。所有規範以「讓三個月後的自己
和第一次看的面試官都能立刻讀懂」為目標。

---

## 1. 註解規範(最重要)

- **所有 exported 符號(型別、函式、常數)必須有 godoc 註解**,
  以符號名開頭,說明「它是什麼、什麼時候用」。繁體中文。
- **每個 package 必須有 `doc.go`**,套件註解寫清楚:這個套件的職責、
  對應 PLAN.md 哪個里程碑、參考了什麼設計。
- 函式內部註解寫「**為什麼**」,不寫「做什麼」:
  ```go
  // 不好:i 加一
  i++

  // 好:成功率取 0.1 下限,避免冷啟動節點因樣本不足被權重歸零
  if successRate < 0.1 {
      successRate = 0.1
  }
  ```
- 演算法實作處(WRR 權重公式、Guard 閾值、滑動視窗)必須附註解寫出
  公式與出處(論文名或設計文檔編號),讓讀者不用跳出去查。
- 已知的取捨用 `// NOTE(取捨):` 標記;未來要做的用 `// TODO(M5):`
  標記並掛里程碑,不留無主 TODO。

## 2. 命名

- 套件名:小寫單字,不用底線、不用複數(`ecode`、`registry`,
  不是 `error_codes`)。
- 介面以行為命名(`Picker`、`Resolver`),實作不加 `Impl` 後綴。
- 縮寫保持一致大小寫:`ID`、`HTTP`、`QPS`(不是 `Id`、`Http`)。
- 測試函式:`Test<被測物>_<情境>`,例如 `TestGuard_MassHeartbeatLoss`。

## 3. 錯誤處理

- 業務錯誤一律用 `pkg/ecode`,不裸傳 `errors.New` 字串。
- 錯誤要嘛處理、要嘛往上傳,**不得吞掉**(`_ = err` 需要一行註解解釋為什麼)。
- wrap 錯誤用 `fmt.Errorf("...: %w", err)`,保留原因鏈。
- `pkg/` 下的庫程式碼**不准 panic**,唯一例外:ecode 重複註冊
  (程式設計錯誤,fail-fast)。
- 不用 `log.Fatal`,只有 `cmd/*/main.go` 可以決定程序退出。

## 4. 併發

- 每個有共享狀態的結構,在型別註解寫明**鎖的保護範圍**:
  ```go
  // mu 保護 nodes 與 weights;Pick() 只讀,持 RLock。
  ```
- goroutine 一律有明確的退出路徑(ctx cancel 或 channel close),
  禁止裸 `go func()` 不管生死。
- 所有時間參數經 config 注入、不寫死(見 PLAN.md M3 設計注意),
  測試用毫秒級參數。
- `go test -race ./...` 是底線,CI 強制。

## 5. 測試

- 多案例一律 table-driven,案例命名用中文描述情境。
- 測試不依賴真實時間(不 `time.Sleep` 等待邏輯生效;用注入的短週期
  或顯式觸發),不依賴網路與外部服務,全部可單機離線跑。
- 覆蓋率目標:`pkg/` 各套件 ≥ 80%(`make cover` 檢查),
  `cmd/`、`internal/` 不強制。
- 每個里程碑的驗收標準(PLAN.md §4)必須有對應的測試函式。

## 6. Commit 規範

- 格式:`<type>(<scope>): <subject>`,type 用
  `feat / fix / docs / test / refactor / chore`,
  scope 用套件名或里程碑(`ecode`、`m3`)。
  例:`feat(ecode): Code 型別與全域註冊表`
- 一個 commit 一件事:實作與大規模格式調整分開;
  每個 commit 都應該能編譯、測試通過。
- 主幹開發,直接 commit 到 main;里程碑完成時打 tag(`m1`、`m2`…)。

## 7. 提交前自查(每次 commit 前過一遍)

```
make check   # = fmt + vet + lint + test -race
```

- [ ] 新的 exported 符號都有 godoc 註解
- [ ] 沒有無主 TODO、沒有被註解掉的死代碼
- [ ] 沒有從任何非公開授權來源複製的片段(PLAN.md §7 Clean-room 聲明)
- [ ] 測試涵蓋了這次改動對應的驗收標準
