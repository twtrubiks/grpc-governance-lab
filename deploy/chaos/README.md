# 混沌腳本

對照 README「能演示什麼(六個場景)」。腳本檔名沿用場景編號,因此會跳過 1 與 4 ——
這兩個場景各只有一行手動指令,不需要腳本。

| 場景 | 演示 | 腳本 |
|---|---|---|
| 1 | 服務發現:新副本自動接流量 | 無(手動 `docker run`,見 README 場景 1) |
| 2 | 故障轉移:kill 副本、流量自動轉移 | `scenario2-kill-replica.sh` |
| 3 | 動態加權:注入延遲、占比自動下滑 | `scenario3-latency.sh` |
| 4 | 錯誤碼透傳:最內層 `-404` 穿到 HTTP | 無(手動 `curl id=9999`,見 README 場景 4) |
| 5 | Guard 自我保護:大量心跳消失、拒絕剔除 | `scenario5-partition.sh` |
| 6 | 控制面/資料面分離:kill 註冊中心、業務無感 | `scenario6-kill-discovery.sh` |

前置:先 `docker compose up --build -d`。各腳本開頭的註解寫了原理與前置步驟,
多數需要另開視窗持續壓測並用 `watch` 觀測 `/debug/backends`(細節見 README)。
