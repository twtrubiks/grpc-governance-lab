#!/usr/bin/env bash
# 場景 5:模擬網路分區,讓大量心跳同時消失,觀察註冊中心進入自我保護、
# 拒絕剔除任何節點,服務照常互打。
#
# 用 docker network disconnect 把所有 account 與 relation 從網路切離
# (它們還活著,只是連不到 discovery,心跳全部消失)——這正是「大量心跳
# 同時消失更可能是網路分區而非服務全滅」的情境(docs/03 §Guard 推導)。
set -euo pipefail

NET="${NET:-grpc-governance-lab_default}"
NODES=(account1 account2 account3 relation)

echo "[1/3] 切斷 ${NODES[*]} 與 discovery 的網路(模擬分區)"
for n in "${NODES[@]}"; do docker network disconnect "$NET" "$n" || true; done

echo "[2/3] 等待超過剔除門檻(預設 90s);觀察 discovery 日誌應出現「進入自我保護」"
echo "      docker compose logs -f discovery   # 找 'Guard 進入自我保護'"
echo "      此時 curl 'http://localhost:7171/fetch?service=account' 仍回完整 3 副本"
sleep 95
curl -s 'http://localhost:7171/fetch?service=account'; echo

echo "[3/3] 恢復網路,Guard 在下個統計窗自動退出自保"
for n in "${NODES[@]}"; do docker network connect "$NET" "$n" || true; done
# disconnect/connect 會重配容器 IP,gateway 的 gRPC 連線可能卡在舊 IP;
# 補一個 restart 讓資料面完全恢復(Guard 行為本身不受影響)。
docker compose restart gateway || true
echo "完成。對照:若只切斷單一節點(正常故障),它會照常被剔除。"
