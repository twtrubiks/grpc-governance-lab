#!/usr/bin/env bash
# 場景 3:對 account1 注入延遲,觀察它的流量占比自動下降、移除後回升。
#
# 前置:docker compose up -d 已起好;另開一個終端跑壓測:
#   while true; do curl -s 'http://localhost:8080/user/profile?id=1' >/dev/null; done
# 然後執行本腳本,全程開著 watch 看分布:
#   watch -n1 "curl -s http://localhost:8080/debug/backends | jq '.backends[]|select(.service==\"account\")|{addr,ewt:.effective_weight,picks,lat:.latency_ms}'"
set -euo pipefail

# 注入延遲必須「低於 gateway 聚合超時」(預設 200ms,見 cmd/gateway -timeout):
# 等於或超過超時,呼叫會在 deadline 邊界被取消(code -498),變成「失敗降權」而非
# 本場景要演示的「慢但成功、純靠延遲降權」。150ms 安全落在超時內、零錯誤。
ADMIN="${ACCOUNT1_ADMIN:-http://localhost:9101}"
DELAY="${DELAY:-150}"

echo "[1/3] 對 account1 ($ADMIN) 注入 ${DELAY}ms 延遲"
curl -s -X POST "$ADMIN/inject?delay=$DELAY" && echo

echo "[2/3] 持續壓測 30 秒,觀察 account1 流量占比明顯下滑(實測約掉到 15~20%,權重重算需數個週期)"
echo "      開另一個視窗看:watch -n1 'curl -s http://localhost:8080/debug/backends'"
sleep 30

echo "[3/3] 移除延遲,account1 權重在約 30 秒內隨滑動視窗過期而回升"
curl -s -X POST "$ADMIN/reset" && echo
echo "完成。"
