#!/usr/bin/env bash
# 場景 2:kill 一個 account 副本,壓測錯誤率不飆升、流量自動轉移。
#
# 原理(見 docs/05 §兩層故障感知):TCP 斷線是 gRPC 連線層「立即」感知的,
# 死副本瞬間被移出 ready 集合;註冊中心 90 秒剔除只是最終一致的兜底。
set -euo pipefail

VICTIM="${VICTIM:-account2}"
URL="${URL:-http://localhost:8080/user/profile?id=1}"

echo "[1/3] 背景壓測 15 秒,統計 HTTP 狀態分布"
( end=$((SECONDS+15)); ok=0; err=0
  while [ $SECONDS -lt $end ]; do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$URL")
    [ "$code" = "200" ] && ok=$((ok+1)) || err=$((err+1))
  done
  echo "壓測結果:成功 $ok / 失敗 $err" ) &
LOAD=$!

sleep 3
echo "[2/3] docker kill $VICTIM"
docker kill "$VICTIM" >/dev/null

wait $LOAD
echo "[3/3] 觀察 /debug/backends:$VICTIM 已不在 ready 列表,流量落在其餘副本"
curl -s 'http://localhost:8080/debug/backends'; echo
echo "提示:docker start $VICTIM 後它會自動重新註冊、重新接到流量(場景 1)。"
