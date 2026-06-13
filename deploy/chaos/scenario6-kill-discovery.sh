#!/usr/bin/env bash
# 場景 6:kill 註冊中心本身,業務流量完全不受影響;重啟後訂閱自動恢復。
#
# 原理(docs/04 §控制面/資料面分離):RPC 流量走 client↔server 直連,
# 不經過 discovery;gateway 的 resolver 快取最後一次地址繼續服務,
# discovery 回來後 poll 版本不一致即自動對齊。
set -euo pipefail

URL="${URL:-http://localhost:8080/user/profile?id=1}"

echo "[1/4] 背景壓測 30 秒,統計錯誤數"
( end=$((SECONDS+30)); ok=0; err=0
  while [ $SECONDS -lt $end ]; do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$URL")
    [ "$code" = "200" ] && ok=$((ok+1)) || err=$((err+1))
  done
  echo "壓測結果(整段橫跨 discovery 掛掉與重啟):成功 $ok / 失敗 $err" ) &
LOAD=$!

sleep 3
echo "[2/4] docker kill discovery"
docker kill discovery >/dev/null

sleep 15
echo "[3/4] discovery 已掛 ~12 秒,業務流量應仍全數成功(資料面不依賴控制面)"

echo "[4/4] 重啟 discovery,服務心跳自動補註冊、gateway 訂閱自動恢復"
docker start discovery >/dev/null
wait $LOAD
echo "完成。預期失敗數為 0。"
