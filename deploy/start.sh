#!/usr/bin/env bash
# 單一容器的啟動腳本：依序啟動 Redis、派車服務、司機模擬器、網頁後端。
# 任何一個程式結束，整個容器就跟著結束，讓平台看得到錯誤而不是半殘狀態。
set -euo pipefail

PORT="${PORT:-7860}"
CARS="${CARS:-100}"
SPEED="${SPEED:-150}"

# Redis 只聽容器內的 127.0.0.1，不對外開放；不落地存檔（重啟即清空）。
# notify-keyspace-events Ex：逾時改派與離線偵測需要 key 過期通知。
redis-server --bind 127.0.0.1 --port 6379 \
    --notify-keyspace-events Ex \
    --save "" --appendonly no --dir /tmp &

for _ in $(seq 1 50); do
    if redis-cli -h 127.0.0.1 ping >/dev/null 2>&1; then break; fi
    sleep 0.2
done
redis-cli -h 127.0.0.1 ping >/dev/null

python dispatcher.py --name dispatcher-1 &
python dispatcher.py --name dispatcher-2 &
# 容器裡沒有鍵盤，stdin 接 /dev/null；統計改成每分鐘印一次，避免洗版
python simulator.py --cars "$CARS" --speed "$SPEED" --report 60 </dev/null &
python -m uvicorn app:app --host 0.0.0.0 --port "$PORT" &

wait -n
echo "有程式意外結束，停止容器" >&2
exit 1
