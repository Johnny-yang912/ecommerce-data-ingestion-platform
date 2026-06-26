#!/bin/bash
# restart_test.sh
# 測試 server 重啟後 pending 記錄是否會卡住（無 recovery 機制）
#
# 流程：
#   1. 啟動 server
#   2. 送 500 筆訂單（背景執行）
#   3. 等 2 秒（讓部分訂單寫入 DB、部分背景任務跑完）
#   4. SIGKILL 殺掉 server（模擬 crash）
#   5. 查 DB → 確認有 pending 記錄
#   6. 重啟 server，等 5 秒
#   7. 再查 DB → pending 沒有減少 → 無自動 recovery

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_SERVER1="/tmp/uvicorn_before_crash.log"
LOG_SERVER2="/tmp/uvicorn_after_restart.log"
LOG_LOAD="/tmp/load_test_restart.log"

# DB 連線字串：不在腳本硬編（避免密碼進版控）。優先吃環境變數 DB_URL；
# 未設則從專案 .env 讀取（與 config.settings 同一真相來源）。兩者皆無則中止並提示。
if [[ -z "${DB_URL:-}" && -f "$PROJECT_DIR/.env" ]]; then
    DB_URL="$(grep -E '^DB_URL=' "$PROJECT_DIR/.env" | head -n1 | cut -d= -f2-)"
fi
: "${DB_URL:?請設定環境變數 DB_URL，或在專案根目錄 .env 提供 DB_URL}"

cd "$PROJECT_DIR"

# ── Step 0: 清理殘留 ──────────────────────────────────────────────────────────
echo "▶ 清理殘留 uvicorn 程序..."
pkill -f "uvicorn main:app" 2>/dev/null && sleep 1 || true

# ── Step 1: 啟動 server ───────────────────────────────────────────────────────
echo "▶ 啟動 server（無 --reload）"
uvicorn main:app --port 8000 > "$LOG_SERVER1" 2>&1 &
SERVER_PID=$!
echo "  PID=$SERVER_PID，等待啟動..."
sleep 2

# 確認有起來
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "  [ERROR] server 未啟動，請檢查 $LOG_SERVER1"
    exit 1
fi
echo "  server 就緒"

# ── Step 2: 送出 500 筆訂單（背景執行）────────────────────────────────────────
echo ""
echo "▶ 送出 500 筆訂單（背景執行中）..."
python load_test.py --n 500 --concurrency 50 > "$LOG_LOAD" 2>&1 &
LOAD_PID=$!

# ── Step 3: 等 2 秒 ───────────────────────────────────────────────────────────
echo "  等 2 秒（讓訂單寫入 DB、部分背景任務開始執行）..."
sleep 2

# ── Step 4: 強制終止 server ───────────────────────────────────────────────────
echo ""
echo "▶ SIGKILL server（模擬 crash）"
kill -9 "$SERVER_PID" 2>/dev/null || true
echo "  server 已被殺（PID=$SERVER_PID）"

wait "$LOAD_PID" 2>/dev/null || true   # 等 load test 結束（會有連線錯誤）

# ── Step 5: 查 DB（server 死亡後）────────────────────────────────────────────
echo ""
echo "▶ DB 狀態（crash 後）："
psql "$DB_URL" -c "SELECT status, COUNT(*) FROM raw GROUP BY status ORDER BY status;"

# ── Step 6: 重啟 server ───────────────────────────────────────────────────────
echo ""
echo "▶ 重啟 server..."
uvicorn main:app --port 8000 > "$LOG_SERVER2" 2>&1 &
NEW_PID=$!
echo "  新 PID=$NEW_PID，等待 5 秒..."
sleep 5

# ── Step 7: 再查 DB ───────────────────────────────────────────────────────────
echo ""
echo "▶ DB 狀態（重啟後 5 秒）："
psql "$DB_URL" -c "SELECT status, COUNT(*) FROM raw GROUP BY status ORDER BY status;"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  判斷標準："
echo "  pending 筆數 不變 → 無自動 recovery（符合預期）"
echo "  pending 筆數 歸零 → 有自動 recovery（代表有實作）"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  load test log : $LOG_LOAD"
echo "  server log    : $LOG_SERVER2"
echo ""
echo "server 仍在執行 (PID=$NEW_PID)，按 Ctrl+C 停止"
wait "$NEW_PID"
