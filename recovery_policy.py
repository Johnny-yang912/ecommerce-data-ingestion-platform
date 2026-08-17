"""恢復路徑的時間門檻——這兩個數字的唯一真相來源。

它們回答的是同一個問題的兩半：**一筆 Raw 列躺多久才算「不正常」**。
`process.scan_and_recover` 用它們決定要撿哪些列回來重派，
`check_raw_pending.py` 用它們推導「超過多久還是 pending 就該告警」的門檻。

────────────────────────────────────────────────────────────────────────────
為什麼它們住在自己的模組，而不是留在 `process.py`（它們原本的位置）⭐

`check_raw_pending.py` 是**唯讀探針**，但它要用這兩個門檻推導告警線（不寫死，
見該檔 ④）。原本的 `from process import ...` 讓它連坐整條寫入路徑的依賴樹——
2026-08-17 的 OTel 上線就是這樣把它弄紅的：`process.py` 多了一行
`from telemetry import ...`，於是探針在還沒查任何東西以前就死在
`ModuleNotFoundError: No module named 'opentelemetry'`（Airflow 的
`venvs/analytics` 當時是舊映像，還沒裝上那批新依賴）。

**單一個常數把一支探針綁到了它根本不執行的程式碼路徑上。** 拆開之後：

    check_raw_pending.py → recovery_policy（純常數，零第三方依賴）
    process.py           → recovery_policy（同一份值，不再是定義者）

於是探針的依賴樹裡不再有 telemetry / celery / clean，也就不會再被寫入路徑的
依賴變動連坐。這比「把缺的套件補進探針的環境」更根本：後者治的是這一次的症狀，
而下一個往 `process.py` 加 import 的人不會知道有一支探針掛在後面。
`tests/test_script_deps.py` 釘住這件事，讓它不必靠人記得。

⚠️ 為什麼不搬進 `config.py`：那裡的設計邊界寫得很明白——只放「會因部署環境而異」
的環境設定，不放演算法常數。這兩個是程式行為的一部分（改動要走 code review，
不該能被 `.env` 覆寫），放進去會侵蝕那條邊界。而 `scan_interval_seconds`
反過來確實屬於環境設定，所以它留在 config——門檻推導同時讀兩邊是正確的。
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

# 被搶佔後多久沒完成就視為 worker 猝死，重設回 pending 讓別人接手。
# ⚠️ 這個判定問的是「這次處理跑了多久」，故以 processing_started_at 為基準。
STALE_PROCESSING_MINUTES = 10

# 剛攝入的 pending 不由掃描接手：正常情況下攝入路徑會在毫秒內把它派出去，
# 掃描此時介入只會為同一筆多送一則訊息（CAS 擋得住，但純屬浪費）。
# 只有「躺了超過寬限期還是 pending」才代表快路徑真的失手了。
# ⚠️ 這裡用 received_at 是對的——問的正是「這筆資料躺了多久」；
#    而 stale 判定問的是「這次處理跑了多久」，故用 processing_started_at。
#    兩個問題不同，基準不同，別把它們混在一起（見 process.scan_and_recover）。
PENDING_GRACE_SECONDS = 60
