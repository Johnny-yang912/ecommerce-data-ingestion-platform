"""派工路徑（API → Redis → worker 取件）的存活檢查。

量的是 `raw.status='pending'` 中【最舊那一筆躺了多久】，以及超過門檻的筆數。

────────────────────────────────────────────────────────────────────────────
① ⚠️ 它量的是「有沒有人來取」，不是「有沒有寫進 ODS」——別把兩者混為一談 ⭐
   Raw 的終態有三種：`processed`、`duplicate`、`error`。**後兩者不會產生 ODS 列，
   而且那是正確行為**（duplicate 是刻意保留的監控訊號，見 CLAUDE.md 架構約束）。
   所以「Raw 有列沒有對應的 ODS 列」不能當成故障的定義——照那個定義做檢查，
   每一筆重複訂單都會讓它紅。

   `pending` 則不同：它表示**已落到 Raw、但還沒有任何 worker 把它取走**。
   這個狀態沒有任何正當理由長期存在，所以它是一個乾淨的故障訊號。
   取走之後變成 processed 還是 duplicate/error，是資料內容的事，由 DQ 機制負責。

② 為什麼量 pending 年齡，而不是 Raw 與 ODS 的 MAX(received_at) 差值 ⭐
   兩者都指向派工這一段，但差值是**落後的彙總**（只告訴你落後多少），
   pending 年齡則**指認出卡住的那些列並數得出來**。故障排查時要的是後者。
   而且差值會被 duplicate/error 汙染——那些列永遠不會讓 ODS 前進。

③ 為什麼這件事需要一個常設探針——`POST /orders` 回 202 是會說謊的 ⭐
   202 只代表 Raw 落地；後續處理由 Celery worker 非同步進行，而 `main._enqueue`
   **刻意吞掉 broker 故障**。於是 redis/worker 掛掉時：API 照常回 202、
   客戶端毫無感覺、Raw 一直長、卻沒有任何行程來取件，而且【沒有任何地方會報錯】。
   下游看得到的症狀是 ODS 不再成長，但根因在派工——本檔量的正是根因那一端。

   `seed_demo.py --require-landed-pct` 擋的是同一件事，但它**活在資料產生器裡**，
   只在 seeding 執行的那幾分鐘有效。真實系統的上游是別人的，不會有那個保護；
   平台自己必須有。**這是把一個只存在於模擬器裡的保護，搬到它該在的位置。**

④ 門檻【推導】而來，不是選一個看起來安全的數字 ⭐
   門檻的下界由**恢復路徑自己的週期**決定——低於系統自癒時間的話，會對
   「正在被正確處理」的列告警。兩條自癒路徑的最壞延遲：

     派工失敗留在 pending：恢復掃描只收超過寬限期的列，每 scan_interval 跑一次
       → 最壞 PENDING_GRACE_SECONDS + scan_interval

     被搶佔後 worker 猝死：要等 stale 判定才被重設回 pending，而重設【不會更新
     received_at】（process.scan_and_dispatch），所以它的 pending 年齡是從原始
     攝入時刻起算的
       → 最壞 STALE_PROCESSING_MINUTES + scan_interval

   取兩者較大者再加安全邊際。⚠️ 這三個常數任何一個被調動（`scan_interval_seconds`
   是 .env 可調的），門檻就必須跟著動——所以這裡【在執行時讀它們】而不是寫死一個
   數字。寫死等於把一個推導結果凍結成魔術數字，而下一個調 SCAN_INTERVAL_SECONDS
   的人不會知道要回來改這裡。

   兩個門檻常數讀自 `recovery_policy`（純常數模組），掃描間隔讀自 `config.settings`
   ——前者是演算法常數、後者是環境設定，分屬兩處是刻意的（見 recovery_policy 檔頭）。

⑤ 為什麼不把 status='processing' 也算進來
   processing 是有自己逾時機制的暫態（STALE_PROCESSING_MINUTES → 重設回 pending）。
   算進來會讓同一筆列在兩個狀態下被報兩次，而它最終仍會以 pending 的身分被本檢查
   看見——晚一點，但不會漏。**重複計數比晚幾分鐘更糟。**

⑥ exit code：0=綠、1=紅（有列超過門檻）、2=環境錯誤（連不上 DB 等）
   2 與 1 分開，是因為「探針自己壞了」與「被探測的東西壞了」需要不同處置，
   混成同一個 exit code 會讓前者被當成後者處理。

用法：
  python check_raw_pending.py
  python check_raw_pending.py --max-age-seconds 600   # 覆寫推導值（除錯用）
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from config import settings
from database import SessionLocal
from models import Raw
# ⚠️ 刻意【不】從 process import 這兩個門檻，儘管它們是它在用的：那會讓這支唯讀探針
#    連坐整條寫入路徑的依賴樹（telemetry / celery / clean），而探針的價值正在於
#    它的故障域要比被監控對象小。理由與 2026-08-17 的事故見 recovery_policy 檔頭；
#    由 tests/test_script_deps.py 釘住，不靠人記得。
from recovery_policy import PENDING_GRACE_SECONDS, STALE_PROCESSING_MINUTES

# 安全邊際：推導出來的是【最壞的合法自癒時間】，門檻壓在它身上會在恢復路徑
# 正常運作、只是剛好跑到最壞情況時誤報。多給 4 分鐘讓「正常但慢」不會變成紅燈。
SAFETY_MARGIN_SECONDS = 240


def derive_threshold_seconds() -> tuple[int, str]:
    """回傳 (門檻秒數, 推導說明)。說明會被印進 log——門檻必須自己解釋自己。"""
    scan = int(settings.scan_interval_seconds)
    enqueue_failed = PENDING_GRACE_SECONDS + scan
    worker_died = STALE_PROCESSING_MINUTES * 60 + scan
    worst = max(enqueue_failed, worker_died)
    threshold = worst + SAFETY_MARGIN_SECONDS
    explanation = (
        f"門檻推導：max(派工失敗 {PENDING_GRACE_SECONDS}+{scan}={enqueue_failed}s, "
        f"worker 猝死 {STALE_PROCESSING_MINUTES}*60+{scan}={worker_died}s) "
        f"+ 安全邊際 {SAFETY_MARGIN_SECONDS}s = {threshold}s（{threshold / 60:.1f} 分鐘）"
    )
    return threshold, explanation


def inspect_pending(threshold_seconds: int) -> tuple[int, float | None, int]:
    """回傳 (超過門檻的筆數, 最舊 pending 的年齡秒數 or None, pending 總筆數)。

    Session 依專案慣例手動管理（非 context manager），見 CLAUDE.md。
    """
    db = SessionLocal()
    try:
        cutoff = datetime.now(UTC) - timedelta(seconds=threshold_seconds)
        over = db.execute(
            select(func.count())
            .select_from(Raw)
            .where(Raw.status == "pending", Raw.received_at < cutoff)
        ).scalar_one()
        total = db.execute(
            select(func.count()).select_from(Raw).where(Raw.status == "pending")
        ).scalar_one()
        oldest = db.execute(
            select(func.min(Raw.received_at)).where(Raw.status == "pending")
        ).scalar_one()
        age = (datetime.now(UTC) - oldest).total_seconds() if oldest else None
        return over, age, total
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-age-seconds",
        type=int,
        default=None,
        help="覆寫推導出來的門檻（除錯用；正常情況下不要傳，讓它跟著恢復路徑的設定走）",
    )
    args = parser.parse_args()

    if args.max_age_seconds is not None:
        threshold = args.max_age_seconds
        print(f"▶ 門檻：{threshold}s（由 --max-age-seconds 覆寫，未使用推導值）", flush=True)
    else:
        threshold, explanation = derive_threshold_seconds()
        print(f"▶ {explanation}", flush=True)

    try:
        over, oldest_age, total = inspect_pending(threshold)
    except SQLAlchemyError as e:
        print(f"✗ 無法查詢 Raw（連線或查詢失敗）：\n  {e}", file=sys.stderr)
        return 2

    oldest_desc = f"{oldest_age:.0f}s（{oldest_age / 60:.1f} 分鐘）" if oldest_age is not None else "無 pending"
    print(f"▶ pending 總數 {total}｜最舊 {oldest_desc}｜超過門檻 {over} 筆", flush=True)

    if over == 0:
        print("✅ 派工路徑正常：沒有任何 pending 超過門檻。")
        return 0

    print(
        f"❌ {over} 筆 pending 已超過 {threshold}s 仍未被任何 worker 取走——"
        "恢復路徑本應在這個時間內把它們重新派出去。\n"
        "  可能原因：redis 不可用、Celery worker 未啟動或已死、Beat（恢復掃描）未啟動。\n"
        "  ⚠️ 這個狀態下 API 仍會對新訂單回 202（_enqueue 吞掉 broker 故障），"
        "所以客戶端與上游都不會察覺。",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
