"""Proposal B：規則升版後的重評估（`quality_events` 的事件產生端）

下游的回流路徑早就就緒——`int_orders` 每次 run 都會把「ODS 快照 ⊕ `quality_events`
最新事件」合成成有效品質狀態。缺的一直是**產生事件的那一端**，本腳本就是它。
（見 docs/zh-TW/design/data-quality.md〈Proposal B：不重跑的重評估流程〉。）

    BQ int_ 層（誰值得重評估） ──► business_clean（新版規則）──► PG quality_events（只 append）
                                                                        │
                                              下次 dbt run，int_orders 合成有效狀態 → 流回 Gold

## 為什麼候選讀 BQ、狀態卻讀 PG（兩個來源不是不一致，是分工）⭐

- **候選發現讀 BQ 的 `int_` 層**：① 「誰還卡著」的正確基準是**有效品質狀態**而非
  `has_clean_error` 字面快照，而 `int_orders` / `int_orders_quarantine` 已經算好了——
  直接用它，重評估與 Row Filter 對「誰被隔離」的認定**在定義上不可能分歧**；自己再實作
  一次「每個 raw_id 取最新事件」就是那段共用邏輯的第三份複製，且住在 dbt 外面、
  `assert_orders_split_is_partition` 管不到（ecommerce_dbt/README.zh-TW §5.3）。
  ② 這是分析型全掃，打在 ODS 上會與 `POST /orders` 的熱路徑搶資源——把它移走正是雲端層存在的理由。

- **狀態判定（有沒有變）讀 PG 的 `quality_events`**：BQ 是**有保留期的鏡射**
  （sandbox 強制 60 天，見 docs/zh-TW/design/cloud-layer.md）。若拿它判斷「狀態變了沒」，事件過期時
  會誤判成「沒有事件」→ 對已 promote 的記錄再 append 一次 promotion →
  污染 `rpt_quality_events_daily.promotions`，而那正是〈歷史指標為何不會被追溯性改寫〉
  要保護的數字，且 append-only 刪不掉。**冪等的保證只能來自寫入目標本身，不能來自它的鏡射。**

這對「BQ 端過濾 vs PG 端判定」的分工，與攝入層的 pre-check + UNIQUE 是同一個手法：
便宜的快路徑負責把大多數不必處理的先剔掉，權威來源負責正確性兜底。

## 冪等：只在狀態真的改變時才 append ⭐

`decide_target_state()` 是唯一的判定點，狀態沒變就回 None、不寫事件。這一條規則同時買到：
重跑不會灌水 `promotions`、狀態機每條邊都可達（含 `re_quarantined`）、不需要第四個狀態容器、
`quality_events` 維持「狀態轉移日誌」而非「作業執行日誌」（後者屬 structlog／Airflow task log，
對應 DQ 文件〈歷史品質指標〉層次一與層次二的分工）。

## 兩條不可跨越的線

1. **Bounded Writeback**：只 append `quality_events`，永不改 ODS。ODS 是攝入當下的
   不可變錨點，`has_clean_error=TRUE, dq_rule_version=v1` 就該永遠停在那裡。
2. **`permanently_rejected` 是人工的終局決定，自動任務不得推翻。** 這條在 PG 端
   （權威來源）強制，BQ 端的同名過濾只是省流量的快路徑。

## 判定不了的那類：`NON_REPRODUCIBLE_CODES`

有些規則在標記的同時把值就地正規化掉（`NON_FINITE_NUMBER` 把 NaN/Inf 設為 None），
重評估時輸入已是清理後的值 → 原判定條件結構性地無法再觸發 → 必然「通過」。但那是**證據
消失**而非規則放寬，據此 promote 會讓一筆金額其實是缺的訂單流進 Gold。這類記錄一律不
自動 promote，只計數回報——要救它必須從 Raw 重產值，按定義是 Proposal C 的領域。

執行方式（**預設 dry-run，要寫入必須顯式 `--commit`**）：

    python reevaluate_quality.py                          # 只看會發生什麼
    python reevaluate_quality.py --commit                 # 真的寫
    python reevaluate_quality.py --commit --expect-rule-version v3   # 防打錯部署版本
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import structlog

from bq import get_bq_client
from clean import DQ_RULE_VERSION, NON_REPRODUCIBLE_CODES, business_clean
from config import settings
from database import SessionLocal
from models import QualityEvent
from schema import ODSOrder

logger = structlog.get_logger()

PROJECT = settings.bq_project
DBT_DATASET = settings.bq_dbt_dataset

# --- 狀態機（值域與 docs/zh-TW/design/data-quality.md〈狀態機〉一致）------------------------
STATE_QUARANTINED = "quarantined"
STATE_PROMOTED = "promoted"
STATE_RE_QUARANTINED = "re_quarantined"
STATE_PERMANENTLY_REJECTED = "permanently_rejected"

EVENT_PROMOTION = "promotion"
EVENT_RE_QUARANTINATION = "re_quarantination"

# 目標狀態 → 事件類型。`rejection`（→ permanently_rejected）刻意不在這裡：
# 那是人工放棄的路徑，本腳本永遠不會產生它（見檔頭「兩條不可跨越的線」）。
EVENT_TYPE_BY_TARGET = {
    STATE_PROMOTED: EVENT_PROMOTION,
    STATE_RE_QUARANTINED: EVENT_RE_QUARANTINATION,
}

# 候選查詢要撈的欄位：ODS 側直接由 ODSOrder 的宣告推導，不手寫清單。
# 這條鏈已經被既有測試逐段焊住：ODSOrder ↔ ODS（test_schema_db_consistency）
# ↔ ORDERS_FIELDS（test_schema_bq_consistency）↔ staging ↔ stg_ ↔ int_。
# 手寫子集的話，未來某條 business_clean 新規則碰到沒撈的欄位時，會靜默地對 NULL 評估。
ODS_FIELDS = tuple(ODSOrder.model_fields)
META_FIELDS = ("raw_id", "received_at", "clean_error_message")

# PG 的 IN (...) 有參數上限；候選在假想規模下可能上百萬筆，故分批查。
_STATE_LOOKUP_CHUNK = 1000


# ─── 候選發現（BQ）────────────────────────────────────────────────────────────

def candidate_sql(limit: Optional[int] = None) -> str:
    """兩張 int_ 表的聯集＝「攝入當下判髒、且尚未被人工終結」的全部記錄。

    - `int_orders_quarantine` + `has_clean_error`：仍被隔離者（quarantined / re_quarantined）。
    - `int_orders` + `has_clean_error`：已被 promote 者。**必須納入**，否則規則變嚴時
      `promoted → re_quarantined` 這條邊永遠不可達（DQ 狀態機的邊緣情況）。
    - `effective_quality_state != 'permanently_rejected'`：省流量的快路徑；
      真正的保證在 decide_target_state()，因為 BQ 可能是過期的鏡射。
    """
    cols = ", ".join(f"`{c}`" for c in (*META_FIELDS, *ODS_FIELDS))
    sql = f"""
        SELECT {cols}
        FROM `{PROJECT}.{DBT_DATASET}.int_orders_quarantine`
        WHERE has_clean_error
          AND effective_quality_state != '{STATE_PERMANENTLY_REJECTED}'
        UNION ALL
        SELECT {cols}
        FROM `{PROJECT}.{DBT_DATASET}.int_orders`
        WHERE has_clean_error
        ORDER BY raw_id
    """
    if limit is not None:
        sql += f"\n        LIMIT {int(limit)}"
    return sql


def fetch_candidates(client, limit: Optional[int] = None) -> list:
    return list(client.query(candidate_sql(limit)).result())


# ─── 反序列化（BQ 列 → ODSOrder）──────────────────────────────────────────────

def _json_value(value):
    """BQ JSON 欄位 → Python 物件。

    google-cloud-bigquery 對 JSON 欄位的回傳型別隨版本而異（字串或已解析物件），
    而 `ODSOrder.items` 宣告為 `Any` → Pydantic **不會**替我們攔下字串。原樣塞進去的話，
    business_clean 會對字串逐字元迭代、直到 `item.get` 才炸，錯誤現場離根因很遠。
    故在此顯式收斂成物件。
    """
    if isinstance(value, (str, bytes)):
        return json.loads(value)
    return value


def to_ods_order(row) -> ODSOrder:
    """BQ 列 → ODSOrder。只轉表示形式，不做任何清洗——值必須與攝入當下逐字相同，
    否則重評估就不是在評估同一筆資料了。"""
    data = {name: row[name] for name in ODS_FIELDS}
    data["items"] = _json_value(data["items"])
    return ODSOrder(**data)


def error_codes(clean_error_message) -> set:
    """從 ODS 的 clean_error_message（JSON 物件陣列）取出穩定的 code 集合。"""
    message = _json_value(clean_error_message)
    if not message:
        return set()
    return {e.get("code") for e in message if isinstance(e, dict)}


# ─── 狀態轉移判定（純函數，冪等的唯一來源）⭐ ─────────────────────────────────

def decide_target_state(current_state: Optional[str],
                        original_codes: set,
                        new_errors: list) -> Optional[str]:
    """回傳目標狀態；**None＝狀態沒變 → 不寫事件**。

    | 現況 | 新版判定 | 目標 | 說明 |
    |---|---|---|---|
    | permanently_rejected | 任意 | None | 人工終局決定，自動任務不得推翻 |
    | quarantined / re_quarantined | 通過 | promoted | 規則放寬，撈回來 |
    | quarantined / re_quarantined | 不過 | None | 沒變，等下一版（重跑仍不寫 → 冪等） |
    | promoted | 不過 | re_quarantined | 規則變嚴，降級 |
    | promoted | 通過 | None | 沒變（重跑不會灌水 promotions） |
    | 無事件 | 通過 | promoted | from_state=NULL，仍須記錄否則流不回 Gold |
    | 無事件 | 不過 | None | 不憑空造一筆 quarantined 事件 |

    `original_codes` 帶不可重現碼時一律視為「不通過」：那種「通過」來自證據消失
    （值在攝入時已被正規化），不是規則放寬——見檔頭與 clean.NON_REPRODUCIBLE_CODES。
    """
    if current_state == STATE_PERMANENTLY_REJECTED:
        return None

    blocked = bool(NON_REPRODUCIBLE_CODES & original_codes)
    is_clean_now = not new_errors and not blocked

    if is_clean_now:
        target = STATE_PROMOTED
    elif current_state == STATE_PROMOTED:
        target = STATE_RE_QUARANTINED
    else:
        return None   # 仍不通過、且本來就不是 promoted → 沒有轉移可記

    return target if target != current_state else None


# ─── 現況查詢（PG＝權威來源）──────────────────────────────────────────────────

def fetch_current_states(raw_ids: list) -> dict:
    """每個 raw_id 的最新事件 `to_state`；沒有事件的 raw_id 不會出現在回傳值裡。

    決勝鍵 `(event_at, id)` **必須與 `int_` 層的 `order by event_at desc, id desc` 一致**
    （此處取升冪、以最後一筆覆蓋），否則兩邊對「最新事件」的認定會分歧，
    重評估就會基於一個 Gold 看不到的狀態做決定。
    """
    latest = {}
    if not raw_ids:
        return latest

    session = SessionLocal()
    try:
        for i in range(0, len(raw_ids), _STATE_LOOKUP_CHUNK):
            chunk = raw_ids[i:i + _STATE_LOOKUP_CHUNK]
            rows = (
                session.query(QualityEvent.raw_id, QualityEvent.to_state)
                .filter(QualityEvent.raw_id.in_(chunk))
                .order_by(QualityEvent.event_at, QualityEvent.id)
                .all()
            )
            for raw_id, to_state in rows:
                latest[raw_id] = to_state
    finally:
        session.close()   # 沿用專案手動 session 慣例
    return latest


# ─── 規劃（純函數）────────────────────────────────────────────────────────────

def plan_events(rows: list, current_states: dict, event_at: datetime) -> tuple:
    """把候選列規劃成待寫事件；回傳 (events, stats)。不碰任何 I/O。"""
    events = []
    stats = {"candidates": len(rows), "promoted": 0, "re_quarantined": 0,
             "unchanged": 0, "blocked_non_reproducible": 0}

    for row in rows:
        original = error_codes(row["clean_error_message"])
        if NON_REPRODUCIBLE_CODES & original:
            stats["blocked_non_reproducible"] += 1

        _, new_errors = business_clean(
            to_ods_order(row),
            as_of=row["received_at"],   # ⭐ 不傳的話時間相依規則會隨 wall clock 漂移 → 偽 promote
        )

        current = current_states.get(row["raw_id"])
        target = decide_target_state(current, original, new_errors)
        if target is None:
            stats["unchanged"] += 1
            continue

        stats["promoted" if target == STATE_PROMOTED else "re_quarantined"] += 1
        events.append({
            "raw_id": row["raw_id"],
            "order_id": row["order_id"],
            "event_type": EVENT_TYPE_BY_TARGET[target],
            "from_state": current,
            "to_state": target,
            "rule_version": DQ_RULE_VERSION,
            "event_at": event_at,
            # promote 時沒有殘留訊息；降級時記下「為什麼現在不過」，供 RCA。
            "reason": new_errors or None,
        })

    return events, stats


# ─── 寫入（PG）────────────────────────────────────────────────────────────────

def append_events(events: list) -> int:
    """整批一個 transaction。刻意不逐筆 commit——這是一次批次決策，半套 append 會讓
    backlog 統計對不上帳，而且沒有任何 pipeline 會自動把它補完（不像攝入層有 scan recovery）。"""
    if not events:
        return 0
    session = SessionLocal()
    try:
        session.add_all([QualityEvent(**e) for e in events])
        session.commit()
        return len(events)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ─── CLI ──────────────────────────────────────────────────────────────────────

def _parse_args(argv: Optional[list]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Proposal B：以當前規則重評估被隔離的訂單，append quality_events",
    )
    parser.add_argument(
        "--commit", action="store_true",
        help="真的寫入事件。預設為 dry-run——quality_events 是 append-only，"
             "寫錯刪不掉，故把寫入設為需顯式opt-in。",
    )
    parser.add_argument("--limit", type=int, default=None, help="候選筆數上限（分批推進用）")
    parser.add_argument(
        "--expect-rule-version", default=None,
        help="斷言部署中的 DQ_RULE_VERSION 等於此值，不符即中止。"
             "手動觸發的任務容易打在舊部署上，而事件一旦寫下就帶著錯的版本號永存。",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> None:
    args = _parse_args(argv)

    if not PROJECT:
        raise RuntimeError("BQ_PROJECT 未設定：請在 .env 設定 BQ_PROJECT=<你的 GCP 專案 ID>")
    if args.expect_rule_version and args.expect_rule_version != DQ_RULE_VERSION:
        raise RuntimeError(
            f"規則版本不符：部署中為 {DQ_RULE_VERSION}，但預期 {args.expect_rule_version}。"
            "事件會帶著版本號永久留存，請先確認部署的是你要的那一版。"
        )

    t0 = time.monotonic()
    rows = fetch_candidates(get_bq_client(), args.limit)
    current_states = fetch_current_states([r["raw_id"] for r in rows])

    # 整批共用同一個 event_at：讓一次重評估在事件軸上是可辨識的一批，
    # rpt_quality_events_daily 的日彙總也不會被跨午夜的執行切成兩天。
    events, stats = plan_events(rows, current_states, datetime.now(timezone.utc))

    if not args.commit:
        logger.info("reevaluate_dry_run", rule_version=DQ_RULE_VERSION,
                    would_write=len(events), **stats,
                    elapsed_s=round(time.monotonic() - t0, 2))
        return

    written = append_events(events)
    logger.info("reevaluate_done", rule_version=DQ_RULE_VERSION, written=written, **stats,
                elapsed_s=round(time.monotonic() - t0, 2))


if __name__ == "__main__":  # pragma: no cover — 進程進入點，pytest 下依定義不可達
    main(sys.argv[1:])
