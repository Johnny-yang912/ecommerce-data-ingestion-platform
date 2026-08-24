"""失敗通知：把「這個紅代表什麼」送到失敗發生的當下。

⚠️ 本檔受兩條與 DAG 檔相同的紀律約束：

  · **不得 import 任何專案模組**（同 orders_analytics_daily.py 檔頭 ①）。
    dag-processor 每隔幾十秒重新解析一次 dags 資料夾，top-level `import config`
    會讓整條 DAG 因缺 DB_URL 而【從 UI 消失】——那比失敗更危險，因為沒有紅燈。
  · **檔名的 `_` 前綴是必要的**。tests/test_dags.py 的 test_every_dag_file_produces_a_dag
    以 `glob("*.py")` 排除 `_` 開頭的檔案，再斷言「檔案數 <= DAG 數」。不加前綴，
    這支不產出 DAG 的模組會讓那條斷言紅。

────────────────────────────────────────────────────────────────────────────
① 為什麼預設通道是 log，而不是先掛一個指向 Slack 的 notifier ⭐

   掛一個指向不存在 connection 的 notifier，結果是：task 紅 → callback 觸發 →
   拋例外 → Airflow 記進 log → **沒有任何人收到東西**。那正是 collector-config.yaml
   與 docker-compose.yml 的 otel-collector 段落反覆強調的那個形態——
   **「以為有告警但其實沒有」比「明擺著沒有告警」危險得多。**

   所以預設是一個必然送得出去的通道（log），真實通道由環境變數啟用。
   代價是預設狀態下沒有真正的告警——這一點寫進 docs/zh-TW/design/orchestration.md 的口徑裡，
   不在 README 上記一個名不副實的勾。

② 這【不是】告警，而是告警內容的來源

   log 通道與被監控系統同故障域、且需要人主動去看——它不滿足告警的定義。
   它的價值在**訊息本身**：Airflow 的 task log 本來就有 traceback，callback 多帶的是
   「這個紅代表什麼、該做什麼」。那句話目前只存在於各 DAG 的 docstring 裡，
   而**出事的人不會去讀 docstring**。換上真通道之後，這些文案原封不動繼續用。

③ 涵蓋範圍：只有「跑了而且失敗」⭐

   on_failure_callback 需要一個真的執行過的 task run。以下三種抓不到：
     · **該跑卻沒跑**——沒有 run 就沒有 failure。Airflow 3 已移除 SLA 功能
       （`sla` 參數還留在 BaseOperator 簽章裡，但別依賴它）。
     · **機器關機／斷網**——callback 與被監控系統住在同一台機器上。
     · **warn 等級**——`dbt source freshness` 的 warn 是 exit 0，task 是綠的。
   這三個洞要靠雲端側的 absent 告警補，見 docs/zh-TW/design/orchestration.md。

④ 通道是接縫，不是設定的終點

   `_deliver()` 是唯一知道「怎麼送」的地方，其餘程式碼只知道「送什麼」。
   webhook 的 payload 用 `{"text": ...}`（Slack Incoming Webhook 的格式，多數服務
   也吃這個形狀）；換 Discord（要 `content`）或 ntfy（吃純文字 body）就改那一行。

⑤ 為什麼用環境變數而不是 Airflow Connection

   Connection 能讓 Airflow 自動遮蔽 log 裡的密碼，這一點確實比較好。但它把這個檔
   綁死在特定 provider 的 notifier 上，而通道的重點正是可換。折衷是：**URL 永遠不進
   log**（連失敗時也只記 status code），所以遮蔽的價值在這裡趨近於零。
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable

logger = logging.getLogger(__name__)

CHANNEL_LOG = "log"
CHANNEL_WEBHOOK = "webhook"

# 空字串與未設定等價：compose 透傳一個沒填的變數時拿到的是空字串，
# 若只判斷 `is None` 會誤判成「已設定」而每次都送到空 URL。
_WEBHOOK_URL = os.environ.get("NOTIFY_WEBHOOK_URL", "").strip()

# 逾時是硬需求而非保險：LocalExecutor 下 callback 跑在 task 行程裡，
# 一個吊住的 POST 會佔住執行槽。3 秒足夠任何正常的 webhook。
_WEBHOOK_TIMEOUT_S = float(os.environ.get("NOTIFY_WEBHOOK_TIMEOUT_S", "3"))

CHANNEL = CHANNEL_WEBHOOK if _WEBHOOK_URL else CHANNEL_LOG

# 例外訊息截斷長度。完整 traceback 在 task log 裡，通知只需要夠辨認是哪一類錯。
_MAX_EXCEPTION_CHARS = 200


def _summarize_exception(exc: Any) -> str:
    if exc is None:
        return "（無例外物件；多為非零 exit 或逾時）"
    text = f"{type(exc).__name__}: {exc}"
    if len(text) > _MAX_EXCEPTION_CHARS:
        text = text[:_MAX_EXCEPTION_CHARS] + "…"
    return text


def _format(context: dict, meaning: str) -> str:
    """組出通知內容。所有欄位都用 getattr/get 取——context 的形狀跨 Airflow 版本
    會變，而**通知在這裡拋例外等於通知消失**，比欄位缺一個糟得多。"""
    ti = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")

    dag_id = getattr(ti, "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    try_number = getattr(ti, "try_number", "?")
    run_id = getattr(dag_run, "run_id", "?")

    lines = [
        f"🔴 {dag_id} / {task_id} 失敗",
        f"run={run_id} try={try_number}",
        f"意義：{meaning}",
        f"錯誤：{_summarize_exception(context.get('exception'))}",
    ]

    # log_url 在部分 Airflow 版本的 task 執行期物件上不存在，有才附上。
    log_url = getattr(ti, "log_url", None)
    if log_url:
        lines.append(f"log: {log_url}")

    return "\n".join(lines)


def _deliver(text: str) -> None:
    """唯一知道「怎麼送」的地方（見檔頭 ④）。

    ⚠️ 本函式不得對外拋例外。Airflow 確實會捕捉 callback 的例外並記 log，
       但那條 log 長得像「通知系統壞了」而不是「你的 DAG 壞了」，會蓋掉真正的訊息。
    """
    if CHANNEL == CHANNEL_LOG:
        # channel 一併記上：失敗當下看到 channel=log 就知道「沒有人被通知」，
        # 不必回頭翻設定確認。這取代了在 import 時記一行啟動訊息的做法——
        # dag-processor 每次解析都會重新 import 本模組，那樣會把日誌洗掉。
        logger.error("dag_failure_notification channel=%s\n%s", CHANNEL, text)
        return

    try:
        import httpx

        response = httpx.post(
            _WEBHOOK_URL,
            json={"text": text},          # Slack Incoming Webhook 的形狀，見檔頭 ④
            timeout=_WEBHOOK_TIMEOUT_S,
        )
        response.raise_for_status()
    except Exception as exc:
        # 只記 status code / 例外型別，**不記 URL**——它是這個通道唯一的秘密（檔頭 ⑤）。
        logger.error(
            "通知送出失敗（channel=%s，error=%s）；原始訊息：\n%s",
            CHANNEL,
            type(exc).__name__,
            text,
        )


def build_failure_callback(meaning: str) -> Callable[[dict], None]:
    """回傳可掛在 default_args["on_failure_callback"] 上的 callback。

    `meaning` 是這支 DAG 紅掉代表的**處置語意**，不是它在做什麼——
    README 說「每一個的紅意味著不同的處置，這就是它們分開的全部理由」，
    這個參數就是那句話的落地位置。
    """

    def _on_failure(context: dict) -> None:
        try:
            _deliver(_format(context, meaning))
        except Exception:  # noqa: BLE001 — 見 _deliver 的註解，通知永不上拋
            logger.exception("組裝失敗通知時出錯，通知已丟失")

    return _on_failure
