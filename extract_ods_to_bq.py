"""ODS（PostgreSQL）→ BigQuery staging 抽取（Phase 4 E/L）

職責邊界：本腳本只做 E/L（抽取＋載入），T（轉換）交給下游 dbt。
詳見 PHASE4_EXTRACTION.md。

關鍵設計（已與設計文件對齊）：
- staging 維持「1:1 原樣落地」：不清洗、不改名、不轉型（那是 dbt stg_ 的責任）。
- 載入一律 batch load job（免費），絕不用 streaming insert（§5-①）。
- 增量切片：watermark by received_at，邊界用 `>=`（寧可重抓不漏抓，去重交給 dbt stg_）。
- watermark 採「方案 A」：從 INFORMATION_SCHEMA.PARTITIONS 推導（免費 metadata，
  且不受 require_partition_filter 限制）。讀取邏輯全部封裝在 get_watermark()——
  這是將來改「方案 B（獨立 watermark 表，精確到 timestamp，供分鐘級微批）」的唯一接縫。
- 本腳本沒有 advance_watermark()：方案 A 的 watermark 由 staging 自身推導，
  load 完下次呼叫 get_watermark() 即自然反映新資料。換 B 時才需新增 advance 步驟。
"""

import time

import structlog
from google.cloud import bigquery

from bq import get_bq_client
from config import settings
from database import SessionLocal
from models import ODS

logger = structlog.get_logger()

# --- BQ 目的地 ---
# PROJECT 隨部署環境而異（dev/prod 可能是不同專案），且是會曝光的基礎設施座標，
# 故由 settings 注入（BQ_PROJECT），真實 ID 不寫死進版控。
# DATASET / TABLE / LOCATION 是結構性、穩定的架構決定（US 見 §4），留模組常數。
PROJECT = settings.bq_project
DATASET = "staging"
TABLE = "orders"
LOCATION = "US"  # 所有 dataset 一致建在 US（§4，避免跨 location 查詢報錯）
STAGING_TABLE = f"{PROJECT}.{DATASET}.{TABLE}"

# 分區與叢集（已定）
PARTITION_FIELD = "received_at"
CLUSTERING_FIELDS = ["order_id", "has_clean_error"]


# --- 欄位單一真相來源（決定 2）---------------------------------------------
# (ODS 欄位名, BQ 型別, mode)；1:1 鏡射 models.py 的 ODS。
# ensure_staging_table() 由此建 schema，_to_bq_dict() 由此決定序列化方式，
# 兩處共用同一份清單，避免 schema 與列轉換漂移。
FIELDS: list[tuple[str, str, str]] = [
    ("id", "INTEGER", "REQUIRED"),            # ODS PK，保留以維持 1:1 原樣鏡射
    ("received_at", "TIMESTAMP", "REQUIRED"),  # 分區欄位
    ("raw_id", "INTEGER", "REQUIRED"),         # 與 ODS NOT NULL 鏡射；REQUIRED 兼作 fail-loud 護欄
    # 訂單主體
    ("order_id", "STRING", "REQUIRED"),        # 叢集欄位①
    ("order_date", "DATE", "NULLABLE"),
    ("ship_mode", "STRING", "NULLABLE"),
    ("order_status", "STRING", "NULLABLE"),
    ("delivery_date", "DATE", "NULLABLE"),
    ("delivery_days", "INTEGER", "NULLABLE"),
    ("returned", "BOOL", "NULLABLE"),
    # 顧客
    ("customer_id", "STRING", "NULLABLE"),
    ("customer_name", "STRING", "NULLABLE"),
    ("age", "INTEGER", "NULLABLE"),
    ("gender", "STRING", "NULLABLE"),
    ("membership_tier", "STRING", "NULLABLE"),
    ("registration_date", "DATE", "NULLABLE"),
    ("acquisition_channel", "STRING", "NULLABLE"),
    ("newsletter_subscribed", "BOOL", "NULLABLE"),
    ("preferred_payment_method", "STRING", "NULLABLE"),
    ("preferred_device", "STRING", "NULLABLE"),
    # 地址
    ("country", "STRING", "NULLABLE"),
    ("region", "STRING", "NULLABLE"),
    ("state", "STRING", "NULLABLE"),
    ("city", "STRING", "NULLABLE"),
    ("postal_code", "STRING", "NULLABLE"),
    # 金流
    ("payment_method", "STRING", "NULLABLE"),
    ("tax_pct", "FLOAT", "NULLABLE"),
    # 行為
    ("device_used", "STRING", "NULLABLE"),
    ("customer_rating", "FLOAT", "NULLABLE"),
    ("is_repeat_customer", "BOOL", "NULLABLE"),
    # items 整包
    ("items", "JSON", "NULLABLE"),
    # 清洗錯誤標籤（業務值品質）
    ("has_clean_error", "BOOL", "REQUIRED"),   # 叢集欄位②
    ("clean_error_message", "JSON", "NULLABLE"),
    # Schema drift 標籤
    ("has_schema_drift", "BOOL", "REQUIRED"),
    ("schema_drift_message", "JSON", "NULLABLE"),
    ("unmapped_fields", "JSON", "NULLABLE"),
    # 不可變 metadata
    ("dq_rule_version", "STRING", "NULLABLE"),
    ("source_client_id", "STRING", "NULLABLE"),
]


def _bq_schema() -> list[bigquery.SchemaField]:
    """由 FIELDS 推出 BQ schema；建表與 load job 共用同一份（單一真相來源）。"""
    return [bigquery.SchemaField(name, bq_type, mode=mode) for name, bq_type, mode in FIELDS]


def ensure_staging_table(client: bigquery.Client) -> None:
    """建立 dataset（US）與 staging 實體表（分區+叢集+保險絲）。冪等。"""
    # dataset 明確指定 location，不靠預設（§4）
    dataset = bigquery.Dataset(f"{PROJECT}.{DATASET}")
    dataset.location = LOCATION
    client.create_dataset(dataset, exists_ok=True)

    table = bigquery.Table(STAGING_TABLE, schema=_bq_schema())

    # 分區：received_at 每天一塊 + 強制分區過濾（保險絲：沒帶 received_at 過濾的查詢直接報錯）
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=PARTITION_FIELD,
        require_partition_filter=True,
    )
    table.clustering_fields = CLUSTERING_FIELDS

    client.create_table(table, exists_ok=True)


def get_watermark(client: bigquery.Client):
    """方案 A：從 INFORMATION_SCHEMA.PARTITIONS 取最新分區（天粒度）。

    回傳最新分區當天 00:00:00 UTC 的 timestamp；表空/不存在 → None（觸發首次全量抽取）。

    - 免費(近似):查 INFORMATION_SCHEMA 是 metadata 操作,不掃 staging 本體;on-demand 下每次有 10 MB 最低計費門檻,金額可忽略,且通常在免費額度內。
    - 不受 require_partition_filter 限制：查的是 metadata view，非 staging 表本體。
    - 天粒度：每次抽取會重抽「最新分區整天」，配 `>=` 邊界與 dbt stg_ 去重 → 不漏不錯。

    ★ 換方案 B 的唯一接縫：屆時改為讀獨立 watermark 表（精確 timestamp），
      並在 load 成功後新增 advance_watermark()；main() 的呼叫不變。
    """
    sql = f"""
        SELECT PARSE_TIMESTAMP('%Y%m%d', MAX(partition_id)) AS wm
        FROM `{PROJECT}.{DATASET}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name = '{TABLE}'
          AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
    """
    row = next(iter(client.query(sql).result()), None)
    return row.wm if row else None


def _json_field(value):
    """JSON 型別欄位（items / *_message / unmapped_fields）的序列化。

    實機驗證結論（2026-06）：BQ JSON 欄位經 load_table_from_json 載入時，
    要傳「原生 Python 物件」。值來自 psycopg2 對 JSONB 的解析，本就是 list/dict，
    直接回傳即可——client 寫 NDJSON 時會嵌成原生 JSON，BQ 存成 JSON_TYPE=array/object。
    （曾試 json.dumps → BQ 存成 JSON 字串純量，JSON_TYPE=string、下游 [0] 索引失效。）
    """
    return value


def _serialize(value, bq_type: str):
    """單筆值 → load_table_from_json 可吃的型別。"""
    if value is None:
        return None
    if bq_type in ("TIMESTAMP", "DATE"):
        return value.isoformat()  # tz-aware → 帶 +00:00（UTC 契約）；DATE → YYYY-MM-DD
    if bq_type == "JSON":
        return _json_field(value)
    return value  # STRING / INTEGER / FLOAT / BOOL 直接傳


def _to_bq_dict(ods_row) -> dict:
    """ODS ORM 物件 → BQ NDJSON 列。欄位與序列化方式皆由 FIELDS 決定。"""
    return {name: _serialize(getattr(ods_row, name), bq_type) for name, bq_type, _ in FIELDS}


def extract_from_ods(watermark) -> list[dict]:
    """讀 ODS 增量。watermark=None → 全量首抽。

    staging 維持 1:1 原樣：只查、不清洗/改名/轉型。
    """
    session = SessionLocal()
    try:
        q = session.query(ODS)
        if watermark is not None:
            q = q.filter(ODS.received_at >= watermark)  # 邊界用 >=
        rows = q.order_by(ODS.received_at).all()
        return [_to_bq_dict(r) for r in rows]
    finally:
        session.close()  # 沿用專案手動 session 慣例


def load_to_staging(client: bigquery.Client, rows: list[dict]) -> int:
    """batch load（免費）、只 append。表已預建好分區/叢集，load 自動沿用。回傳實際載入列數。"""
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # 冪等靠 append + dbt stg_ 去重，不在 E/L 做 MERGE（保持原樣落地）
        schema=_bq_schema(),
        # additive evolution：ODS 加 nullable 欄並更新 FIELDS 後，新欄由 load job
        # 自動補進既有表（ensure_staging_table 的 create 只建不改）。改名/改型別/改分區
        # 不在此處理——交給 dbt stg_ 或重建表（見「ODS schema 演進」策略）。
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = client.load_table_from_json(rows, STAGING_TABLE, job_config=job_config)
    job.result()  # 等完成；失敗則拋出，由呼叫端決定重跑
    return job.output_rows


def main() -> None:
    if not PROJECT:
        raise RuntimeError("BQ_PROJECT 未設定：請在 .env 設定 BQ_PROJECT=<你的 GCP 專案 ID>")

    t0 = time.monotonic()
    client = get_bq_client()

    ensure_staging_table(client)
    watermark = get_watermark(client)
    rows = extract_from_ods(watermark)

    if not rows:
        logger.info("extract_skip", watermark=str(watermark), reason="no_new_rows")
        return

    loaded = load_to_staging(client, rows)
    logger.info(
        "extract_done",
        watermark=str(watermark),
        extracted=len(rows),
        loaded=loaded,
        elapsed_s=round(time.monotonic() - t0, 2),
    )
    # 方案 A：無 advance_watermark —— staging 本身即 watermark


if __name__ == "__main__":
    main()
