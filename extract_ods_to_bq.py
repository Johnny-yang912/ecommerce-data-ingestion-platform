"""ODS（PostgreSQL）→ BigQuery staging 抽取（Phase 4 E/L）

職責邊界：本腳本只做 E/L（抽取＋載入），T（轉換）交給下游 dbt。
詳見 PHASE4_EXTRACTION.md。

關鍵設計（已與設計文件對齊）：
- staging 維持「1:1 原樣落地」：不清洗、不改名、不轉型（那是 dbt stg_ 的責任）。
- 載入一律 batch load job（免費），絕不用 streaming insert（§5-①）。
- 增量切片：watermark by spec.time_col，邊界用 `>=`（寧可重抓不漏抓，去重交給 dbt stg_）。
- watermark 採「方案 A」：從 INFORMATION_SCHEMA.PARTITIONS 推導（免費 metadata，
  且不受 require_partition_filter 限制）。讀取邏輯全部封裝在 get_watermark()——
  這是將來改「方案 B（獨立 watermark 表，精確到 timestamp，供分鐘級微批）」的唯一接縫。
- 本腳本沒有 advance_watermark()：方案 A 的 watermark 由 staging 自身推導，
  load 完下次呼叫 get_watermark() 即自然反映新資料。換 B 時才需新增 advance 步驟。

多表（每張表一份 TableSpec）：orders 與 quality_events 各自獨立抽取、獨立 watermark、
獨立 load job。跨表無 BQ 交易（load job 只保證單表原子），故一致性靠：
  ① 各表 watermark 獨立、失敗不推進 → 下輪 `>=` 自動補抽（append-only + dbt stg_ 去重）；
  ② main() 的 gate：任一張失敗即整體失敗（非零 exit），下游 dbt(T) 不得在半套資料上開跑。

執行方式（`--table`，預設 all）：
    python extract_ods_to_bq.py                      # 兩張表，gate 在腳本內
    python extract_ods_to_bq.py --table orders       # 單表，供 Airflow 一表一 task

  單表模式是 Phase 5 的形狀：gate 從「腳本內彙整後 raise」搬到「Airflow 的依賴邊」——
  dbt task 的上游＝兩個 extract task 都 success，語意與 ② 完全相同（見 docs/zh-TW/design/cloud-layer.md）。
  之所以值得拆，是因為 ① 的自癒本來就是【逐表】的：一張失敗只該重試那一張，
  合成一個 task 會讓重試連帶重跑已經成功的另一張，也看不出是哪張壞了。
"""

import argparse
import sys
import time
from dataclasses import dataclass

import structlog
from google.cloud import bigquery

from bq import get_bq_client
from config import settings
from database import SessionLocal
from models import ODS, QualityEvent

logger = structlog.get_logger()

# --- BQ 目的地（跨表共用）---
# PROJECT 隨部署環境而異（dev/prod 可能是不同專案），且是會曝光的基礎設施座標，
# 故由 settings 注入（BQ_PROJECT），真實 ID 不寫死進版控。
# DATASET / LOCATION 是結構性、穩定的架構決定（US 見 §4），留模組常數。
PROJECT = settings.bq_project
DATASET = "staging"
LOCATION = "US"  # 所有 dataset 一致建在 US（§4，避免跨 location 查詢報錯）


# --- 每張表的抽取契約（單一真相來源，決定 2）--------------------------------
@dataclass(frozen=True)
class TableSpec:
    """一張 ODS→staging 表的完整抽取契約。

    fields：(ODS 欄位名, BQ 型別, mode)，1:1 鏡射 models.py。
      ensure_staging_table() 由此建 schema、_to_bq_dict() 由此決定序列化、
      test_schema_bq_consistency 由此守一致性——三處共用同一份，避免漂移。
    """

    table: str                                 # BQ staging 表名 + INFORMATION_SCHEMA 過濾
    model: type                                # SQLAlchemy model（讀 ODS 增量用）
    time_col: str                              # watermark 與增量切片欄位
    fields: list[tuple[str, str, str]]
    partition_field: str
    clustering_fields: list[str]
    require_partition_filter: bool

    @property
    def staging_table(self) -> str:
        return f"{PROJECT}.{DATASET}.{self.table}"


# orders：1:1 鏡射 models.ODS
ORDERS_FIELDS: list[tuple[str, str, str]] = [
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

# quality_events：1:1 鏡射 models.QualityEvent（append-only 品質事件日誌）
# NOT NULL 欄一律 REQUIRED，兼作 fail-loud 護欄；reason(JSONB)→JSON、event_at→TIMESTAMP。
QUALITY_EVENTS_FIELDS: list[tuple[str, str, str]] = [
    ("id", "INTEGER", "REQUIRED"),           # PK
    ("raw_id", "INTEGER", "REQUIRED"),       # 叢集欄位①：下游 int_* 以 raw_id 取最新狀態
    ("order_id", "STRING", "REQUIRED"),
    ("event_type", "STRING", "REQUIRED"),    # initial_evaluation | promotion | rejection
    ("from_state", "STRING", "NULLABLE"),
    ("to_state", "STRING", "REQUIRED"),      # 叢集欄位②：clean|quarantined|promoted|permanently_rejected
    ("rule_version", "STRING", "REQUIRED"),
    ("event_at", "TIMESTAMP", "REQUIRED"),   # 分區欄位（watermark 依此推導）
    ("reason", "JSON", "NULLABLE"),
]


ORDERS_SPEC = TableSpec(
    table="orders",
    model=ODS,
    time_col="received_at",
    fields=ORDERS_FIELDS,
    partition_field="received_at",
    clustering_fields=["order_id", "has_clean_error"],
    require_partition_filter=True,   # access 永遠帶 received_at 過濾 → 保險絲合理
)

QUALITY_EVENTS_SPEC = TableSpec(
    table="quality_events",
    model=QualityEvent,
    time_col="event_at",
    fields=QUALITY_EVENTS_FIELDS,
    partition_field="event_at",
    clustering_fields=["raw_id", "to_state"],
    # 保險絲關閉：下游 int_* 合成有效狀態要「跨全歷史按 raw_id 取最新」，
    # 本質是非分區過濾的全掃描；開保險絲會擋掉這個必然查詢（見盤點 B）。
    require_partition_filter=False,
)

SPECS = [ORDERS_SPEC, QUALITY_EVENTS_SPEC]

# CLI 的 --table 值域直接由 SPECS 推導，不另外維護一份清單：延續 §5.4「每加一張表
# 只需在 SPECS 掛一份 spec」的單一真相原則——新表自動有 CLI 與一致性守衛，不會漏掛。
SPECS_BY_TABLE = {spec.table: spec for spec in SPECS}
ALL = "all"


def _bq_schema(fields: list[tuple[str, str, str]]) -> list[bigquery.SchemaField]:
    """由 fields 推出 BQ schema；建表與 load job 共用同一份（單一真相來源）。"""
    return [bigquery.SchemaField(name, bq_type, mode=mode) for name, bq_type, mode in fields]


def ensure_staging_table(client: bigquery.Client, spec: TableSpec) -> None:
    """建立 dataset（US）與 staging 實體表（分區+叢集+保險絲）。冪等。"""
    # dataset 明確指定 location，不靠預設（§4）
    dataset = bigquery.Dataset(f"{PROJECT}.{DATASET}")
    dataset.location = LOCATION
    client.create_dataset(dataset, exists_ok=True)

    table = bigquery.Table(spec.staging_table, schema=_bq_schema(spec.fields))

    # 分區：spec.partition_field 每天一塊；保險絲（require_partition_filter）由 spec 決定——
    # orders 開（access 永遠帶時間過濾），quality_events 關（下游需跨全歷史取最新）。
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=spec.partition_field,
        require_partition_filter=spec.require_partition_filter,
    )
    table.clustering_fields = spec.clustering_fields

    client.create_table(table, exists_ok=True)


def get_watermark(client: bigquery.Client, table: str):
    """方案 A：從 INFORMATION_SCHEMA.PARTITIONS 取最新分區（天粒度）。

    回傳最新分區當天 00:00:00 UTC 的 timestamp；表空/不存在 → None（觸發首次全量抽取）。

    - 免費(近似):查 INFORMATION_SCHEMA 是 metadata 操作,不掃 staging 本體;on-demand 下每次有 10 MB 最低計費門檻,金額可忽略,且通常在免費額度內。
    - 不受 require_partition_filter 限制：查的是 metadata view，非表本體。
    - 天粒度：每次抽取會重抽「最新分區整天」，配 `>=` 邊界與 dbt stg_ 去重 → 不漏不錯。

    ★ 換方案 B 的唯一接縫：屆時改為讀獨立 watermark 表（精確 timestamp），
      並在 load 成功後新增 advance_watermark()；呼叫端不變。
    """
    sql = f"""
        SELECT PARSE_TIMESTAMP('%Y%m%d', MAX(partition_id)) AS wm
        FROM `{PROJECT}.{DATASET}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name = '{table}'
          AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
    """
    row = next(iter(client.query(sql).result()), None)
    return row.wm if row else None


def _json_field(value):
    """JSON 型別欄位（items / *_message / unmapped_fields / reason）的序列化。

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


def _to_bq_dict(row, fields: list[tuple[str, str, str]]) -> dict:
    """ODS ORM 物件 → BQ NDJSON 列。欄位與序列化方式皆由 fields 決定。"""
    return {name: _serialize(getattr(row, name), bq_type) for name, bq_type, _ in fields}


def extract_from_ods(spec: TableSpec, watermark) -> list[dict]:
    """讀 ODS 增量。watermark=None → 全量首抽。

    staging 維持 1:1 原樣：只查、不清洗/改名/轉型。
    """
    time_col = getattr(spec.model, spec.time_col)
    session = SessionLocal()
    try:
        q = session.query(spec.model)
        if watermark is not None:
            q = q.filter(time_col >= watermark)  # 邊界用 >=
        rows = q.order_by(time_col).all()
        return [_to_bq_dict(r, spec.fields) for r in rows]
    finally:
        session.close()  # 沿用專案手動 session 慣例


def load_to_staging(client: bigquery.Client, spec: TableSpec, rows: list[dict]) -> int:
    """batch load（免費）、只 append。表已預建好分區/叢集，load 自動沿用。回傳實際載入列數。"""
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # 冪等靠 append + dbt stg_ 去重，不在 E/L 做 MERGE（保持原樣落地）
        schema=_bq_schema(spec.fields),
        # additive evolution：ODS 加 nullable 欄並更新 fields 後，新欄由 load job
        # 自動補進既有表（ensure_staging_table 的 create 只建不改）。改名/改型別/改分區
        # 不在此處理——交給 dbt stg_ 或重建表（見「ODS schema 演進」策略）。
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = client.load_table_from_json(rows, spec.staging_table, job_config=job_config)
    job.result()  # 等完成；失敗則拋出，由呼叫端決定重跑
    return job.output_rows


def extract_and_load_one(client: bigquery.Client, spec: TableSpec) -> int:
    """單張表一條龍：建表 → watermark → 抽取 → load。回傳載入列數（0＝無新資料）。
    失敗直接往上拋，交由 main() 的 gate 彙整。"""
    ensure_staging_table(client, spec)
    watermark = get_watermark(client, spec.table)
    rows = extract_from_ods(spec, watermark)
    if not rows:
        logger.info("extract_skip", table=spec.table, watermark=str(watermark), reason="no_new_rows")
        return 0
    loaded = load_to_staging(client, spec, rows)
    logger.info(
        "extract_table_done",
        table=spec.table,
        watermark=str(watermark),
        extracted=len(rows),
        loaded=loaded,
    )
    return loaded


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ODS（PostgreSQL）→ BigQuery staging 抽取（E/L）",
    )
    parser.add_argument(
        "--table",
        choices=[*SPECS_BY_TABLE, ALL],
        default=ALL,
        help=f"要抽哪張表；預設 {ALL}（兩張都抽，gate 留在腳本內）。"
             "指定單表時 gate 由呼叫端負責（Airflow 的依賴邊）。",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    specs = SPECS if args.table == ALL else [SPECS_BY_TABLE[args.table]]

    if not PROJECT:
        raise RuntimeError("BQ_PROJECT 未設定：請在 .env 設定 BQ_PROJECT=<你的 GCP 專案 ID>")

    t0 = time.monotonic()
    client = get_bq_client()

    # 選中的表逐一盡力抽取（多表時一張失敗不擋另一張各自推進 watermark），失敗彙整後 gate。
    failures: list[tuple[str, Exception]] = []
    for spec in specs:
        try:
            extract_and_load_one(client, spec)
        except Exception as e:  # noqa: BLE001 — 逐表隔離失敗，最後統一 gate
            failures.append((spec.table, e))
            logger.error("extract_table_failed", table=spec.table, error=str(e))

    # ── GATE ────────────────────────────────────────────────────────────────
    # 任一表失敗即整體失敗（非零 exit）→ 下游 dbt(T) 不得在半套資料上開跑。
    # 失敗那張的 watermark 未推進（方案 A），下輪 `>=` 自動補抽（append-only + stg_ 去重）。
    # 單表模式下這條只剩「把失敗轉成非零 exit」，跨表 gate 改由 Airflow 的依賴邊承擔——
    # 語意相同，只是判斷點從腳本內搬到 DAG 上（見檔頭〈執行方式〉）。
    if failures:
        raise RuntimeError(
            "E/L gate 未通過，dbt 不應開跑；失敗表："
            + ", ".join(f"{t}({type(e).__name__})" for t, e in failures)
        )

    logger.info(
        "extract_all_done",
        tables=[s.table for s in specs],
        elapsed_s=round(time.monotonic() - t0, 2),
    )
    # 方案 A：無 advance_watermark —— staging 本身即 watermark


if __name__ == "__main__":
    main(sys.argv[1:])
