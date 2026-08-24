-- stg_quality_events：ODS.quality_events 的 Silver 入口（append-only 品質事件日誌）
--
-- 職責：
--   1. 1:1 對應 ODS，型別對齊、欄位命名標準化（顯式欄位清單＝改名接縫）
--   2. 把 staging 的 append 重複列還原回「每個事件一列」的粒度（去重）
--   ★ 去重鍵＝id（事件 PK），【不是】raw_id——quality_events 是狀態機日誌，
--     一個 raw_id 合法地有多個事件（initial_evaluation → promotion → rejection…）。
--     照 raw_id 去重會把事件歷史壓成一列、毀掉狀態機。「按 raw_id 取最新狀態」的
--     收斂是下游 int_ 的責任（見 docs/zh-TW/design/data-quality.md〈機制二：Row Filter〉），不在此層。
--
-- 物化＝incremental + insert_overwrite，依 event_at(DAY) 分區（比照 stg_orders）：
--   例行跑批只重算「回看窗」內的近期分區，成本 ∝ 近期資料、不隨歷史總量成長。
--   正確性靠不變式「同 id 的所有副本都在同一 event_at 分區」（同事件同 event_at）：
--   insert_overwrite 整分區原子覆寫，窗內去重完整無漏。首建 / --full-refresh 走全表路徑。
--   append-only：舊事件不可變，新事件(promotion 等) event_at=now() 落當天分區，回看窗撈得到。
--
-- copy_partitions=true：以 copy job（非 DML、免費）覆寫分區，取代預設 MERGE，
--   讓 insert_overwrite 在 BQ sandbox（未啟用帳單、禁 DML）仍可運作。
--
-- on_schema_change='append_new_columns'：與 stg_orders 同策略（加欄走 ALTER ADD、免費、
--   只加不刪）；觸發閘門＝下方顯式欄位清單。改型別/改分區走 --full-refresh。

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            'field': 'event_at',
            'data_type': 'timestamp',
            'granularity': 'day',
            'copy_partitions': true,
        },
        cluster_by=['raw_id', 'to_state'],
        on_schema_change='append_new_columns',
    )
}}

with source as (

    select * from {{ source('ecommerce_staging', 'quality_events') }}
    {% if is_incremental() %}
    -- 增量：只讀回看窗（需 ≥ `>=` watermark 的重抽範圍 + 安全邊際，預設 3 天）。
    -- append-only 事件不改；新事件 event_at=now() 落當天分區，窗內撈得到。
    where event_at >= timestamp_sub(current_timestamp(), interval {{ var('stg_quality_events_lookback_days', 3) }} day)
    {% else %}
    -- 全量（首建 / --full-refresh）：哨兵過濾，實際全表掃。
    where event_at >= timestamp('1970-01-01')
    {% endif %}

),

deduped as (

    -- 去重鍵＝id（事件 PK）。E/L `>=` 重抽的副本是同一事件、byte-identical，
    -- 取哪一份都一樣；order 僅為決勝穩定性。
    -- 注意：此處【不】按 raw_id 去重——保留每個 raw_id 的完整事件序列。
    select
        *,
        row_number() over (
            partition by id
            order by event_at desc
        ) as _row_num
    from source

)

select
    -- 鍵與血緣
    id,
    raw_id,
    order_id,

    -- 狀態機
    event_type,
    from_state,
    to_state,
    rule_version,
    event_at,

    -- 失敗原因（quarantined/rejection 才有內容；clean 為 JSON null）
    reason

from deduped
where _row_num = 1
