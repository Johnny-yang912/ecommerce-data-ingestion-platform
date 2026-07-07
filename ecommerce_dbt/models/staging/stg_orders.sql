-- stg_orders：ODS.orders 的 Silver 入口
--
-- 職責（DQ_ARCHITECTURE-TW Q0 / CLOUD_LAYER-TW）：
--   1. 1:1 對應 ODS，型別對齊、欄位命名標準化（顯式欄位清單＝改名接縫）
--   2. 品質＝與 ODS 相同：保留所有資料「含髒的」（has_clean_error 不過濾；攔截在 int_）
--   3. 把 staging 的 append 重複列還原回 ODS 粒度（去重）
--
-- 物化＝incremental + insert_overwrite，依 received_at(DAY) 分區（見 CLOUD_LAYER-TW §1.2）：
--   例行跑批只重算「回看窗」內的近期分區，成本 ∝ 近期資料、不隨歷史總量成長。
--   正確性靠不變式「同 raw_id 的所有副本都在同一 received_at 分區」：insert_overwrite
--   整分區原子覆寫，窗內去重完整無漏。首建 / --full-refresh 走全表路徑。
--   註：stg_orders 本身不設 require_partition_filter，下游 int_ 與 Hard Gate 測試可自由查詢。
--
-- copy_partitions=true：以 copy job（非 DML、免費）覆寫分區，取代預設的 MERGE。
--   BQ sandbox（未啟用帳單）禁止 DML，此選項讓 insert_overwrite 仍可運作。

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            'field': 'received_at',
            'data_type': 'timestamp',
            'granularity': 'day',
            'copy_partitions': true,
        },
        cluster_by=['raw_id'],
    )
}}

with source as (

    select * from {{ source('ecommerce_staging', 'orders') }}
    {% if is_incremental() %}
    -- 增量：只讀回看窗。窗需 ≥ `>=` watermark 的重抽範圍 + 安全邊際（預設 3 天）。
    -- 同時滿足 staging 的 require_partition_filter（range 過濾即可裁切分區）。
    where received_at >= timestamp_sub(current_timestamp(), interval {{ var('stg_orders_lookback_days', 3) }} day)
    {% else %}
    -- 全量（首建 / --full-refresh）：哨兵過濾滿足保險絲、實際全表掃。
    where received_at >= timestamp('1970-01-01')
    {% endif %}

),

deduped as (

    -- 去重鍵＝raw_id（ODS UNIQUE、1:1；且 Proposal C 遷移式修正列為「新 id、同 raw_id」，
    -- 唯有以 raw_id 分組才能讓修正列與已在 staging 的舊列競爭）。
    -- 決勝：目前重複列 byte-identical，順序無所謂。
    -- TODO(Proposal C)：欄位 rebuild_batch_id 落地後，插到 order by 最前：
    --   order by rebuild_batch_id desc nulls last, received_at desc, id desc
    -- late-arriving：Proposal C 修正列落在舊分區，例行增量的回看窗看不到；由修復 runbook
    --   的 targeted refresh（insert_overwrite 災區分區 / --full-refresh）補上（CLOUD §7.4、DQ C-2 #7）。
    select
        *,
        row_number() over (
            partition by raw_id
            order by received_at desc, id desc
        ) as _row_num
    from source

)

select
    -- 鍵與 metadata
    id,
    received_at,
    raw_id,

    -- 訂單主體
    order_id,
    order_date,
    ship_mode,
    order_status,
    delivery_date,
    delivery_days,
    returned,

    -- 顧客
    customer_id,
    customer_name,
    age,
    gender,
    membership_tier,
    registration_date,
    acquisition_channel,
    newsletter_subscribed,
    preferred_payment_method,
    preferred_device,

    -- 地址
    country,
    region,
    state,
    city,
    postal_code,

    -- 金流
    payment_method,
    tax_pct,

    -- 行為
    device_used,
    customer_rating,
    is_repeat_customer,

    -- items 整包（保持 JSON，攤平交下游）
    items,

    -- 清洗錯誤標籤（業務值品質；此層保留、不攔截）
    has_clean_error,
    clean_error_message,

    -- schema drift 標籤（非阻斷監控訊號）
    has_schema_drift,
    schema_drift_message,
    unmapped_fields,

    -- 不可變 metadata
    dq_rule_version,
    source_client_id

from deduped
where _row_num = 1
