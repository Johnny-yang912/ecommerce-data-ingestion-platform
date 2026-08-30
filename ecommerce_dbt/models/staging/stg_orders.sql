-- stg_orders：ODS.orders 的 Silver 入口
--
-- 職責（docs/zh-TW/design/data-quality.md / docs/zh-TW/design/cloud-layer.md）：
--   1. 1:1 對應 ODS，型別對齊、欄位命名標準化（顯式欄位清單＝改名接縫）
--   2. 品質＝與 ODS 相同：保留所有資料「含髒的」（has_clean_error 不過濾；攔截在 int_）
--   3. 把 staging 的 append 重複列還原回 ODS 粒度（去重）
--
-- 物化＝incremental + insert_overwrite，依 received_at(DAY) 分區（見 docs/zh-TW/design/cloud-layer.md）：
--   例行跑批只重算「回看窗」內的近期分區，成本 ∝ 近期資料、不隨歷史總量成長。
--   正確性靠不變式「同 raw_id 的所有副本都在同一 received_at 分區」：insert_overwrite
--   整分區原子覆寫，窗內去重完整無漏。首建 / --full-refresh 走全表路徑。
--   註：stg_orders 本身不設 require_partition_filter，下游 int_ 與 Hard Gate 測試可自由查詢。
--
-- copy_partitions=true：以 copy job（非 DML、免費）覆寫分區，取代預設的 MERGE。
--   BQ sandbox（未啟用帳單）禁止 DML，此選項讓 insert_overwrite 仍可運作。
--
-- on_schema_change='append_new_columns'：加欄走 ALTER ADD COLUMN（metadata、免費、
--   既有列自動 NULL），只覆寫回看窗分區，避開大表 --full-refresh 的全表掃＋全分區重寫成本。
--   成本 ∝ 近期資料，是 staging 端 ALLOW_FIELD_ADDITION 的鏡像（docs/zh-TW/design/cloud-layer.md）。
--   觸發閘門＝下方顯式欄位清單：staging 靠 ALLOW_FIELD_ADDITION 長的欄，未加進 SELECT 前
--   偵測不到、不會自己 ALTER——只在刻意改清單（進 git、被 review）時才觸發，不吃 drift。
--   刻意不用 sync_all_columns：它會 DROP 欄，牴觸「staging 只做加法、刪欄留 legacy」（§5.2/§5.3）。
--   改型別/改分區/歷史回填仍走 --full-refresh / targeted refresh（見 README §4.7、§6）。
--
-- ⚠️ 增量過濾的左邊界【必須對齊日界】（timestamp_trunc(..., day)）——這是正確性、不是風格：
--   insert_overwrite 的原子單位是【整個分區】，而 dbt 只覆寫「查詢結果裡出現過的分區」。
--   左邊界若落在某天的【中間】（例如 current_timestamp() 直接減 N 天），邊界那天就只有
--   一部分的列進得了窗，insert_overwrite 便會拿【半天】原子覆寫【整天】——窗外的列被
--   靜默刪除，不報錯、dbt test 不紅、上游 staging 完好無損所以事後也查不出是誰刪的。
--   實例：2026-08-29 20:38 的一次手動跑批（早於例行的 22:30）把 2026-08-26 分區從
--   800 列砍成 250 列，cutoff 落在 2026-08-26 20:38，當天只剩最後一批（21:00）在窗內。
--   對齊日界後，邊界那天只有「整天在窗內」或「整天在窗外」兩種狀態，「半天」不再存在，
--   **跑批時刻也就不再是正確性的隱含前提**——這正是原本那個 bug 的本體：
--   同一支模型在 20:38 與 22:30 跑會產出不同結果，也就是它不是冪等的。
--   守門的是 tests/assert_stg_orders_matches_staging.sql（逐分區與 staging 對帳）。
--
-- 定點回填（stg_orders_backfill_start / _end）：把「補哪幾天」從時鐘手上拿回來。
--   例行跑批走滾動窗；要修特定分區時改傳日期，與跑批時刻完全無關：
--     dbt run -s stg_orders --vars '{stg_orders_backfill_start: "2026-08-26"}'
--   只給 start ＝ 補那一天；另給 end ＝ 補 [start, end) 這一段（end 為【不含】）。
--   刻意讓 end 可省略：補單日卻把 end 填成當天（而非隔天）會靜默補出空集合，
--   而回填正是最不該留隱形陷阱的路徑——出事時開指令的人往往不熟這條管線。
--   下游是 table 物化會自動跟上，跑完接：dbt run -s stg_orders+ --exclude stg_orders

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
        on_schema_change='append_new_columns',
    )
}}

with source as (

    select * from {{ source('ecommerce_staging', 'orders') }}
    {% if is_incremental() %}
    {%- set backfill_start = var('stg_orders_backfill_start', none) %}
    {%- set backfill_end = var('stg_orders_backfill_end', none) %}
    {% if backfill_start %}
    -- 定點回填：分區範圍由呼叫者明確指定，不讀時鐘（見檔頭）。
    where received_at >= timestamp('{{ backfill_start }}')
      and received_at < {% if backfill_end %}timestamp('{{ backfill_end }}')
                        {%- else %}timestamp_add(timestamp('{{ backfill_start }}'), interval 1 day){% endif %}
    {% else %}
    -- 增量：只讀回看窗。窗需 ≥ `>=` watermark 的重抽範圍 + 安全邊際（預設 3 天）。
    -- 同時滿足 staging 的 require_partition_filter（range 過濾即可裁切分區）。
    -- ⚠️ timestamp_trunc(..., day) 不可拿掉——它讓左邊界落在分區邊界上。見檔頭。
    where received_at >= timestamp_sub(
        timestamp_trunc(current_timestamp(), day),
        interval {{ var('stg_orders_lookback_days', 3) }} day
    )
    {% endif %}
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
