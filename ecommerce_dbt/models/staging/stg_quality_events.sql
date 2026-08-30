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
--
-- ⚠️ 增量過濾的左邊界【必須對齊日界】（timestamp_trunc(..., day)）——這是正確性、不是風格。
--   理由與 stg_orders 完全相同（該檔頭有完整推導）：insert_overwrite 的原子單位是
--   【整個分區】，而 dbt 只覆寫「查詢結果裡出現過的分區」。左邊界落在某天的【中間】時，
--   邊界那天只有一部分事件進得了窗，insert_overwrite 便拿【半天】原子覆寫【整天】——
--   窗外的事件被靜默刪除，不報錯、dbt test 不紅、上游 staging 完好無損。
--   實例：2026-08-29 20:38 的手動跑批同時打中兩支模型，把 2026-08-26 分區從 800 列
--   砍成 250 列。2026-08-30 上午只修了 stg_orders，這一支照原樣留著、當晚照樣是 250，
--   由 rpt_quality_events_daily 一路顯示到 BI；且因為 int_orders 是 LEFT JOIN 取
--   quality_state_at，被救回的 550 筆訂單是【帶著 NULL 品質狀態】回到 Gold 的——
--   列數齊全所以任何「數列」的測試都抓不到。
--   ⭐ 教訓不是「這支忘了改」，而是**同一個缺陷同時存在於兩支模型，修一支不會讓另一支變好**：
--     兩支的邊界寫法必須一起看、一起改（未來抽成共用 macro 才能結構性地擋住）。
--   守門的是 tests/assert_stg_quality_events_matches_staging.sql（逐分區與 staging 對帳）。
--
-- 定點回填（stg_quality_events_backfill_start / _end）：把「補哪幾天」從時鐘手上拿回來。
--   例行跑批走滾動窗；要修特定分區時改傳日期，與跑批時刻完全無關：
--     dbt run -s stg_quality_events --vars '{stg_quality_events_backfill_start: "2026-08-26"}'
--   只給 start ＝ 補那一天；另給 end ＝ 補 [start, end) 這一段（end 為【不含】）。
--   刻意讓 end 可省略：補單日卻把 end 填成當天（而非隔天）會靜默補出空集合，
--   而回填正是最不該留隱形陷阱的路徑（與 stg_orders 同一決策）。
--   ⚠️ 回填完【必須】接下游重建，否則 int_/fct_/rpt 那些 quality_state_at 的 NULL 不會自己消失：
--     dbt run -s stg_quality_events+ --exclude stg_quality_events

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
    {%- set backfill_start = var('stg_quality_events_backfill_start', none) %}
    {%- set backfill_end = var('stg_quality_events_backfill_end', none) %}
    {% if backfill_start %}
    -- 定點回填：分區範圍由呼叫者明確指定，不讀時鐘（見檔頭）。
    where event_at >= timestamp('{{ backfill_start }}')
      and event_at < {% if backfill_end %}timestamp('{{ backfill_end }}')
                     {%- else %}timestamp_add(timestamp('{{ backfill_start }}'), interval 1 day){% endif %}
    {% else %}
    -- 增量：只讀回看窗（需 ≥ `>=` watermark 的重抽範圍 + 安全邊際，預設 3 天）。
    -- append-only 事件不改；新事件 event_at=now() 落當天分區，窗內撈得到。
    -- ⚠️ timestamp_trunc(..., day) 不可拿掉——它讓左邊界落在分區邊界上。見檔頭。
    where event_at >= timestamp_sub(
        timestamp_trunc(current_timestamp(), day),
        interval {{ var('stg_quality_events_lookback_days', 3) }} day
    )
    {% endif %}
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
