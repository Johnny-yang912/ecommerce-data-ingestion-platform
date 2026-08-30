-- rpt_quality_events_daily：品質事件流的日聚合（一列 = 一天 × 一個規則版本）
--
-- 定位：【資料可信度與規則效果的分析】，不是管線健康監控。
--   後者（分鐘級 error rate、Hard Gate 即時告警、批次 SLA）屬 OTel/Grafana，
--   即 docs/zh-TW/design/data-quality.md〈層次一：即時運維指標〉。本模型是〈層次二：批次分析指標〉。
--   兩者刻意重疊一部分信號（error rate 兩邊都有），差別在消費形態：
--   OTel 那份是「現在、單一數字、為了告警」，本表是「歷史、可切片、為了歸因」。
--   這一行定位寫在這裡，是為了防止日後兩邊功能漂移互相蓋台。
--
-- ⭐ 掛在【事件軸】（event_at）而非攝入軸（received_at）。這個選擇同時決定了三件事：
--   1. 數字【不會被追溯性改寫】——quality_events 是 append-only，
--      今天 promote 一筆三個月前的訂單，只會【新增】一列 promotion 事件，
--      不會改動三個月前那天的 initial_evaluation 計數。
--      （對照組：若按 received_at 分組，「當天攝入的 N 筆裡現在有幾筆是髒的」
--        會隨每次 promote 而變，那是狀態不是事件。狀態由 rpt_quality_backlog 承載。）
--   2. 因此本模型【可以增量】，且是本專案唯一一個增量在語意上天生正確的下游模型——
--      時間軸與「什麼會變」對齊，回看窗就夠，不需要 int_/fct_ 那套受影響分區 discovery。
--   3. 它直接對應 docs/zh-TW/design/data-quality.md〈歷史指標為何不會被追溯性改寫〉那兩支範例 SQL。
--
-- ⭐ 刻意【不】輸出 quarantine_rate / promotion_rate 等比率欄位，只輸出可加的分子與分母。
--   預聚合層存比率是頭號陷阱：BI 一旦把日粒度 roll up 到週，Looker Studio 算的是
--   AVG(daily_rate)——「比率的平均」而非「總和的比率」，兩者只在每日分母相等時才一樣，
--   而分母永遠不相等。rate 交給 BI 計算欄位（它做的是 SUM(分子)/SUM(分母)，任何粒度都對）。
--   為何不比照 fct_orders.tax_pct 那樣「留著並註明不可加」：tax_pct 是【原始事實】，
--   不存就沒了；rate 是純衍生值，留一個「只在日粒度正確」的欄位等於主動製造誤用機會。
--
-- 寬表（每個狀態一個計數欄）而非長表（event_type × to_state 各一列），兩個理由：
--   1. rate 的分子與分母必須落在【同一列】，BI 才做得出 SUM(num)/SUM(den)；
--   2. event_type / to_state 的值域是【狀態機定義的封閉小集合】（models.py:101-107、
--      docs/zh-TW/design/data-quality.md 的事件 schema），寬表最大的風險——值域擴張要改 schema——
--      在這裡不存在。值域真的擴張時，assert_rpt_quality_events_split 會先紅給你看。
--
-- ⚠️ 現況：Proposal B（Airflow 重評估）尚未實作，quality_events 目前只有攝入時的
--   initial_evaluation 事件（docs/zh-TW/design/data-quality.md §「Proposal B 尚未實作」）。
--   → promotions / rejections / re_quarantines 目前【恆為 0】。
--   欄位先留著是對的（事件產生端一上線就有值，不需改 schema），
--   但 BI 上先不要放一張永遠空白的「回流趨勢」圖。

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            'field': 'event_date',
            'data_type': 'date',
            'granularity': 'day',
            'copy_partitions': true,
        },
        cluster_by=['rule_version'],
        on_schema_change='append_new_columns',
    )
}}

with events as (

    select * from {{ ref('stg_quality_events') }}

    {% if is_incremental() %}
    {%- set backfill_start = var('rpt_quality_events_backfill_start', none) %}
    {%- set backfill_end = var('rpt_quality_events_backfill_end', none) %}
    {% if backfill_start %}
    -- 定點回填：分區範圍由呼叫者明確指定，不讀時鐘。
    --   上游 stg_quality_events 補了舊分區時，這裡【必須】用同一組日期跟著補一次——
    --   例行回看窗看不到舊分區，補了上游不補這裡，BI 會繼續顯示舊值且一路全綠。
    --   2026-08-30 的 08-26 修復就卡在這一步：stg_ 已是 800，這張表仍是 250。
    where event_at >= timestamp('{{ backfill_start }}')
      and event_at < {% if backfill_end %}timestamp('{{ backfill_end }}')
                     {%- else %}timestamp_add(timestamp('{{ backfill_start }}'), interval 1 day){% endif %}
    {% else %}
    -- ⚠️ 本回看窗【必須 ≥ var('stg_quality_events_lookback_days')】。
    --    上游窗比下游窗大時，上游今天才補進來的舊事件會落在下游窗外 → 永遠撈不到，
    --    而且不報錯（分區存在、只是內容少算）。兩個 var 要一起調。
    -- ⚠️ timestamp_trunc(..., day) 不可拿掉——本模型同樣是 insert_overwrite + DAY 分區，
    --    左邊界落在日中時，邊界那天會拿【半天】原子覆寫【整天】。與 stg_orders /
    --    stg_quality_events 是同一個缺陷的第三個實例（2026-08-30 一併修）。
    --    這裡先前之所以沒炸，只是因為攝入批次都在 UTC 13:00 前、而例行跑批在 14:30——
    --    邊界那天恰好篩出 0 列、分區因此沒被納入覆寫集合而倖存。
    --    **「靠跑批時刻與攝入時刻的相對位置僥倖」不是正確性**，改攝入排程就會炸。
    where event_at >= timestamp_sub(
        timestamp_trunc(current_timestamp(), day),
        interval {{ var('rpt_quality_events_lookback_days', 3) }} day
    )
    {% endif %}
    {% endif %}
    -- 全量路徑（首建 / --full-refresh）不加哨兵：stg_quality_events 本身不設
    -- require_partition_filter（保險絲被 stg_ 層擋掉了，見 ecommerce_dbt/README.zh-TW §4.6）。

)

select
    -- ── grain ───────────────────────────────────────────────────────────────
    -- ⚠️ 時區＝UTC，刻意【不】寫 date(event_at, 'Asia/Taipei')。
    --    時區轉換會讓分區裁切的謂詞下推失效（過濾條件不再是分區欄位的純函數）。
    --    倉儲層落地 UTC、時區呈現交給 BI，是標準分工。
    --    但這一點【必須】讓消費者看得到——否則「當日」對不上業務直覺時沒人查得到原因，
    --    故 _reports__models.yml 的 column description 也寫了一次。
    date(event_at) as event_date,

    -- quality_events.rule_version 在 ODS 是 NOT NULL（models.py:109），故不需 unknown member。
    rule_version,

    -- ── 可加計數（比率的分子與分母，見檔頭）─────────────────────────────────
    count(*) as events_total,

    -- ⭐ 品質率的【分母】：每筆訂單攝入時恰好產生一筆 initial_evaluation
    --    （process.py 寫 ODS 成功後 append），故它就是「當日評估筆數」。
    countif(event_type = 'initial_evaluation')                              as initial_evaluations,
    countif(event_type = 'initial_evaluation' and to_state = 'clean')       as initial_clean,
    -- ⭐ quarantine_rate 的【分子】
    countif(event_type = 'initial_evaluation' and to_state = 'quarantined') as initial_quarantined,

    -- 狀態機的後續轉移（Proposal B 上線前恆為 0，見檔頭）。
    -- 這裡只看 to_state 不看 event_type：促進/放棄的判準是【落到哪個狀態】，
    -- 而 event_type 是「誰發動的」——用 to_state 對齊 int_ 層合成有效品質狀態的判準。
    countif(to_state = 'promoted')             as promotions,
    countif(to_state = 'permanently_rejected') as rejections,
    countif(to_state = 're_quarantined')       as re_quarantines

from events
group by event_date, rule_version
