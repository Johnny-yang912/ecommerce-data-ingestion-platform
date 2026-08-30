-- int_orders：Gold 入口——通過 Row Filter 的乾淨資料流
--
-- 職責（docs/zh-TW/design/data-quality.md 機制二）：攔截發生在這一層。判定基準是「有效品質狀態」，
--   【不是】ODS 的 has_clean_error 字面快照——ODS 是不可變錨點，被 Proposal B promote 的
--   記錄在 ODS 裡永遠是 has_clean_error=TRUE；只讀快照它會永遠卡在 quarantine、流不回 Gold。
--   有效品質狀態 = ODS 攝入快照 ⊕ quality_events 最新事件（合成於下方共用區塊）。
--
-- 物化＝table（全量重建），刻意【不】比照 stg_orders 用 received_at 增量：
--   Proposal B 的 promotion 事件 event_at=now()、落當天分區，但它救的那筆訂單 received_at
--   在很久以前的舊分區。若本模型按 received_at 回看窗增量，該舊分區永遠不會被重算
--   → 被 promote 的記錄永遠流不回 Gold，回流機制在此層被切斷。
--   （與 docs/zh-TW/design/cloud-layer.md 的 late-arriving 同構，但軸不同：那裡是「值」變、這裡是「狀態」變。）
--   table 走 CREATE OR REPLACE（DDL），不受 BQ sandbox 禁 DML 限制。
-- TODO(成本)：資料量讓全量重建有感時改增量。重選集合必須是
--   「回看窗分區 ∪ 近期有品質事件的 raw_id 所屬分區」，且【整分區】重選——
--   只選那幾列會讓 insert_overwrite 把同分區的其他列一併洗掉。
--
-- 刻意不分區：int_ 只被 DAG 內部消費（非分析師 ad-hoc），分區收益 ≈ 0；
--   order_date 分區留給 dim_/fct_（docs/zh-TW/design/cloud-layer.md「每張表依自己的 access pattern 各自選」）。
--
-- ⚠️ 下方的 LEFT JOIN 是【損害換形狀的地方】：上游 stg_quality_events 掉列時，本模型的
--   列數完全正確、只有 quality_state_at 變 NULL——掉列偽裝成缺值，逐分區對帳測試看不見
--   （2026-08-30 事故第二階段：800 列一列不少，其中 550 列的品質狀態是空的，94 條測試全綠）。
--   守門的是 tests/assert_int_orders_quality_state_resolved.sql：它不禁止 NULL（事件缺席的
--   fallback 是刻意設計），而是禁止 NULL【持續超過 2 天】——fallback 只該造成延遲，不該造成
--   永久缺席。門檻的推導與升級條件寫在該測試的檔頭。

{{
    config(
        materialized='table',
        cluster_by=['order_id'],
    )
}}

-- ═══════════════════════════════════════════════════════════════════════════
-- ⚠️ 共用區塊：以下 CTE 在 int_orders.sql 與 int_orders_quarantine.sql 中
--    【必須逐字相同】。兩模型是 stg_orders 的完整劃分（互斥 + 窮盡），
--    改一邊沒改另一邊 → 靜默漏列或重複列。對齊清單見 README.zh-TW §5.2；
--    tests/assert_orders_split_is_partition.sql 是唯一的自動化保證，不可跳過。
--    （刻意選複製而非共用模型：目前只有 2 個消費者，複製成本低於間接層的認知成本。
--      出現第 3 份複製時即為收斂成共用模型的觸發點——見 README.zh-TW §5.3。）
-- ═══════════════════════════════════════════════════════════════════════════

with latest_event as (

    -- 每個 raw_id 取最新一筆品質事件（append-only，故按 event_at 取首列）。
    -- 決勝鍵加 id desc：同一時戳有多事件時保證確定性，兩模型必須一致。
    select raw_id, to_state, event_type, rule_version, event_at
    from (
        select
            *,
            row_number() over (
                partition by raw_id
                order by event_at desc, id desc
            ) as _rn
        from {{ ref('stg_quality_events') }}
    )
    where _rn = 1

),

resolved as (

    select
        s.*,
        e.to_state     as quality_state_latest,
        e.rule_version as quality_state_rule_version,
        e.event_at     as quality_state_at,

        -- ⚠️ coalesce 不可省：has_clean_error=TRUE 且無事件時，
        --    FALSE OR NULL = NULL，`where not NULL` 也是 NULL
        --    → 該列會從「兩張」表同時消失（靜默漏資料）。
        -- 事件缺席 → fall back 到 ODS 快照，即 docs/zh-TW/design/cloud-layer.md 要求的「保守合成」：
        --    orders 上了但 quality_events 沒上時，乾淨照流、髒的續留 quarantine，
        --    只造成延遲、不造成髒資料。
        coalesce(
            s.has_clean_error = false
            or e.to_state = 'promoted',
            false
        ) as is_effectively_clean

    from {{ ref('stg_orders') }} s
    left join latest_event e   -- ⚠️ 必須 LEFT：INNER 會讓所有無事件的列整批消失
        on s.raw_id = e.raw_id

),

classified as (

    select
        *,
        -- 血緣標籤：分辨「攝入當下即乾淨」與「被新版規則救回」，供 rpt_quality_* 切片。
        -- 註：has_clean_error=FALSE 的列不可能落到 re_quarantined——該狀態只跟在 promoted
        --     之後，而 promoted 的前提是曾經 quarantined（前提是 has_clean_error=TRUE）。
        case
            when is_effectively_clean and has_clean_error = false then 'clean'
            when is_effectively_clean                             then 'promoted'
            when quality_state_latest = 'permanently_rejected'    then 'permanently_rejected'
            else 'quarantined'
        end as effective_quality_state
    from resolved

)

-- ═══════════ 共用區塊到此為止；以下為 int_orders 專屬 ═══════════

select
    -- is_effectively_clean 在本模型恆為 TRUE，不輸出（避免下游誤以為它有資訊量）；
    -- effective_quality_state 保留：clean / promoted 的差異是血緣資訊。
    * except (is_effectively_clean)
from classified
where is_effectively_clean
