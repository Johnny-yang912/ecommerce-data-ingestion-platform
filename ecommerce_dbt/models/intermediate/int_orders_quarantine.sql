-- int_orders_quarantine：被 Row Filter 隔離的資料（有效品質狀態非乾淨）
--
-- 對象＝Raw.status='processed' 且有效品質狀態仍為 quarantined / permanently_rejected。
--   這些記錄「已在 ODS」，只是不流入 dim_/fct_。它們的問題是【規則評估】而非 pipeline 失敗，
--   故 remediation 走 Proposal B（規則升版重評估），不是 force=true（對 processed 回 400）。
--   見 docs/zh-TW/design/data-quality.md〈Remediation：A + B + C 並用〉。
--
-- quarantined_at 語意：刻意【不】用 CURRENT_TIMESTAMP()（DQ 文件早期範例）。
--   本模型是 table 全量重建，CURRENT_TIMESTAMP 每次跑批都會變，記錄的是「這次 run 的時間」
--   而非「這筆被隔離的時刻」，會讓 rpt_quality_* 的時間軸失真。改取 quality_events 的
--   event_at（initial_evaluation 事件＝真正的隔離時刻），事件缺席時退回 received_at。
--
-- 刻意【不】按 order_date 分區：int_ 只被 DAG 內部消費（非分析師 ad-hoc），分區收益 ≈ 0。
--   ⚠️ 早期版本另記了「本表是 ORDER_DATE_IN_FUTURE 髒列的收容處，離譜的未來日期會超出
--      BQ 分區合法區間、讓整張表建立失敗」——【該理由經 2026-08 實測推翻】：超出
--      1960-01-01 ~ 2159-12-31 的值不會炸表，會靜默落進 __UNPARTITIONED__ 分區
--      （見 docs/zh-TW/design/cloud-layer.md）。不分區的決定仍然成立，但只剩上面那一條理由。
--
-- 物化＝table：理由同 int_orders（Proposal B 的狀態變更落在舊分區，按 received_at
--   增量會看不到）。兩模型的物化策略必須一致，否則劃分不變式會在跑批之間破裂。

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

-- ═══════════ 共用區塊到此為止；以下為 int_orders_quarantine 專屬 ═══════════

select
    * except (is_effectively_clean),

    -- 攤平 clean_error_message 的穩定 code，供 RCA 與 rpt_quality_field_breakdown
    -- 直接聚合，免得每個消費者各自 UNNEST 一次 JSON（DQ 機制三以 code 比對，不比對措辭）。
    {{ dq_error_codes('clean_error_message') }} as error_codes,

    -- 真正的隔離時刻（見檔頭）：優先取品質事件時間，事件缺席退回攝入時間。
    coalesce(quality_state_at, received_at) as quarantined_at

from classified
where not is_effectively_clean
