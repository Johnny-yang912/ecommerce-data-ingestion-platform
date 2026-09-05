-- rpt_quality_backlog 的加總配平：orders_primary_code 跨 error_code 加總必須等於實際訂單數
--
-- 背景（見 rpt_quality_backlog.sql 檔頭）：一張訂單可能帶多個 error_code，攤平後會出現在
-- 多列，所以 orders_with_code 跨 code 加總會重複計數。為了讓「現在總共卡了幾筆」這個 KPI
-- 仍然算得出來，表裡另放了一個可加的 orders_primary_code（主要碼一張訂單只有一個）。
--
-- 這支測試就是那個設計的安全網——若主要碼的挑法失效（例如陣列去重漏掉、
-- 或 primary_error_code 與攤平後的 error_code 對不上），配平會破，而症狀是
-- 【BI 上的 backlog 總數直接錯掉】，不報錯、不自癒。
--
-- 這與 assert_fct_orders_rollup_matches_items 是同一個手法：用一支測試把
-- 「兩處數字可能不一致」從紀律保證升級成機制保證，換取單表可查詢（ADR-0047）。
--
-- 不加時間窗：本表是全量重建的狀態快照，上游 int_orders_quarantine 也是全量重建，
-- 兩者在同一次 run 內產生，沒有 assert_fct_orders_complete_projection 那種
-- 「兩個 60 天時鐘掛在不同軸上」的問題。

with rpt as (

    select
        quarantined_date,
        dq_rule_version,
        effective_quality_state,
        sum(orders_primary_code) as orders
    from {{ ref('rpt_quality_backlog') }}
    group by quarantined_date, dq_rule_version, effective_quality_state

),

src as (

    select
        date(quarantined_at) as quarantined_date,
        -- 必須與模型內的 coalesce 逐字一致，否則 NULL 版本的那一群會在等值 join 落空，
        -- 兩邊各自留下一列孤兒 → 測試紅得莫名其妙（或更糟：兩個錯誤互相抵銷）。
        coalesce(dq_rule_version, '{{ var("unknown_member_key", "__UNKNOWN__") }}') as dq_rule_version,
        effective_quality_state,
        count(distinct order_id) as orders
    from {{ ref('int_orders_quarantine') }}
    group by quarantined_date, dq_rule_version, effective_quality_state

)

select
    coalesce(r.quarantined_date, s.quarantined_date)             as quarantined_date,
    coalesce(r.dq_rule_version, s.dq_rule_version)               as dq_rule_version,
    coalesce(r.effective_quality_state, s.effective_quality_state) as effective_quality_state,
    r.orders as rpt_orders,
    s.orders as src_orders
from rpt r
full outer join src s
    on  r.quarantined_date        = s.quarantined_date
    and r.dq_rule_version         = s.dq_rule_version
    and r.effective_quality_state = s.effective_quality_state
-- is distinct from：未命中側為 NULL，用 != 會讓「整個切片不見了」被 where 靜默濾掉。
where r.orders is distinct from s.orders
