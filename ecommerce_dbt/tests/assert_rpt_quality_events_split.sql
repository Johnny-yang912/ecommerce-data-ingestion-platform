-- initial_evaluation 的狀態劃分不變式：clean + quarantined 必須等於全部
--
-- process.py 攝入時對每筆訂單寫一筆 initial_evaluation，to_state 只可能是
-- 'quarantined' 或 'clean'（process.py:184 的三元式）。因此在 rpt_ 層必然有：
--     initial_clean + initial_quarantined = initial_evaluations
--
-- 這是 assert_orders_split_is_partition（int_ 層的互斥+窮盡）在報表層的同構檢查，
-- 但它守的是【不同的】東西——那支守「兩個模型的 WHERE 條件互補」，
-- 這支守「寬表的計數欄位窮盡了 to_state 的值域」。
--
-- ⭐ 它真正的作用是【值域擴張的警報器】：
--   rpt_quality_events_daily 選了寬表（每個狀態一個 countif 欄），而寬表的代價是
--   「上游多一個 to_state，下游要改 schema 才看得到」。若哪天攝入層新增一個
--   initial_evaluation 的目標狀態，count(*) 會漲、兩個 countif 不會 → 這支立刻紅，
--   而不是讓那些事件從報表裡靜默蒸發。寬表能安心用，靠的就是這支測試。
--
-- 用 != 而非 is distinct from：countif 永遠回傳 0 而非 NULL（沒有列就是 0），
-- 三個欄位都不可能是 NULL，故等值比較安全。

select
    event_date,
    rule_version,
    initial_evaluations,
    initial_clean,
    initial_quarantined
from {{ ref('rpt_quality_events_daily') }}
where initial_clean + initial_quarantined != initial_evaluations
