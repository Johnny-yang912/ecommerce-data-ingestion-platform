-- Gold 契約（docs/zh-TW/design/data-quality.md）：int_orders 不得含 has_clean_error=TRUE 的列，
-- 【除非】它是被 Proposal B 重評估 promote 回來的。
--
-- 這條之所以不能直接寫成 `not_null` / `accepted_values` 之類的欄位測試，是因為它是
-- 「兩欄之間的條件關係」：has_clean_error=TRUE 本身在此表中合法（ODS 是不可變錨點，
-- promoted 記錄在 ODS 裡永遠是髒的），只有「髒且未被 promote」才是違規。

select
    raw_id,
    order_id,
    has_clean_error,
    effective_quality_state,
    quality_state_latest
from {{ ref('int_orders') }}
where has_clean_error = true
  and effective_quality_state != 'promoted'
