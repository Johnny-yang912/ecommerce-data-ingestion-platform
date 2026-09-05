-- 不變式：int_orders ∪ int_orders_quarantine 對 stg_orders 是「完整劃分」——
-- 互斥（不重複）+ 窮盡（不遺漏），每個 raw_id 必須恰好出現一次。
--
-- 為什麼這支測試是必要的：兩個模型各自持有一份逐字相同的共用區塊（刻意的複製，
-- 見 ADR-0045），互補性靠紀律而非機制保證。這支測試是唯一的自動化安全網，
-- 它守的具體破口有三個（docs/zh-TW/design/transformation.md §3〈對齊清單〉）：
--   #2 漏了 coalesce → 無事件的髒列 is_effectively_clean 為 NULL，
--      `where flag` 與 `where not flag` 都不成立 → 該列從【兩張表同時消失】
--   #3 誤把 LEFT JOIN 寫成 INNER → 所有無品質事件的列整批消失
--   #1/#4 兩邊 WHERE 條件或視窗決勝鍵不一致 → 某些列重複出現或遺漏
--
-- ⚠️ severity 維持預設 error，永不降級、永不 --exclude。

with landed as (

    select raw_id from {{ ref('int_orders') }}
    union all
    select raw_id from {{ ref('int_orders_quarantine') }}

),

counted as (

    select raw_id, count(*) as appearances
    from landed
    group by raw_id

)

select
    coalesce(s.raw_id, c.raw_id)   as raw_id,
    coalesce(c.appearances, 0)     as appearances,
    case
        when s.raw_id is null then 'orphan_in_int_layer'   -- 憑空多出來（stg 沒有）
        when c.appearances is null then 'missing'          -- 兩張表都沒有（漏列）
        else 'duplicated'                                  -- 兩張表都有（互斥破裂）
    end as violation
from {{ ref('stg_orders') }} s
full outer join counted c
    on s.raw_id = c.raw_id
where coalesce(c.appearances, 0) != 1
   or s.raw_id is null
