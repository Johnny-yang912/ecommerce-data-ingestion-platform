{{ config(severity = 'warn') }}

-- 商品屬性穩定性監控（決策 D 第 3 層）
--
-- 抓的是：同一個 product_id 在不同訂單品項上帶著不同的 product_name / category /
-- brand / sub_category / product_condition。發生時 dim_product 的 SCD1 只挑得到一個
-- 版本，維度屬性就會與部分訂單的實際值不符。
--
-- ⚠️ severity 刻意是 warn，不是 error：
--   這是【上游契約訊號】，不是本層的正確性缺陷——product_id 若無法唯一決定商品屬性，
--   該修的是上游或 data contract，不是讓整條 DAG 停下來。判斷邏輯與
--   DQ_ARCHITECTURE-TW 的 has_schema_drift 一致：drift 沒有攔截權限，只能告警。
--   （對照組：assert_orders_split_is_partition 與 assert_fct_orders_* 是 error，
--     因為那些是【我們自己的 SQL 對不對】。）
--
-- 歷史：2026-08 首測 342 個 product_id 中 163 個衝突，根因是 load_test.py 對
--   product_id 與其屬性各抽一次獨立亂數；已於 make_product() 修正為以 product_id
--   為 seed 決定屬性。此測試留著，是為了在上游【下次】漂移時仍能被看見。

select
    product_id,
    count(distinct product_name)      as n_names,
    count(distinct category)          as n_categories,
    count(distinct sub_category)      as n_sub_categories,
    count(distinct brand)             as n_brands,
    count(distinct product_condition) as n_conditions
from {{ ref('int_order_items') }}
where product_id is not null
group by product_id
having
       count(distinct product_name)      > 1
    or count(distinct category)          > 1
    or count(distinct sub_category)      > 1
    or count(distinct brand)             > 1
    or count(distinct product_condition) > 1
