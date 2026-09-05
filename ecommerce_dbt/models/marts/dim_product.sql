-- dim_product：商品維度（SCD1，取最新一筆訂單品項的屬性）
--
-- 與 dim_customer 同構：沒有商品主檔，屬性隨每筆訂單品項帶進來，故從 int_order_items
-- 反推。決勝鍵取最新（order_date desc, item_index desc）。
--
-- ⚠️ 這裡有一個【資料本身的】問題，ADR-0048 選擇「標記而非阻擋」：
--   同一個 product_id 可能在不同訂單帶著不同的 product_name / category / brand
--   （2026-08 實測：342 個 product_id 中 163 個衝突，根因是 load_test.py 對 product_id
--     與其屬性各抽一次獨立亂數；已修正，見該檔 make_product()）。
--   衝突發生時 SCD1 只能挑一個，dim_product 的屬性就會與部分訂單的實際值不符。
--
--   處置分三層，刻意【不】用 unique 測試把它變成硬錯誤：
--     1. 本模型用明確決勝鍵保證【確定性】——衝突不會讓 grain 破裂，只會挑最新的那個
--     2. fct_order_items 保留 line 級的 product_name 快照——真值不遺失，可對照
--     3. tests/assert_product_attributes_stable.sql（severity: warn）監控衝突數
--   warn 而非 error，因為這是【上游契約訊號】而非本層的正確性缺陷：product_id 若真的
--   無法唯一決定商品屬性，該修的是上游或 data contract，不是讓整條 DAG 停下來。
--   （同一個判斷邏輯見 docs/zh-TW/design/data-quality.md：has_schema_drift 沒有攔截權限、只能告警。）
--
-- 不分區、cluster_by(product_id)：理由同 dim_customer（維度按鍵 join，非按日期掃）。

{{
    config(
        materialized='table',
        cluster_by=['product_id'],
    )
}}

with ranked as (

    select
        *,
        row_number() over (
            partition by product_id
            order by order_date desc, item_index desc
        ) as _rn
    from {{ ref('int_order_items') }}
    where product_id is not null   -- NULL 的商品由下方 unknown member 承接

),

latest as (

    select
        product_id,
        product_name,
        category,
        sub_category,
        brand,
        product_condition,

        -- 血緣：這筆屬性取自哪一個訂單品項（衝突時用來查「為什麼是這個名字」）
        order_id   as sourced_from_order_id,
        order_date as sourced_from_order_date
    from ranked
    where _rn = 1

)

select * from latest

union all

-- unknown member（理由見 dim_customer 檔頭 2）
select
    '{{ var("unknown_member_key", "__UNKNOWN__") }}' as product_id,
    cast(null as string) as product_name,
    cast(null as string) as category,
    cast(null as string) as sub_category,
    cast(null as string) as brand,
    cast(null as string) as product_condition,
    cast(null as string) as sourced_from_order_id,
    cast(null as date)   as sourced_from_order_date
