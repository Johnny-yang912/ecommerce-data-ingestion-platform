-- fct_order_items：訂單品項粒度事實表（一列 = 一個訂單的一個品項）
--
-- grain = (order_id, item_index)。刻意用 order_id 而非上游 int_order_items 的 raw_id：
--   Gold 面向分析師，README〈raw_id 是物理身分、order_id 是業務身分〉——分析師不需要
--   知道 raw_id 是什麼。兩者在 ODS 皆為 UNIQUE、1:1，改用 order_id 不損失唯一性。
--   raw_id 仍保留為血緣欄位（供追溯回 ODS/Raw），但【不】是鍵。
--   上游那支代理鍵已改名為 int_order_item_key 並停在 int_ 層，避免同名不同值。
--
-- 為什麼把 order 層的維度 FK（customer_id）與 order_date 一路帶下來：
--   line fact 應該能【獨立查詢】而不必回 join header——「某商品在白金會員間的銷量」
--   不該被迫掃兩張表。BQ 是列式儲存，多帶幾個低基數欄位的成本接近零，
--   換來的是消費端少一次 join、以及本表能與 fct_orders 用同一組分區裁切。
--
-- product_name 刻意【重複】保留一份 line 級快照（決策 D 第 2 層）：
--   dim_product 是 SCD1，衝突時只挑得到一個屬性版本；這裡留的是【那筆訂單當下】
--   實際收到的商品名，真值不遺失、可與維度對照。與 fct_orders 保留
--   membership_tier_at_order 是同一個手法：讓事實表承載「當時」，維度承載「現在」。
--
-- 分區與過期政策同 fct_orders（見該檔頭）。兩表【必須】用同一組分區設定——
--   分區欄位或過期政策不一致，兩邊被回收的列集合就會不同，
--   relationships 與 rollup 一致性測試都會破裂。

{{
    config(
        materialized='table',
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day',
        },
        partition_expiration_days=var('gold_partition_expiration_days', none),
        cluster_by=['product_id', 'order_id'],
    )
}}

select
    -- ── 鍵與 grain ──────────────────────────────────────────────────────────
    -- 代理鍵與宣告的 grain 同基底（見檔頭）。item_index 補零，讓字典序＝數值序，
    -- 否則 'A-10' 會排在 'A-2' 前面，做為排序鍵或分頁游標時會出錯。
    format('%s-%03d', i.order_id, i.item_index) as order_item_key,
    i.order_id,
    i.item_index,
    i.order_date,                                -- 分區欄位（承自訂單）

    -- ── 維度 FK（決策 F/G：natural key + unknown member）─────────────────────
    coalesce(i.product_id, '{{ var("unknown_member_key", "__UNKNOWN__") }}')  as product_id,
    coalesce(o.customer_id, '{{ var("unknown_member_key", "__UNKNOWN__") }}') as customer_id,

    -- ── line 級屬性快照（見檔頭：真值保留，供與 dim_product 對照）────────────
    i.product_name as product_name_at_order,

    -- ── line 度量 ───────────────────────────────────────────────────────────
    -- 全部承自 int_order_items 的 safe_cast + 嚴格 NULL 傳播：任一輸入為 NULL
    -- 則衍生金額為 NULL。刻意不 COALESCE（CLOUD_LAYER-TW §5.5.5），
    -- 訂單層的加總可信度由 fct_orders.items_missing_amount 表達。
    i.quantity,
    i.unit_price,
    i.cost_price,
    i.discount_pct,          -- 業務值域 0~100；NULL＝無折扣資料，不是 0
    i.shipping_fee,
    i.gross_amount,          -- quantity × unit_price
    i.net_amount,            -- gross × (1 - discount_pct/100)
    i.cost_amount,           -- quantity × cost_price

    -- ── 血緣 ────────────────────────────────────────────────────────────────
    i.raw_id,                -- 物理身分（非鍵，僅供追溯回 ODS/Raw）
    i.received_at

from {{ ref('int_order_items') }} i
-- 取 order 層的維度 FK。int_order_items 的來源已是 int_orders，故此 join 必定命中；
-- 仍用 LEFT 以免未來上游改動時靜默掉列（掉列在 Gold 是最危險的失效模式）。
left join {{ ref('int_orders') }} o
    on i.raw_id = o.raw_id
