-- int_order_items：把 ODS.items（JSON 陣列）攤平到 item 粒度，供未來的 fct_order_items
--
-- 來源選 int_orders（已過濾）而非 stg_orders：item 層的錯誤（quantity_non_positive、
--   unit_price_negative、discount_pct_out_of_range、non_finite_number）在攝入層就會讓
--   【整張訂單】has_clean_error=TRUE 被隔離，所以從 int_orders 出發天然保證
--   「Gold 不含 has_clean_error=TRUE」（DQ_ARCHITECTURE-TW Q0）。
--   若日後要做 item 層的髒資料 RCA，另建讀 int_orders_quarantine 的對應模型。
--
-- safe_cast 不可省：clean.py 明載「items 內的值未經 Pydantic 強轉，可能是字串」——
--   items 整包以 JSONB 落地，欄位值不經 ODSOrder 的型別強轉。用 cast 會讓一筆髒 item
--   炸掉整批；safe_cast 轉不動 → NULL，符合本專案「標記不阻斷」的品質哲學。
--
-- 衍生金額採【嚴格 NULL 傳播】，不做 coalesce：
--   NULL 帶資訊（「這筆沒有折扣資料」≠「折扣為 0」），COALESCE 是有損且單向的——
--   一旦在 int_ 把 NULL 壓成 0，全下游再也分不出「沒收集」與「真的是 0」
--   （CLOUD_LAYER-TW §5.5.5）。填值屬業務/呈現決定，應留到 dim_/fct_/rpt_ 依問題各自處理。
--
-- 物化＝table：上游 int_orders 已是 table 全量重建，本模型跟隨；不分區（理由同 int_orders）。

{{
    config(
        materialized='table',
        cluster_by=['order_id'],
    )
}}

with exploded as (

    -- WITH OFFSET 取陣列位置作為 item 身分：items 在 ODS 內是不可變的 JSONB 快照，
    -- 陣列順序固定，故 (raw_id, item_index) 是穩定的 item 粒度鍵。
    select
        o.raw_id,
        o.order_id,
        o.order_date,
        o.received_at,
        item_index,
        item
    from {{ ref('int_orders') }} as o,
        unnest(json_query_array(o.items)) as item with offset as item_index

),

typed as (

    select
        raw_id,
        order_id,
        order_date,
        received_at,
        item_index,

        -- 商品屬性（product 是 items 內的巢狀物件，見 schema.py ProductInfo）
        json_value(item, '$.product.product_id')   as product_id,
        json_value(item, '$.product.product_name') as product_name,
        json_value(item, '$.product.category')     as category,
        json_value(item, '$.product.sub_category') as sub_category,
        json_value(item, '$.product.brand')        as brand,
        -- 改名接縫：condition 在 SQL 語境易混淆，於此層標準化為 product_condition
        json_value(item, '$.product.condition')    as product_condition,

        -- 數值（safe_cast，理由見檔頭）
        safe_cast(json_value(item, '$.quantity')     as int64)   as quantity,
        safe_cast(json_value(item, '$.unit_price')   as float64) as unit_price,
        safe_cast(json_value(item, '$.cost_price')   as float64) as cost_price,
        safe_cast(json_value(item, '$.discount_pct') as float64) as discount_pct,
        safe_cast(json_value(item, '$.shipping_fee') as float64) as shipping_fee

    from exploded

)

select
    *,

    -- 代理鍵：raw_id 是物理身分（README〈raw_id 是物理身分、order_id 是業務身分〉），
    -- 配 item_index 即 item 粒度的唯一鍵。
    format('%d-%d', raw_id, item_index) as order_item_key,

    -- 衍生金額：嚴格 NULL 傳播（任一輸入為 NULL → 結果 NULL），見檔頭。
    -- discount_pct 的業務值域為 0~100（clean.py DISCOUNT_PCT_OUT_OF_RANGE），故除以 100。
    quantity * unit_price                        as gross_amount,
    quantity * unit_price * (1 - discount_pct / 100) as net_amount,
    quantity * cost_price                        as cost_amount

from typed
