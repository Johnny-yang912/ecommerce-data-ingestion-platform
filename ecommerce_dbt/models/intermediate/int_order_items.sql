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
        --
        -- ⭐ 金額一律 NUMERIC，【不】用 FLOAT64（2026-08 實測後改）：
        --   FLOAT64 的 SUM() 不滿足結合律——同一組數字換個累加順序，尾數會差一個 bit。
        --   fct_orders 的 rollup 與 fct_order_items 的重新聚合走不同執行計畫，
        --   於是 assert_fct_orders_rollup_matches_items 的精確比對必然隨機失敗。
        --   實測：39 筆訂單不一致，最大相對誤差 3.442e-16（≈1 ULP，FLOAT64 epsilon 2.22e-16）。
        --
        --   這個問題潛伏了很久才浮現：在此之前 60 天窗內的資料每張訂單【剛好一個品項】，
        --   單值 SUM() 沒有累加就沒有順序問題。第一批多品項資料進來當天就炸了。
        --
        --   NUMERIC 是精確十進位（precision 38 / scale 9），加總精確且與順序無關，
        --   所以測試可以維持【精確比對】而不必退讓成容差比對——容差要設多少、
        --   隨資料量成長要不要調，那是個會回來找你的決定。
        --   金額本來就不該用浮點數，這裡只是補上這條規則。
        --
        --   safe_cast 的容錯行為在 NUMERIC 下同樣成立（已實測）：
        --     轉不動（'abc'）→ NULL；超出 precision（1e400）→ NULL；
        --     小數超過 scale 9 → 四捨五入，【不】報錯。
        --   所以「一筆髒 item 不會炸掉整批」這個保證沒有因為換型別而失效。
        --
        --   quantity 維持 INT64：它是計數不是金額，整數才是正確型別
        --   （INT64 × NUMERIC → NUMERIC，下方衍生金額不受影響）。
        safe_cast(json_value(item, '$.quantity')     as int64)   as quantity,
        safe_cast(json_value(item, '$.unit_price')   as numeric) as unit_price,
        safe_cast(json_value(item, '$.cost_price')   as numeric) as cost_price,
        safe_cast(json_value(item, '$.discount_pct') as numeric) as discount_pct,
        safe_cast(json_value(item, '$.shipping_fee') as numeric) as shipping_fee

    from exploded

)

select
    *,

    -- 內部代理鍵：raw_id 是物理身分（README〈raw_id 是物理身分、order_id 是業務身分〉），
    -- 配 item_index 即 item 粒度的唯一鍵。
    -- 刻意帶 int_ 前綴、【不】往 Gold 帶：fct_order_items 的 grain 宣告為
    -- (order_id, item_index)，代理鍵必須與宣告同基底，故在該層另造 order_item_key。
    -- 同名不同值會誤導消費者，所以本層改名而非沿用。
    format('%d-%d', raw_id, item_index) as int_order_item_key,

    -- 衍生金額：嚴格 NULL 傳播（任一輸入為 NULL → 結果 NULL），見檔頭。
    -- discount_pct 的業務值域為 0~100（clean.py DISCOUNT_PCT_OUT_OF_RANGE），故除以 100。
    quantity * unit_price                        as gross_amount,
    quantity * unit_price * (1 - discount_pct / 100) as net_amount,
    quantity * cost_price                        as cost_amount

from typed
