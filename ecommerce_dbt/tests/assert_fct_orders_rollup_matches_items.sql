-- rollup 一致性不變式（決策 A 方案 C 的核心安全網）
--
-- fct_orders 把訂單總額 rollup 進來，換取單表可查詢；代價是「同一個數字存在兩處、
-- 可能不一致」。這支測試把那個代價從紀律保證升級成機制保證——與 int_ 層用
-- assert_orders_split_is_partition 買回「刻意複製共用區塊」的風險，是同一個手法。
--
-- ⚠️ is distinct from（而非 =）不可省：金額是嚴格 NULL 傳播的，NULL = NULL 結果是
--    NULL 而非 TRUE，用 = 會讓「兩邊都是 NULL」的列被 WHERE 靜默濾掉，
--    測試就永遠不會發現兩邊 NULL 的成因不同。is distinct from 把 NULL 當值比。
--
-- 兩表分區設定相同（order_date/DAY + 同一份過期政策），故被分區過期回收的列集合
-- 一致，本測試不需要加時間窗——這與 assert_fct_orders_complete_projection 不同，
-- 那支比對的是【不分區的 int_orders】，兩邊保留期不同才需要窗。

with items as (

    select
        order_id,
        count(*)                      as item_count,
        -- 三隻金絲雀都要比：counter 也是 rollup 欄位，同樣「同一個數字存在兩處」，
        -- 漏掉哪一隻，哪一隻就是本表唯一沒有機制保證的 rollup 欄位。
        countif(net_amount is null)   as items_missing_amount,
        countif(cost_amount is null)  as items_missing_cost,
        countif(shipping_fee is null) as items_missing_shipping,
        sum(quantity)                 as total_quantity,
        sum(gross_amount)           as gross_amount,
        sum(net_amount)             as net_amount,
        sum(cost_amount)            as cost_amount,
        sum(shipping_fee)           as shipping_amount
    from {{ ref('fct_order_items') }}
    group by order_id

)

select
    o.order_id,
    o.item_count           as fct_item_count,
    i.item_count           as items_item_count,
    o.net_amount           as fct_net_amount,
    i.net_amount           as items_net_amount
from {{ ref('fct_orders') }} o
-- LEFT：沒有任何品項的訂單在 items 端無列，其 rollup 應為 item_count=0、金額 NULL
left join items i
    on o.order_id = i.order_id
where
       o.item_count           is distinct from coalesce(i.item_count, 0)
    or o.items_missing_amount   is distinct from coalesce(i.items_missing_amount, 0)
    or o.items_missing_cost     is distinct from coalesce(i.items_missing_cost, 0)
    or o.items_missing_shipping is distinct from coalesce(i.items_missing_shipping, 0)
    or o.total_quantity       is distinct from i.total_quantity
    or o.gross_amount         is distinct from i.gross_amount
    or o.net_amount           is distinct from i.net_amount
    or o.cost_amount          is distinct from i.cost_amount
    or o.shipping_amount      is distinct from i.shipping_amount
