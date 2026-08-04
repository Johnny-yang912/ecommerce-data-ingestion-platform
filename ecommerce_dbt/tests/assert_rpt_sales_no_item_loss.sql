-- rpt_sales_daily_by_category 不得掉品項
--
-- rpt_sales 引入了整條 DAG 到目前為止【唯一一組新的 join】（× dim_product、× fct_orders）。
-- join 悄悄變成 INNER、或維度鍵對不上，會讓品項整批消失——而在報表層，掉列的表現是
-- 「營收慢慢變小」，不報錯、不會有人發現。這支測試守的就是那個契約。
--
-- ⭐ 為什麼在 rpt_ 是 table 全量重建的現在【就要寫】（而 assert_rpt_sales_matches_fct
--    那種逐格金額對帳卻刻意先不寫）：
--    金額對帳在全量重建下是【同義反覆】——rpt_ 的 sum 就是把 fct_ 的欄位加起來，
--    用同一段 SQL 驗自己，恆綠、零資訊，價值要到切增量那天才兌現。
--    但【列數】不一樣：它跨越了兩個 join，而 join 掉列是真實可能發生的失效，
--    與物化策略無關。所以這支現在就有價值，那支現在沒有。
--
-- ⚠️ 錨在 order_date 窗上而非直接比 count(*)，理由同
--    assert_fct_orders_complete_projection：兩個 60 天時鐘掛在不同軸上，
--    邊界日的 reaper 回收不同步會讓無窗的比對變成每天固定紅一陣子的 flaky test。
--    這裡兩表都按 order_date 分區、都是 CREATE OR REPLACE 同步回收，風險比那支低，
--    但窗是免費的，且啟用帳單後兩支要一起改成與保留政策一致，留著比較好對齊。
--
-- 用 full outer join 而非單向 join：單向只抓得到「rpt_ 少了」，
-- 抓不到「rpt_ 多了」（例如 dim_product 哪天不再是 product_id 唯一 → join 扇出放大）。

with rpt as (

    select
        order_date,
        sum(items) as items
    from {{ ref('rpt_sales_daily_by_category') }}
    where order_date >= date_sub(current_date(), interval {{ var('gold_projection_window_days', 59) }} day)
    group by order_date

),

fct as (

    select
        order_date,
        count(*) as items
    from {{ ref('fct_order_items') }}
    where order_date >= date_sub(current_date(), interval {{ var('gold_projection_window_days', 59) }} day)
    group by order_date

)

select
    coalesce(r.order_date, f.order_date) as order_date,
    r.items as rpt_items,
    f.items as fct_items
from rpt r
full outer join fct f
    on r.order_date = f.order_date
-- is distinct from（而非 !=）：full outer join 未命中的那一側是 NULL，
-- NULL != n 的結果是 NULL，會被 where 靜默濾掉——正好把最該抓的「整天不見了」放走。
where r.items is distinct from f.items
