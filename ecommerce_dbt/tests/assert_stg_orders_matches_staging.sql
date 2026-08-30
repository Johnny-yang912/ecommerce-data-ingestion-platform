-- stg_orders 逐分區與 staging 對帳：Silver 不得掉列
--
-- ⭐ 這支測的東西與其它所有測試都不同：不是「列本身對不對」，而是「列還在不在」。
--    專案現有的測試（Hard Gate、投影、split、rollup）問的都是資料的【內容】是否合約；
--    但 2026-08-26 那次事故裡，活下來的每一列都完全正常——問題是【少了 550 列】。
--    內容型測試對這種失效是結構性地盲的，因為它們只檢查看得見的那些列。
--
-- 契約：stg_orders 對 staging 只做【去重】，不做任何過濾（髒列一律保留，攔截在 int_）。
--    所以 staging 的 distinct raw_id 數必須【逐分區】等於 stg_orders 的列數。
--    不等 ＝ 有列在 Silver 層消失了。已知的兩條路徑：
--      ① insert_overwrite 拿部分切片覆寫整個分區（見 stg_orders.sql 檔頭）
--      ② 去重鍵誤判，把不同 raw_id 當成同一筆折掉
--    兩者都不會拋錯，也都不會讓上游 staging 少一列——沒有這支測試就只能靠肉眼看 BI。
--
-- ⚠️ 對帳窗【必須 > stg_orders_lookback_days】。理由是這支測試唯一的失效模式：
--    壞掉的分區一旦滑出對帳窗，測試就再也看不到它，而它也同時滑出了回看窗、
--    例行跑批不會再碰它 —— 於是損壞被永久固化，且從此全綠。
--    2026-08-26 正是這樣熬過一夜才被發現的（回看窗 3 天，發現時已是第 4 天）。
--    窗開太小是「安靜地失去偵測能力」，不是「省錢」；兩個 var 要一起調。
--
-- 用 full outer join 而非單向 join：單向抓不到「stg_orders 多了」
-- （例如去重鍵失效導致重複列漏網），而扇出與掉列同樣是真實可能的失效。

with mirror as (

    select
        timestamp_trunc(received_at, day) as received_day,
        count(distinct raw_id) as rows_expected
    from {{ source('ecommerce_staging', 'orders') }}
    where received_at >= timestamp_sub(
        timestamp_trunc(current_timestamp(), day),
        interval {{ var('stg_orders_recon_window_days', 7) }} day
    )
    group by received_day

),

silver as (

    select
        timestamp_trunc(received_at, day) as received_day,
        count(*) as rows_actual
    from {{ ref('stg_orders') }}
    where received_at >= timestamp_sub(
        timestamp_trunc(current_timestamp(), day),
        interval {{ var('stg_orders_recon_window_days', 7) }} day
    )
    group by received_day

)

select
    coalesce(m.received_day, s.received_day) as received_day,
    m.rows_expected,
    s.rows_actual,
    m.rows_expected - s.rows_actual as missing
from mirror m
full outer join silver s
    on m.received_day = s.received_day
-- is distinct from（而非 !=）：full outer join 未命中的那一側是 NULL，
-- NULL != n 的結果是 NULL，會被 where 靜默濾掉——正好把最該抓的「整天不見了」放走。
where m.rows_expected is distinct from s.rows_actual
