-- stg_quality_events 逐分區與 staging 對帳：Silver 不得掉列
--
-- 這是 assert_stg_orders_matches_staging 的孿生測試。兩支都存在的理由，本身就是
-- 2026-08-26 事故的第二幕：同一個「左邊界未對齊日界」的缺陷同時存在於兩支增量模型，
-- 08-30 上午只修了 stg_orders 並補了它的對帳測試——而 stg_quality_events 照樣是 250 列，
-- 一路綠著顯示到 BI。**一支模型一支對帳測試**，不能靠「另一支有測」推論這一支安全。
--
-- ⭐ 測的是「列還在不在」，不是「列對不對」。專案其它測試（Hard Gate、投影、split、
--    rollup）問的都是內容合約；掉列的失效裡活下來的每一列都完全正常，內容型測試
--    對它結構性地盲。
--
-- 契約：stg_quality_events 對 staging 只做【去重】，不做任何過濾。
--    所以 staging 的 distinct id 數必須【逐分區】等於 stg_quality_events 的列數。
--    ★ 去重鍵＝id（事件 PK）而非 raw_id——一個 raw_id 合法地有多個事件，
--      拿 raw_id 對帳會把正常的事件序列誤判成「多出來的列」。這與模型的去重鍵一致。
--    不等 ＝ 有事件在 Silver 層消失了。已知路徑同 stg_orders：
--      ① insert_overwrite 拿部分切片覆寫整個分區（見 stg_quality_events.sql 檔頭）
--      ② 去重鍵誤判，把不同 id 當成同一筆折掉
--
-- ⚠️ 對帳窗【必須 > stg_quality_events_lookback_days】。這支測試唯一的失效模式是：
--    壞掉的分區一旦滑出對帳窗，測試就再也看不到它，而它同時也滑出了回看窗、
--    例行跑批不會再碰它 —— 損壞被永久固化，且從此全綠。兩個 var 要一起調。
--
-- 用 full outer join 而非單向 join：單向抓不到「stg_quality_events 多了」
-- （去重鍵失效導致重複列漏網），而扇出與掉列同樣是真實可能的失效。

with mirror as (

    select
        timestamp_trunc(event_at, day) as event_day,
        count(distinct id) as rows_expected
    from {{ source('ecommerce_staging', 'quality_events') }}
    where event_at >= timestamp_sub(
        timestamp_trunc(current_timestamp(), day),
        interval {{ var('stg_quality_events_recon_window_days', 7) }} day
    )
    group by event_day

),

silver as (

    select
        timestamp_trunc(event_at, day) as event_day,
        count(*) as rows_actual
    from {{ ref('stg_quality_events') }}
    where event_at >= timestamp_sub(
        timestamp_trunc(current_timestamp(), day),
        interval {{ var('stg_quality_events_recon_window_days', 7) }} day
    )
    group by event_day

)

select
    coalesce(m.event_day, s.event_day) as event_day,
    m.rows_expected,
    s.rows_actual,
    m.rows_expected - s.rows_actual as missing
from mirror m
full outer join silver s
    on m.event_day = s.event_day
-- is distinct from（而非 !=）：full outer join 未命中的那一側是 NULL，
-- NULL != n 的結果是 NULL，會被 where 靜默濾掉——正好把最該抓的「整天不見了」放走。
where m.rows_expected is distinct from s.rows_actual
