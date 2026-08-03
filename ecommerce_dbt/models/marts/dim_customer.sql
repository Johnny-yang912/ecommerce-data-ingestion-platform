-- dim_customer：顧客維度（SCD1，取最新一筆訂單的屬性）
--
-- 沒有獨立的顧客主檔——顧客屬性是隨每筆訂單帶進來的，所以維度必須從 int_orders 反推。
-- 這帶出兩個決定：
--
-- 1. SCD1 而非 SCD2（README.zh-TW §6.3 決策 E）：同一個 customer_id 的 membership_tier
--    會隨升等改變，SCD1 會讓歷史訂單被貼上「現在的」等級。補救不是引入 dbt snapshot，
--    而是【由事實表承載當下快照】——fct_orders.membership_tier_at_order 記錄下單當時的
--    等級。訂單裡帶的顧客屬性本來就是下單當下的快照，用事實表承載它等於免費拿到
--    type-2 的效果。於是兩種問題都答得了：
--      「白金會員【現在】的總消費」→ join dim_customer.membership_tier
--      「下單【當時】是白金的訂單」→ 直接讀 fct_orders.membership_tier_at_order
--    SCD2 是備妥但未啟用的設計，觸發點＝啟用帳單（見 README.zh-TW §6.3）。
--
-- 2. unknown member（決策 G）：customer_id 在 ODS 是 nullable。星狀模型不該讓事實表
--    帶 NULL FK——INNER JOIN 會靜默掉列、LEFT JOIN 讓 BI 顯示空白，兩種都不好。
--    故維度補一筆 '__UNKNOWN__'，事實表 coalesce 到它。
--    這【不】牴觸 CLOUD_LAYER-TW §5.5.5 的 NULL 鐵律：鐵律禁止的是在共享層對【度量】
--    做有損 collapse（NULL→0 之後分不出「沒收集」與「真的是 0」）；這裡動的是【鍵】，
--    且 '__UNKNOWN__' 可完整反查回「這筆沒有顧客識別」，是無損的。
--
-- 刻意【不】分區（決策：見 README.zh-TW §6.2）：維度是按鍵 join 進來的，不是按日期
--   範圍掃的，分區欄位對 join 沒有裁切作用，只會換來一堆小分區與 metadata 開銷。
--   改 cluster_by(customer_id)，對齊實際的 access pattern。

{{
    config(
        materialized='table',
        cluster_by=['customer_id'],
    )
}}

with ranked as (

    -- 決勝鍵：最新一筆訂單的屬性勝出。received_at / raw_id 是為了在同一天多筆訂單時
    -- 保證確定性——少了它們，同日訂單的屬性誰勝出會隨 BQ 的執行順序漂移。
    select
        *,
        row_number() over (
            partition by customer_id
            order by order_date desc, received_at desc, raw_id desc
        ) as _rn
    from {{ ref('int_orders') }}
    where customer_id is not null   -- NULL 的顧客由下方 unknown member 承接

),

latest as (

    select
        customer_id,
        customer_name,
        age,
        gender,
        membership_tier,
        registration_date,
        acquisition_channel,
        newsletter_subscribed,
        preferred_payment_method,
        preferred_device,

        -- 維度自身的血緣：這筆屬性是從哪張訂單取來的，供追溯 SCD1 的「最新」是哪一筆
        order_id   as sourced_from_order_id,
        order_date as sourced_from_order_date
    from ranked
    where _rn = 1

)

select * from latest

union all

-- unknown member（見檔頭 2）。屬性一律 NULL——我們不知道這位顧客是誰，
-- 任何填值都是憑空捏造；NULL 在此正確表達「未知」。
select
    '{{ var("unknown_member_key", "__UNKNOWN__") }}' as customer_id,
    cast(null as string) as customer_name,
    cast(null as int64)  as age,
    cast(null as string) as gender,
    cast(null as string) as membership_tier,
    cast(null as date)   as registration_date,
    cast(null as string) as acquisition_channel,
    cast(null as bool)   as newsletter_subscribed,
    cast(null as string) as preferred_payment_method,
    cast(null as string) as preferred_device,
    cast(null as string) as sourced_from_order_id,
    cast(null as date)   as sourced_from_order_date
