-- rpt_sales_daily_by_category：分類別日銷售預聚合（一列 = 一天 × 一分類 × 退貨旗標）
--
-- 上游一律走 Gold（fct_/dim_），刻意【不】從 int_ 直接拉。四個理由，後兩個是本專案特有的：
--   1. 口徑單一——「營收」只能有一條血緣，兩條就會有兩個數字而沒人知道誰錯；
--   2. 不重做 Gold 已經做過的語意決定（unknown member、item_count=0 的 LEFT JOIN…）；
--   3. 繞過 fct_ 會讓既有測試失效：assert_fct_orders_rollup_matches_items 保的是
--      fct_orders 的 rollup，rpt_ 若自己從 int_ 重算，那支測試完全不覆蓋它；
--   4. ⭐ 會推翻 int_ 層的架構前提——README.zh-TW §5.5「int_ 只被 DAG 內部消費
--      （非分析師 ad-hoc）」是 int_ 不分區的【唯一】理由。rpt_ 讀 int_ 等於把 int_
--      從內部建材升格成對外契約，那個分區決策就得重審，且 int_ 從此改不動。
--
-- 從 fct_order_items 出發（而非 fct_orders）：category 只存在於 item 層。
--   ⚠️ 因此 orders 必須是 count(distinct order_id)【不是】count(*)——
--      一張訂單在同一分類有多個品項時，count(*) 會把訂單重複計數。
--   ⚠️ 也因此本表【不放】訂單層度額（tax、delivery_days…）：那些是 header 度量，
--      會被 line 的列數放大，正是 fct_orders 檔頭那條「不得 join 兩張事實表後
--      同時聚合兩邊度量」禁令要防的事。
--
-- 取 fct_orders.returned 【不違反】上述禁令：這裡只取 header 的一個【維度旗標】，
--   不聚合它的任何度量。這個區別要說清楚，否則下一個人會以為註解自相矛盾。
--
-- ⭐ is_returned 進 grain，而不是輸出 net_amount / net_amount_excl_returned 兩組度量：
--   README.zh-TW §6.6 明載「淨營收要不要扣退貨尚無業務定義，returned 留在事實表當 flag、
--   【由下游決定】」——rpt_ 就是那個下游。進 grain 等於把選擇權完整交給 BI
--   （要含就全加、要扣就篩 is_returned = false）；輸出兩組度量等於我們替業務
--   挑了兩個它可能都不要的定義，把一個未定義的規則偷偷固化成事實。
--
-- ⭐ 不輸出任何比率欄位（AOV、毛利率、退貨率…）：理由同 rpt_quality_events_daily 檔頭——
--   BI roll up 到週會變成「比率的平均」而非「總和的比率」。分子分母都在這裡，BI 現算即可。
--
-- 物化＝table 全量重建（理由同 fct_，README.zh-TW §6.2：分區軸是 order_date＝業務時間，
--   與「資料何時變動」完全脫鉤，被 promote 的舊訂單任何回看窗都看不到）。
--   ⭐ 但【現在就掛 order_date 分區】，即使目前不增量——分區欄位現在加是免費的，
--   事後補要重建表。切增量的路徑見下方 TODO。

{{
    config(
        materialized='table',
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day',
        },
        partition_expiration_days=var('gold_partition_expiration_days', none),
        cluster_by=['category'],
    )
}}

-- TODO(增量)：全量重建的 wall-clock 或 slot 排擠有感時切增量。
--   路徑是「日 incremental + order_date 回看窗 ＋ 排程每週一次 --full-refresh」，
--   不是自己寫受影響分區 discovery——後者會讓一張【純業務報表】被迫依賴 quality_events
--   當變更偵測器（為了非語意的理由建立耦合），而且在 Proposal B 大規模回流那天
--   會退化成比全量還貴（README.zh-TW §5.4 第 2 點）。
--   代價是可寫進文件的一句話：追溯性修正在本報表的可見延遲 ≤ 7 天。
--   ⚠️ 切增量與補逐格對帳測試是同一件事的兩半，不得只做前者（見 _reports__models.yml）。

with items as (

    select
        i.order_date,

        -- dim_product 的 unknown member 屬性一律 NULL（見該檔），故 category 可能為 NULL；
        -- 補 '__UNKNOWN__' 讓 BI 不顯示空白分類。動的是【維度值】不是度量，可完整反查，
        -- 無損（同 README.zh-TW §6.5 對鍵的論證），不牴觸 docs/zh-TW/design/cloud-layer.md 的 NULL 鐵律。
        coalesce(p.category, '{{ var("unknown_member_key", "__UNKNOWN__") }}') as category,

        -- ⚠️ ODS.returned 是 nullable（models.py:42）→ 本欄可能為 NULL，
        --    且刻意【不】coalesce 成 false：NULL＝「退貨狀態未知」，不是「沒退貨」，
        --    壓成 false 就是 §5.5.5 鐵律禁止的有損單向 collapse。
        --    代價是 grain 裡有一個可為 NULL 的欄位，故 yml 對它【不】掛 not_null。
        o.returned as is_returned,

        i.order_id,
        i.customer_id,
        i.quantity,
        i.gross_amount,
        i.net_amount,
        i.cost_amount,
        i.shipping_fee

    from {{ ref('fct_order_items') }} i

    -- 兩個 join 都必須 LEFT：
    --   dim_product 有 unknown member 且 fct_ 的 FK 已 coalesce 過，理論上必定命中；
    --   INNER 只要哪天上游改動就會靜默掉列，而掉列在報表層的表現是「營收慢慢變小」，
    --   不報錯、沒人發現。assert_rpt_sales_no_item_loss 專門守這條。
    left join {{ ref('dim_product') }} p
        on i.product_id = p.product_id
    left join {{ ref('fct_orders') }} o
        on i.order_id = o.order_id

)

select
    -- ── grain ───────────────────────────────────────────────────────────────
    order_date,      -- 分區欄位
    category,
    is_returned,     -- 可為 NULL＝退貨狀態未知（見上方註解）

    -- ── 不可加度量 ⚠️ ────────────────────────────────────────────────────────
    -- 預聚合層的 COUNT(DISTINCT) 是結構性不可加的：跨 category 加總會重複
    -- （一張訂單的品項橫跨多分類）、跨日加總 customers 也會重複（同一顧客多天下單）。
    -- 只在【本表宣告的粒度】上有效，BI 上請勿對它做跨維度 SUM。
    -- 【觸發點】需要跨維度正確彙總 distinct 時，改存 BQ 原生 HLL sketch
    --   （HLL_COUNT.INIT → BYTES，上層用 HLL_COUNT.MERGE 合併，誤差 ~1%）。
    --   現在不做的原因不是麻煩，是 Looker Studio 的計算欄位呼叫不了 HLL_COUNT.MERGE，
    --   得再包一層 view——那個摩擦點目前不存在（同 §5.3「設計寫下來，觸發點到了才實作」）。
    count(distinct order_id)    as orders,
    -- ⚠️ 額外注意：fct_ 的 customer_id 已 coalesce 到 '__UNKNOWN__'，
    --    所有無識別的顧客會塌成【一個】顧客。這是 unknown member 的必然代價，不是 bug。
    count(distinct customer_id) as customers,

    -- ── 可加度量 ✅ ──────────────────────────────────────────────────────────
    count(*)            as items,
    sum(quantity)       as total_quantity,
    -- 全部承自上游的嚴格 NULL 傳播，刻意不 coalesce（docs/zh-TW/design/cloud-layer.md）。
    sum(gross_amount)   as gross_amount,
    sum(net_amount)     as net_amount,
    sum(cost_amount)    as cost_amount,
    sum(shipping_fee)   as shipping_amount,

    -- ⭐ 沿用 fct_orders 的顯性化【手法】（不是取用它的欄位——這三個 counter 在本層
    --    重新算，fct_ 的同名欄位是兄弟不是父母）：BQ 的 SUM() 靜默忽略 NULL，
    --    一個 safe_cast 失敗或根本沒送的 item 會讓上面的加總少算一筆而不報錯。
    --    不 COALESCE（有損單向），改成把不完整性攤在陽光下，讓 BI 端自己判斷
    --    這個分類的金額加總可不可信。
    --
    -- ⚠️【一條 NULL 傳播鏈一隻】，理由與實測數字見 fct_orders.sql 檔頭。
    --    本層特別容易踩：整格 sum 為 NULL 時 Looker Studio 只顯示一個空白，
    --    而【部分】缺失更危險——sum 會回一個看起來完全正常的數字，只是少算幾筆，
    --    連空白都不會有。這三個 counter 是分辨兩者的唯一依據。
    --
    -- ⭐ 這三個 counter 的資料來源是【可重現的】：seed_demo.py 的 --missing-cost-rate
    --    以 item 為粒度、cost_price 與 shipping_fee 各自獨立抽。這兩件事都是刻意的：
    --      · 獨立抽 → items_missing_cost 與 items_missing_shipping 會真的分岔。
    --        早期手動灌的 demo 資料兩者永遠同時缺，於是這兩欄恆等，看起來像同一個
    --        訊號的複本，分開設計的意義完全看不出來。
    --      · item 粒度 → 產得出「同一張訂單裡有些品項有成本、有些沒有」，也就是
    --        上面那個【部分缺失】的危險狀態。舊資料是整張訂單全缺，只會讓整格
    --        變 NULL（BI 上是空白），演不到真正該防的那一種。
    countif(net_amount is null)   as items_missing_amount,
    countif(cost_amount is null)  as items_missing_cost,
    countif(shipping_fee is null) as items_missing_shipping

from items
group by order_date, category, is_returned
