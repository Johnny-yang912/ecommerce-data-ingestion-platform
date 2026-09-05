-- fct_orders：訂單粒度事實表（一列 = 一筆訂單）
--
-- 與 fct_order_items 構成 Kimball 的 header/line 雙事實表。兩者以 order_id 對齊。
-- ⚠️ 【不得】join 兩表後同時聚合兩邊的度量——header 的訂單總額會被 line 的列數放大
--    （重複計算）。要 item 明細就查 fct_order_items，要訂單總額就查本表，不要混。
--
-- 度量分配（ADR-0047）：訂單總額 rollup 進本表，並以
--   tests/assert_fct_orders_rollup_matches_items.sql 逐單斷言與 fct_order_items 一致。
--   理由與 int_ 層「刻意複製 + assert_orders_split_is_partition 買回風險」同構：
--   用一支測試把「兩處數字可能不一致」從紀律保證升級成機制保證，換來單表可查詢。
--
-- ⭐ 嚴格 NULL 傳播在 rollup 處的陷阱（docs/zh-TW/design/cloud-layer.md）：
--   int_order_items 的金額是嚴格 NULL 傳播的（任一輸入 NULL → 結果 NULL），
--   而 BQ 的 SUM() 【會靜默忽略 NULL】——一筆訂單裡只要有一個 item 的 discount_pct
--   safe_cast 失敗，訂單總額就會少算一個品項，不報錯、不留痕跡。
--   處置刻意【不】用 COALESCE（那是有損且單向的，一旦壓成 0 就分不出「沒收集」與
--   「真的是 0」），改成把不完整性【顯性化】，讓下游自己判斷這個加總可不可信。
--   填值是業務決定，屬 rpt_ 或消費端的高度，不是本層。
--
-- ⭐ 金絲雀必須【一條 NULL 傳播鏈養一隻】，不能只養一隻（2026-08 實測後改）：
--   int_order_items.sql:106-108 是三條【互相獨立】的鏈——
--     gross → net：quantity × unit_price × (1 - discount_pct / 100)
--     cost       ：quantity × cost_price
--     shipping   ：直接欄位，不衍生
--   本檔原本只有 items_missing_amount（盯 net），並在此處論證「net 是最末端的
--   衍生值（吃三個輸入），故用它當金絲雀」。那句話在【net 這條鏈上】成立——
--   gross 為 NULL ⇒ net 必為 NULL，故 net 涵蓋 gross——但它被錯誤地推廣成
--   「整張表的金絲雀」，而 cost_price 與 shipping_fee 根本不在 net 的上游。
--   實測代價（dev, 2026-08）：631 個品項中 207 個 cost_amount 為 NULL，
--   items_missing_amount 對它們的偵測率是 0%；反映到 rpt_ 是整整 27 個切片
--   成本欄空白卻沒有任何旗標，靠人眼在 Looker Studio 上看見才發現。
--   ⚠️ 根因值得記一筆：cost_price 在攝入層【完全沒有規則】（schema.py:46 是
--   Optional、clean.py:118 只把它列入 non-finite 檢查），缺值合法、不產生
--   quality_event、不會 has_clean_error、不會被 quarantine——整條 DQ 管線在設計上
--   就不管它。所以這裡的 counter 是它唯一可能現形的地方，不是錦上添花。
--
--   命名刻意【不】對稱：items_missing_amount 沒有跟著改名為 items_missing_net。
--   改名要同步動 rollup 測試、rpt_ 層與 Looker 既有報表，換到的只有命名整潔；
--   歧義改用文件補（見 _marts__models.yml 該欄描述）。
--
-- 分區（見 docs/zh-TW/design/transformation.md §4〈分區〉）：按 order_date(DAY)，因為 Gold 服務分析師，
--   最常、最貴的查詢是按業務時間過濾（docs/zh-TW/design/cloud-layer.md）。
--   實測（2026-08，540 筆）：cluster_by 單獨已裁掉 82% 掃描量，分區再往下拿 9pp——
--   分區的主要價值不在裁切量，而在【成本可預測】（分區裁切由 metadata 在查詢前決定，
--   dry run 準確；clustering 是 block 級、dry run 會高估）與【分區級操作】。
--   刻意【不】上 require_partition_filter：staging 開它零副作用（存取一律帶 received_at），
--   但 Gold 服務分析師 ad-hoc 與 Looker Studio 的探索式查詢，常常不帶日期過濾，
--   開了就是 400。防線改用 BQ custom quota（每日掃描上限）——那防的是系統性濫用，
--   保險絲防的是單次失誤，兩者不是同一件事。
--
-- ⚠️ partition_expiration_days 用 var gate 住，預設不輸出：BQ sandbox 硬鎖 60 天，
--   任何 > 60 天的設定會讓 job 回 billingNotEnabled 而【整個 dbt run 失敗】
--   （實測見 docs/zh-TW/design/cloud-layer.md）。啟用帳單後：
--     dbt run --vars '{gold_partition_expiration_days: 1825}'
--   1825 天（5 年）是為了避開單表 4000 分區上限——DAY 粒度約 11 年就撞頂，
--   而 Gold 與 staging 不同，是要留全歷史的。超過 5 年的需求改 MONTH 粒度（需重建表）。
--
-- 物化＝table 全量重建，刻意【不】增量：理由同 int_ 層（ADR-0046）且更強——
--   Gold 的分區軸是 order_date（業務時間），與「資料何時變動」完全脫鉤。一筆 2024 年的
--   訂單今天被 Proposal B promote，任何按 order_date 的回看窗都永遠看不到它。

{{
    config(
        materialized='table',
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day',
        },
        partition_expiration_days=var('gold_partition_expiration_days', none),
        cluster_by=['customer_id'],
    )
}}

with item_rollup as (

    select
        order_id,

        count(*)                      as item_count,

        -- 加總可信度的顯性標記，【一條 NULL 傳播鏈一隻金絲雀】（見檔頭）。
        -- net 這隻涵蓋 gross（gross 為 NULL ⇒ net 必為 NULL），
        -- 但 cost 與 shipping 是獨立的鏈，不在它的下游，故各自需要一隻。
        countif(net_amount is null)   as items_missing_amount,
        countif(cost_amount is null)  as items_missing_cost,
        countif(shipping_fee is null) as items_missing_shipping,

        sum(quantity)      as total_quantity,
        sum(gross_amount)  as gross_amount,
        sum(net_amount)    as net_amount,
        sum(cost_amount)   as cost_amount,
        sum(shipping_fee)  as shipping_amount

    from {{ ref('int_order_items') }}
    group by order_id

)

select
    -- ── 鍵與 grain ──────────────────────────────────────────────────────────
    o.order_id,                              -- 自然鍵兼退化維度（ODS UNIQUE）
    o.order_date,                            -- 分區欄位

    -- ── 維度 FK：natural key 直連，BQ 無 index，surrogate key 不帶收益 ───────────
    -- coalesce 到 unknown member（見 docs/zh-TW/design/transformation.md §4〈鍵的處理〉）：
    -- 事實表不留 NULL FK。
    coalesce(o.customer_id, '{{ var("unknown_member_key", "__UNKNOWN__") }}') as customer_id,

    -- ── 下單當下的顧客快照（ADR-0048：由事實表承載 SCD2 的效果）─────────────
    -- dim_customer.membership_tier 是【現在】的等級；這裡是【下單當時】的。
    o.membership_tier as membership_tier_at_order,

    -- ── 退化維度（ADR-0048：無獨立主檔者不抽維度表，直接留在事實表）─────────
    o.order_status,
    o.ship_mode,
    o.payment_method,
    o.device_used,
    o.delivery_date,
    -- 地址：沒有 conformed 的地理主檔，抽成 dim_geography 只是把欄位搬家再 join 回來；
    -- BQ 是列式儲存，寬表不吃虧。日後要接人口統計或做地理階層 drill-down 再抽出。
    o.country,
    o.region,
    o.state,
    o.city,
    o.postal_code,

    -- ── 訂單層度量（不可從 item 加總得出者）─────────────────────────────────
    -- tax_pct 是【比率】，非可加度量：不要對它做 SUM。
    -- 刻意不算 tax_amount——稅基是 net 還是 net+shipping 尚無業務定義，
    -- 憑空假設會讓一個錯誤的數字看起來像事實（見 docs/zh-TW/design/transformation.md §4〈刻意未定義的業務規則〉）。
    o.tax_pct,
    o.delivery_days,
    o.customer_rating,
    o.returned,
    o.is_repeat_customer,

    -- ── rollup 度量（來自 fct_order_items，見檔頭與 rollup 一致性測試）──────
    -- 三個 counter 一律 coalesce 到 0：沒有任何品項的訂單（LEFT JOIN 未命中）
    -- 缺的不是「金額算不出來」而是「根本沒有品項」，那個事實由 item_count = 0 表達；
    -- 讓 counter 留 NULL 會讓下游把兩件事混為一談。
    coalesce(i.item_count, 0)             as item_count,
    coalesce(i.items_missing_amount, 0)   as items_missing_amount,
    coalesce(i.items_missing_cost, 0)     as items_missing_cost,
    coalesce(i.items_missing_shipping, 0) as items_missing_shipping,
    i.total_quantity,
    i.gross_amount,
    i.net_amount,
    i.cost_amount,
    i.shipping_amount,

    -- ── 血緣與品質（供 rpt_quality_* 與追溯回 ODS/Raw）──────────────────────
    o.raw_id,                     -- 物理身分：追溯回 ODS / Raw 的鍵
    o.received_at,                -- 攝入時間（與 order_date 是兩條不同的時間軸）
    o.effective_quality_state,    -- clean＝攝入即乾淨；promoted＝被新版規則救回
    o.has_clean_error,            -- 本表允許 TRUE，但僅限已 promote 者（見上游測試）
    o.has_schema_drift,           -- 非阻斷訊號：值乾淨但上游契約變了，仍流入 Gold
    o.dq_rule_version,
    o.source_client_id

from {{ ref('int_orders') }} o
-- ⚠️ 必須 LEFT：INNER 會讓「沒有任何品項的訂單」整批從 Gold 消失（靜默掉列）。
-- 那類訂單以 item_count = 0 表達，而非以缺席表達。
left join item_rollup i
    on o.order_id = i.order_id
