-- rpt_quality_backlog：目前仍卡在 quarantine 的訂單（狀態快照）
--
-- 與 rpt_quality_events_daily 的分工：
--   事件軸表回答「發生了什麼」（不可變的歷史），本表回答「現在還剩什麼」（會變的現況）。
--
-- ⭐ 為什麼需要這張，而不是從事件軸累加算出 backlog：
--   理論上 backlog(t) = 累計 quarantined − 累計 promoted − 累計 rejected，
--   事件流是狀態的完整導數，看似不必另存。但 quality_events 有 60 天分區過期
--   （event_at 軸，CLOUD_LAYER-TW §1.6）——【過期之後累加的起點就丟了，曲線會系統性失真】，
--   而且失真是單向的（起點永遠只會少算 quarantined，backlog 被低估）。
--   本表直接讀 int_orders_quarantine 的當下內容，不受事件保留期影響。
--
-- 物化＝table 全量重建，且【天生不可增量】：這是狀態快照，任何一筆舊訂單被 promote
--   就會從本表消失。與 int_orders_quarantine 同構的理由（ecommerce_dbt/README.zh-TW §5.4）：
--   增量失誤在這裡不是延遲、是【永久錯誤】——幽靈列留在 backlog 裡，不報錯、不自癒。
--   也因此刻意【不】分區：分區的價值在分區級增量替換，而本層永遠不會增量。
--
-- ⚠️ fan-out：一張訂單可能帶多個 error_code，攤平後會出現在多列。
--   處置是同一張表放【兩個語意不同的度量】（見下方欄位註解），
--   而不是加一列 error_code = '__TOTAL__' 的彙總列——那會讓「不小心把 __TOTAL__
--   也加進去」變成新的誤用面，比 fan-out 本身更危險。
--
-- ⚠️ 目前【沒有】金額曝險度量（「被卡住的訂單值多少錢」），這是本報表最有業務說服力
--   但現在算不出來的東西：int_order_items 的來源是 int_orders（已過濾的乾淨路徑），
--   quarantine 的 items 從來沒有被攤平過（ecommerce_dbt/README.zh-TW §5.7 明載
--   「要做 item 層 RCA 時另建讀 quarantine 的模型」）。
--   刻意不用其他欄位湊一個金額——那是 §6.6 說的「憑空假設會讓一個錯誤的數字看起來像事實」。
--   【觸發點】：需要 int_order_items_quarantine，啟用時機＝品質報表需要業務曝險金額。

{{
    config(
        materialized='table',
        cluster_by=['error_code'],
    )
}}

with quarantined as (

    select
        order_id,

        -- quarantined_at 在上游已正確取【事件時間】而非跑批時間
        -- （ecommerce_dbt/README.zh-TW §5.6：coalesce(quality_state_at, received_at)），
        -- 所以這個日期在全量重建下是穩定的，可以拿來做老化分析。
        date(quarantined_at) as quarantined_date,

        -- ODS.dq_rule_version 是 nullable（models.py:88）→ 補 unknown member。
        -- 動的是【維度值】不是度量，且可完整反查回「這筆沒有版本標記」，是無損的，
        -- 不牴觸 CLOUD_LAYER-TW §5.5.5 的 NULL 鐵律（同 §6.5 對鍵的論證）。
        -- 不補的話：NULL 在 grain 裡會讓 BI 顯示空白，且下游對帳測試的等值 join 會靜默漏掉。
        coalesce(dq_rule_version, '{{ var("unknown_member_key", "__UNKNOWN__") }}') as dq_rule_version,

        effective_quality_state,

        -- ⚠️ 陣列去重不可省：同一個 code 可能在一張訂單裡重複出現
        --    （例如多個 item 各觸發一次 non_finite_number，見 README.zh-TW §5.3
        --      「不可用 array_length(codes) = 1 判斷」那條）。
        --    不去重會讓 orders_with_code 把一張訂單算成多張。
        array(select distinct c from unnest(error_codes) as c) as error_codes,

        -- 主要碼＝排序後的第一個 code。
        -- ⚠️ 這【只是】為了確定性與加總配平，【不】代表嚴重性排序——
        --    嚴重性優先級是業務定義，目前沒有，不憑空造（同 §6.6 的紀律）。
        --    真的定義出優先級之後，改這一行即可，下游語意不變。
        (select c from unnest(error_codes) as c order by c limit 1) as primary_error_code

    from {{ ref('int_orders_quarantine') }}

),

exploded as (

    select
        q.quarantined_date,
        q.dq_rule_version,
        q.effective_quality_state,

        -- ⚠️ 必須 LEFT JOIN UNNEST：CROSS JOIN（`from q, unnest(...)`）會讓
        --    error_codes 為空陣列的訂單【整批消失】。
        --    照目前的 int_ 邏輯這種列不該存在（落到 quarantine 的前提是 has_clean_error=TRUE，
        --    故必有 clean_error_message），但「不該存在」不是「不會存在」——
        --    靜默掉列是本專案反覆強調的最危險失效模式，用 LEFT 換一個 '__NONE__' 分桶便宜得多。
        coalesce(code, '__NONE__')                as error_code,
        coalesce(q.primary_error_code, '__NONE__') as primary_error_code,

        q.order_id

    from quarantined q
    left join unnest(q.error_codes) as code

)

select
    -- ── grain ───────────────────────────────────────────────────────────────
    quarantined_date,
    dq_rule_version,
    -- quarantined（還可能被 promote 救回）與 permanently_rejected（已人工放棄）
    -- 語意完全不同，混算會讓「待處理量」虛高，故進 grain 而非合併。
    effective_quality_state,
    error_code,

    -- ── 度量（兩者語意不同，見檔頭 fan-out 說明）─────────────────────────────
    -- ⚠️ 不可加：跨 error_code 加總會把一張多碼訂單重複計數。做 Top N 錯誤碼圖用這個。
    count(distinct order_id) as orders_with_code,

    -- ✅ 可加：主要碼一張訂單只有一個，跨 error_code 加總＝該切片的訂單總數。
    --    「現在總共卡了幾筆」這個 KPI 用這個欄位。
    count(distinct if(error_code = primary_error_code, order_id, null)) as orders_primary_code

from exploded
group by quarantined_date, dq_rule_version, effective_quality_state, error_code
