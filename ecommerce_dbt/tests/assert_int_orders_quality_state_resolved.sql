{{ config(severity = 'warn') }}

-- 品質狀態不得【永久】缺席：LEFT JOIN 把上游掉列翻譯成下游缺值
--
-- ⭐ 這支測的是專案裡第三種失效形狀。前兩種已經有守門的：
--     內容失效（值錯了）        → 20 幾條內容測試
--     掉列失效（列不見了）      → 兩支 *_matches_staging 逐分區對帳
--     **缺值失效（列在、欄位空）** → 就是這一支
--
--   `int_orders` 以 LEFT JOIN 從 stg_quality_events 取 quality_state_at。上游掉列時，
--   下游的列數【完全正確】、只是某個欄位變成 NULL——於是掉列偽裝成缺值，
--   逐分區對帳測試（它只數自己那張表的列）結構性地看不見它。
--
--   實例：2026-08-30 事故第二階段。stg_quality_events 掉了 550 列，
--   int_orders 的 2026-08-26 有完整的 800 列一列不少，其中 550 列的
--   quality_state_at 是 NULL（int_orders 499 + quarantine 51）。
--   當時全部 94 條測試皆綠。
--
-- ⚠️ 測的是「不准【一直】是 NULL」，【不是】「不准是 NULL」——這個區別是這支測試的全部。
--
--   NULL 在設計上是合法的。int_orders.sql 的 LEFT JOIN 有一個刻意的 fallback：
--   事件缺席時 fall back 到 ODS 快照（「保守合成」——乾淨照流、髒的續留 quarantine），
--   docs/zh-TW/design/cloud-layer.md 明定它**只造成延遲、不造成髒資料**。
--   直接 not_null 會把這個合法暫態判成錯誤，在每次跑批的最新分區固定閃黃。
--   **routine 的黃燈等於沒有燈**——本專案已為同一件事付過代價（freshness 恆紅，
--   最後被迫拆成獨立 DAG，見 ADR-0039）。
--
--   所以這支斷言的是那個 fallback 的【時效上界】：延遲可以，永久缺席不行。
--
-- ═══ severity=warn 是【最終決定】，不是過渡 ═══════════════════════════════════
--   severity 編碼的是【正確的處置動作】，不是【我們有多確定】。判準沿用
--   assert_product_attributes_stable：error ＝「我們自己的 SQL 對不對」，
--   warn ＝ 其他來源的訊號。這支屬於後者，三個理由：
--
--   ① 它紅的時候 **Gold 沒有錯**。上面那個 fallback 保證「延遲、不髒」——漏掉的是一次
--      可能的 promotion（品質管線的偽陰性），不是 Gold 的污染。
--      **阻斷權的用途是擋住錯的資料流向下游，而這裡沒有錯的資料要擋。**
--   ② 它**不是重跑能清掉的**（需人工回填）。DAG 是 dbt build 逐層 + retries=0，設成 error
--      的實際後果是：從那天起每天的排程都失敗，直到有人處理——
--      **為了一個三天前就已經缺了的屬性，停掉今天新資料的正確流動。**
--      這與 DAG 檔頭⑤「deterministic 的失敗不該重試」是同一條推理。
--   ③ 該阻斷的情境**上游已經擋了**：兩支 *_matches_staging 是 error。掉列若落在 7 天窗內
--      它們先紅先擋；這支的價值只在那兩支漏掉的情況，依定義更舊、更不緊急。
--
--   ⚠️ 代價是能見度：warn → task success → on_failure_callback 不會響，只留在 dbt log。
--      能見度的解法是【通知路徑】，不是【阻斷權】——ADR-0039 已為同一問題立過決策：
--      觀察訊號自成路徑，不靠阻斷主線換取能見度。
--      而本專案【沒有】真實通知通道，這是全域決策而非待辦：PORTFOLIO_SCOPE #7
--      「沒有值班對象」——把 notifier 指向不存在的連線，行為是「紅燈→回呼觸發→
--      它自己拋錯→沒有人收到」，**相信自己有告警、實際上沒有，遠比坦白地沒有告警危險**。
--      所以這支的能見度上限就是 dbt log 與 run_results.json，**這是已知且刻意的**。
-- ═════════════════════════════════════════════════════════════════════════════
--
-- 合法暫態有多大？（決定下界的依據，改門檻前先重讀這段）
--   process.py 在【同一個 transaction】裡 db.add(ods) + db.add(quality_event) 後一起 commit，
--   兩者時戳都是 server_default=func.now()。
--   ⚠️ 這條相等【只對 initial_evaluation 成立】——後續事件（promotion 等）event_at=now()，
--     落在【較晚】的分區。所以正確的敘述是：
--       訂單與它的 initial_evaluation 必在同一分區（同 transaction、同時戳）；
--       後續事件落在更晚的分區，因此只會【比訂單更晚】過期，不會讓訂單變成遺孤。
--     （守門的是 assert_initial_event_shares_order_timestamp。實測 0/15,631 不相等。）
--   唯一的縫隙是 extract_orders 與 extract_quality_events 兩個並行 task 相隔數秒讀 ODS；
--   偏斜只可能丟掉【最前緣】的列，而最前緣正是 watermark 的來源（destination-derived
--   MAX(partition_id)），所以 watermark 永遠不會越過被丟掉的那一列，下一輪 `>=` 必然重抓
--   （ADR-0023）。**暫態、有界、自癒——這是建構證明，不是觀測結論。**
--
-- 年齡【下界】2 天 ＝ 一個完整日批週期（1 天）+ 開機補跑與週末的餘裕。
--   ⚠️ 必須 > 一個 DAG 週期，否則正常的自癒過程本身會讓這支測試變紅。
--
-- 年齡【上界】50 天：把保留期邊緣排除在窗外。
--   ⚠️ 這是【作品限制】的直接後果，不是一般性需求的全部理由。
--     兩張表的分區過期是【各自獨立的背景作業】，即使過期日相同也不保證同時生效。
--     若 stg_quality_events 的舊分區先一步消失、stg_orders 的還在，那批訂單會瞬間
--     變成「有訂單、無事件」且年齡遠大於下界 → **誤報，但資料沒壞**。
--     BQ sandbox（未啟用帳單）硬鎖分區過期 < 60 天（見 dbt_project.yml 的
--     gold_partition_expiration_days），所以這條邊 60 天就會撞到一次。
--   實務上（啟用帳單、保留期 1825 天）這條邊在五年後，等同不存在——但上界本身仍是
--     通用解：**任何有分區過期的表都有這條邊**，不是繞過限制的權宜。
--   ⚠️ 上界與保留期【成組維護】：保留期若下修，上界必須跟著下修，否則窗會變空
--     （空窗 ＝ 這支測試靜默失效，永遠綠）。
--   不損失覆蓋率：一筆永久缺事件的列，在 2～50 天大的期間每天都會被檢查到；
--     「等它 55 天大才第一次被看見」的情況不存在（本測試掛在日批 DAG 上）。
--
-- 兩個邊界都對齊日界（timestamp_trunc）：這是唯讀路徑、不會刪資料，所以理由比 ADR-0055 弱，
--   但不對齊會讓「同一份資料在 08:00 與 10:00 跑出不同結論」——**測試的結論不該取決於
--   它幾點跑**。對齊後有效下界落在 2～3 天之間，偏保守，方向是對的（寧可晚報，不可誤報）。
--   ⚠️ 日界是 **UTC**（BQ 的 current_timestamp() 與 timestamp_trunc 都是 UTC），與
--     rpt_quality_events_daily 的 date(event_at) 刻意採 UTC 一致。台北時間讀者請注意
--     台北日界比它早 8 小時——這不是 bug，全專案的日粒度都掛在同一條 UTC 軸上。
--
-- 何時重新檢視：**啟用 BQ 帳單時**（上界隨保留期一起調），或
--   **dq_reevaluation 的產出頻率改變時**（下界的自癒假設要重算）。

with candidates as (

    select 'int_orders' as source_table, raw_id, order_id, received_at, quality_state_at
    from {{ ref('int_orders') }}

    union all

    -- 兩張表都要：8/26 的損害是 499（int_orders）+ 51（quarantine），
    -- 只測乾淨流會漏掉隔離區那批，而隔離區正是品質狀態最要緊的地方。
    select 'int_orders_quarantine', raw_id, order_id, received_at, quality_state_at
    from {{ ref('int_orders_quarantine') }}

)

select
    source_table,
    raw_id,
    order_id,
    received_at,
    date_diff(current_date(), date(received_at), day) as age_days
from candidates
where quality_state_at is null
  -- 下界：比這新的 NULL 是合法暫態（見檔頭）
  and received_at < timestamp_sub(
      timestamp_trunc(current_timestamp(), day),
      interval {{ var('int_orders_quality_state_max_lag_days', 2) }} day
  )
  -- 上界：比這舊的落在保留期邊緣，過期競態會製造假紅（見檔頭）
  and received_at >= timestamp_sub(
      timestamp_trunc(current_timestamp(), day),
      interval {{ var('int_orders_quality_state_max_age_days', 50) }} day
  )
