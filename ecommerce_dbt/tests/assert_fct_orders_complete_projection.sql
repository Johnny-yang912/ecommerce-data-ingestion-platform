-- 完整投影不變式：Gold 不得靜默掉列
--
-- 攔截已經在 int_ 層做完了（DQ_ARCHITECTURE-TW 機制二）——fct_orders 對 int_orders
-- 應該是【無損投影】，不再過濾任何一列。這支測試守的就是那個契約，
-- 它要抓的是「某個 join 悄悄變成 INNER」這類失效（Gold 最危險的失效模式，
-- 因為它不報錯、只是數字慢慢變小）。
--
-- ⭐ 為什麼是反向 join + 時間窗，而不是 count(*) = count(*)：
--   本專案有【兩個 60 天時鐘掛在不同軸上】——
--     int_orders  ← stg_orders ← staging：按 received_at 分區過期
--     fct_orders：按 order_date 分區過期
--   兩表的保留期本來就不同，所以 count = count 即使綠也是「兩個 reaper 剛好同步」
--   而不是「你的 SQL 對」。更糟的是回收時機不同步：fct_orders 是 CREATE OR REPLACE，
--   回收同步發生；stg_orders 是 incremental，舊分區靠 BQ 背景 reaper 非同步回收
--   → 邊界日 count 必然對不上，測試會變成每天固定紅一陣子的 flaky test。
--
--   錨在 order_date 窗上就沒有這個問題：受 reaper 延遲影響的列在 received_at 軸上已老，
--   而 load_test.py 已讓 order_date ≈ received_at（見該檔 ORDER_DATE_LOOKBACK_DAYS），
--   故它們同樣落在 order_date 窗外、被謂詞排除。盲區只有窗外的列，
--   而窗外的列在 fct_orders 裡本來就不該存在。
--
--   窗取 59 天（< sandbox 的 60 天硬上限）留一天安全邊際。啟用帳單、
--   gold_partition_expiration_days 打開之後，此窗應改為與該保留政策一致。

select o.order_id
from {{ ref('int_orders') }} o
where o.order_date >= date_sub(current_date(), interval {{ var('gold_projection_window_days', 59) }} day)

except distinct

select f.order_id
from {{ ref('fct_orders') }} f
