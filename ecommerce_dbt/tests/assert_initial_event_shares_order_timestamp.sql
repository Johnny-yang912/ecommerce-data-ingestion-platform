{{ config(severity = 'warn') }}

-- 訂單與它的 initial_evaluation 必須同時戳：寫入路徑仍然是原子的
--
-- ⭐ 這支【不是】正確性測試，是**假設的金絲雀**。它保護的不是資料，是另一支測試的
--    推導基礎——assert_int_orders_quality_state_resolved 的 2 天下界能成立，
--    完全建立在「訂單與它的初始品質事件在同一個 transaction 裡寫入」這件事上。
--    那個假設若被改掉，門檻的推導無聲失效，而症狀要很久以後才會出現。
--
-- 斷言：對每一筆訂單，其 event_type='initial_evaluation' 的事件，
--       event_at 必須等於該訂單的 received_at。
--
-- 為什麼時戳相等可以代表「原子寫入」：
--   process.py（success path）在同一個 transaction 裡 db.add(ods) + db.add(quality_event)
--   後一起 commit，兩者時戳都是 server_default=func.now() → 由 DB 在同一個語句批次求值。
--   時戳相等是那個結構的【可觀測代理】：寫入若改成非同步／分兩次 commit，
--   時戳就會分開，而這支測試會看見。
--
-- ⚠️ 【必須】限定 event_type='initial_evaluation'。不限定的版本是錯的：
--   Proposal B 的 promotion 事件 event_at=now()，與訂單的 received_at 沒有任何關係。
--   截至 2026-08-31 已有 31 筆 promotion，它們碰巧與訂單同分區（重評估剛好當天跑），
--   **但那是巧合不是結構**——只要有一筆訂單隔天才被 promote，不限定的版本就會紅。
--   一個「碰巧為真」的斷言比明顯為假的更危險：它會通過每一次抽查，直到某天不通過。
--
-- 這條相等撐住的兩件事（改動前先確認替代方案）：
--   ① **抽取同步**：兩條 extract 線的 watermark 都是 destination-derived MAX(partition_id)。
--      訂單與事件同時戳 → 同分區 → 同一個 watermark 推進，不會有一邊落單。
--   ② **過期對稱**：staging / Silver 四張表都是 60 天過期，掛在這兩個相等的時戳上，
--      所以 initial_evaluation 不會比它的訂單早過期。
--      （後續事件 event_at 更晚 → 過期更晚 → 只會比訂單更晚消失，不會遺孤訂單。）
--
-- severity=warn，理由同 assert_int_orders_quality_state_resolved：
--   它紅的時候 Gold 沒有錯——錯的是「我們對另一支測試門檻的推導是否還成立」。
--   正確處置是重算門檻並更新該測試檔頭，不是停掉當日管線。
--
-- 用 full outer 風格的雙向檢查沒有必要：訂單無事件的情況由
--   assert_int_orders_quality_state_resolved 覆蓋（且那支才有正確的年齡窗），
--   這裡只問「有事件的那些，時戳對不對」。

select
    s.raw_id,
    s.order_id,
    s.received_at,
    e.event_at,
    timestamp_diff(e.event_at, s.received_at, microsecond) as drift_micros
from {{ ref('stg_orders') }} s
join {{ ref('stg_quality_events') }} e
    on s.raw_id = e.raw_id
   and e.event_type = 'initial_evaluation'
where e.event_at != s.received_at
