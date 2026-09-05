# 測試策略

[English](../../en/design/testing.md) | **繁體中文**

測試了什麼、在哪裡測，以及——最重要的——**測試在哪裡是盲的**。

---

## 1. 分層

| 層 | 數量 | 在哪 | 需要 |
|---|---|---|---|
| 單元 + 整合（mock DB） | 445 | CI，`ci.yml` | 什麼都不需要——幾秒跑完 |
| DAG 結構 | 52 | CI，`dags.yml` | Airflow，不需 DB、不需專案環境變數 |
| dbt | 97 | 在 DAG 內 | BigQuery |
| 手動腳本 | 3 | 手動 | 真實 server + 真實 Postgres |

單元測試覆蓋率為**受管的 12 個模組 100%**，跨 **Python 3.10 與 3.12** 矩陣。測試依賴釘在 `requirements-dev.txt`。

> **這些數字會過期。重新取得它，而不是相信它**——上表最後一次核對是 2026-08-30：
>
> ```bash
> pytest --collect-only -q | tail -1            # 單元 + 整合
> pytest tests/test_dags.py --collect-only -q   # DAG（需 Airflow；本機自動跳過）
> python -c "import json;print(sum(1 for r in json.load(open('ecommerce_dbt/target/run_results.json'))['results'] if r['unique_id'].startswith('test.')))"
> ```
>
> **一個被寫進文件的數字，沒有任何機制讓它保持為真。指令有。**

---

## 2. 兩個 CI workflow，刻意不合併

`ci.yml` 用 mock 的資料庫跑主測試套件——不需要真實 DB，幾秒完成。

`dags.yml` 依官方 constraints 安裝 Airflow 並用 DagBag 解析 `orchestration/dags/`。**把它併進主 job 會摧毀主 job「mock DB、幾秒跑完」的性質**，因為 Airflow 的安裝很重且會釘住許多套件版本。

它不需要 `DB_URL`，因為 DAG 檔刻意不 import 任何專案模組（[ADR-0036](../adr/0036-dag-no-toplevel-import.md)）——**那條紀律正是讓 DAG 得以被 CI 測試的原因。**

---

## 3. CI 涵蓋什麼、在哪裡是盲的

CI 驗證的是**應用邏輯與型別契約**。**DB 層契約在它的範圍之外**，因為 CI 內的測試以 mock 取代資料庫：

| 未自動化 | 由什麼演練 |
|---|---|
| 真實併發下的 CAS 認領 | `scripts/load_test.py --cas-test` |
| `order_id` 去重 | `scripts/load_test.py --duplicate` |
| 崩潰後恢復 | `scripts/restart_test.sh`（SIGKILL） |
| Alembic ↔ `models.py` 漂移 | `check_migration_drift.py` |

> ⚠️ **不要把綠色勾勾讀成「一切都好」。** CI 通過代表**邏輯層**沒有回歸。它**不**代表去重／CAS／遷移契約已被驗證。改動那些邏輯時，要用手動腳本重新佐證。

**為何資料庫沒有接進 CI**：CAS 與恢復的價值只在真正的併發下才顯現，而撰寫測試的工夫加上容器啟動 flake 的維護，目前成本高於不自動化的風險。`check_migration_drift.py` 是例外——確定性、無並發、低 flake，**今天就能進 CI**。見 [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md)。

---

## 4. 釘住決策而非行為的測試

有少數幾個測試存在的目的，是阻止未來的變更靜默移除一個保證。它們值得被知道名字：

| 測試 | 釘住什麼 |
|---|---|
| `assert_orders_split_is_partition` | `int_orders` + `int_orders_quarantine` 互斥且窮盡——**那段重複的有效狀態區塊唯一的自動化安全網**（[ADR-0045](../adr/0045-int-effective-state-duplication.md)） |
| `assert_fct_orders_rollup_matches_items` | 表頭上捲等於明細總和（[ADR-0047](../adr/0047-measures-roll-up-to-header.md)） |
| `test_dbt_never_splits_run_and_test` | 拆開 `dbt build` 會靜默停用 Hard Gate（[ADR-0040](../adr/0040-layered-dbt-execution.md)） |
| `TestFreshnessIsolation` | 任何產出真實輸出的 DAG 都不得撿走 `dbt source freshness`（[ADR-0039](../adr/0039-observation-signals-own-dag.md)） |
| `test_schema_bq_consistency` | `FIELDS` 與 `models.py` 相符——否則忘記的欄位會**靜默**失敗（[ADR-0026](../adr/0026-fields-single-source.md)） |
| `test_script_deps` | 唯讀探針不繼承寫入路徑的依賴樹（[ADR-0039](../adr/0039-observation-signals-own-dag.md)） |
| `test_seed_demo` | 缺少的選填欄位不可以變成髒資料——產生器的跨模組不變式 |
| `test_dag_param_injection` | 進 `bash_command` 的 string 型 DAG param 必須**同時**受約束（`pattern`）與被包裹（`\| q`）。worker 容器握有 `DB_URL`、`API_KEYS` 與 GCP 金鑰，**這條路徑的失效是任意程式碼執行，不是壞掉的參數** |

**這些不是普通的單元測試。** 每一個都把一份紀律轉成一個機制，**而降級其中任何一個，都會移除它所保護的那個設計決策的正當性。**

---

## 5. 手動驗證腳本

| 腳本 | 驗證 |
|---|---|
| `scripts/load_test.py` | 吞吐量、真實併發下的 CAS（`--cas-test`）、去重（`--duplicate`） |
| `scripts/restart_test.sh` | 處理中途 `SIGKILL`，然後恢復 `pending` 的列 |
| `check_migration_drift.py` | `alembic upgrade head` + `compare_metadata`；漂移時以非零碼結束 |

三者都打真實 server 與真實 PostgreSQL。它們的結果記錄在 `docs/*/verification/`（第 4 階段）。

---

## 6. dbt 測試清單

共 97 個測試。完整列出，因為「哪個測試守什麼」無法從 model 檔推導出來：

| 測試 | 目標 | 嚴重度 | 說明 |
|---|---|---|---|
| `hard_gate_latest_batch_error_rate` | `stg_orders` **最新 `received_at` 分區**的 `has_clean_error` 比率 | error @15% | **Hard Gate**——唯一有阻斷權的測試。逐批而非全表：全表分母隨歷史成長、會把單批異常稀釋掉，而且無法自癒。不能用 `dbt_utils.expression_is_true`（逐列、會摺進 `WHERE`；聚合會報錯）→ 自訂的 `error_rate_below` 改用 `HAVING` |
| `monitor_dataset_error_rate` | `stg_orders` 的全表比率 | warn @10% | **儀表**，刻意不給阻斷權。它的分母是保留期／回填政策的函數——**對品質以外的事情敏感** |
| `unique` + `not_null` | `stg_` 的 `raw_id`／`id`／`order_id`；`int_` 的 `raw_id`／`order_id` | error | `stg_` 的 `unique(raw_id)` **就是**去重檢查 |
| `not_null` | `received_at`／`has_clean_error`／`has_schema_drift` | error | REQUIRED 欄位 |
| source freshness | `staging.orders`、`staging.quality_events` | warn 26h／error 50h | 帶 `filter` 以繞過保險絲 |
| ⭐ `assert_stg_orders_matches_staging` | `staging.orders` 的 `distinct raw_id` vs `stg_orders` 的列數，**逐分區** | error | **對帳，不是內容。** `stg_` 對 staging 只做去重、不做過濾，所以兩個數字必須逐分區相等。這是清單裡**兩支**問「列還在不在」的測試之一（另一支見下列）——[2026-08-30 事故](../incidents/2026-08-30-stg-partition-truncation.md)裡活下來的每一列都完全正常，問題是少了 550 列，其餘所有測試對此結構性地盲。窗（7 天）**必須 > 回看窗**（3 天），否則損壞會在被看見前滑出兩個窗 |
| ⭐ `assert_stg_quality_events_matches_staging` | `staging.quality_events` 的 `distinct id` vs `stg_quality_events` 的列數，**逐分區** | error | 上一列的孿生測試。去重鍵是 `id`（事件 PK）而非 `raw_id`——一個 `raw_id` 合法地有多個事件，拿 `raw_id` 對帳會把正常的事件序列誤判成「多出來的列」。**兩支必須各自存在**：[2026-08-30 事故](../incidents/2026-08-30-stg-partition-truncation.md)第二階段裡，事件線同樣掉了 550 列，而上一列那支測試一列都沒抓到——它只指名 `stg_orders` |
| ⭐ `assert_orders_split_is_partition` | `int_orders` ∪ `int_orders_quarantine` 對 `stg_orders` | error | **劃分不變式**——每個 `raw_id` 恰好出現一次。重複區塊之下唯一的自動化安全網，守住對齊清單第 1–4 項。**永不降級** |
| `assert_int_orders_no_unpromoted_dirty` | `int_orders` | error | **Gold 契約**——不得有未被 promote 的 `has_clean_error=TRUE` 列。寫成 singular 而非欄位測試，因為它是**兩個欄位之間的條件關係**：`has_clean_error=TRUE` 在這裡是合法的 |
| `accepted_values` | 兩張 `int_` 表的 `effective_quality_state` | error | 兩者的值域互斥（`clean`／`promoted` vs `quarantined`／`permanently_rejected`）——**從另一個角度交叉驗證那個劃分** |
| `unique_combination_of_columns` + `relationships` | `int_order_items` 的 `(raw_id, item_index)`；`raw_id → int_orders` | error | item 粒度唯一性與血緣完整性 |
| ⭐ `assert_fct_orders_rollup_matches_items` | `fct_orders` 的上捲 vs `fct_order_items` 的聚合 | error | **上捲一致性不變式。** 逐訂單以 `is distinct from` 比對——用 `=` 會讓「兩邊都是 NULL」的列被靜默濾掉 |
| ⭐ `assert_fct_orders_complete_projection` | 窗口內的 `int_orders` vs `fct_orders` | error | **無損投影**——攔截已經在 `int_` 發生過，所以 Gold 不得掉任何一列。寫成對 `order_date` 窗口的 anti-join 而非 `count = count`，因為**兩張表的 60 天時鐘掛在不同軸上**，用計數比較會每天 flaky |
| ⭐ `assert_int_orders_quality_state_resolved` | 兩張 `int_` 表的 `quality_state_at` 為 NULL **且** `received_at` 落在 2～50 天窗內 | **warn** | **缺值失效**——專案裡的第三種失效形狀。`int_orders` 以 LEFT JOIN 取品質狀態，上游掉列時本層**列數完全正確、只有欄位變 NULL**，逐分區對帳（只數自己那張表）結構性地看不見。[2026-08-30 事故](../incidents/2026-08-30-stg-partition-truncation.md)第二階段即如此：800 列一列不少，其中 550 列品質狀態是空的，94 條測試全綠。**測的是「不准一直是 NULL」而非「不准是 NULL」**——事件缺席的 fallback 是刻意設計，只允許造成延遲。下界**必須 > 一個 DAG 週期**；上界排除保留期過期競態（作品限制）。**warn 是最終決定**，理由見下文 |
| `assert_initial_event_shares_order_timestamp` | `initial_evaluation` 的 `event_at` vs 該訂單的 `received_at` | **warn** | **假設的金絲雀**，不是正確性測試——它保護的是上一列那支測試的**推導基礎**：2 天下界能成立，全靠「訂單與初始事件同 transaction 寫入」。時戳相等是那個結構的可觀測代理；寫入若改成非同步，這裡會先看見。⚠️ **必須限定 `initial_evaluation`**：promotion 的 `event_at=now()` 與 `received_at` 無關，不限定的版本是錯的（目前 31 筆 promotion 碰巧同分區，那是巧合不是結構） |
| `assert_product_attributes_stable` | `int_order_items` 上的 `product_id` → 屬性 | **warn** | 那是上游契約的訊號，不是這一層的缺陷——若 `product_id` 決定不了屬性，該修上游而非停下 DAG |
| `unique` + `not_null` | 維度鍵；`fct_orders.order_id`；`fct_order_items.order_item_key` | error | 維度粒度與代理鍵唯一性 |
| `relationships` | `customer_id`／`product_id` → `dim_*`；`fct_order_items.order_id` → `fct_orders` | error | 星型結構的 FK 完整性，搭配 `not_null`（unknown member 保證 FK 永不為 NULL） |
| `unique_combination_of_columns` | `fct_order_items` 的 `(order_id, item_index)` | error | 宣告的粒度 |
| ⭐ `assert_rpt_sales_no_item_loss` | `rpt_sales` 的 `sum(items)` vs `fct_order_items` 列數 | error | `rpt_sales` 引入了**整個 DAG 中唯一的新 join**。一個 join 悄悄變成 INNER，表現出來是「營收慢慢縮水」而且什麼都不報。full outer join 讓「太多」（維度扇出）也抓得到 |
| ⭐ `assert_rpt_quality_events_split` | `initial_clean + initial_quarantined = initial_evaluations` | error | **寬表的值域擴張警報。** 寬表的代價是「上游多一個 `to_state`，下游就需要一次 schema 變更才看得到它」。新狀態會讓 `count(*)` 成長而 `countif` 不動 → 這條會立刻變紅，而不是讓那些事件蒸發。**它就是讓寬表能安心使用的東西** |
| `assert_rpt_backlog_primary_code_balances` | `sum(orders_primary_code)` vs 隔離區的實際計數 | error | 它壞掉時的症狀是 BI 裡的 backlog KPI 就是錯的，**而且不會自癒** |
| `unique_combination_of_columns` + `not_null` | 三張 `rpt_` 表各自宣告的粒度 | error | **預先聚合裡壞掉的粒度，會讓每一個數字翻倍，而且是靜默的** |
| `expression_is_true` | `orders <= items`、`items_missing_amount <= items`、`orders_with_code >= orders_primary_code` | error | 便宜的合理性下限 |

> 自訂 generic test（以及部分內建的）需要把參數收在 `arguments:` 之下——dbt 1.11 的要求，否則會出現 `MissingArgumentsPropertyInGenericTestDeprecation`。

### 一個刻意不寫的測試

`rpt_sales` 與 `fct_` 之間的**逐格金額對帳**。在 `table` 全量重建之下它是一個**恆真句**——`rpt_` 的總和**就是** `fct_` 的欄位加起來——所以它會永遠是綠的，攜帶零資訊。

**它的價值只在模型改成增量的那天才顯現**，屆時它會抓到漏掉的分區。

> **「把 `rpt_sales` 改成增量」與「加上逐格對帳」是同一次變更的兩半。只做前者是不被允許的。**

對照 `assert_rpt_sales_no_item_loss`——它**現在就寫了**：它測的是**跨兩個 join 的列數**，與物化策略無關，而且今天就是一個真實可能發生的失效。

### 對帳測試與內容測試

上表除了兩支 `*_matches_staging`，其餘每一條問的都是**內容**：這個值合不合約、
這個關係成不成立、這個粒度對不對。它們共有一個前提——**要被檢查的列，得先在那裡**。

掉列是一種正交的失效，而且它安靜得多：

| | 內容失效 | 掉列失效 |
|---|---|---|
| 症狀 | 某個值錯了 | 每個值都對，只是少了一些 |
| 誰會發現 | 測試 | 肉眼看 BI，如果剛好有人在看 |
| 上游證據 | 通常還在 | **上游完好無損**——所以事後也查不出是誰刪的 |

> **能被內容測試抓到的前提，是那一列還在。** 一個只做內容測試的測試套件，
> 在資料被刪掉時是全綠的。

對帳測試的成本是一次 `count(*)`，所以「有沒有必要」不是成本問題，是有沒有想到的問題。

### 對帳測試保護的是「表」，不是「掉列」這個類別

⚠️ **每支增量模型都需要自己的一支對帳測試。不能靠「另一支有測」推論這一支安全。**

這條寫得像廢話，但它是實際踩過的：2026-08-30 上午加上 `assert_stg_orders_matches_staging`
之後，ADR-0055 曾經寫下「下一次掉列會被自動抓到」。**同一天稍晚，`stg_quality_events`
掉了 550 列，那支測試一列都沒抓到**——因為它只指名 `stg_orders`。

覆蓋率不等於「有沒有這類測試」，而等於**有幾支模型各自有一支**。

### 還有一層：掉列會偽裝成缺值

對帳測試比內容測試強了一級（它問「列還在不在」），但它仍然有前提——**它只看得見它那張表**。

同一次事故的第二階段裡，`int_orders` 的 `2026-08-26` 有完整的 800 列，一列不少，
但其中 550 列的 `quality_state_at` 是 NULL：上游 `stg_quality_events` 掉的列，
經過 **LEFT JOIN** 之後變成了下游的**空欄位**。

| | 掉列 | 經 LEFT JOIN 後 |
|---|---|---|
| 症狀 | 少了一些列 | 列數完全正確，某個欄位變 NULL |
| 誰抓得到 | 對帳測試 | `assert_int_orders_quality_state_resolved` |

> **防線是照損害的形狀設計的，而損害會換形狀。**

**這支測的是「不准【一直】是 NULL」，不是「不准是 NULL」。** 這個區別是它的全部：

`int_orders` 的 LEFT JOIN 有一個刻意的 fallback——事件缺席時 fall back 到 ODS 快照
（乾淨照流、髒的續留 quarantine），[cloud-layer](./cloud-layer.md) 明定它**只造成延遲、
不造成髒資料**。所以 NULL 本身合法，直接 `not_null` 會在每次跑批的最新分區固定閃黃，
而 **routine 的黃燈等於沒有燈**（本專案已為同一件事付過代價：freshness 恆紅，
最後被迫拆成獨立 DAG，[ADR-0039](../adr/0039-observation-signals-own-dag.md)）。

這支斷言的是那個 fallback 的**時效上界**：延遲可以，永久缺席不行。檢查窗是
**2 天 ～ 50 天**——下界必須 > 一個完整 DAG 週期（否則正常的自癒過程本身會讓它變紅），
上界把保留期邊緣排除在外（見下）。

合法暫態為什麼這麼窄，值得記下來——它是下界的推導依據：`process.py` 在**同一個
transaction** 裡寫 ODS 與 quality_event，兩者時戳都是 `func.now()`。

> ⚠️ 這條相等**只對 `initial_evaluation` 成立**。Proposal B 的 promotion 事件
> `event_at = now()`，落在**較晚**的分區——所以「訂單與它的事件永遠同分區」是**錯的**。
> 正確的敘述是：**訂單與它的 `initial_evaluation` 必在同一分區**（同 transaction、同時戳），
> 而後續事件落在更晚的分區，因此只會**比訂單更晚過期**，不會讓訂單變成遺孤。
> 過期對稱靠的是前半句。守門的是 `assert_initial_event_shares_order_timestamp`。

唯一的縫隙是兩個並行 extract task 相隔數秒讀 ODS。這個縫隙**由建構證明是自癒的**：
偏斜只可能丟掉**最前緣**的列，而最前緣正是 watermark 的來源（destination-derived
`MAX(partition_id)`），所以 watermark 永遠不會越過被丟掉的那一列，下一輪 `>=` 必然重抓。

### 為什麼是 warn，而且是最終決定

**severity 編碼的是「正確的處置動作」，不是「我們有多確定」。** 判準沿用
`assert_product_attributes_stable`：error ＝「我們自己的 SQL 對不對」，warn ＝ 其他來源的訊號。

1. **它紅的時候 Gold 沒有錯。** fallback 保證「延遲、不髒」——漏掉的是一次可能的 promotion
   （品質管線的偽陰性），不是 Gold 的污染。**阻斷權的用途是擋住錯的資料流向下游，
   而這裡沒有錯的資料要擋。**
2. **它不是重跑能清掉的**（需人工回填）。DAG 是逐層 `dbt build` + `retries=0`，設成 error
   的實際後果是：從那天起每天的排程都失敗，直到有人處理——**為了一個三天前就已經缺了的
   屬性，停掉今天新資料的正確流動。** 與 DAG 檔頭⑤「deterministic 的失敗不該重試」同一條推理。
3. **該阻斷的情境上游已經擋了**：兩支 `*_matches_staging` 是 error，掉列若落在 7 天窗內
   由它們先紅先擋。這支的價值只在那兩支漏掉的情況，依定義更舊、更不緊急。

> ⚠️ 代價是**能見度**：warn → task success → `on_failure_callback` 不會響，只留在 dbt log。
> 能見度的解法是**通知路徑**，不是**阻斷權**——[ADR-0039](../adr/0039-observation-signals-own-dag.md)
> 已為同一問題立過決策：觀察訊號自成路徑，不靠阻斷主線換取能見度。
>
> 而本專案**沒有真實通知通道，這是全域決策，不是待辦**：[PORTFOLIO_SCOPE #7](../PORTFOLIO_SCOPE.md)
> ——沒有值班對象。把 notifier 指向不存在的連線，行為會是「紅燈 → 回呼觸發 → 它自己拋錯
> → 沒有人收到」，而**相信自己有告警、實際上沒有，遠比坦白地沒有告警危險**。
> 所以這支的能見度上限就是 dbt log 與 `run_results.json`——**已知、刻意，且不因為多了這支測試而改變**。

### 上界 50 天：作品限制

兩張表的分區過期是**各自獨立的背景作業**，即使過期日相同也不保證同時生效。事件線若先一步
過期，那批訂單會瞬間變成「有訂單、無事件」且年齡遠大於下界 → **誤報，但資料沒壞**。

BQ sandbox（未啟用帳單）硬鎖分區過期 < 60 天，所以這條邊**每 60 天就會撞到一次**。
啟用帳單、保留期 1825 天之後它在五年後，等同不存在。

> 這是**作品限制**留下的痕跡，但上界本身是**通用解**：任何有分區過期的表都有這條邊，
> 不是繞過限制的權宜。⚠️ 上界與保留期**成組維護**——保留期下修時上界必須跟著下修，
> 否則檢查窗會變成空窗，而**空窗＝測試靜默失效、永遠綠**，比沒有這支測試更糟。

不損失覆蓋率：一筆永久缺事件的列，在 2～50 天大的期間每天都會被檢查到。

---

## 7. Fixture 與慣例

- `asyncio_mode=auto` 取代手動的 `asyncio.run()`。
- `reset_limiter` fixture 消除跨測試的限流計數器汙染。
- 驗證經 `dependency_overrides` 繞過，所以非驗證測試不必逐個請求附上 header。
- `tests/helpers.py` 放 mock factory 與測試資料；`tests/conftest.py` 放共用 fixture。

---

## 8. 相關

- [PORTFOLIO_SCOPE](../PORTFOLIO_SCOPE.md) — E2E 測試與 `check_migration_drift.py` 進 CI，兩者皆暫緩
- [transformation](./transformation.md) — dbt 測試套件
- [orchestration](./orchestration.md) — dbt 測試在哪裡執行
