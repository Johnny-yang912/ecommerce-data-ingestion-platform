# 測試策略

[English](../../en/design/testing.md) | **繁體中文**

測試了什麼、在哪裡測，以及——最重要的——**測試在哪裡是盲的**。

---

## 1. 分層

| 層 | 數量 | 在哪 | 需要 |
|---|---|---|---|
| 單元 + 整合（mock DB） | 445 | CI，`ci.yml` | 什麼都不需要——幾秒跑完 |
| DAG 結構 | 52 | CI，`dags.yml` | Airflow，不需 DB、不需專案環境變數 |
| dbt | 95 | 在 DAG 內 | BigQuery |
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
| 真實併發下的 CAS 認領 | `load_test.py --cas-test` |
| `order_id` 去重 | `load_test.py --duplicate` |
| 崩潰後恢復 | `restart_test.sh`（SIGKILL） |
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
| `load_test.py` | 吞吐量、真實併發下的 CAS（`--cas-test`）、去重（`--duplicate`） |
| `restart_test.sh` | 處理中途 `SIGKILL`，然後恢復 `pending` 的列 |
| `check_migration_drift.py` | `alembic upgrade head` + `compare_metadata`；漂移時以非零碼結束 |

三者都打真實 server 與真實 PostgreSQL。它們的結果記錄在 `docs/*/verification/`（第 4 階段）。

---

## 6. dbt 測試清單

共 95 個測試。完整列出，因為「哪個測試守什麼」無法從 model 檔推導出來：

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
| 誰抓得到 | 對帳測試 | **目前沒有任何一條測試** |

> **防線是照損害的形狀設計的，而損害會換形狀。** 這個缺口目前是開著的——
> 對它的補法（`quality_state_at` 的 `not_null`，或跨層的事件覆蓋率對帳）尚未實作。

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
