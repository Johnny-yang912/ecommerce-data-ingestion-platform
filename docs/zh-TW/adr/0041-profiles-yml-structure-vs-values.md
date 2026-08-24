# ADR-0041：`profiles.yml`——結構進版控，值進環境

[English](../../en/adr/0041-profiles-yml-structure-vs-values.md) | **繁體中文**

| | |
|---|---|
| **狀態** | Accepted |
| **日期** | 2026-08 |
| **層** | 編排 |

---

## 背景

dbt 需要一份 `profiles.yml`。它包含連線的**形狀**——哪個 adapter、哪個 target、哪些重試設定——以及它的**值**——專案 id、dataset、憑證路徑。形狀應該像程式碼一樣被審查；值絕不可進版控。

**這個檔案住在哪裡是第二個問題，而它有一個不明顯的答案。**

## 決策

檔案住在 `orchestration/dbt_profiles/`，由 `DBT_PROFILES_DIR` 明確指向。它的結構在版控裡；每一個值都是 `env_var()` 引用。

**⚠️ 刻意不放在 `ecommerce_dbt/`。** dbt 尋找 `profiles.yml` 的順序，把**目前工作目錄排在 `~/.dbt` 之前**。放進 dbt 專案目錄，會讓本機的 `cd ecommerce_dbt && dbt run` 突然吃進那份編排用的 profile，並因為環境變數未設而失敗。**專屬目錄讓既有的本機工作流程完全不受影響。**

**⭐ 它刻意重用與 `config.py` 相同的環境變數**——`BQ_PROJECT`、`BQ_DBT_DATASET`、`GOOGLE_APPLICATION_CREDENTIALS`。

這不只是方便。**`reevaluate_quality.py` 讀的那個 `int_orders`，就是 dbt 寫的那張表。** 分開設定的話，兩者會**靜默地指向不同的 dataset**，而再評估會掃描一張過期或不存在的表**卻不報錯**——產出「找不到候選」，看起來與一次健康、剛好沒事可做的執行**一模一樣**。

> **一個共用變數讓那種分歧根本無從表達。**

## 後果

**連線的形狀可被審查。** `job_retries: 1`——ADR-0038 倚賴它而非 Airflow 重試的那個 adapter 層重試——在 diff 裡看得見。

**憑證絕不進入版控庫**，而同一份檔案靠改變環境、而非改變檔案，就能跨環境運作。

**產生者與消費者不可能在 dataset 上分歧。** 這是實質的收穫，**而且它關掉的是一個原本會靜默、而非大聲的失效。**

**本機 dbt 工作流程不受影響。** `~/.dbt/profiles.yml` 繼續服務互動式工作；編排 profile 只在 `DBT_PROFILES_DIR` 指向它時才被使用。

**代價是多一個目錄與多一個要正確設定的環境變數**，而且缺少的 `env_var()` 會在 dbt 啟動時失敗、而非在解析時——大聲，但比理想中晚。

## 考慮過的替代方案

**放在映像內的 `~/.dbt/profiles.yml`。** 不在版控裡，所以形狀不可審查也不可發現；而且它得被烤進映像或掛載進去，兩者都把憑證放在尷尬的地方。

**放進 `ecommerce_dbt/`。** 依上述的尋找順序陷阱，會破壞本機工作流程。

**dbt 與分析腳本各用一套環境變數。** 更明確，**而它把這個決策所關掉的那個靜默 dataset 分歧又放了回來。**

## 相關

- [ADR-0038](./0038-asymmetric-retries.md) — 這份檔案所承載的 `job_retries` 設定
- [ADR-0008](./0008-config-boundary.md) — Python 側的同一批環境變數
- [ADR-0030](./0030-proposal-b-event-driven-reevaluation.md) — 那個必須對 dataset 有共識的消費者
