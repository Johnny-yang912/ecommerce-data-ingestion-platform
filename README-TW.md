# 訂單資料管線系統

### 一條從攝入到分析的資料管線，以電商訂單為場域

[English](./README.md) | **繁體中文**

[![CI](https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform/actions/workflows/ci.yml)

---

## 這是什麼

一套**資料管線系統**：把不可信的入站資料，經由分層的品質契約，逐步變成可信的分析資料——而**每一次品質判斷的演進都保持可稽核**。

它同時是一個後端服務與一條資料管線。**攝入端的正確性是後端問題，下游的可信度是資料問題**，而這個作品的重點正是兩者的交界——資料在那裡從「請求」變成「事實」。

電商訂單是選定的**場域**，不是主題。選它，是因為三個問題在這裡同時成立，而它們分別逼出了系統的三個部分：

| 這個場域帶來的 | 它逼出了什麼 |
|---|---|
| **高併發下單** | 攝入層的容錯——多點重試、CAS 認領、崩潰恢復、有斷路器的派工。確保**資料進得來** |
| **重複提交** | 兩種身分、first-write-wins 冪等、`duplicate` 作為終端狀態而非錯誤。確保**一筆訂單只存在一次，而重複本身仍是讀得出來的訊號** |
| **上游髒資料** | 分層品質契約（Raw → ODS → `stg_` → `int_` → `dim_`／`fct_` → `rpt_`，恰好阻斷一次）、規則版本化、append-only 事件日誌。確保**一個判斷可以被修訂，而不必改寫歷史** |

這三件事不是電商獨有——點擊流、IoT 遙測、物流事件都會遇到，有些量級更大、代價也不見得更低。選電商訂單，是因為它們在這裡同時成立，而且這個領域的資料模型夠完整：一筆訂單天然帶著顧客、商品、金流與時間，能一路走到星型結構與報表層——**讓這條管線有真正的終點，而不是停在一張洗乾淨的表。**

## 這不是什麼

它是一個**作品專案**。沒有真實上游、沒有真實流量；資料源是一個透過真實攝入路徑投遞的模擬器。有些生產系統會有的東西刻意不做——每一項都有記錄，附上真實系統的做法與重啟它的觸發條件：**[PORTFOLIO_SCOPE](./docs/zh-TW/PORTFOLIO_SCOPE.md)**。

---

## 架構

```
POST /orders                                    ← 需要 X-API-Key
    ↓
[Raw]  逐字保留、不可變                                    status: pending
    ↓  Celery 派工（有斷路器；失敗 → 恢復掃描）
[Worker]  CAS 認領 → 解析 → 攤平 → 清洗 → 冪等檢查
    ↓
[ODS] + [quality_events]        ← 不可變錨點 + append-only 判斷日誌
    ↓  Airflow，每日台北時間 22:30
[BigQuery staging]  orders + quality_events，watermark 增量
    ↓
dbt stg_*     1:1 鏡像、去重、Hard Gate            ← Silver，保留所有列
    ↓
─────────────────── 阻斷發生在這裡 ───────────────────
dbt int_*     依有效品質狀態的 Row Filter           ← Gold 入口
    ├── 有效乾淨  → int_orders → int_order_items
    └── 非乾淨    → int_orders_quarantine
    ↓
dbt dim_*/fct_*  Kimball 星型結構  →  dbt rpt_*  固定粒度預先聚合
```

完整說明：**[ARCHITECTURE](./docs/zh-TW/ARCHITECTURE.md)**

---

## 技術棧

| 層 | 技術 |
|---|---|
| API | FastAPI · Pydantic · slowapi |
| 儲存 | PostgreSQL 16 · SQLAlchemy · Alembic |
| 佇列 | Celery · Redis（broker db0、限流計數器 db1） |
| 倉庫 | BigQuery（分區 + 叢集 staging、成本保險絲） |
| 轉換 | dbt-core / dbt-bigquery 1.11 |
| 編排 | Airflow 3.0.0、LocalExecutor、六個 DAG |
| 可觀測性 | OpenTelemetry → 常駐 Collector → Grafana Cloud |
| 執行環境 | Docker Compose（兩份疊加的檔案） |

---

## 快速開始

```bash
git clone https://github.com/Johnny-yang912/ecommerce-data-ingestion-platform.git
cd ecommerce-data-ingestion-platform
cp .env.example .env      # 至少要設 API_KEYS
```

**攝入層**——一道指令，不需要本機 Python 或 Postgres：

```bash
docker compose up -d --build
```

`db` + `redis` 先起（healthcheck 把關其餘）→ 一次性的 `migrate` 跑 `alembic upgrade head` → `api` / `worker` / `beat` 在兩者健康**且**遷移成功之後才啟動。

API 在 `http://localhost:8000`（docs `/docs`、health `/health`）。

**加上分析管線**——`.env` 還需要 `BQ_PROJECT` 與 `GOOGLE_APPLICATION_CREDENTIALS`：

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d --build
```

⚠️ **兩份 compose 檔必須疊加成同一個 project**——那才讓 DAG 觸及得到 `db`、讓 seeding 觸及得到 `api`。Airflow UI 在 `http://localhost:8080`。

**不用 Docker 的本機開發**：

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
alembic upgrade head          # Alembic 是單一真相，不是 create_all
uvicorn main:app --reload
pytest
```

完整啟動說明與兩個主機側陷阱：**[runbooks/airflow-startup](./docs/zh-TW/runbooks/airflow-startup.md)**

---

## API

所有端點都需要 `X-API-Key` header；缺失或無效回 `401`。

| Method | Path | 說明 | 限額 |
|---|---|---|---|
| `POST` | `/orders` | 攝入一筆訂單。回 `200` + `pending`——**即使佇列派工失敗也一樣**，因為 Raw 那一列已經 commit | 60/分 |
| `POST` | `/process_raw/{id}` | 手動重新處理。`?force=true` 只接受 `error` 與 `duplicate`——絕不接受 `processed` | 20/分 |
| `GET` | `/raw/{id}` | 檢視一筆 Raw 記錄與它當前的狀態 | 120/分 |
| `GET` | `/health` | 給容器 healthcheck 用的存活探針 | — |

限額是**逐已驗證客戶的，沒有全域限額**——全域上限會讓一個吵鬧的上游有能力對其他所有上游造成阻斷服務。

---

## 現況

| 層 | 狀態 |
|---|---|
| 攝入 · 處理 · 任務佇列 | ✅ |
| 抽取 · 轉換（dbt）· 編排 | ✅ |
| 可觀測性（traces + 營運指標） | ✅ |
| BI（Looker Studio 接 `rpt_`） | ✅ |
| 告警 · 監控儀表板 | ⏸ 暫緩——閾值需要真實流量 |

**445 個單元 + 整合測試**（受管模組 100% 覆蓋、Python 3.10/3.12 矩陣）· **52 個 DAG 測試**（獨立 workflow）· **93 個 dbt 測試**。

⚠️ **CI 的綠燈不代表 DB 層契約已被驗證**——CAS、去重與崩潰恢復在 CI 裡用的是 mock 資料庫，靠手動腳本佐證。見 [design/testing](./docs/zh-TW/design/testing.md)。

完整矩陣與已知風險：**[STATUS](./docs/zh-TW/STATUS.md)**

---

## 文件

| 文件 | 給誰 | 何時看 |
|---|---|---|
| [ARCHITECTURE](./docs/zh-TW/ARCHITECTURE.md) | 系統如何組成 | 最先 |
| [STATUS](./docs/zh-TW/STATUS.md) | 做了什麼、沒做什麼、為什麼 | 在評斷一個缺口之前 |
| [PORTFOLIO_SCOPE](./docs/zh-TW/PORTFOLIO_SCOPE.md) | 每一個暫緩項，以及真實系統的做法 | 當某個東西看起來缺席時 |
| [ADR](./docs/zh-TW/adr/README.md)（54 條） | 每個決策為何如此、否決了什麼 | 當一個選擇看起來奇怪時 |
| [design/](./docs/zh-TW/design/)（8 份） | 每一層如何運作 | 要改動它時 |
| [runbooks/](./docs/zh-TW/runbooks/)（8 份） | 東西壞掉時該做什麼 | 事故當下 |
| [verification/](./docs/zh-TW/verification/)（14 份） | 量到了什麼、推翻了什麼 | 當你懷疑某個宣稱時 |
| [incidents/](./docs/zh-TW/incidents/)（2 份） | 什麼壞了、怎麼診斷出來的 | — |
| [CHANGELOG](./CHANGELOG-TW.md) | 系統如何走到今天 | — |

### 建議的閱讀路徑

| 你有 | 讀 |
|---|---|
| **5 分鐘** | 上面那張架構圖 → [STATUS](./docs/zh-TW/STATUS.md) |
| **15 分鐘** | 再加三條 ADR —— [0002](./docs/zh-TW/adr/0002-has-clean-error-non-blocking.md)（核心不變式）、[0015](./docs/zh-TW/adr/0015-staleness-from-processing-started-at.md)（一個缺陷與它的修法）、[0028](./docs/zh-TW/adr/0028-hard-gate-per-batch-scope.md)（一個被修訂過的決策） |
| **30 分鐘** | 再加兩份驗證記錄 —— [SIGKILL 恢復](./docs/zh-TW/verification/2026-08-10-celery-sigkill-recovery.md) 與 [逾時判定基準](./docs/zh-TW/verification/2026-08-10-staleness-basis-self-collision.md) |
| **想找漏洞** | [PORTFOLIO_SCOPE](./docs/zh-TW/PORTFOLIO_SCOPE.md)，然後 [事故報告](./docs/zh-TW/incidents/2026-08-silent-scheduling-stalls.md) |

commit 歷史也是記錄的一部分——120 多個 commit，訊息陳述的是理由而不只是改動。

---

## 授權

[MIT](./LICENSE)
