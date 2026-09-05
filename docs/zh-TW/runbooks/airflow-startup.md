# Runbook：啟動整套環境

[English](../../en/runbooks/airflow-startup.md) | **繁體中文**

---

## 前置條件

`.env` 必須包含：

```
DB_URL, API_KEYS                            # 一律需要
BQ_PROJECT, GOOGLE_APPLICATION_CREDENTIALS  # 分析路徑需要（金鑰的主機路徑）
AIRFLOW_UID                                 # 建議設定，見下
```

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

---

## 啟動

```bash
# 只起攝入層
docker compose up -d --build

# 攝入層 + Airflow
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d --build
```

**兩份 compose 檔必須疊加成同一個 project。** 那才讓 DAG 能觸及主機名 `db` 的業務資料庫，也才讓 seeding 觸及得到 `api`。當成兩個獨立 project 跑會把它們放在不同網路，上述兩件事都會壞。

| 服務 | 網址 |
|---|---|
| API | `http://localhost:8000`（docs 在 `/docs`，health 在 `/health`） |
| Airflow | `http://localhost:8080`（本機用 SimpleAuthManager，不需登入） |

啟動順序自動把關：`db` + `redis`（healthcheck）→ `migrate`（`alembic upgrade head`，一次性）→ `api` / `worker` / `beat`。

---

## ⚠️ 兩個主機側的陷阱

### `db` 發布在 **5433**，不是 5432

由 `DB_PUBLISH_PORT` 控制。若主機上已有另一個 PostgreSQL 佔著 5432，`5432:5432` 的對映會讓服務直接綁定失敗。

容器之間走 `db:5432`，從不經過這個對映——**它存在的唯一理由是讓主機端的 `psql` 連得上。**

### 已 export 的 `DB_URL` 會靜默壓過 `.env`

主機側工具（`scripts/seed_demo.py --verify`、`psql`）必須連 `localhost:5433/orders`。`.env` 已經指在那裡——但 **`load_dotenv` 預設 `override=False`，所以你 shell 裡的環境變數會贏。**

若有舊的 `DB_URL` 被 export，腳本會安靜地連到別的地方，並對錯的資料庫回報結果。

```bash
# 確認你的 shell 實際 export 了什麼
echo "$DB_URL"
```

`verify()` 會印出它實際連到的資料庫。**那一行是這個錯誤唯一會自己浮現的地方**——請讀它。

---

## 驗證有起來

```bash
# 所有服務健康
docker compose -f docker-compose.yml -f docker-compose.airflow.yml ps

# DAG 解析成功，無 import error
docker exec api-airflow-apiserver-1 airflow dags list-import-errors

# ⚠️ 沒有任何 DAG 被標記 stale —— 若非零，見 airflow-silent-stall
docker exec api-airflow-apiserver-1 airflow dags list | grep -c True

# 攝入路徑有回應
curl -s localhost:8000/health
```

---

## 停止

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml down

# 連 volume 一起 —— 會摧毀資料庫
docker compose -f docker-compose.yml -f docker-compose.airflow.yml down -v
```

---

## 相關

- [airflow-silent-stall](./airflow-silent-stall.md) — DAG 解析成功但什麼都沒排程時
- [design/orchestration](../design/orchestration.md) — 每個容器是做什麼的
