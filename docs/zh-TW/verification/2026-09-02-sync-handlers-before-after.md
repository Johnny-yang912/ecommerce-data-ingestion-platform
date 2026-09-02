# 2026-09-02 — 端點改同步：解除 event loop 阻塞的前後對照

[English](../../en/verification/2026-09-02-sync-handlers-before-after.md) | **繁體中文**

---

## 驗證的假設

三個端點（`POST /orders`、`POST /process_raw/{raw_id}`、`GET /raw/{raw_id}`）掛在 `async def` 上，但內部是**同步阻塞的 psycopg2 呼叫**，且連線持有窗口內沒有任何 `await`。

被驗證的假設有三層：

1. 這是否真的佔住 event loop——**能不能量到**，而不只是從程式碼推論？
2. 改成 `def`（Starlette 將 handler 移入 anyio 執行緒池）能否解除？代價是什麼？
3. 同日稍早那份 [ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md) 的哪些結論會因此失效？

⚠️ 這是**正確性**修正，不是效能優化。效能是副作用，見結論六。

## 環境

與 [ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md) 完全相同（WSL2 16 核、僅保留 `db` / `redis`、限流關閉、以 `docker run` 起受測容器），另加：

| 項目 | 設定 |
|---|---|
| **before** | `api-api:latest` — 改動前的 image（`async def` + 阻塞 DB） |
| **after** | `api-a:latest` — 改動後的 image（`def` + 執行緒池） |
| 比較方式 | 同一台機器、同一組容器、**交替執行**，避免機器狀態漂移 |

程式碼改動：`main.py` 三個端點改 `def`、新增 `get_raw_body` 非同步依賴、`asyncio.sleep`→`time.sleep`、`_enqueue` 不再包 `to_thread`、移除 `import asyncio`。共 68 行變動。

## 方法

1. **兩個 image 並存**，`start_api` 以 `LT_IMAGE` 切換，其餘參數逐項對齊。
2. **event loop 探針**：負載進行中持續打 `GET /health`（每 10ms 一次，共 12 秒），量它的延遲分佈。
   ⭐ `/health` 不碰資料庫、不掛認證、不掛限流——**它唯一需要的資源就是 event loop**。因此它的延遲就是「loop 被佔住多久」的直接量測，這是本文的核心證據。
3. **吞吐**：`load_test.py` 各 1200 筆、併發 50，**三輪交替**（before→after→before→after…）。
4. 測試 1／2／3 以原文件的方法原樣重跑。
5. 每輪之間 `DELETE FROM ods` → `DELETE FROM raw`（FK 順序）。

## 觀測

### 測試 A — event loop 響應性 ⭐

workers=1，負載進行中持續打 `/health`：

| | **before** | **after** | 改善 |
|---|---:|---:|---:|
| 樣本數 | 534 | 725 | |
| p50 | 4.1 ms | 2.5 ms | 1.6× |
| p95 | 47.6 ms | 24.0 ms | 2.0× |
| **p99** | **167.2 ms** | **33.9 ms** | **4.9×** |
| **max** | **234.7 ms** | **53.0 ms** | **4.4×** |
| 狀態碼 | 全 200 | 全 200 | |

**這張表就是缺陷本身。** `/health` 完全不碰資料庫，卻在負載下被拖到 167ms（p99）——那 167ms 全部是「排在別人的 `db.commit()` 後面」。改動後降到 34ms。

⚠️ 這裡用 workers=1 是為了讓效應可見。多行程部署下另外三個 worker 還能應答，症狀被**遮蔽**而非消失——遮蔽的代價是每個被佔住的行程仍然完全停擺。

### 測試 B — 吞吐（三輪交替）

各 1200 筆、併發 50、workers=1、pool=5+10：

| 輪次 | before | after |
|---|---:|---:|
| 1 | 130.7 | 183.5 |
| 2 | 130.1 | 180.9 |
| 3 | 126.7 | 186.8 |
| **平均** | **129.2** | **183.7** |

**+42%，三輪交替兩組完全沒有重疊。** 另：before 三輪中有 1 筆失敗、首次比較時有 2 筆，after 全數為 0。

### 測試 C — 延遲拆解重跑（結論不變）

| | before | after |
|---|---:|---:|
| wall clock p50（OTel off，n=3500）| 8.34 ms | 8.09 ms |
| wall clock p50（OTel on）| 9.87 ms | 8.60 ms |
| **server span p50** | 8.05 ms | **7.29 ms** |
| 殘差（框架自身）| 6.48 ms (80.5%) | 5.87 ms (**80.6%**) |
| 派工 celery publish → Redis | 0.65 ms | 0.60 ms |
| `INSERT INTO raw` | 0.38 ms | 0.34 ms |
| `db.refresh()` 的 SELECT | 0.32 ms | 0.28 ms |
| DB 連線 checkout ×2 | 0.09 ms | 0.08 ms |

**單筆延遲與各段比例幾乎沒變**（殘差仍是 80.6%、資料庫仍不到一成）。這符合預期——**這個改動不讓單筆變快，它讓並發不互相阻塞**。server span 少掉的 0.76ms 主要來自省下的 `to_thread` 跳躍。

### 測試 D — Pool sweep 重跑（結論反轉）

workers=1、4 客戶端、總併發 52、n=2000：

| pool | before RPS（兩輪）| **after RPS（兩輪）** | before 連線峰值 | **after 連線峰值** |
|---|---:|---:|---:|---:|
| 1 (1+0) | 312 / 285 | **167 / 153** | 11 | 6 / 8 |
| 2 (2+0) | 240 / 253 | 202 / 183 | 11 | 7 / 9 |
| 5 (5+0) | 180 / 234 | 205 / 186 | 12 | 11 / 12 |
| 15 (5+10) | 180 / 203 | 201 / 183 | 11 | 20 / 13 |
| 40 (30+10) | 248 / 222 | 194 / 180 | 11 | 19 / 20 |
| 80 (60+20) | 145 / 267 | 195 / 178 | 12 | 19 / 25 |

兩個性質都變了：

1. **`pool=1` 現在明顯較差（約 -18%），兩輪一致。** before 那一欄的數字上下亂跳、沒有任何趨勢——那正是「pool 無關」的表現。
2. **連線峰值現在會隨 pool 成長**（6 → 25）。before 恆為 10–12，不管 pool 設 1 還是 100。

新的形狀是 **1→2 有效、2 以上平坦**。原因是瓶頸回到 GIL：workers=1 時同一時間只有一條執行緒能跑 Python，可重疊的僅有那 0.62ms 的 DB I/O，平均約需 1.1 條連線——**pool=2 就已經夠了**。

### 測試 E — Worker sweep 重跑（結論反轉）

pool=3+5（compose 實際值）、4 客戶端、總併發 52、n=2000、兩輪平均：

| workers | before | **after** | 變化 | after 連線峰值 |
|---:|---:|---:|---|---:|
| 1 | 130.8 | 160.5 | +23% | 13–14 |
| 2 | 201.0 | 260.2 | +29% | 21 |
| 4 | 298.1 | **367.3** | +23% | 29–32 |
| 8 | **207.1（反轉）** | **485.4** | **+134%** | 35–40 |

**before 那個「8 個 worker 會反轉」的結論消失了——after 的曲線一路上升，8 是最高點。**

確認不是客戶端造成的：

| 組態 | 總 RPS | 連線峰值 |
|---|---:|---:|
| workers=8、4 客戶端、n=4000、併發 104 | 507.0 | 57 |
| workers=8、**8 客戶端**、n=4000、併發 104 | 534.6 | 61 |
| workers=4、8 客戶端、n=4000、併發 104 | 334.5 | 34 |

客戶端行程加倍只多 5%（507→535），**所以 workers=8 的 510–535 RPS 是伺服器的真實數字，不是量到客戶端**。

### 測試 F — 連線預算的實際佔用 ⚠️

| 組態 | 預算上限 | **實測峰值** | 佔用率 |
|---|---:|---:|---:|
| before，workers=4 | 32 | 10–12 | ~35% |
| **after，workers=4（現行 compose）** | 32 | **29–34** | **~100%** |
| after，workers=8 | 64 | **57–61** | ~95% |

（峰值含 worker 容器與基礎連線約 4–8 條。）

## 結論

### 一、缺陷確實存在，而且是故障放大不是效能問題

測試 A 是直接證據：一個**完全不碰資料庫**的端點，在負載下 p99 被拖到 167ms。那段時間 event loop 停在別的請求的 `db.commit()` 裡。

推到極端：`statement_timeout` 是 30 秒，所以單一個卡住的查詢在舊寫法下會**凍結整個 uvicorn 行程 30 秒**——不只那筆請求，是那個行程上的全部請求，包含 `/health`。四個 worker 就是 25% 的服務容量消失。

⭐ **這個性質在正常運作時完全看不見，只在出事時出現——而那正是最需要它還活著的時刻。**

⚠️ 三個端點裡風險最高的其實是 `POST /process_raw/{raw_id}`，不是流量最大的 `/orders`：它的 `force=True` 會對 `raw` 下 `UPDATE`，而 worker 的 CAS claim（`try_claim_raw`）同時也在 `UPDATE` 同一列。鎖等待最長可達 30 秒。**低流量不等於低風險**——`/orders` 的 DB 操作是 0.34ms 的 INSERT，`/process_raw` 的是一個可能等鎖的 UPDATE。

### 二、單筆延遲與拆解比例不變

server span 8.05 → 7.29ms，各段比例幾乎相同。**修的不是單筆成本，是並發時的互相阻塞。**

### 三、連線池從「無關」變成「每行程 2 條就夠」

before 的 pool sweep 是一條沒有趨勢的雜訊線（因為每行程恆定只用 1 條連線）；after 出現真實的 1→2 落差，且連線峰值隨 pool 成長。

但 2 以上仍然平坦——GIL 讓每行程的 Python 運算無法真正並行，可重疊的只有 DB I/O。

### 四、worker 曲線不再反轉

before 在 8 反轉（298→207），after 一路上升到 485。**現行 `UVICORN_WORKERS=4` 從「曲線最高點」變成「保守的選擇」。**

⚠️ 但不要直接把它改成 8：本量測是單機、且壓測客戶端與服務共用 16 核。真實部署的核心數、以及下一節的連線上限，才是決定值。

### 五、⚠️ 撤回前一份文件的可執行建議

[ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md) 結論二寫著：

> API 的連線預算可由 32 條（`4 × (3+5)`）縮減至 8 條

**那個建議現在會造成 503。** 測試 F 顯示現行的 32 條在負載下實測峰值 29–34——**幾乎用滿**。砍到 8 會讓請求排隊等 pool、`pool_timeout=30s` 逾時、然後 `SATimeoutError → 503`。

那個建議在寫下的當下是對的（當時實測只用 4 條），**它失效的原因是產生它的那個前提被這次改動移除了**。這正是驗證記錄需要日期與推翻鏈的理由。

**同時，連線預算現在給了 `UVICORN_WORKERS` 一個上界**：`max_connections=100` 扣掉 `superuser_reserved_connections=3`、worker 容器 16、Airflow 與人工約 4 → API 約可用 **75 條**；以每行程 8 條（`3+5`）計，**最多約 9 個 uvicorn worker**。

### 六、預估與實測的落差，以及為什麼會差

改動前的預估是 **+10%**，實測 **+42%**。

預估錯在模型：它只把 trace 裡看得見的 DB span（0.79ms / 8.05ms ≈ 10%）算作可重疊，其餘一律當成 GIL-bound 而無法重疊。實際上 `def` 讓整個 handler 離開 event loop 之後，**loop 得以持續驅動 50 條連線的 socket / ASGI 收送**，而那部分原本是與 handler 搶同一條執行緒的——它不在任何一個 span 裡，所以模型看不到它。

⭐ **教訓：用 span 推估「拿掉阻塞能省多少」會系統性低估，因為 event loop 自己的工作不在 span 裡。**

### 七、實作上的三個具體陷阱（都已驗證）

| 陷阱 | 後果 | 結果 |
|---|---|---|
| 執行緒裡沒有 running loop，`asyncio.to_thread` / `asyncio.sleep` 會 `RuntimeError` | **執行期才炸**，不是 import 時 | 現有 `tests/test_auth.py` 會當場抓到 |
| slowapi 裝飾器檢查簽名，缺 `request` 參數直接 raise | **import 時炸** | `request: Request` 必須保留，即使 handler 內不再使用 |
| contextvars（structlog 的 `request_id`／`client_id`、OTel span）可能傳不進執行緒 | **靜默失效**，log 關聯不上 | **實測會正確傳播**（Starlette 的 `run_in_threadpool` 有 `copy_context`）——這是唯一會靜默壞掉的一項，已排除 |

另：8 個直接呼叫 handler 函式的單元測試需要更新（去 `await`、補 `raw_body=`、`patch("asyncio.sleep")`→`patch("main.time.sleep")`）。**它們會被打到，是因為它們繞過 FastAPI 的依賴解析直接呼叫函式**——走 TestClient 的測試完全不受影響。

### ⚠️ 本文不代表什麼

- **不代表故障下的行為。** 全程 DB / Redis / worker 健康，未做故障注入。結論一那個「30 秒凍結」是從 `statement_timeout` 推論的，**沒有實際注入慢查詢驗證過**。
- **不代表生產環境數字。** 單機、壓測客戶端與服務共用 16 核、資料量僅約 1.7 萬筆。
- **不代表 pool=2 就該設 2。** 測試 D 的平坦區間是在 workers=1 下量的；不同行程數下每行程的並發不同，而且 DB 降速時所需連線會上升——**餘裕是為了異常而留的**。
- **限流全程關閉。** 線上是 `60/minute`。

## 這推翻了什麼

⭐ **推翻同一天稍早的 [2026-09-02-ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md) 的結論二與三。**

| 被推翻 | 原本 | 現在 |
|---|---|---|
| 結論二（連線池）| 「pool 從 1 到 100 對吞吐與連線數皆無影響；預算可由 32 縮減至 8」 | pool 1→2 有 18% 落差；連線峰值隨 pool 成長；**預算 32 幾乎用滿，砍了會 503** |
| 結論三（worker 數）| 「1→4 接近線性，8 反轉；現行 `UVICORN_WORKERS=4` 位於曲線最高點」 | 曲線不再反轉，8 為 485 RPS；4 是保守值而非最高點 |

**結論一（時間在框架層不在資料庫）、四（天花板在 worker）、五（Python 是成本不是天花板）、六（高併發四面向）仍然成立**——測試 C 重跑後比例不變。

⚠️ 同一天寫下、同一天被推翻，這不是流程失誤：**前一份量的是「這個系統現在是什麼樣子」，本份量的是「把已知缺陷修掉之後變成什麼樣子」。** 兩者都必須存在，因為結論二的那個可執行建議如果被誰照做了，得知道它為什麼不再適用。

## 相關

- [2026-09-02-ingestion-capacity-and-bottlenecks](./2026-09-02-ingestion-capacity-and-bottlenecks.md) — 本文推翻其結論二與三
- [2026-08-03-load-test-ingestion](./2026-08-03-load-test-ingestion.md) — 其測試 2 的成因（`BackgroundTasks` 把同步處理丟進 40 條 threadpool）與本文是同一個機制的鏡像：那次是**執行緒太多、連線池太小**，這次是**執行緒太少、連線池用不到**
- [design/queue](../design/queue.md) · [ADR-0004](../adr/0004-cas-claim-rowcount.md)
