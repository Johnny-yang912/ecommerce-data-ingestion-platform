# Runbook：排程靜默停擺

[English](../../en/runbooks/airflow-silent-stall.md) | **繁體中文**

---

## 症狀

DAG 該跑卻沒跑——**而且 UI 上沒有任何東西是紅的。**

從來沒有 run 被建立，而沒有 run 就沒有失敗的 run 可以顯示。如果 dag-processor 無法完成解析，DAG 會在 `dag_stale_not_seen_duration`（600 秒）之後被標記為 stale，而**排程器不會為 stale 的 DAG 建立任何 run。**

> ⚠️ **這個失效模式沒有內建告警。** 任何偵測它的東西都必須住在 Airflow 之外——一旦每個 DAG 都 stale，一個寫成 DAG 的看門狗也不會跑。

---

## 排查順序——最快的訊號優先

```bash
# ① 有沒有任何 DAG 是 stale？（最快、最直接）
docker exec api-airflow-apiserver-1 airflow dags list | grep -c True
#    非零 = 中獎

# ② 解析是什麼時候停的？（在 is_stale=True 之後查這個）
docker exec api-airflow-apiserver-1 airflow dags details <dag_id> \
  | grep -E "is_stale|last_parsed_time"

# ③ dag-processor 是不是在砍它的解析子行程？
docker logs api-airflow-dag-processor-1 | grep -c "killing it"

# ④ 排除真正的語法／import 問題
docker exec api-airflow-apiserver-1 airflow dags list-import-errors
```

### 省時間的那一步

**④ 乾淨並不能證明 DAG 檔沒問題**——但如果 ② 與 ③ 看起來不對，DAG 檔大概不是問題所在。

直接在容器內手動解析，把程式碼排除掉：

```bash
docker exec api-airflow-dag-processor-1 python -c \
  "from airflow.models.dagbag import DagBag; \
   d=DagBag('/opt/airflow/dags/<file>.py', include_examples=False); \
   print(list(d.dags), d.import_errors)"
```

> 若手動解析**成功**而 dag-processor **失敗**，故障在 processor 的監督機制裡——逾時算術、資源、子行程生命週期——**不在 DAG 程式碼裡。** 那個岔路省下很多時間。

---

## 兩個旋鈕——別混為一談

| 設定 | 預設 | 支配 |
|---|---|---|
| `[dag_processor] dag_file_processor_timeout` | 50 | 一個解析子行程被砍掉並重試前能活多久 |
| `[scheduler] dag_stale_not_seen_duration` | 600 | 多久沒有成功解析才把 DAG 標記為 stale |

**調高前者並不會延後偵測**——那是後者的職責。前者只改變一次卡住的解析等多久才被砍掉重試，而且它對**持續性**的卡死毫無幫助（砍掉只是重跑同一個檔案）。它只對「重試就會過」的暫時性卡死有意義。

---

## 容器是 Up 但什麼都不動時

⚠️ **容器 `Up` 不代表掛載成功。** 一個在容器建立當下來源不可用的 bind mount，會靜默退化成空的 tmpfs。

```bash
# 在容器內——本該是 bind 的路徑出現 "type tmpfs" = 中獎
docker exec <container> mount | grep -E "/opt/(airflow|project)"
#   健康：ext4 或 virtiofs
#   壞掉：tmpfs
```

**重啟修不好它。** Bind 對映是在容器**建立**時登記的；`start` 不會重新登記。強制重建：

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d --force-recreate
```

---

## 一切全綠但仍然沒有 run

心跳、容器數、healthcheck 可以全部正常，而排程器什麼都沒建。**唯一可靠的訊號是 `dag_run` 表有沒有新的列。**

```bash
docker compose exec airflow-db psql -U airflow -d airflow -c \
  "select dag_id, max(run_after) from dag_run group by dag_id order by 2 desc;"
```

⚠️ **空的結果只有在「你看的那個窗口內確實有一個排程點」時才算證據。** 若沒有，就把問題逼出來：

```bash
# 手動觸發唯讀探針——推一個 run 走完整條派工路徑
docker exec api-airflow-apiserver-1 airflow dags trigger raw_pending_watch
```

它會寫入一筆排隊中的紀錄，並把它推進到執行、派工、回寫——全都是排程器主迴圈的工作，**正是殭屍狀態下卡死的那一段**。一兩秒內就會有答案。

---

## 相關

- [incidents/2026-08-silent-scheduling-stalls](../incidents/2026-08-silent-scheduling-stalls.md) — 四次發生、四個彼此無關的根因、沒有一次是紅的
- [design/liveness-alerting](../design/liveness-alerting.md) — 為何偵測器必須住在它所監看的系統之外
