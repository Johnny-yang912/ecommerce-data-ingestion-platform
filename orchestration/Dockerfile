# Airflow 映像：排程器本體 + 兩個【互相隔離】的執行環境。
#
# 為什麼不把 dbt 與專案依賴直接 pip 進 Airflow 本體：Airflow 與 dbt-bigquery 都重度依賴
# google-cloud-* / protobuf / jinja2，裝在同一個環境是典型的版本衝突來源，且每次升
# Airflow 都可能炸掉 dbt。官方也建議不要。這裡用兩個 venv 隔離，DAG 以 BashOperator
# 呼叫各自的直譯器——零額外基建，故障域乾淨（見 ORCHESTRATION-TW.md 決策 A2）。
#
#   /home/airflow/venvs/analytics   extract_ods_to_bq.py / reevaluate_quality.py
#   /home/airflow/venvs/dbt         dbt build / test / source freshness
#
# 專案原始碼【不】烤進映像，改用 bind mount（見 docker-compose.airflow.yml）：
# 本機開發時改一行程式不必重 build。

ARG AIRFLOW_VERSION=3.0.0
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

# 不需要 build-essential / libpq-dev：requirements.txt 用的是 psycopg2-binary（wheel）。

USER airflow

# 分析管線：requirements-analytics.txt = requirements.txt + google-cloud-bigquery。
# 與 dbt 分開兩層 COPY/RUN，讓其中一邊改版時另一邊的 layer cache 仍有效。
COPY requirements.txt requirements-analytics.txt /tmp/reqs/
RUN python -m venv /home/airflow/venvs/analytics \
    && /home/airflow/venvs/analytics/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/venvs/analytics/bin/pip install --no-cache-dir -r /tmp/reqs/requirements-analytics.txt

# dbt：版本對齊 ecommerce_dbt/README §10（dbt-core 1.11 / dbt-bigquery 1.11）。
ARG DBT_VERSION=1.11.*
RUN python -m venv /home/airflow/venvs/dbt \
    && /home/airflow/venvs/dbt/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/venvs/dbt/bin/pip install --no-cache-dir \
        "dbt-core==${DBT_VERSION}" "dbt-bigquery==${DBT_VERSION}"
