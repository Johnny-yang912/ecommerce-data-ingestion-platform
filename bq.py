"""BigQuery 連線（Phase 4 分析管線專用）

金鑰連接方法：採 ADC（Application Default Credentials）。
- 本機：讀 settings.google_application_credentials（來自 .env），橋接到
  GOOGLE_APPLICATION_CREDENTIALS 環境變數後，bigquery.Client() 自動認證。
- 正式環境（Airflow / Cloud Run）：平台已注入真環境變數或掛載 SA，
  setdefault 不會覆蓋它 → 同一份程式碼零修改即可換環境（prod-parity）。

與 dbt 共用同一把 service-account 金鑰（dbt-runner），不另發金鑰。
"""

import os

from google.cloud import bigquery

from config import settings


def get_bq_client() -> bigquery.Client:
    """回傳已認證的 BigQuery client。

    若 settings 提供金鑰路徑且環境變數尚未設定，則以該路徑補上（不覆蓋既有值）。
    兩者皆無時，bigquery.Client() 會自行 raise DefaultCredentialsError。
    """
    if settings.google_application_credentials:
        os.environ.setdefault(
            "GOOGLE_APPLICATION_CREDENTIALS",
            settings.google_application_credentials,
        )
    return bigquery.Client()
