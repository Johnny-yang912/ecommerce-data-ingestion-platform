"""
設定集中管理（Phase 3 - 工程化）

單一真相來源：所有「會因部署環境而異」的設定都在此宣告，啟動時實例化一次，
其他模組一律 `from config import settings` 取用，不再各自 `load_dotenv()` / `os.getenv`。

設計邊界：
- 這裡只放「環境設定」（會隨 dev/staging/prod 變動的值），不放「演算法常數」。
  retry 次數、stale 門檻等屬於程式行為的一部分，刻意留在各自模組開頭，改動需走 code review。
- `db_url` 不給預設 → 缺值時實例化即 raise，fail-fast，避免延遲到第一次連線才炸。
- `api_keys` 維持原始字串（非 dict）→ 避開 pydantic-settings 對 dict 欄位的 JSON 自動解析，
  並把「key 字串如何解讀」這段 auth 領域邏輯留在 auth.py。
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # .env 內未宣告的鍵不報錯，保留前向相容
    )

    # --- 必填（缺值即 fail-fast）---
    db_url: str

    # --- 認證（原始字串，交給 auth._parse_api_keys 解析）---
    api_keys: str = ""

    # --- DB pool / timeout（帶預設，需要時以 env 覆寫）---
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    statement_timeout_ms: int = 30000

    # --- 背景掃描間隔 ---
    scan_interval_seconds: int = 300

    # --- 日誌輸出格式：console（dev）| json（prod）---
    log_format: str = "console"


settings = Settings()
