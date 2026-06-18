"""
API Key 驗證（靜態 key + 多 key 輪替）

定位說明：此服務是內部資料網格中的攝取單元，呼叫者是少數且穩定的上游服務
（machine-to-machine），不是會自助來去的外部客戶。因此採「.env 靜態對應」而非
執行期可管理的 DB 表——新增來源是有計畫的基建事件，輪替以「多把有效 key 重疊期
+ 重部署」處理。若未來擴張為多 domain / 多租戶，再遷移到 api_clients 表。

被驗證出的 client_id 同時作為資料血緣的起點（source_client_id），會落地到
Raw / ODS，回答「這筆資料是哪個上游送的」。
"""

import secrets

import structlog
from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader

from config import settings

logger = structlog.get_logger()

API_KEY_HEADER_NAME = "X-API-Key"
# auto_error=False：自行回傳 401（預設行為是 403），並可在拒絕時記 log。
api_key_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)


def _parse_api_keys(raw: str | None) -> dict[str, str]:
    """解析 API_KEYS env：'key1:client_a,key2:client_a,key3:client_b' → {key: client_id}。

    同一個 client_id 可對應多把 key（輪替重疊期 / 罕見的第二來源）。
    格式錯誤或空白的項目直接略過，不讓單一壞項拖垮整份設定。
    """
    mapping: dict[str, str] = {}
    if not raw:
        return mapping
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair or ":" not in pair:
            continue
        key, _, client_id = pair.partition(":")
        key, client_id = key.strip(), client_id.strip()
        if key and client_id:
            mapping[key] = client_id
    return mapping


# 啟動時解析一次，存記憶體 dict。測試可 monkeypatch 此變數。
API_KEYS: dict[str, str] = _parse_api_keys(settings.api_keys)


def verify_api_key(request: Request, api_key: str | None = Security(api_key_header)) -> str:
    """驗證 X-API-Key，命中回傳對應的 client_id，否則回 401。

    比對使用 secrets.compare_digest（constant-time），避免逐字元 timing 洩漏。

    命中後把 client_id 落到 request.state，作為限流的 key 來源：限流主體是「認證身分」
    而非「網路位置」。此處是唯一能設定的時機——驗證屬依賴解析階段，早於 slowapi
    wrapper 的限流檢查（限流檢查跑在 wrapper 最前面，比 endpoint 本體還早），
    若改在 endpoint 本體設定就來不及被 key_func 讀到。
    """
    if not api_key:
        logger.warning("API key 缺失")
        raise HTTPException(status_code=401, detail="Missing API key")

    for key, client_id in API_KEYS.items():
        if secrets.compare_digest(api_key, key):
            request.state.client_id = client_id
            return client_id

    # 只記前綴，絕不記完整 key
    logger.warning("API key 無效", key_prefix=api_key[:6])
    raise HTTPException(status_code=401, detail="Invalid API key")
