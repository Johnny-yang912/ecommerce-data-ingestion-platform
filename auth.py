"""
API Key 驗證（靜態 key + 多 key 輪替）

定位說明：此服務是內部資料網格中的攝取單元，呼叫者是少數且穩定的上游服務
（machine-to-machine），不是會自助來去的外部客戶。因此採「.env 靜態對應」而非
執行期可管理的 DB 表——新增來源是有計畫的基建事件，輪替以「多把有效 key 重疊期
+ 重部署」處理。若未來擴張為多 domain / 多租戶，再遷移到 api_clients 表。

被驗證出的 client_id 同時作為資料血緣的起點（source_client_id），會落地到
Raw / ODS，回答「這筆資料是哪個上游送的」。

信任模型：所有持 key 者互為信任，不做物件層級隔離 ⭐
─────────────────────────────────────────────────────────────────────────
`GET /raw/{raw_id}` 與 `POST /process_raw/{raw_id}` 刻意【不】比對 source_client_id
——任何有效 key 都能讀取／重放任何一筆 Raw。這是決策，不是漏寫，理由在那兩個端點
的定位：

    它們的使用者是【維運者】，不是上游。上游的角色只有 POST /orders；查任意
    raw_id、重放某筆失敗記錄，都是人工排查動作（見 main.py 對 /process_raw
    如實回傳 triggered 的說明——那個與 /orders 的不對稱，正是為維運者設計的）。
    維運者本來就該看得到全部 Raw，那正是除錯需要的能力。

以下三層常被混為一談，這裡分開講：

  ① API key ＝ 認證 +【邊界授權】。這是 key 最主要的職責：擋在 POST /orders
     前面，確保只有可信任的來源能把資料寫進 Raw。這一層是實實在在的授權——
     它回答「你能不能進來、能不能寫」。

  ② source_client_id ＝ ①的結果被記錄下來，成為【血緣】。它回答「這筆資料是
     哪個【已通過認證】的來源寫的」，一路帶到 ODS。它是認證的產物，不是 ACL。

  ③ 物件層級授權（逐列判斷「誰可以讀哪一筆」）＝ 本服務【沒有】這一層。

不做③，不是因為①不重要——恰恰相反，①正是整個信任模型的地基：不可信任的來源
在邊界就被擋住了，能拿到 key 的都是自己人。正因為邊界守住了，內部才不需要再切
租戶——③要保護的對象在這個系統裡不存在。

⚠️ 把②當成③用是很自然的誤會（欄位就在那裡，還建了 index），所以講明白：
   source_client_id 是為了回答「這筆資料哪來的」而生的，不是為了回答「誰有權
   讀它」。它【可以】被拿來做③（見下方失效條件），但那會是一個新決策，不是它
   原本的職責。

⚠️ 失效條件（可判定，不是「以防萬一」）：
   只要有一把 key 的持有者【不再是本系統的維運者或內部元件】——外部合作方、
   公開 demo、瀏覽器端直接持有——①「能拿到 key 的都是自己人」這個前提就破了，
   整段推論隨之失效。屆時二選一：
     ① 兩個端點加上 `.where(Raw.source_client_id == client_id)`；或
     ② 在 API_KEYS 格式中引入角色（key:client:role），讓維運身分保留跨 client
        讀取，其餘身分只能讀自己的。
   Raw.source_client_id 已建 index（models.py），前者的成本是一行 WHERE。

   ⭐ 為什麼這條只能是註解：專案裡的同類約束都有機械守衛——GCP_SA_KEY_PATH 的
      `:?`、--table 的 choices=、DAG param 的 pattern + shlex.quote。但「持 key 者
      是不是自己人」無法從程式碼判定，只能靠這段文字維持。而沒有失效條件的文字，
      會被下一個人當成永遠有效——所以上面那段⚠️才是這整節真正的重點。
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
