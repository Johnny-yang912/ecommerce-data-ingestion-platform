import math
import types
import typing
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from pydantic import BaseModel

from schema import ODSOrder, OrderIN

DQ_RULE_VERSION = "v3"  # 每次規則改動時 bump，搭配 git tag 記錄變更內容
# v2：新增 FIELD_TOO_LONG / NON_FINITE_NUMBER / ORDER_DATE_IN_FUTURE 規則，
#     並以 sentinel 正規化影響值評估（同一筆 raw 重跑會得到不同 has_clean_error）。
# v3：age 上限 120 → 130（見 AGE_MAX）。**本專案第一次「放寬」的規則改動**——
#     v1→v2 都是變嚴、只往後生效即可；放寬則會讓舊 quarantine 有機會被撈回，
#     這正是 Proposal B 回溯重評估存在的理由，也是它第一次真的有事可做。


class DQCode:
    """business_clean 違規碼（常數枚舉）。

    下游（quality_events.reason / clean_error_message JSONB）以 code 作為穩定識別，
    與人類可讀訊息解耦：訊息措辭可調整，code 不變，避免下游字串比對隨文案漂移。
    """
    QUANTITY_NON_POSITIVE        = "quantity_non_positive"
    UNIT_PRICE_NEGATIVE          = "unit_price_negative"
    DISCOUNT_PCT_OUT_OF_RANGE    = "discount_pct_out_of_range"
    TAX_PCT_OUT_OF_RANGE         = "tax_pct_out_of_range"
    DELIVERY_BEFORE_ORDER        = "delivery_before_order"
    CUSTOMER_RATING_OUT_OF_RANGE = "customer_rating_out_of_range"
    AGE_OUT_OF_RANGE             = "age_out_of_range"
    FIELD_TOO_LONG               = "field_too_long"
    ORDER_DATE_IN_FUTURE         = "order_date_in_future"
    NON_FINITE_NUMBER            = "non_finite_number"


# 「判定不可重現」的違規碼：這些規則在標記的同時把值就地正規化掉（NaN/Inf → None），
# 所以重評估時輸入已經是清理後的值，原判定條件【結構性地】無法再觸發——重跑必然「通過」。
#
# 這不是 bug，是攝入層的必要行為（PostgreSQL 的 JSONB/TEXT 存不下 NaN，見 business_clean
# 的 sanitize 註解），但它替 Proposal B 劃了一條邊界：帶有這些碼的記錄【不得】被自動
# promote——那個「通過」來自證據消失，而不是規則放寬。原始值只逐字留在 Raw，要救它必須
# 從 Raw 重產值，那按定義是 Proposal C 的領域，不是 B（見 DQ_ARCHITECTURE-TW 的 A/B/C 邊界）。
#
# 新增規則時的判準：這條規則會不會【修改 ods 的值】？會 → 加進這裡。
# 時間相依不算——ORDER_DATE_IN_FUTURE 已由 business_clean 的 as_of 參數修成可重現。
NON_REPRODUCIBLE_CODES = frozenset({DQCode.NON_FINITE_NUMBER})


# 自由文字欄位的「軟性」長度上限：超過則標記 has_clean_error（資料仍落地 ODS），
# 由下游 quarantine 處理——對應「接受一定程度的意外，但標記出來」。
# 此閾值刻意低於 models.py 的 DB 欄位硬牆（customer_name 255 / city 128）：
# 偏長 → 軟規則標記並落地；離譜塞爆 → 撞 DB 硬牆，由 process.py 的 DataError fast-fail 終態 error。
SOFT_MAX_LENGTHS = {
    "customer_name": 100,
    "city": 80,
}

# order_date 晚於今天視為異常；容差吸收時區/時鐘偏移，避免邊界誤報。
FUTURE_DATE_TOLERANCE_DAYS = 1

# 顧客年齡的合理區間。這條規則的目的是攔【資料輸入錯誤】（-3、999、把郵遞區號填進來），
# 不是判斷「這個年齡有沒有可能」——所以上限該取「任何真實值都不會超過」的保守高標，
# 而不是「人類活得到的極限」。
#
# v2 的 120 是在沒有真實流量資料時訂的保守估計（DQ 文件〈Hard Gate 閾值為業務判斷〉
# 早已預告這類閾值要在有資料後校準）。實務上有紀錄的最長壽命已達 122，120 會把
# 合法的高齡值標成髒資料；v3 放寬到 130，留出真實值不可能觸及的餘裕，同時仍能攔下
# 明顯的輸入錯誤。**放寬規則會讓既有 quarantine 記錄有機會被 promote，
# 故此改動需 bump DQ_RULE_VERSION 並跑一次 Proposal B 重評估。**
AGE_MIN = 0
AGE_MAX = 130

# 已知假空值：在 format_clean 正規化為 None（與 lowercase/strip 同屬正規化層，不標記）。
# 刻意保守，排除 "na"（可能是 North America）與 "-"（可能是合法佔位）以免誤殺。
SENTINEL_VALUES = {"", "null", "none", "n/a"}

# format_clean 會做 sentinel 正規化的描述性字串欄位（不含 order_id/customer_id 等鍵）。
_NORMALIZED_STR_FIELDS = (
    "gender", "ship_mode", "membership_tier", "payment_method",
    "preferred_payment_method", "preferred_device",
    "order_status", "country", "region", "state", "city",
    "postal_code", "customer_name",
)


def _is_number(v) -> bool:
    """真正的數值（排除 bool，因 bool 是 int 子類）。items 內的值未經 Pydantic 強轉，可能是字串。"""
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def format_clean(ods: ODSOrder) -> ODSOrder:

    # 統一小寫
    if ods.gender:
        ods.gender = ods.gender.strip().lower()
    if ods.ship_mode:
        ods.ship_mode = ods.ship_mode.strip().lower()
    if ods.membership_tier:
        ods.membership_tier = ods.membership_tier.strip().lower()
    if ods.payment_method:
        ods.payment_method = ods.payment_method.strip().lower()
    if ods.preferred_payment_method:
        ods.preferred_payment_method = ods.preferred_payment_method.strip().lower()
    if ods.preferred_device:
        ods.preferred_device = ods.preferred_device.strip().lower()

    # 去頭尾空白
    if ods.order_status:
        ods.order_status = ods.order_status.strip()
    if ods.country:
        ods.country = ods.country.strip()
    if ods.region:
        ods.region = ods.region.strip()
    if ods.state:
        ods.state = ods.state.strip()
    if ods.city:
        ods.city = ods.city.strip()
    if ods.postal_code:
        ods.postal_code = ods.postal_code.strip()
    if ods.customer_name:
        ods.customer_name = ods.customer_name.strip()

    # 假空值（sentinel）正規化為 None（涵蓋只剩空白被 strip 成 "" 的情況）
    for field in _NORMALIZED_STR_FIELDS:
        value = getattr(ods, field)
        if value is not None and value.strip().lower() in SENTINEL_VALUES:
            setattr(ods, field, None)

    if ods.order_date and ods.delivery_date:
        ods.delivery_days = (ods.delivery_date - ods.order_date).days

    return ods


def business_clean(ods: ODSOrder, as_of: Optional[date] = None) -> tuple[ODSOrder, list]:
    """業務規則驗證。回傳 (ods, errors)；ods 可能被就地正規化（見 NON_REPRODUCIBLE_CODES）。

    as_of：時間相依規則（目前只有 ORDER_DATE_IN_FUTURE）的判定基準日，預設為執行當下（UTC）。

      攝入路徑不傳 → 行為與過去完全相同。但【重評估】必須傳入該筆的 `received_at`，
      否則判定基準會隨 wall clock 漂移：一筆攝入當下被標記為未來日期的訂單，數月後
      重跑時那個日期已成過去 → 憑空通過 → 在規則一個字都沒放寬的情況下被 promote
      回 Gold（偽 promote）。傳入 as_of 讓這條規則變回可重現的純函數。

      接受 date 或 datetime；datetime 一律先轉 UTC 再取日期（時區契約收在此處，
      與 DQ_ARCHITECTURE-TW〈設計邊界〉「時區語意屬契約」一致——呼叫端應傳 tz-aware 值）。
    """
    errors = []

    # datetime 必須先判（它是 date 的子類）；date 物件恆為 truthy，故 `or` 安全。
    if isinstance(as_of, datetime):
        as_of = as_of.astimezone(timezone.utc).date()
    reference_date = as_of or datetime.now(timezone.utc).date()

    # items 是 list of dict，逐一檢查。range 檢查皆以 isfinite 守衛：
    # 非有限值（NaN/±Inf）只報一次 non_finite_number，不再意外觸發 range 違規。
    if ods.items:
        for i, item in enumerate(ods.items):
            # 非有限值優先攔截（NaN/±Inf）：標記後正規化為 None。
            # items 是 JSONB 欄位、PostgreSQL 不接受 NaN token，必須 sanitize 才能落地；
            # 同時建立不變量——ODS 永不儲存 NaN/Inf（原始值仍逐字保留在 Raw）。
            for num_field in ("quantity", "unit_price", "cost_price", "discount_pct", "shipping_fee"):
                v = item.get(num_field)
                if _is_number(v) and not math.isfinite(v):
                    errors.append({"code": DQCode.NON_FINITE_NUMBER, "field": num_field, "value": str(v), "index": i})
                    item[num_field] = None

            # quantity 不能是 0 或負數
            qty = item.get("quantity")
            if _is_number(qty) and math.isfinite(qty) and qty <= 0:
                errors.append({"code": DQCode.QUANTITY_NON_POSITIVE, "field": "quantity", "value": qty, "index": i})

            unit_price = item.get("unit_price")
            if _is_number(unit_price) and math.isfinite(unit_price) and unit_price < 0:
                errors.append({"code": DQCode.UNIT_PRICE_NEGATIVE, "field": "unit_price", "value": unit_price, "index": i})

            discount_pct = item.get("discount_pct")
            if _is_number(discount_pct) and math.isfinite(discount_pct) and not (0 <= discount_pct <= 100):
                errors.append({"code": DQCode.DISCOUNT_PCT_OUT_OF_RANGE, "field": "discount_pct", "value": discount_pct, "index": i})

    # tax_pct 0~100（非有限值標記後正規化為 None，維持 ODS 不存 NaN/Inf 的不變量）
    if ods.tax_pct is not None:
        if not math.isfinite(ods.tax_pct):
            errors.append({"code": DQCode.NON_FINITE_NUMBER, "field": "tax_pct", "value": str(ods.tax_pct)})
            ods.tax_pct = None
        elif not (0 <= ods.tax_pct <= 100):
            errors.append({"code": DQCode.TAX_PCT_OUT_OF_RANGE, "field": "tax_pct", "value": ods.tax_pct})

    # delivery_date 不能早於 order_date
    # value 存 isoformat 字串：clean_error_message / quality_events.reason 是 JSONB，date 物件無法直接序列化
    if ods.delivery_date and ods.order_date:
        if ods.delivery_date < ods.order_date:
            errors.append({"code": DQCode.DELIVERY_BEFORE_ORDER, "field": "delivery_date",
                           "value": ods.delivery_date.isoformat(), "order_date": ods.order_date.isoformat()})

    # order_date 不能是未來（+容差吸收時區/時鐘偏移）
    # 基準是 reference_date 而非直接讀時鐘：重評估／重建時傳 received_at 才能重現原判定。
    if ods.order_date is not None:
        cutoff = reference_date + timedelta(days=FUTURE_DATE_TOLERANCE_DAYS)
        if ods.order_date > cutoff:
            errors.append({"code": DQCode.ORDER_DATE_IN_FUTURE, "field": "order_date",
                           "value": ods.order_date.isoformat()})

    # customer_rating 1~5（非有限值標記後正規化為 None）
    if ods.customer_rating is not None:
        if not math.isfinite(ods.customer_rating):
            errors.append({"code": DQCode.NON_FINITE_NUMBER, "field": "customer_rating", "value": str(ods.customer_rating)})
            ods.customer_rating = None
        elif not (1 <= ods.customer_rating <= 5):
            errors.append({"code": DQCode.CUSTOMER_RATING_OUT_OF_RANGE, "field": "customer_rating", "value": ods.customer_rating})

    # age 合理區間（見 AGE_MIN / AGE_MAX 的閾值理由）
    if ods.age is not None and not (AGE_MIN <= ods.age <= AGE_MAX):
        errors.append({"code": DQCode.AGE_OUT_OF_RANGE, "field": "age", "value": ods.age})

    # 自由文字欄位軟性長度上限（超過則標記，資料仍落地；硬牆與 fast-fail 為下一道防線）
    for field, max_len in SOFT_MAX_LENGTHS.items():
        value = getattr(ods, field, None)
        if value is not None and len(value) > max_len:
            errors.append({"code": DQCode.FIELD_TOO_LONG, "field": field, "length": len(value), "max": max_len})

    return ods, errors


def clean_order(ods: ODSOrder, as_of: Optional[date] = None) -> tuple[ODSOrder, bool, Optional[list]]:
    """format_clean → business_clean 的整合入口（process.py 的唯一呼叫點）。

    as_of 原樣透傳給 business_clean。攝入路徑不傳（維持 wall clock）；
    Proposal C 從 Raw 重產值時必須傳入原始 `received_at`——它重用的正是這條純函數路徑
    （DQ_ARCHITECTURE-TW C-2 #3），不傳的話重建出來的評估結果會與攝入當下不一致。
    """
    ods = format_clean(ods)
    ods, business_errors = business_clean(ods, as_of=as_of)

    has_clean_error = len(business_errors) > 0
    clean_error_message = business_errors if business_errors else None

    return ods, has_clean_error, clean_error_message


# ─── Schema drift 偵測 ──────────────────────────────────────────────────────────
#
# 與 has_clean_error（業務值品質）平行、互不混用的獨立訊號：偵測「上游契約漂移」。
# 依設計決策只在記錄層偵測「多欄位 #1 + 型別漂移 #4」；少欄位/改名 #2/#3 留給觀測層
# （null-rate 監控），改名的新名字會被多欄位偵測捕捉。非阻斷：drift 不阻止資料落地。
#
# 預期 schema 直接從 Pydantic model 反射（model_fields / annotations），模型即契約，
# 單一真相——新增欄位自動同步，不需另維護一份清單。

class DriftCode:
    UNEXPECTED_FIELD = "unexpected_field"   # 多了契約外的欄位
    TYPE_DRIFT       = "type_drift"         # 已知欄位的 JSON 型別與契約不符
    NON_OBJECT_GROUP = "non_object_group"   # 巢狀群組/陣列元素應為物件卻不是


_SCALAR_KIND = {
    str: "string", int: "integer", float: "number", bool: "boolean",
    date: "string", datetime: "string",  # 日期以 ISO 字串表示
}


def _unwrap_optional(ann):
    """剝掉 Optional/Union[..., None]，回傳實際型別。"""
    origin = typing.get_origin(ann)
    if origin is typing.Union or origin is getattr(types, "UnionType", None):
        args = [a for a in typing.get_args(ann) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return ann


def _expected_kind(ann):
    """回傳 (category, payload)：
    ("scalar", kind_str) / ("dict", SubModel) / ("list", ElemModel|None) / ("other", None)。"""
    ann = _unwrap_optional(ann)
    if typing.get_origin(ann) in (list, typing.List):
        args = typing.get_args(ann)
        elem = args[0] if args else None
        sub = elem if (isinstance(elem, type) and issubclass(elem, BaseModel)) else None
        return ("list", sub)
    if isinstance(ann, type) and issubclass(ann, BaseModel):
        return ("dict", ann)
    if ann in _SCALAR_KIND:
        return ("scalar", _SCALAR_KIND[ann])
    return ("other", None)


def _json_kind(v):
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, int):
        return "integer"
    if isinstance(v, float):
        return "number"
    if isinstance(v, str):
        return "string"
    if isinstance(v, list):
        return "array"
    if isinstance(v, dict):
        return "object"
    return "null"


def _type_compatible(expected_kind: str, actual_kind: str) -> bool:
    if expected_kind == "number":
        return actual_kind in ("number", "integer")  # int 進 float 欄不算漂移
    if expected_kind in ("string", "integer", "boolean"):
        return actual_kind == expected_kind
    return True  # 未知預期型別不誤報


def _walk(model, data: dict, path: str, messages: list, unmapped: dict) -> None:
    fields = model.model_fields
    expected = set(fields)

    # 多欄位（#1）：契約外的 key → 標記 + 收進 unmapped（保留路徑與值）
    for key, value in data.items():
        if key not in expected:
            full = f"{path}{key}"
            messages.append({"code": DriftCode.UNEXPECTED_FIELD, "field": full})
            unmapped[full] = value

    # 已知欄位：純量做型別漂移（#4），巢狀則遞迴。缺欄位不在記錄層偵測（留給觀測層）。
    for name, field_info in fields.items():
        if name not in data:
            continue
        value = data[name]
        if value is None:
            continue

        category, sub = _expected_kind(field_info.annotation)
        full = f"{path}{name}"

        if category == "scalar":
            actual = _json_kind(value)
            if not _type_compatible(sub, actual):
                messages.append({"code": DriftCode.TYPE_DRIFT, "field": full,
                                 "expected": sub, "actual": actual})
        elif category == "dict":
            if isinstance(value, dict):
                _walk(sub, value, f"{full}.", messages, unmapped)
            else:
                messages.append({"code": DriftCode.NON_OBJECT_GROUP, "field": full,
                                 "actual": _json_kind(value)})
        elif category == "list":
            if isinstance(value, list):
                if sub is not None:
                    for i, elem in enumerate(value):
                        if isinstance(elem, dict):
                            _walk(sub, elem, f"{full}[{i}].", messages, unmapped)
                        else:
                            messages.append({"code": DriftCode.NON_OBJECT_GROUP,
                                             "field": f"{full}[{i}]", "actual": _json_kind(elem)})
            else:
                messages.append({"code": DriftCode.NON_OBJECT_GROUP, "field": full,
                                 "actual": _json_kind(value)})
        # "other" → 不檢查


def detect_schema_drift(payload: dict) -> tuple[bool, Optional[list], Optional[dict]]:
    """偵測上游契約漂移。回傳 (has_schema_drift, schema_drift_message, unmapped_fields)。"""
    messages: list = []
    unmapped: dict = {}
    if isinstance(payload, dict):
        _walk(OrderIN, payload, "", messages, unmapped)
    has_drift = len(messages) > 0
    return has_drift, (messages or None), (unmapped or None)