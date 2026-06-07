import math
import types
import typing
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from pydantic import BaseModel

from schema import ODSOrder, OrderIN

DQ_RULE_VERSION = "v2"  # 每次規則改動時 bump，搭配 git tag 記錄變更內容
# v2：新增 FIELD_TOO_LONG / NON_FINITE_NUMBER / ORDER_DATE_IN_FUTURE 規則，
#     並以 sentinel 正規化影響值評估（同一筆 raw 重跑會得到不同 has_clean_error）。


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


def business_clean(ods: ODSOrder) -> ODSOrder:
    errors = []

    # items 是 list of dict，逐一檢查。range 檢查皆以 isfinite 守衛：
    # 非有限值（NaN/±Inf）只報一次 non_finite_number，不再意外觸發 range 違規。
    if ods.items:
        for i, item in enumerate(ods.items):
            # 非有限值優先攔截（NaN/±Inf）
            for num_field in ("quantity", "unit_price", "cost_price", "discount_pct", "shipping_fee"):
                v = item.get(num_field)
                if _is_number(v) and not math.isfinite(v):
                    errors.append({"code": DQCode.NON_FINITE_NUMBER, "field": num_field, "value": str(v), "index": i})

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

    # tax_pct 0~100（非有限值只報 non_finite）
    if ods.tax_pct is not None:
        if not math.isfinite(ods.tax_pct):
            errors.append({"code": DQCode.NON_FINITE_NUMBER, "field": "tax_pct", "value": str(ods.tax_pct)})
        elif not (0 <= ods.tax_pct <= 100):
            errors.append({"code": DQCode.TAX_PCT_OUT_OF_RANGE, "field": "tax_pct", "value": ods.tax_pct})

    # delivery_date 不能早於 order_date
    # value 存 isoformat 字串：clean_error_message / quality_events.reason 是 JSONB，date 物件無法直接序列化
    if ods.delivery_date and ods.order_date:
        if ods.delivery_date < ods.order_date:
            errors.append({"code": DQCode.DELIVERY_BEFORE_ORDER, "field": "delivery_date",
                           "value": ods.delivery_date.isoformat(), "order_date": ods.order_date.isoformat()})

    # order_date 不能是未來（+容差吸收時區/時鐘偏移）
    if ods.order_date is not None:
        cutoff = datetime.now(timezone.utc).date() + timedelta(days=FUTURE_DATE_TOLERANCE_DAYS)
        if ods.order_date > cutoff:
            errors.append({"code": DQCode.ORDER_DATE_IN_FUTURE, "field": "order_date",
                           "value": ods.order_date.isoformat()})

    # customer_rating 1~5（非有限值只報 non_finite）
    if ods.customer_rating is not None:
        if not math.isfinite(ods.customer_rating):
            errors.append({"code": DQCode.NON_FINITE_NUMBER, "field": "customer_rating", "value": str(ods.customer_rating)})
        elif not (1 <= ods.customer_rating <= 5):
            errors.append({"code": DQCode.CUSTOMER_RATING_OUT_OF_RANGE, "field": "customer_rating", "value": ods.customer_rating})

    # age 0~120
    if ods.age is not None and not (0 <= ods.age <= 120):
        errors.append({"code": DQCode.AGE_OUT_OF_RANGE, "field": "age", "value": ods.age})

    # 自由文字欄位軟性長度上限（超過則標記，資料仍落地；硬牆與 fast-fail 為下一道防線）
    for field, max_len in SOFT_MAX_LENGTHS.items():
        value = getattr(ods, field, None)
        if value is not None and len(value) > max_len:
            errors.append({"code": DQCode.FIELD_TOO_LONG, "field": field, "length": len(value), "max": max_len})

    return ods, errors


def clean_order(ods: ODSOrder) -> tuple[ODSOrder, bool, Optional[list]]:
    ods = format_clean(ods)
    ods, business_errors = business_clean(ods)

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