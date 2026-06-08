"""
跨層型別一致性：ODSOrder（Pydantic）↔ ODS（SQLAlchemy 資料表）。

目的：把「改了 schema.py 卻忘了改 models.py（或反之）」這種靜默不一致，
變成會紅的測試。本測試只保證兩層『一致』，不主張型別宣告『正確』——
正確性（該不該是 int）需另以上游資料契約把關。
"""
import typing
import types

import pytest
from sqlalchemy.dialects.postgresql import JSONB

from schema import ODSOrder
from models import ODS


def _unwrap_optional(ann):
    """剝掉 Optional/Union[..., None]，回傳實際型別。"""
    origin = typing.get_origin(ann)
    if origin is typing.Union or origin is getattr(types, "UnionType", None):
        args = [a for a in typing.get_args(ann) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return ann


# ODS 表上屬於基礎設施 / DQ metadata、ODSOrder 不該有對應的欄位，跳過比對。
_DB_ONLY = {
    "id", "received_at", "raw_id",
    "has_clean_error", "clean_error_message",
    "has_schema_drift", "schema_drift_message", "unmapped_fields",
    "dq_rule_version", "source_client_id",
}


def _columns():
    return {c.name: c for c in ODS.__table__.columns}


def test_every_ods_order_field_has_matching_column():
    """ODSOrder 的每個欄位都應在 ODS 表存在同名 Column。"""
    cols = _columns()
    missing = [n for n in ODSOrder.model_fields if n not in cols]
    assert not missing, f"ODSOrder 有欄位在 ODS 表找不到對應 Column：{missing}"


def test_no_unexpected_db_only_columns():
    """ODS 表多出、ODSOrder 沒有的欄位，必須都在白名單。
    新增業務欄位只加在 DB 端、忘了加進 ODSOrder 時，這條會紅。"""
    cols = _columns()
    extra = [n for n in cols
             if n not in ODSOrder.model_fields and n not in _DB_ONLY]
    assert not extra, f"ODS 表有業務欄位未反映到 ODSOrder（或漏加白名單）：{extra}"


@pytest.mark.parametrize("name, field", list(ODSOrder.model_fields.items()))
def test_pydantic_type_matches_column_type(name, field):
    """逐欄位斷言 Pydantic 型別與 SQLAlchemy 欄位的 python_type 相符。"""
    cols = _columns()
    assert name in cols, f"{name} 在 ODS 表無對應 Column"
    py_type = _unwrap_optional(field.annotation)

    # items 宣告為 Any（整包 JSONB），不做純量比對，只確認落在 JSONB 欄位。
    if py_type is typing.Any:
        assert isinstance(cols[name].type, JSONB), (
            f"{name} 在 Pydantic 為 Any，預期 DB 對應 JSONB，"
            f"實際為 {type(cols[name].type).__name__}"
        )
        return

    db_type = cols[name].type.python_type
    assert py_type is db_type, (
        f"型別不一致：{name} 在 ODSOrder 是 {py_type.__name__}，"
        f"但 ODS.{name} 的 python_type 是 {db_type.__name__}"
    )
