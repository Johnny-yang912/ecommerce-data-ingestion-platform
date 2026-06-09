"""
跨層一致性：extract_ods_to_bq.FIELDS（BQ staging schema）↔ ODS（SQLAlchemy 資料表）。

目的：FIELDS 是 ODS schema 的第三份手維護宣告（schema.py、models.py 之外）。
把「改了 models.py 卻忘了改 FIELDS」這種靜默不一致變成會紅的測試——
最危險的情境是「ODS 加了欄、FIELDS 沒加」，會導致該欄靜默不被抽取、資料默默漏到 BQ。

與 test_schema_db_consistency 同精神：只保證『一致』，不主張型別宣告『正確』。
"""
import pytest
from sqlalchemy import Boolean, Date, DateTime, Float, Integer, String
from sqlalchemy.dialects.postgresql import JSONB

from extract_ods_to_bq import FIELDS
from models import ODS


def _expected_bq_type(sa_type) -> str | None:
    """SQLAlchemy 欄位型別 → 預期的 BQ 型別字串。未知回 None（讓測試標記）。"""
    # JSONB 先判（它也是 TypeEngine）；DateTime 在 Date 之前判，避免次序誤判。
    if isinstance(sa_type, JSONB):
        return "JSON"
    if isinstance(sa_type, Boolean):
        return "BOOL"
    if isinstance(sa_type, DateTime):
        return "TIMESTAMP"
    if isinstance(sa_type, Date):
        return "DATE"
    if isinstance(sa_type, Integer):
        return "INTEGER"
    if isinstance(sa_type, Float):
        return "FLOAT"
    if isinstance(sa_type, String):
        return "STRING"
    return None


_FIELDS_BY_NAME = {name: (bq_type, mode) for name, bq_type, mode in FIELDS}
_ODS_COLUMNS = list(ODS.__table__.columns)


def test_no_ods_column_missing_from_fields():
    """ODS 每個欄位都要在 FIELDS 出現。
    缺了 → 該欄靜默不被抽取（資料默默漏到 BQ），這條會紅。"""
    missing = [c.name for c in _ODS_COLUMNS if c.name not in _FIELDS_BY_NAME]
    assert not missing, f"ODS 有欄位未出現在 FIELDS（會被靜默漏抽）：{missing}"


def test_no_stale_field_without_ods_column():
    """FIELDS 不該有 ODS 已不存在的欄位（刪欄後忘了同步 FIELDS）。"""
    ods_names = {c.name for c in _ODS_COLUMNS}
    stale = [name for name in _FIELDS_BY_NAME if name not in ods_names]
    assert not stale, f"FIELDS 有 ODS 已無對應的殘留欄位：{stale}"


@pytest.mark.parametrize("col", _ODS_COLUMNS, ids=lambda c: c.name)
def test_field_type_matches_ods(col):
    """逐欄位斷言 FIELDS 的 BQ 型別與 ODS 欄位型別相符。"""
    assert col.name in _FIELDS_BY_NAME, f"{col.name} 不在 FIELDS"
    expected = _expected_bq_type(col.type)
    assert expected is not None, (
        f"{col.name}: 未知的 SA 型別 {type(col.type).__name__}，請在 _expected_bq_type 補對應 BQ 型別"
    )
    bq_type = _FIELDS_BY_NAME[col.name][0]
    assert bq_type == expected, (
        f"型別不一致：FIELDS['{col.name}']={bq_type}，但 ODS.{col.name} 應對應 {expected}"
    )


@pytest.mark.parametrize("col", _ODS_COLUMNS, ids=lambda c: c.name)
def test_field_mode_matches_nullability(col):
    """REQUIRED iff ODS 欄位 NOT NULL；其餘 NULLABLE。
    讓 staging 忠實鏡射 ODS 的可空性（非空欄在 BQ 為 REQUIRED，兼作 fail-loud 保護）。"""
    expected_mode = "NULLABLE" if col.nullable else "REQUIRED"
    actual_mode = _FIELDS_BY_NAME[col.name][1]
    assert actual_mode == expected_mode, (
        f"mode 不一致：FIELDS['{col.name}']={actual_mode}，"
        f"但 ODS.{col.name} nullable={col.nullable} → 應為 {expected_mode}"
    )
