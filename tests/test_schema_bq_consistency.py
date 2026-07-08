"""
跨層一致性：extract_ods_to_bq 的每張表 FIELDS（BQ staging schema）↔ 對應 SQLAlchemy 資料表。

目的：FIELDS 是各表 schema 的第三份手維護宣告（schema.py、models.py 之外）。
把「改了 models.py 卻忘了改 FIELDS」這種靜默不一致變成會紅的測試——
最危險的情境是「表加了欄、FIELDS 沒加」，會導致該欄靜默不被抽取、資料默默漏到 BQ。

逐表守衛：orders(ODS) 與 quality_events(QualityEvent) 各自跑同一組斷言。
與 test_schema_db_consistency 同精神：只保證『一致』，不主張型別宣告『正確』。
"""
import pytest
from sqlalchemy import Boolean, Date, DateTime, Float, Integer, String
from sqlalchemy.dialects.postgresql import JSONB

from extract_ods_to_bq import SPECS


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


def _fields_by_name(spec):
    return {name: (bq_type, mode) for name, bq_type, mode in spec.fields}


# (spec, column) 逐欄位案例；id 帶表名好辨識（orders.received_at / quality_events.event_at ...）
_COL_CASES = [(spec, col) for spec in SPECS for col in spec.model.__table__.columns]
_COL_IDS = [f"{spec.table}.{col.name}" for spec, col in _COL_CASES]


@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.table)
def test_no_column_missing_from_fields(spec):
    """每張表每個欄位都要在 FIELDS 出現。
    缺了 → 該欄靜默不被抽取（資料默默漏到 BQ），這條會紅。"""
    by_name = _fields_by_name(spec)
    missing = [c.name for c in spec.model.__table__.columns if c.name not in by_name]
    assert not missing, f"[{spec.table}] 有欄位未出現在 FIELDS（會被靜默漏抽）：{missing}"


@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.table)
def test_no_stale_field_without_column(spec):
    """FIELDS 不該有 model 已不存在的欄位（刪欄後忘了同步 FIELDS）。"""
    names = {c.name for c in spec.model.__table__.columns}
    stale = [name for name in _fields_by_name(spec) if name not in names]
    assert not stale, f"[{spec.table}] FIELDS 有 model 已無對應的殘留欄位：{stale}"


@pytest.mark.parametrize("spec,col", _COL_CASES, ids=_COL_IDS)
def test_field_type_matches_model(spec, col):
    """逐欄位斷言 FIELDS 的 BQ 型別與 model 欄位型別相符。"""
    by_name = _fields_by_name(spec)
    assert col.name in by_name, f"[{spec.table}] {col.name} 不在 FIELDS"
    expected = _expected_bq_type(col.type)
    assert expected is not None, (
        f"[{spec.table}] {col.name}: 未知的 SA 型別 {type(col.type).__name__}，"
        f"請在 _expected_bq_type 補對應 BQ 型別"
    )
    bq_type = by_name[col.name][0]
    assert bq_type == expected, (
        f"[{spec.table}] 型別不一致：FIELDS['{col.name}']={bq_type}，應為 {expected}"
    )


@pytest.mark.parametrize("spec,col", _COL_CASES, ids=_COL_IDS)
def test_field_mode_matches_nullability(spec, col):
    """REQUIRED iff model 欄位 NOT NULL；其餘 NULLABLE。
    讓 staging 忠實鏡射 model 的可空性（非空欄在 BQ 為 REQUIRED，兼作 fail-loud 保護）。"""
    by_name = _fields_by_name(spec)
    expected_mode = "NULLABLE" if col.nullable else "REQUIRED"
    actual_mode = by_name[col.name][1]
    assert actual_mode == expected_mode, (
        f"[{spec.table}] mode 不一致：FIELDS['{col.name}']={actual_mode}，"
        f"但 {spec.table}.{col.name} nullable={col.nullable} → 應為 {expected_mode}"
    )
