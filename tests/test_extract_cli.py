"""extract_ods_to_bq 的 CLI 分派與 gate 行為。

守的是 Phase 5 的接縫：Airflow 一表一 task（`--table orders` / `--table quality_events`），
gate 從「腳本內彙整後 raise」搬到「DAG 的依賴邊」。這裡驗證三件事：

  1. 選表正確——單表模式【只】碰那一張（碰到另一張＝白做工，且會誤推進它的 watermark）；
  2. 失敗一定轉成非零 exit——不論單表或多表，否則 dbt task 會在半套資料上開跑；
  3. 多表模式仍是「盡力全試」——一張失敗不擋另一張推進，那是各表 watermark 獨立自癒的來源。

不觸網、不連 DB：get_bq_client 與 extract_and_load_one 都被 patch 掉。
"""
import pytest

import extract_ods_to_bq as ex


@pytest.fixture
def calls(monkeypatch):
    """記錄 extract_and_load_one 被以哪些表呼叫；順便擋掉 BQ client 建立。"""
    seen: list[str] = []
    monkeypatch.setattr(ex, "PROJECT", "fake-project")
    monkeypatch.setattr(ex, "get_bq_client", lambda: object())
    monkeypatch.setattr(ex, "extract_and_load_one", lambda client, spec: seen.append(spec.table))
    return seen


def _fail_on(monkeypatch, *failing_tables: str):
    """讓指定的表拋例外，其餘正常；回傳實際被嘗試的表清單。"""
    attempted: list[str] = []

    def _one(client, spec):
        attempted.append(spec.table)
        if spec.table in failing_tables:
            raise RuntimeError(f"boom:{spec.table}")

    monkeypatch.setattr(ex, "extract_and_load_one", _one)
    return attempted


# ─── 選表 ─────────────────────────────────────────────────────────────────────

class TestTableSelection:

    def test_default_runs_all_tables(self, calls):
        """不帶參數＝既有手動路徑，行為不變（向後相容）。"""
        ex.main([])
        assert calls == [s.table for s in ex.SPECS]

    def test_explicit_all_runs_all_tables(self, calls):
        ex.main(["--table", "all"])
        assert calls == [s.table for s in ex.SPECS]

    @pytest.mark.parametrize("table", [s.table for s in ex.SPECS])
    def test_single_table_runs_only_that_table(self, calls, table):
        ex.main(["--table", table])
        assert calls == [table]

    def test_choices_are_derived_from_specs(self):
        """--table 的值域由 SPECS 推導：新增一張表就自動有 CLI，不需另外維護清單。"""
        args = ex._parse_args([])
        assert args.table == ex.ALL
        for spec in ex.SPECS:
            assert ex._parse_args(["--table", spec.table]).table == spec.table

    def test_unknown_table_is_rejected(self):
        with pytest.raises(SystemExit) as exc:
            ex._parse_args(["--table", "no_such_table"])
        assert exc.value.code == 2   # argparse 的用法錯誤


# ─── Gate ─────────────────────────────────────────────────────────────────────

class TestGate:

    def test_single_table_failure_raises(self, calls, monkeypatch):
        """單表失敗必須非零 exit——Airflow 靠它把該 task 標紅、擋住 dbt。"""
        _fail_on(monkeypatch, "orders")
        with pytest.raises(RuntimeError, match="E/L gate"):
            ex.main(["--table", "orders"])

    def test_partial_failure_still_attempts_every_table(self, calls, monkeypatch):
        """多表模式：第一張失敗【不得】中斷第二張。

        各表 watermark 獨立、失敗不推進，是 CLOUD_LAYER-TW §3.2 自癒模型的前提；
        提早 return 會讓沒壞的那張也停在舊 watermark，把單表故障放大成整批延遲。
        """
        attempted = _fail_on(monkeypatch, ex.SPECS[0].table)
        with pytest.raises(RuntimeError, match=ex.SPECS[0].table):
            ex.main([])
        assert attempted == [s.table for s in ex.SPECS]

    def test_all_tables_failing_are_listed(self, calls, monkeypatch):
        _fail_on(monkeypatch, *[s.table for s in ex.SPECS])
        with pytest.raises(RuntimeError) as exc:
            ex.main([])
        for spec in ex.SPECS:
            assert spec.table in str(exc.value)

    def test_success_does_not_raise(self, calls):
        ex.main([])   # 不應拋出


# ─── 設定 ─────────────────────────────────────────────────────────────────────

class TestSettings:

    def test_missing_bq_project_fails_before_touching_bq(self, monkeypatch):
        """缺 BQ_PROJECT 要在建 client 之前就 fail-fast，訊息指出要設什麼。"""
        monkeypatch.setattr(ex, "PROJECT", "")
        monkeypatch.setattr(
            ex, "get_bq_client",
            lambda: pytest.fail("不該在缺 BQ_PROJECT 時嘗試建立 BQ client"),
        )
        with pytest.raises(RuntimeError, match="BQ_PROJECT"):
            ex.main([])
