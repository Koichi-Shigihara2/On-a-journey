"""Market Pulse: 設計上の暫定値（夜の実行の時点で構造上必ず暫定）と想定外の暫定値の区別（清算値の実測〈2026-09-30〉を受けた修正）

- 先物（清算前）・為替とドル指数（日の区切り前）の暫定値はdata_qualityをpartialにせず、種類（清算前・日中）を記録する
- それ以外の暫定値（本来は確定しているはずの値）は今までどおりpartialにする
- 区別は銘柄ごとの日足の区切り（fetcher.bar_final_at）から判定し、銘柄の一覧を持たない
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "src", "market", "market_pulse"))

import collect_and_send as cs  # noqa: E402
import sector_rotation as sr  # noqa: E402
import stage_conclusions as sc  # noqa: E402
from common.market_data import fetcher  # noqa: E402


class TestExpectedProvisionalKind:
    def test_futures_are_before_settlement(self):
        assert fetcher.expected_provisional_kind("CL=F", "2026-09-29") == "清算前"
        assert fetcher.expected_provisional_kind("GC=F", "2026-09-29") == "清算前"

    def test_fx_and_dollar_index_are_intraday(self):
        assert fetcher.expected_provisional_kind("JPY=X", "2026-09-29") == "日中"
        assert fetcher.expected_provisional_kind("DX-Y.NYB", "2026-09-29") == "日中"

    def test_bars_final_by_the_nightly_run_are_not_expected(self):
        # 米国株・ETF・指数はNYSEの引け、^VIXは引け＋15分、^TNXは引け−1時間、^N225は東京の引けで確定する
        # （夜の取得〈引け＋20分以降〉の時点で確定しているはず）
        for sym in ("SPY", "^GSPC", "GLD", "^VIX", "^TNX", "^N225", "XLK"):
            assert fetcher.expected_provisional_kind(sym, "2026-09-29") is None, sym

    def test_us_holiday_row(self):
        # 09-07（Labor Day、NYSE休場）のドル円の足: その日の16:00（NY）を引けとみなす
        assert fetcher.expected_provisional_kind("JPY=X", "2026-09-07") == "日中"
        assert fetcher.expected_provisional_kind("SPY", "2026-09-07") is None

    def test_derived_from_day_boundary_not_a_list(self, monkeypatch):
        # 先物の一覧（FUTURES_SYMBOLS）に加えた銘柄は、日足の区切りの定義から自動的に「清算前」になる
        assert fetcher.expected_provisional_kind("ZZ=F", "2026-09-29") is None
        monkeypatch.setattr(fetcher, "FUTURES_SYMBOLS", set(fetcher.FUTURES_SYMBOLS) | {"ZZ=F"})
        assert fetcher.expected_provisional_kind("ZZ=F", "2026-09-29") == "清算前"


class TestCollectorMarksKind:
    def test_futures_row_gets_kind(self):
        it = cs._mark_provisional_item({"value": 1.0}, {"date": "2026-09-29", "_provisional": True}, "CL=F")
        assert it["provisional"] is True and it["provisional_kind"] == "清算前"

    def test_fx_row_gets_kind(self):
        it = cs._mark_provisional_item({"value": 1.0}, {"date": "2026-09-29", "_provisional": True}, "JPY=X")
        assert it["provisional"] is True and it["provisional_kind"] == "日中"

    def test_unexpected_row_has_no_kind(self):
        it = cs._mark_provisional_item({"value": 1.0}, {"date": "2026-09-29", "_provisional": True}, "^N225")
        assert it["provisional"] is True and "provisional_kind" not in it

    def test_final_row_untouched(self):
        it = cs._mark_provisional_item({"value": 1.0}, {"date": "2026-09-29"}, "CL=F")
        assert "provisional" not in it and "provisional_kind" not in it


def _ind(date="2026-09-29"):
    return {k: {"change_percent": 0.1, "date": date} for k in sc.DQ_INDICATORS}


def _af(date="2026-09-29"):
    return {k: {"change_pct": 0.1, "date": date} for k in sc.DQ_ASSET_FLOW}


class TestDataQuality:
    def test_expected_provisional_stays_complete(self):
        ind = _ind()
        ind["WTI原油"] = {"change_percent": -1.0, "date": "2026-09-29", "provisional": True, "provisional_kind": "清算前"}
        ind["ドル円"] = {"change_percent": 0.1, "date": "2026-09-29", "provisional": True, "provisional_kind": "日中"}
        dq = sc.data_quality(ind, _af(), {"date": "2026-09-29"}, "2026-09-29")
        assert dq["status"] == "complete"
        assert dq["provisional_elements"] == [] and dq["old_elements"] == []
        assert dq["expected_provisional_elements"] == {"WTI原油": "清算前", "ドル円": "日中"}
        assert sc.stage0(dq)["line"] == "2026-09-29の終値"

    def test_unexpected_provisional_still_partial(self):
        ind = _ind()
        ind["S&P500グロース(IVW)"] = {"change_percent": 0.5, "date": "2026-09-29", "provisional": True}
        ind["WTI原油"] = {"change_percent": -1.0, "date": "2026-09-29", "provisional": True, "provisional_kind": "清算前"}
        dq = sc.data_quality(ind, _af(), {"date": "2026-09-29"}, "2026-09-29")
        assert dq["status"] == "partial"
        assert dq["provisional_elements"] == ["S&P500グロース(IVW)"] and dq["old_elements"] == ["S&P500グロース(IVW)"]
        assert dq["expected_provisional_elements"] == {"WTI原油": "清算前"}


class TestBreakdownKind:
    def test_breakdown_rows_carry_kind(self):
        def series(sym, as_of, days):
            last = {"date": "2026-09-29", "close": 101.0}
            if sym in ("CL=F", "JPY=X", "DX-Y.NYB"):
                last["_provisional"] = True
            return [{"date": "2026-09-28", "close": 100.0}, last]
        rows = {r["symbol"]: r for r in sr.breakdown(series, "2026-09-29")}
        assert rows["CL=F"]["provisional"] is True and rows["CL=F"]["provisional_kind"] == "清算前"
        assert rows["JPY=X"]["provisional_kind"] == "日中" and rows["DX-Y.NYB"]["provisional_kind"] == "日中"
        assert "provisional" not in rows["GC=F"]
        assert rows["CL=F"]["change_pct"] == 1.0
