"""
tests/test_conditional_tags_and_stale_latest.py

1. 条件付きの候補タグ（common/sec_data/conditional_tags.py、[[LAYER3-LATEST-FILING-TAG-DROP-1]]）:
   設備投資のSegmentExpenditureAdditionToLongLivedAssetsは、既存の候補と過去の期間で値が一致する
   銘柄だけ候補の最後に使い、既存の候補に値がある期は上書きしない（Layer3とparser.pyで同じ判定）
2. 直近の値の鮮度（[[LAYER3-STALE-LATEST-VALUE-1]]）: 申告が止まった項目の古い値を、STONKS SILO・TAIL・
   TANUKI（長期借入金の予備）・rotceのどれも「直近の値」として使わない

実行方法:
    venv/Scripts/python.exe -m pytest tests/test_conditional_tags_and_stale_latest.py -v
"""

import contextlib
import io
import os
import sys

import pytest

from common.sec_data import conditional_tags as ct
from common.sec_data import layer3_builder as lb

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_STONKS_SRC = os.path.join(_REPO, "discover", "stonks-silo", "src")
OLD = "PaymentsToAcquireProductiveAssets"
NEW = ct.SEGMENT_TOTAL_CAPEX_TAG


def _f(start, end, val, form="10-Q"):
    return {"start": start, "end": end, "val": val, "form": form, "fp": "Q2", "fy": int(end[:4]),
            "filed": end[:8] + "28", "accn": f"acc-{start}-{end}"}


def _cf(new_matches=True):
    old = [_f("2025-01-01", "2025-03-31", 1172), _f("2025-04-01", "2025-06-30", 1261), _f("2025-01-01", "2025-06-30", 2433),
           _f("2025-07-01", "2025-09-30", 1056), _f("2026-01-01", "2026-03-31", 973)]
    k = 1.0 if new_matches else 1.3
    new = [_f("2025-04-01", "2025-06-30", 1261 * k), _f("2025-01-01", "2025-06-30", 2433 * k),
           _f("2026-04-01", "2026-06-30", 1104), _f("2026-01-01", "2026-06-30", 2077)]
    rev = [_f(s, e, 10000) for s, e in (("2025-01-01", "2025-03-31"), ("2025-04-01", "2025-06-30"),
                                         ("2025-07-01", "2025-09-30"), ("2026-01-01", "2026-03-31"), ("2026-04-01", "2026-06-30"))]
    return {"facts": {"us-gaap": {OLD: {"units": {"USD": old}}, NEW: {"units": {"USD": new}},
                                  "Revenues": {"units": {"USD": rev}}}}}


def _build(monkeypatch, cf):
    monkeypatch.setattr(lb, "load_company_facts", lambda t: cf)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        return lb.build_ticker_store("TEST")


def _capex(st):
    return {(e.get("start"), e["end"]): e for e in lb.get_field_entries(st, "capital_expenditure")
            if not e.get("is_annual") and not e.get("is_ytd")}


class TestConditionalCandidate:
    def test_adopted_when_two_periods_match_and_no_mismatch(self):
        assert ct.conditional_candidate("capital_expenditure", _cf(), [OLD]) == (NEW, "segment_total")

    def test_not_adopted_when_values_differ(self):
        assert ct.conditional_candidate("capital_expenditure", _cf(new_matches=False), [OLD]) is None

    def test_other_fields_have_no_conditional_tag(self):
        assert ct.conditional_candidate("revenue", _cf(), ["Revenues"]) is None

    def test_layer3_fills_only_missing_period(self, monkeypatch):
        q = _capex(_build(monkeypatch, _cf()))
        e = q[("2026-04-01", "2026-06-30")]
        assert abs(e["val"]) == 1104 and e["source_tag"] == NEW and e["fallback_reason"] == "segment_total"
        # Q2単独1,104M + Q1 973M = 上期累計2,077M（新タグの累計）
        assert abs(q[("2026-01-01", "2026-03-31")]["val"]) + abs(e["val"]) == 2077
        # 既存の候補に値がある期は既存の候補のまま
        assert q[("2026-01-01", "2026-03-31")]["source_tag"] == OLD
        assert q[("2025-04-01", "2025-06-30")]["source_tag"] == OLD
        assert "fallback_reason" not in q[("2025-04-01", "2025-06-30")]

    def test_layer3_unchanged_when_condition_fails(self, monkeypatch):
        q = _capex(_build(monkeypatch, _cf(new_matches=False)))
        assert ("2026-04-01", "2026-06-30") not in q
        assert all(e["source_tag"] == OLD for e in q.values())


def _real_store(t):
    if not lb.load_company_facts(t):
        pytest.skip(f"{t}のcompany_facts.jsonが無い")
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        return lb.build_ticker_store(t)


class TestFcxRealData:
    def test_layer3_q2_2026(self):
        q = _capex(_real_store("FCX"))
        e = q.get(("2026-04-01", "2026-06-30"))
        assert e is not None and abs(e["val"]) == 1_104_000_000 and e["fallback_reason"] == "segment_total"
        assert abs(q[("2026-01-01", "2026-03-31")]["val"]) + abs(e["val"]) == 2_077_000_000

    def test_parser_q2_2026(self):
        from common.sec_data.parser import SECParser
        if not lb.load_company_facts("FCX"):
            pytest.skip("FCXのcompany_facts.jsonが無い")
        p = SECParser(data_dir=os.path.join(_REPO, "common", "sec_data", "data"))
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            r = p.parse_company_facts("FCX")
        q2 = r["quarterly"]["2026Q2"]
        assert abs(q2["cf"]["capital_expenditure"]) == 1_104_000_000
        assert q2["cf_provenance"]["capital_expenditure"]["fallback_reason"] == "segment_total"
        assert abs(r["quarterly"]["2026Q1"]["cf"]["capital_expenditure"]) == 973_000_000
        assert "capital_expenditure" not in (r["quarterly"]["2026Q1"].get("cf_provenance") or {})

    # HONは事業の分離後に過去の設備投資を修正再表示しており、Layer3が持つ最後の申告の値とは
    # 一致する（13期間一致・不一致0）ため採用される。最初の申告の値とは一致しない
    @pytest.mark.parametrize("ticker", ["AMZN", "CAT", "XOM", "TER"])
    def test_not_adopted_for_mismatching_tickers(self, ticker):
        cf = lb.load_company_facts(ticker)
        if not cf:
            pytest.skip(f"{ticker}のcompany_facts.jsonが無い")
        cands = lb.load_concept_definitions()["fields"]["capital_expenditure"]["candidates"]
        assert ct.conditional_candidate("capital_expenditure", cf, cands) is None


def _stonks_entries(store, field):
    if _STONKS_SRC not in sys.path:
        sys.path.insert(0, _STONKS_SRC)
    import financial_trend_calculator as ftc
    return ftc._get_quarterly_entries(store, field)


class TestStaleLatestSynthetic:
    def _store(self):
        def q(end, val):
            return {"end": end, "start": end[:5] + "01-01", "val": val, "is_annual": False, "is_ytd": False}
        return {"fields": {
            "revenue": {"entries": [q("2026-03-31", 10), q("2026-06-30", 11)]},
            "operating_income": {"entries": [q("2020-12-31", -1), q("2021-03-31", -2)]},
            "long_term_debt": {"entries": [{"end": "2021-03-31", "val": 500}]},
        }}

    def test_all_consumers_treat_stopped_field_as_missing(self):
        st = self._store()
        assert lb.latest_period_end(st) == "2026-06-30"
        assert lb.get_latest_quarterly(st, "operating_income") is None          # TAIL・TANUKI
        assert lb.get_long_term_debt_latest(st) == 0.0                          # TANUKIの長期借入金の予備
        assert _stonks_entries(st, "OperatingIncome") == []                     # STONKS SILOのベクトル
        assert lb.get_latest_quarterly(st, "revenue")["end"] == "2026-06-30"
        assert lb.CURRENT_VALUE_MAX_LAG_DAYS == 135

    def test_threshold_matches_stonks_freshness(self):
        if _STONKS_SRC not in sys.path:
            sys.path.insert(0, _STONKS_SRC)
        import financial_as_of
        assert lb.CURRENT_VALUE_MAX_LAG_DAYS == financial_as_of.STALE_DAYS_AFTER_QUARTER_END


# 全数調査（2026-10-14）で見つかった、申告が止まった項目
_STOPPED = [("CEG", "stock_based_compensation", "SBC"), ("PM", "stock_based_compensation", "SBC"),
            ("ASTS", "operating_income", "OperatingIncome"), ("INTU", "gross_profit", "GrossProfit"),
            ("CAKE", "gross_profit", "GrossProfit"), ("BROS", "selling_and_marketing", None)]


class TestStaleLatestRealData:
    @pytest.mark.parametrize("ticker,field,stonks_field", _STOPPED)
    def test_stopped_field_is_not_latest_anywhere(self, ticker, field, stonks_field):
        st = _real_store(ticker)
        series = lb.get_quarterly_series(st, field)
        assert series, "全数調査では過去の値がある項目"
        assert not lb.is_current_end(st, series[-1]["end"])
        assert lb.get_latest_quarterly(st, field) is None
        if stonks_field:
            assert _stonks_entries(st, stonks_field) == []
        # rotce.pyは期末日が一致する値だけを引くため、最新の期末にこの項目の値は無い
        from common.sec_data import rotce
        assert lb.latest_period_end(st) not in rotce._quarterly_flow(st, field)
