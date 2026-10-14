"""
tests/test_latest_period_guard.py

最新期欠けのガード（[[LAYER3-LATEST-FILING-TAG-DROP-1]]、2026-10-14）:
- TTMの最新の点で営業CFか設備投資が欠け、年次の値での代用も無い → DCF信頼性LOW（Policy B）→ WATCH、注記
- 欠けていて年次で代用している → 判定は変えず、注記だけ
- 欠けなし → 変化なし

実行方法:
    venv/Scripts/python.exe -m pytest tests/test_latest_period_guard.py -v
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from test_pipeline_logic import _df, _make_pipe, _write_stonks_json  # noqa: E402


def _full():
    return {"val": 1.0, "quarters_used": 4, "missing": 0}


def _reader(flow_latest):
    r = _df.TTMReader(ticker="TEST", repo_root_path=None)
    r._series = [
        {"ttm_end": "2026-06-30", "flow": flow_latest},
        {"ttm_end": "2025-06-30", "flow": {"operating_cash_flow": _full(), "capital_expenditure": _full(), "FCF": {"val": 1.0}}},
    ]
    return r


class TestLatestPointMissing:
    def test_capex_missing_in_latest_point(self):
        r = _reader({"operating_cash_flow": _full(), "capital_expenditure": {"val": 1.0, "quarters_used": 3}})
        assert r.latest_point_missing_fields() == ["capital_expenditure"]

    def test_no_missing(self):
        r = _reader({"operating_cash_flow": _full(), "capital_expenditure": _full()})
        assert r.latest_point_missing_fields() == []

    def test_build_result_ttm_vs_annual(self):
        assert _df.build_latest_period_missing([], "ttm_series") is None
        ttm = _df.build_latest_period_missing(["capital_expenditure"], "ttm_series")
        assert ttm["substituted_by_annual"] is False and ttm["note"] == "最新期の設備投資が未取得"
        ann = _df.build_latest_period_missing(["capital_expenditure"], "annual_fallback")
        assert ann["substituted_by_annual"] is True and "最新期は年次の値で代用" in ann["note"]


def _valuation(lpm=None):
    v = {
        "upside_percent": -5.0,
        "components": {"current_price": 100.0, "diluted_shares": None},
        "fcf_base": {"base_fcf": 500_000_000},
        "financial_health": {},
        "fcf_estimation": {"applied": False},
        "fcf_outlier": {"detected": False},
    }
    if lpm is not None:
        v["latest_period_missing"] = lpm
    return v


class TestGuardInScore:
    def test_missing_without_annual_substitute_rounds_to_watch(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        base = pipe._compute_tanuki_score("LPM_BASE", _valuation())
        assert base["score"] != "WATCH"
        lpm = _df.build_latest_period_missing(["capital_expenditure"], "ttm_series")
        assert pipe._calc_dcf_reliability_policy_b(_valuation(lpm)) == "LOW"
        r = pipe._compute_tanuki_score("LPM_GUARD", _valuation(lpm))
        assert r["score"] == "WATCH"
        assert r["rounded_by_policy"] == "B"
        assert "最新期の設備投資が未取得" in r["score_comment"]

    def test_missing_with_annual_substitute_keeps_judgment(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        base = pipe._compute_tanuki_score("LPM_BASE2", _valuation())
        lpm = _df.build_latest_period_missing(["capital_expenditure"], "annual_fallback")
        assert pipe._calc_dcf_reliability_policy_b(_valuation(lpm)) == "NORMAL"
        r = pipe._compute_tanuki_score("LPM_ANNUAL", _valuation(lpm))
        assert r["score"] == base["score"]
        assert r["rounded_by_policy"] == base["rounded_by_policy"]

    def test_no_missing_no_change(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        a = pipe._compute_tanuki_score("LPM_A", _valuation())
        b = pipe._compute_tanuki_score("LPM_B", _valuation(None))
        assert a["score"] == b["score"]
        assert pipe._calc_dcf_reliability_policy_b(_valuation()) == "NORMAL"


class TestCheck63:
    def test_lists_only_unsubstituted(self, tmp_path):
        import json
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "common", "sec_data"))
        from common.sec_data import report_consistency_check as rcc
        for t, lpm in (("AAA", _df.build_latest_period_missing(["capital_expenditure"], "ttm_series")),
                       ("BBB", _df.build_latest_period_missing(["capital_expenditure"], "annual_fallback")),
                       ("CCC", None)):
            d = tmp_path / t
            d.mkdir()
            (d / "latest.json").write_text(json.dumps({"latest_period_missing": lpm}), encoding="utf-8")
        out = rcc._check_latest_period_missing(str(tmp_path))
        assert [t for t, _ in out] == ["AAA"]
        assert "WARN-63" in out[0][1] and "設備投資" in out[0][1]
