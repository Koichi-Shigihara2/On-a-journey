"""
tests/test_capex_no_cf_line_note.py

CF計算書にCapExの行が無い期（cf_linesの印 CAPEX_NO_CF_LINE_MARK。APP型）は、CapEx 0 と区別して表示する（表示だけ）:
  - TTM系列: capital_expenditure に no_cf_line_quarters（印の付いた四半期の数）
  - compose_fcf_bottom_up: 注記 capex_note（印が無い銘柄では to_dict にキーを出さない）。FCF・理論株価は変えない
"""
import os
import sys

_CALC_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation", "calculator"))
if _CALC_DIR not in sys.path:
    sys.path.insert(0, _CALC_DIR)

from adjustments import CAPEX_NO_LINE_NOTE, compose_fcf_bottom_up  # type: ignore[import]  # noqa: E402

from common.sec_data.fcf import CAPEX_NO_CF_LINE_MARK
from common.sec_data.ttm_calculator import calc_ttm_series


def _q(end, val, mark=None):
    from datetime import date, timedelta
    start = (date.fromisoformat(end) - timedelta(days=89)).isoformat()
    e = {"end": end, "start": start, "val": val, "is_annual": False, "is_ytd": False, "period_days": 90}
    if mark:
        e["mark"] = mark
    return e


def _store(marks):
    ends = ["2025-09-30", "2025-12-31", "2026-03-31", "2026-06-30"]
    return {"fields": {
        "capital_expenditure": {"category": "flow", "entries": [_q(e, 0 if m else 5, CAPEX_NO_CF_LINE_MARK if m else None)
                                                                for e, m in zip(ends, marks)]},
        "operating_cash_flow": {"category": "flow", "entries": [_q(e, 100) for e in ends]},
    }}


def test_ttm_counts_quarters_without_capex_line():
    s = calc_ttm_series("TEST", _store([False, True, True, True]), n_periods=1)
    assert s[0]["flow"]["capital_expenditure"]["no_cf_line_quarters"] == 3
    assert s[0]["flow"]["capital_expenditure"]["val"] == 5          # 値は変えない


def test_ttm_has_no_key_when_every_quarter_has_a_line():
    s = calc_ttm_series("TEST", _store([False] * 4), n_periods=1)
    assert "no_cf_line_quarters" not in s[0]["flow"]["capital_expenditure"]


def _compose(components):
    return compose_fcf_bottom_up("TEST", 1000.0, [0.0], [10.0], [1000.0], [5.0], latest_components=components, period_label="TTM")


def test_note_only_when_marked():
    comp = {"ocf": 1000.0, "capex": 0, "sbc": 10.0, "da": 5.0, "label": "TTM 2026-06-30"}
    d = _compose(dict(comp, capex_no_cf_line=True)).to_dict()
    assert d["capex_note"] == CAPEX_NO_LINE_NOTE and d["capex"] == 0 and d["estimated_fcf"] == 1000.0
    assert "capex_note" not in _compose(comp).to_dict()              # 印の無い銘柄は出力が変わらない
