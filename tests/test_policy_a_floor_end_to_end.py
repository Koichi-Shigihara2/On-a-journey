"""
tests/test_policy_a_floor_end_to_end.py

[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段1b（2026-11-02）の通しの経路:
core_calculator.calculate_pt()（売上×8%の下限・基準の決定・R&Dの資本化）→ pipeline._compute_tanuki_score()（Policy A）。

- 構造的赤字（5年・2年平均とも≤0）で下限を基準にした銘柄は、基準を決めた後の調整（R&Dの資本化）で値が変わってもPolicy A
  （c64ae4ff16の前は、調整後の base_fcf と比べていてCRWV・SOFI・SOUN等でPolicy Aが外れていた）
- 5年平均だけが負で2年平均が正の銘柄（BROS・S型）は、下限を使わずPolicy Aにならない

実行方法:
    venv/Scripts/python.exe -m pytest tests/test_policy_a_floor_end_to_end.py -v
"""

import os
import sys

_TV_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation"))
for p in (_TV_DIR, os.path.dirname(__file__)):
    if p not in sys.path:
        sys.path.insert(0, p)

import core_calculator  # noqa: E402
from calculator.adjustments import RDCapitalizationResult  # noqa: E402
from test_pipeline_logic import _make_pipe, _write_stonks_json  # noqa: E402


def _financials(fcf_5yr, fcf_2yr, fcf_list):
    return {
        "fcf_5yr_avg": fcf_5yr,
        "fcf_2yr_avg": fcf_2yr,
        "fcf_list_raw": fcf_list,
        "latest_revenue": 5_000_000_000,
        "diluted_shares": 100_000_000,
        "current_price": 50.0,
        "beta": 1.0,
        "sector": "Technology",
        "industry": "Software",
        "net_cash_data": {"fiscal_year": 2025},
        "revenue_ttm": 5_000_000_000,
        "ni_ttm": -100_000_000,
    }


def _rd(adjustment):
    return RDCapitalizationResult(applied=True, rd_current=1e9, rd_avg_3yr=9e8, capitalized_rd=adjustment,
                                  amortization_current=0.0, rd_adjustment=adjustment, rd_revenue_ratio=0.2,
                                  threshold=0.05, years_used=3, note="test")


def _run(monkeypatch, tmp_path, financials, ticker):
    monkeypatch.setattr(core_calculator, "capitalize_rd", lambda ticker, sec_data_dir: _rd(300_000_000))
    calc = core_calculator.KoichiValuationCalculator()
    calc.sec_data_dir = str(tmp_path)          # R&Dの資本化の経路を通す（capitalize_rdは差し替え）
    v = calc.calculate_pt(financials)
    assert "error" not in v, v.get("error")
    pipe = _make_pipe(tmp_path)
    _write_stonks_json(tmp_path, {})
    return v, pipe._compute_tanuki_score(ticker, v)


def test_structural_deficit_with_floor_in_base_keeps_policy_a_after_rd(monkeypatch, tmp_path):
    v, score = _run(monkeypatch, tmp_path, _financials(-500e6, -600e6, [-700e6, -500e6, -400e6, -500e6, -400e6]), "CRWVLIKE")
    comps = v["components"]
    assert comps["structural_deficit"] is True
    assert comps["fcf_floor_applied"] > 0
    assert v["fcf_base"]["base_fcf"] == 400e6          # 基準＝下限（売上5B×8%）
    assert comps["fcf_base_used"] == 700e6             # R&Dの資本化（+300M）で、基準を決めた後の値が動いた
    assert comps["fcf_floor_used_in_base"] is True
    assert score["rounded_by_policy"] == "A" and score["score"] == "WATCH"


def test_positive_two_year_average_does_not_use_floor(monkeypatch, tmp_path):
    v, score = _run(monkeypatch, tmp_path, _financials(-26e6, 84e6, [90e6, 78e6, -100e6, -120e6, -78e6]), "BROSLIKE")
    comps = v["components"]
    assert comps["structural_deficit"] is False
    assert comps["fcf_floor_applied"] == 0
    assert comps["fcf_floor_used_in_base"] is False
    assert score["rounded_by_policy"] != "A"
