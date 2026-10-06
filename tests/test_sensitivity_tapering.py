"""
tests/test_sensitivity_tapering.py

逓減型（dcf_type="tapering"）の銘柄で、感応度表の中央セル（Ke=Rm 10%・
基準年数）がメイン理論株価（intrinsic_value_per_share）と一致することの
回帰テスト（2026-10-06）。

修正前は core_calculator.py が create_sensitivity_calc_func() に
tapering_g_end を渡さず（関数側にも引数が無く）、逓減型7銘柄の中央セルが
2段階DCFの値になっていた（ALAB: 中央セル$373.09 vs メインIV$156.77）。

実行方法:
    python -m pytest tests/test_sensitivity_tapering.py -v
"""

import json
import os
import sys

import pytest

_REPO = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_TV_DIR = os.path.join(_REPO, "src", "value", "tanuki_valuation")
if _TV_DIR not in sys.path:
    sys.path.insert(0, _TV_DIR)

import importlib.util

from calculator.sensitivity import create_sensitivity_calc_func  # type: ignore[import]


def _load_real_core_calculator():
    """tests/test_pipeline_logic.py等が収集時にsys.modules["core_calculator"]を
    MagicMockへ差し替えるため、ファイルから別名で読み込む（sys.modulesは触らない）。"""
    spec = importlib.util.spec_from_file_location(
        "_core_calculator_for_sensitivity_test", os.path.join(_TV_DIR, "core_calculator.py")
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


KoichiValuationCalculator = _load_real_core_calculator().KoichiValuationCalculator

_DATA_DIR = os.path.join(_REPO, "docs", "value-monitor", "tanuki_valuation", "data")
TAPERING_TICKERS = ["ALAB", "KULR", "SITM", "IONQ", "S", "RDW", "ASTS"]


def _financials() -> dict:
    """FCFが伸びている（高成長率 > tapering_g_end になる）最小限のfinancials。"""
    return {
        "fcf_5yr_avg": 1_000_000_000,
        "fcf_2yr_avg": 1_000_000_000,
        "fcf_list_raw": [300e6, 450e6, 650e6, 800e6, 1000e6],
        "diluted_shares": 100_000_000,
        "current_price": 50.0,
        "beta": 1.0,
        "sector": "Technology",
        "industry": "Software",
        "net_debt": 0,
        "net_cash_data": {"fiscal_year": 2025},
        "revenue_ttm": 5_000_000_000,
        "ni_ttm": 500_000_000,
    }


def _center(result: dict) -> float:
    sens = result["sensitivity"]
    assert sens["base_wacc"] == pytest.approx(0.10)
    return sens["matrix"][1][1]


class TestCalculatePtSensitivityCenter:
    def test_tapering_center_matches_main_iv(self):
        result = KoichiValuationCalculator().calculate_pt(_financials(), tapering_g_end=0.05)
        assert result["dcf_type"] == "tapering"
        assert _center(result) == pytest.approx(round(result["intrinsic_value_per_share"], 2), abs=0.005)

    def test_two_stage_center_unchanged(self):
        result = KoichiValuationCalculator().calculate_pt(_financials())
        assert result["dcf_type"] == "two_stage"
        assert _center(result) == pytest.approx(round(result["intrinsic_value_per_share"], 2), abs=0.005)


def _inputs_from_latest(d: dict) -> dict:
    """latest.jsonの出力値から、逓減DCFの入力を復元する。
    base_fcf = 1年目FCF/(1+g1)、g_end = 最終年の成長率（FCFの比）、
    terminal_growth = terminal_fcf/最終年FCF - 1。"""
    comps = d["components"]
    detail = d["dcf_components"]["high_growth_detail"]
    g1 = comps["high_growth_rate_used"]
    fcf = [row["fcf"] for row in detail]
    return {
        "base_fcf": fcf[0] / (1 + g1),
        "high_growth_rate": g1,
        "tapering_g_end": fcf[-1] / fcf[-2] - 1,
        "terminal_growth": d["dcf_components"]["terminal_fcf"] / fcf[-1] - 1,
        "years": len(detail),
        "diluted_shares": comps["diluted_shares"],
        "rpo_pv": (comps.get("rpo_pv") or 0) + ((d.get("growth_options") or {}).get("total_pv") or 0),
        "net_cash_per_share": (d.get("bs_adjustment") or {}).get("net_cash_per_share") or 0.0,
    }


@pytest.mark.parametrize("ticker", TAPERING_TICKERS)
def test_tapering_ticker_center_matches_main_iv(ticker):
    """逓減型7銘柄の実データで、Rm 10%・基準年数の計算値がメインIVに一致すること。"""
    path = os.path.join(_DATA_DIR, ticker, "latest.json")
    if not os.path.exists(path):
        pytest.skip(f"{ticker} latest.json なし")
    d = json.load(open(path, encoding="utf-8"))
    if d.get("dcf_type") != "tapering":
        pytest.skip(f"{ticker} は逓減型ではなくなった（{d.get('dcf_type')}）")
    x = _inputs_from_latest(d)
    assert x["years"] == d["sensitivity"]["base_years"]
    calc = create_sensitivity_calc_func(
        base_fcf=x["base_fcf"],
        high_growth_rate=x["high_growth_rate"],
        diluted_shares=x["diluted_shares"],
        rpo_pv=x["rpo_pv"],
        alpha=0.0,
        terminal_growth=x["terminal_growth"],
        net_cash_per_share=x["net_cash_per_share"],
        tapering_g_end=x["tapering_g_end"],
    )
    assert calc(0.10, x["years"]) == pytest.approx(d["intrinsic_value_per_share"], rel=1e-6)
