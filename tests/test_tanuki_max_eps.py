"""
tests/test_tanuki_max_eps.py

[[TANUKI-MAXEPS-NI-SOURCE-1]]: 最大EPS（pipeline.compute_max_eps / _latest_ttm_entry）の回帰テスト。
純利益・株式報酬をTTM系列の最新エントリから取り、DuPont分解の成否（負の純資産・
売上$15M未満で除外）や年次のsbc_ttmに依存しないこと。
"""

import os
import sys

_PIPELINE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation")
)
sys.path.insert(0, _PIPELINE_DIR)
sys.path.insert(0, os.path.dirname(__file__))

from _tanuki_pipeline_stub import load_stubbed_pipeline  # noqa: E402

pipeline = load_stubbed_pipeline()


def _entry(ttm_end, ni=None, sbc=None):
    flow = {}
    if ni is not None:
        flow["net_income"] = {"val": ni, "quarters_used": 4, "missing": 0}
    if sbc is not None:
        flow["stock_based_compensation"] = {"val": sbc, "quarters_used": 4, "missing": 0}
    return {"ttm_end": ttm_end, "flow": flow}


def test_negative_equity_ticker_uses_ttm_net_income():
    """負の純資産でDuPontが除外される銘柄（DELL型）でも、TTM系列の純利益で値が出る。
    以前はdupont.ni_ttmが無く、株式報酬だけで max_eps=1.1371・515.4x になっていた。"""
    entry = _entry("2026-05-01", ni=8_409_000_000, sbc=723_000_000)
    r = pipeline.compute_max_eps(entry, diluted_shares=635_800_000, current_price=586.06)
    assert r["max_eps_reliability"] == "HIGH"
    assert r["max_eps"] == round(9_132_000_000 / 635_800_000, 4)
    assert r["max_eps_per"] == round(586.06 / r["max_eps"], 1)
    assert 40.0 < r["max_eps_per"] < 42.0
    assert r["max_eps_ttm_end"] == "2026-05-01"


def test_non_positive_sum_returns_none():
    """純利益＋株式報酬 ≤ 0（QBTS型）なら max_eps・max_eps_per とも None（信頼性はHIGHのまま）。"""
    entry = _entry("2026-06-30", ni=-249_000_000, sbc=23_000_000)
    r = pipeline.compute_max_eps(entry, diluted_shares=380_000_000, current_price=14.57)
    assert r["max_eps"] is None
    assert r["max_eps_per"] is None
    assert r["max_eps_reliability"] == "HIGH"
    # ちょうど0もNone
    r0 = pipeline.compute_max_eps(_entry("2026-06-30", ni=-5, sbc=5), 100, 10.0)
    assert r0["max_eps"] is None and r0["max_eps_per"] is None


def test_missing_sbc_is_med_and_treated_as_zero():
    """株式報酬が欠けたら0として計算し、信頼性MED。"""
    entry = _entry("2026-06-30", ni=1_000_000_000)
    r = pipeline.compute_max_eps(entry, diluted_shares=100_000_000, current_price=200.0)
    assert r["max_eps_reliability"] == "MED"
    assert r["max_eps"] == 10.0
    assert r["max_eps_per"] == 20.0


def test_missing_net_income_is_low_and_none_even_with_sbc():
    """純利益が無ければ、株式報酬があっても None・LOW（片側だけで計算しない）。"""
    entry = _entry("2026-06-30", sbc=500_000_000)
    r = pipeline.compute_max_eps(entry, diluted_shares=100_000_000, current_price=200.0)
    assert r["max_eps"] is None
    assert r["max_eps_per"] is None
    assert r["max_eps_reliability"] == "LOW"


def test_no_entry_or_no_price():
    r = pipeline.compute_max_eps(None, diluted_shares=100, current_price=10.0)
    assert r == {"max_eps": None, "max_eps_per": None, "max_eps_reliability": "LOW", "max_eps_ttm_end": None}
    # 株価が無ければ max_eps は出すが max_eps_per は None
    r2 = pipeline.compute_max_eps(_entry("2026-06-30", ni=100, sbc=0), diluted_shares=10, current_price=None)
    assert r2["max_eps"] == 10.0 and r2["max_eps_per"] is None


def test_latest_ttm_entry_picks_max_ttm_end_regardless_of_order():
    series = {"series": [_entry("2025-06-30", ni=1), _entry("2026-06-30", ni=2), _entry("2024-06-30", ni=3)]}
    assert pipeline._latest_ttm_entry(series)["ttm_end"] == "2026-06-30"
    assert pipeline._latest_ttm_entry({"series": []}) is None
    assert pipeline._latest_ttm_entry(None) is None
