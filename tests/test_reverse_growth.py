"""市場が織り込んだ成長率の逆算（[[TANUKI-UPSIDE-MAGNITUDE-INFLATED-1]] 6-2、common/reverse_growth.py）。表示だけ"""
import dataclasses
import os
import sys

from common import reverse_growth as rg

_TV_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation"))
if _TV_DIR not in sys.path:
    sys.path.insert(0, _TV_DIR)


def test_solve_finds_growth_matching_price():
    iv = lambda g: 100.0 * (1.0 + g) ** 5            # 成長率について単調増加
    r = rg.solve(iv, 161.051)                        # 100×1.1^5
    assert r["status"] == rg.OK and abs(r["reverse_g"] - 0.10) < 1e-6


def test_solve_unsolved_reasons():
    iv = lambda g: 100.0 * (1.0 + g)
    assert rg.solve(iv, None)["status"] == rg.UNSOLVED
    assert "低い" in rg.solve(iv, 1.0)["reason"]      # 成長率 −30% の理論株価 70 より低い
    assert "高い" in rg.solve(iv, 1000.0)["reason"]
    assert "効かない" in rg.solve(lambda g: 50.0, 50.0)["reason"]
    assert rg.solve(lambda g: None, 50.0)["status"] == rg.UNSOLVED


def test_context_gaps_and_blank_with_reason():
    c = rg.context(65.4, 0.50, 0.714, {"status": rg.OK, "reverse_g": 0.0517, "reason": None})
    assert c["gap_tanuki_pt"] == 44.8 and c["gap_analyst_pt"] == 66.2
    assert "逆算した成長率 5.2%" in rg.report_line(c) and "+65%" in rg.report_line(c)
    blank = rg.context(None, 0.1, None, {"status": rg.UNSOLVED, "reverse_g": None, "reason": "株価が取れていない"})
    assert blank["gap_tanuki_pt"] is None
    line = rg.report_line(blank)
    assert "空欄（株価が取れていない）" in line and "β込みWACCの上昇余地は空欄" in line


def test_reverse_growth_in_calculate_pt_matches_full_recompute(monkeypatch):
    """calculate_pt の reverse_growth（感度分析の式）が、成長率だけ替えてメインの計算を回し直した値（tools/sim の hook）と同じ"""
    import core_calculator as cc
    from test_core_calculator_v0_note import _minimal_financials  # type: ignore[import]
    fin = _minimal_financials()
    base = cc.KoichiValuationCalculator().calculate_pt(fin)
    r = base["reverse_growth"]
    price = fin["current_price"]
    assert r["status"] == rg.OK, r
    orig = cc.determine_growth_rate
    monkeypatch.setattr(cc, "determine_growth_rate",
                        lambda *a, **k: dataclasses.replace(orig(*a, **k), rate=r["reverse_g"]))
    again = cc.KoichiValuationCalculator().calculate_pt(_minimal_financials())
    assert abs(again["intrinsic_value_per_share"] / price - 1.0) < 1e-3
