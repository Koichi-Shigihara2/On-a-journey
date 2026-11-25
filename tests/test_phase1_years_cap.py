"""phase1（高成長期）の年数の上限（[[TANUKI-UPSIDE-MAGNITUDE-INFLATED-1]]、method_change ID -1-1）"""
import os
import sys

_TV_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation"))
if _TV_DIR not in sys.path:
    sys.path.insert(0, _TV_DIR)

from calculator.adjustments import PHASE1_YEARS_MAX, calculate_moat_score  # type: ignore[import]


def test_cap_is_7_years():
    assert PHASE1_YEARS_MAX == 7


def test_strong_moat_is_capped_and_keeps_uncapped_value():
    r = calculate_moat_score(gross_margin_3yr_avg=1.0, roic=0.50, fcf_margin_3yr_avg=0.50)   # Moat Score 1.0 → 3+7=10年
    assert r.phase1_years_uncapped == 10
    assert r.phase1_years == 7


def test_below_cap_unchanged():
    r = calculate_moat_score(gross_margin_3yr_avg=0.3, roic=0.12, fcf_margin_3yr_avg=0.05)
    assert r.phase1_years == r.phase1_years_uncapped and r.phase1_years < 7
    # 上限ちょうどの年数（moat_score 0.57 → 3+4=7年）は変わらない
    r7 = calculate_moat_score(gross_margin_3yr_avg=0.6, roic=0.17, fcf_margin_3yr_avg=0.15)
    assert r7.phase1_years == min(r7.phase1_years_uncapped, 7)
