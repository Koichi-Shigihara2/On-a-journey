"""成長率とアナリストの予想の差（[[TANUKI-GUIDANCE-REVISION-BLIND-1]] 案1、common/guidance_gap.py）。表示だけ"""
from common import guidance_gap as gg

EST = {"revenue_growth": {"0y": 0.080, "+1y": 0.082}, "revenue_analysts": {"0y": 26, "+1y": 28},
       "eps_revisions": {"0y": {"up_30d": 3, "down_30d": 2}, "+1y": {"up_30d": 2, "down_30d": 3, "up_7d": 1, "down_7d": 1}}}


def test_gap_uses_next_fiscal_year_and_revisions_30d():
    r = gg.evaluate(0.1277, EST)
    assert r["status"] == gg.OK
    assert r["growth_gap_pt"] == 4.6          # (12.77% − 8.2%)×100
    assert r["analyst_revenue_growth"] == 0.082 and r["analyst_count"] == 28
    assert (r["revisions_up_30d"], r["revisions_down_30d"]) == (2, 3)


def test_negative_gap_when_tanuki_below_analysts():
    assert gg.evaluate(0.05, EST)["growth_gap_pt"] == -3.2


def test_no_estimate_is_blank_with_reason_in_report():
    r = gg.evaluate(0.10, None)
    assert r["status"] == gg.NO_ESTIMATE and r["growth_gap_pt"] is None and r["reason"]
    line = gg.report_line(r)
    assert line.startswith("Guidance_Gap: 空欄（") and "取れていない" in line


def test_missing_next_year_growth_is_blank():
    r = gg.evaluate(0.10, {"revenue_growth": {"0y": 0.05}})
    assert r["status"] == gg.NO_ESTIMATE and r["growth_gap_pt"] is None


def test_no_tanuki_growth_is_blank():
    r = gg.evaluate(None, EST)
    assert r["status"] == gg.NO_ESTIMATE and r["growth_gap_pt"] is None


def test_report_line_has_values():
    line = gg.report_line(gg.evaluate(0.1277, EST))
    assert "+4.6pt" in line and "引き上げ2・引き下げ3" in line and "28人" in line
