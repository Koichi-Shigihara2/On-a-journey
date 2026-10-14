"""
tests/test_normalized_selection_window.py

[[NORMALIZED-FALLBACK-WINDOW-DEPENDENT-1]]（2026-10-14）: normalized（quarterly.py::build_raw_table()）の
候補タグの選び直しの判定が、保持期間（_QUARTERLY_YEARS）の長さに左右されないこと。

修正前は、主の候補の件数・最新の期末を保持期間で切った後に数えていたため、保持期間を延ばすと
昔だけ申告されていた主の候補が窓に入り、今も申告されている代わりの候補の値（今の窓の中の値）が消えた
（7年でAMZN・LITEのRPO、NOWの長期負債等45件）。

実行方法:
    venv/Scripts/python.exe -m pytest tests/test_normalized_selection_window.py -v
"""

from datetime import date, timedelta

from common.sec_data import quarterly as qmod


def _rel(days):
    return (date.today() - timedelta(days=days)).isoformat()



def _instant(days_ago, val):
    end = _rel(days_ago)
    return {"end": end, "val": val, "form": "10-Q", "fp": "Q2", "fy": int(end[:4]), "filed": end, "accn": "a-" + end}


def _company_facts():
    # RPO（件数の条件の対象外。修正前は「保持期間の中に主の候補の値が無いとき」だけ代わりの候補を選んだ）:
    # 主の候補RevenueRemainingPerformanceObligationは約5.5〜6.25年前の4四半期だけ、
    # 代わりの候補（RemainingPerformanceObligation）は直近の4四半期
    primary = [_instant(int(365 * 5.5) + 91 * i, 100 + i) for i in range(4)]
    fallback = [_instant(91 * i + 30, 200 + i) for i in range(4)]
    return {"facts": {"us-gaap": {
        "RevenueRemainingPerformanceObligation": {"units": {"USD": primary}},
        "RemainingPerformanceObligation": {"units": {"USD": fallback}},
    }}}


def _rpo(monkeypatch, years):
    monkeypatch.setattr(qmod, "_QUARTERLY_YEARS", years)
    monkeypatch.setattr(qmod, "_ANNUAL_YEARS", years + 1)
    return qmod.build_raw_table("TEST", _company_facts())["fields"]["RPO"]


def test_same_selection_for_5_and_7_year_windows(monkeypatch):
    e5, e7 = _rpo(monkeypatch, 5), _rpo(monkeypatch, 7)
    recent = lambda es: sorted((e["end"], e["val"]) for e in es if e["end"] >= _rel(400))  # noqa: E731
    assert len(recent(e5)) == 4
    assert recent(e5) == recent(e7)


def test_selection_period_is_fixed():
    assert qmod._SELECTION_QUARTERLY_YEARS == 5
    assert qmod._SELECTION_ANNUAL_YEARS == 6
