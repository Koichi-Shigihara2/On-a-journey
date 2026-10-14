"""
tests/test_layer3_no_merge_window_independent.py

[[LAYER3-TAG-SELECTION-WINDOW-DEPENDENT-1]]: 候補を期間ごとに混ぜない項目
（NO_CANDIDATE_MERGE_FIELDS、2026-10-14時点でrpo・株式数2項目）の採用タグが、
保持期間（quarterly.pyの_QUARTERLY_YEARS）の長さに左右されないことを検証する。

修正前は「保持期間の中に値がある最初の候補」を採っていたため、期間を延ばすと
昔使われていた優先候補が窓に入り、直近の値が古い期の値に置き換わった
（AMZNのrpo: 5年では2025-12-31まで、7年では2020-06-30が最新）。

実行方法:
    venv/Scripts/python.exe -m pytest tests/test_layer3_no_merge_window_independent.py -v
"""

from datetime import date, timedelta

from common.sec_data import layer3_builder as lb
from common.sec_data import quarterly as qmod


def _fact(end, val, fy):
    return {"end": end, "val": val, "form": "10-Q", "fp": "Q2", "fy": fy,
            "filed": end, "accn": f"0000000000-{end}"}


def _company_facts():
    today = date.today()
    old = [(today - timedelta(days=d)).isoformat() for d in (2600, 2500, 2400)]   # 約6.5〜7年前
    new = [(today - timedelta(days=d)).isoformat() for d in (400, 300, 200, 100)]  # 直近
    return {"facts": {"us-gaap": {
        # 優先候補だが、申告は約7年前に途絶えている
        "RevenueRemainingPerformanceObligation": {"units": {"USD": [_fact(e, 900 + i, int(e[:4])) for i, e in enumerate(old)]}},
        # 次の候補で、今も申告されている
        "ContractWithCustomerLiability": {"units": {"USD": [_fact(e, 100 + i, int(e[:4])) for i, e in enumerate(new)]}},
    }}}


_FIELD_DEF = {"unit": "USD", "candidates": ["RevenueRemainingPerformanceObligation", "ContractWithCustomerLiability"]}


def _extract(monkeypatch, years):
    monkeypatch.setattr(qmod, "_QUARTERLY_YEARS", years)
    monkeypatch.setattr(qmod, "_ANNUAL_YEARS", years + 1)
    return lb.extract_field_raw_entries(_company_facts(), _FIELD_DEF, "rpo", "TEST")


class TestNoMergeFieldWindowIndependent:
    def test_rpo_is_in_no_merge_fields(self):
        assert "rpo" in lb.NO_CANDIDATE_MERGE_FIELDS

    def test_same_tag_and_latest_value_for_5_and_10_year_windows(self, monkeypatch):
        e5, tag5 = _extract(monkeypatch, 5)
        e10, tag10 = _extract(monkeypatch, 10)
        assert tag5 == tag10 == "ContractWithCustomerLiability"
        assert max(e["end"] for e in e5) == max(e["end"] for e in e10)
        assert [e["val"] for e in e5] == [e["val"] for e in e10]

    def test_stale_priority_tag_is_not_picked_with_long_window(self, monkeypatch):
        entries, tag = _extract(monkeypatch, 10)
        assert tag == "ContractWithCustomerLiability"
        assert all(e["val"] < 900 for e in entries)
