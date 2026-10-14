"""
tests/test_retention_and_fcf_points.py

[[LAYER3-RETENTION-PERIOD-1]]（2026-10-14、Koichi了承）:
- Layer3・normalizedの保持期間は四半期7年・年次8年（TTMの6点目の4四半期に届く最低限6.75年＋提出の遅れの余裕）
- TTMは6点まで作るが（RICE用4点＋FCF用5点の余裕）、FCFの平均・方式・外れ値・CAGRには
  直近の5点（4四半期がそろう点）だけを使う。「5年平均」の名前・年次の経路（5年）・FCFのCAGR（5点）と揃える

実行方法:
    venv/Scripts/python.exe -m pytest tests/test_retention_and_fcf_points.py -v
"""

import os
import sys
from datetime import date, timedelta

_PIPELINE_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation"))
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

import data_fetcher as _df  # noqa: E402
from common.sec_data import quarterly as qmod  # noqa: E402
from common.sec_data import ttm_calculator as tc  # noqa: E402


def _rel(days_ago: int) -> str:
    return (date.today() - timedelta(days=days_ago)).isoformat()


def _point(days_ago: int, fcf: float) -> dict:
    full = {"val": 0.0, "quarters_used": 4, "missing": 0}
    return {"ttm_end": _rel(days_ago), "flow": {"operating_cash_flow": dict(full), "capital_expenditure": dict(full),
                                               "FCF": {"val": fcf}}}


def _reader(series):
    r = _df.TTMReader(ticker="TEST", repo_root_path=None)
    r._series = series
    return r


_SIX = [_point(90 + 365 * i, 100.0 - 10 * i) for i in range(6)]  # 100, 90, 80, 70, 60, 50（新しい順）


class TestRetentionPeriod:
    def test_quarterly_7_years_annual_8_years(self):
        assert qmod._QUARTERLY_YEARS == 7
        assert qmod._ANNUAL_YEARS == 8

    def test_process_entries_keeps_6_5_year_old_quarter(self):
        def q(end):
            s = (date.fromisoformat(end) - timedelta(days=90)).isoformat()
            return {"end": end, "start": s, "val": 1, "form": "10-Q", "fp": "Q2", "fy": int(end[:4]),
                    "filed": end, "accn": "x-" + end}
        ends = {e["end"] for e in qmod._process_entries([q(_rel(int(365 * 6.5))), q(_rel(int(365 * 7.5)))])}
        assert _rel(int(365 * 6.5)) in ends
        assert _rel(int(365 * 7.5)) not in ends

    def test_ttm_keeps_six_points(self):
        assert tc.FCF_AVG_POINTS == 5
        import inspect
        assert inspect.signature(tc.calc_ttm_series).parameters["n_periods"].default == 6


class TestFcfFivePoints:
    def test_fcf_series_uses_latest_five_complete_points(self):
        r = _reader(list(_SIX))
        assert r.get_fcf_series() == [100.0, 90.0, 80.0, 70.0, 60.0]
        assert r.get_fcf_dates() == [p["ttm_end"] for p in _SIX[:5]]
        assert r.get_periods() == 5

    def test_sixth_point_is_not_in_average(self):
        vals = _reader(list(_SIX)).get_fcf_series()
        assert 50.0 not in vals
        assert sum(vals) / len(vals) == 80.0

    def test_incomplete_point_is_skipped_before_taking_five(self):
        # 2点目が4四半期そろわない場合は飛ばし、そろう点から5点を取る（6点目が入る）
        series = list(_SIX)
        series[1] = dict(series[1], flow=dict(series[1]["flow"], capital_expenditure={"val": 0.0, "quarters_used": 2}))
        r = _reader(series)
        assert r.get_fcf_series() == [100.0, 80.0, 70.0, 60.0, 50.0]
        assert r.get_periods() == 5

    def test_fewer_than_five_points_are_all_used(self):
        r = _reader(list(_SIX[:4]))
        assert r.get_fcf_series() == [100.0, 90.0, 80.0, 70.0]
        assert r.get_periods() == 4
