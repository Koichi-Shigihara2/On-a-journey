"""
tests/test_ttm_anchor_reach.py

[[TTM-STALE-QUARTER-IN-WINDOW-1]]: calc_ttm_series()が各点（anchor）で足す
4四半期の最新の期末が、その点の期末に接していることを検証する。

修正前は「anchor以前の直近4件」を取り、4件どうしの連続性
（_last4_is_contiguous()）だけを見ていたため、申告が途絶えた項目の
古い4四半期（例: AMATのリース返済額が2023-10-29まで）が、2026年の点の
TTMにそのまま入っていた。

実行方法:
    venv/Scripts/python.exe -m pytest tests/test_ttm_anchor_reach.py -v
"""

from common.sec_data.ttm_calculator import calc_ttm_series


def _q(end, start, val):
    return {"end": end, "start": start, "val": val, "is_annual": False}


_OCF = [
    _q("2025-06-30", "2025-04-01", 40.0),
    _q("2025-03-31", "2025-01-01", 30.0),
    _q("2024-12-31", "2024-10-01", 20.0),
    _q("2024-09-30", "2024-07-01", 10.0),
]


def _store(**extra_fields):
    fields = {"operating_cash_flow": {"entries": list(_OCF)}}
    for name, entries in extra_fields.items():
        fields[name] = {"entries": entries}
    return {"fields": fields}


class TestTtmAnchorReach:
    def test_field_that_stopped_years_ago_is_not_summed_into_current_point(self):
        # リース返済額の申告が2023-12-31で途絶えた（4四半期は連続している）
        lease = [
            _q("2023-12-31", "2023-10-01", 4.0),
            _q("2023-09-30", "2023-07-01", 3.0),
            _q("2023-06-30", "2023-04-01", 2.0),
            _q("2023-03-31", "2023-01-01", 1.0),
        ]
        series = calc_ttm_series("TEST", _store(finance_lease_payments=lease), n_periods=1)
        assert series[0]["ttm_end"] == "2025-06-30"
        assert "finance_lease_payments" not in series[0]["flow"]
        assert series[0]["flow"]["operating_cash_flow"]["val"] == 100.0

    def test_field_one_quarter_behind_is_not_summed(self):
        # FCX型: 最新の四半期（2025-06-30）の設備投資だけが欠け、1四半期古い4件が残る
        capex = [
            _q("2025-03-31", "2025-01-01", -3.0),
            _q("2024-12-31", "2024-10-01", -2.0),
            _q("2024-09-30", "2024-07-01", -1.0),
            _q("2024-06-30", "2024-04-01", -1.0),
        ]
        series = calc_ttm_series("TEST", _store(capital_expenditure=capex), n_periods=1)
        assert "capital_expenditure" not in series[0]["flow"]
        assert "FCF" not in series[0]["flow"]

    def test_partial_window_that_does_not_reach_anchor_is_not_kept(self):
        # 4件そろわない（quarters_used<4）項目でも、点の期末に届かないものは作らない
        sbc = [_q("2023-12-31", "2023-10-01", 5.0), _q("2023-09-30", "2023-07-01", 5.0)]
        series = calc_ttm_series("TEST", _store(stock_based_compensation=sbc), n_periods=1)
        assert "stock_based_compensation" not in series[0]["flow"]

    def test_field_that_reaches_anchor_is_kept(self):
        capex = [
            _q("2025-06-30", "2025-04-01", -4.0),
            _q("2025-03-31", "2025-01-01", -3.0),
            _q("2024-12-31", "2024-10-01", -2.0),
            _q("2024-09-30", "2024-07-01", -1.0),
        ]
        series = calc_ttm_series("TEST", _store(capital_expenditure=capex), n_periods=1)
        assert series[0]["flow"]["capital_expenditure"]["val"] == -10.0
        assert series[0]["flow"]["capital_expenditure"]["quarters_used"] == 4
        assert series[0]["flow"]["FCF"]["val"] == 90.0

    def test_eps_fields_keep_existing_exemption(self):
        # EPSはQ4の推計の対象外で、Q4の期末では構造的に1四半期古い（連続性の検査と同じく対象外）
        eps = [
            _q("2025-03-31", "2025-01-01", 1.0),
            _q("2024-12-31", "2024-10-01", 1.0),
            _q("2024-09-30", "2024-07-01", 1.0),
            _q("2024-06-30", "2024-04-01", 1.0),
        ]
        series = calc_ttm_series("TEST", _store(eps_diluted=eps), n_periods=1)
        assert series[0]["flow"]["eps_diluted"]["val"] == 4.0
