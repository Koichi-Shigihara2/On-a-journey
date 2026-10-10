"""
[[STONKS-PATHSCORE-WITHOUT-ESTIMATE-1]]（2026-10-10）: STONKS SILOの③黒字化パスの点数
（profitability_path.score、overallの30%）を、OCF「金額」の傾向（ocf_trend）ではなく、
OCF「マージン」の回帰による黒字化推定（_margin_breakeven）で決めるようにしたことのテスト。

- 刻み: OCF達成済100／PREDICTED 推定年−回帰の最新年が1年以内90・2年80・3年65・4〜5年50／
  TOO_FAR 30／NO_TREND 20／NO_DATA はocf_trendの点数で上限50
- 変えないもの: overallの重み（40/30/30）、verdictの閾値（75/55/35）、ocf_trendの算出
"""

import importlib.util
import os
import sys
from types import SimpleNamespace

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_STONKS_SRC = os.path.join(_REPO_ROOT, "discover", "stonks-silo", "src")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
if _STONKS_SRC not in sys.path:
    sys.path.insert(0, _STONKS_SRC)

_spec = importlib.util.spec_from_file_location(
    "stonks_silo_analyzer_path_score", os.path.join(_STONKS_SRC, "analyzer.py")
)
am = importlib.util.module_from_spec(_spec)
sys.modules["stonks_silo_analyzer_path_score"] = am
_spec.loader.exec_module(am)


def _pp(reason="", year=None, trend="UNKNOWN", hidden=False, basis=None):
    return am.ProfitabilityPath(
        ocf_trend=trend,
        ocf_breakeven_year=year,
        ocf_breakeven_reason=reason,
        hidden_profit_already=hidden,
        ocf_breakeven_basis_year=basis,
    )


class TestPathScoreSteps:
    @pytest.mark.parametrize("be_year, expected, label", [
        (2025, 90, "OCF黒字化 2025年（1年以内）"),  # 推定年=回帰の最新年（0年）
        (2026, 90, "OCF黒字化 2026年（1年以内）"),
        (2027, 80, "OCF黒字化 2027年（2年後）"),
        (2028, 65, "OCF黒字化 2028年（3年後）"),
        (2029, 50, "OCF黒字化 2029年（4年後）"),
        (2030, 50, "OCF黒字化 2030年（5年後）"),
    ])
    def test_predicted_by_years_ahead(self, be_year, expected, label):
        pp = _pp("PREDICTED", be_year, trend="DETERIORATING", basis=2025)
        assert am._path_score_from_breakeven(pp, 2025) == (expected, label)

    def test_predicted_gap_counts_from_regression_latest_year_not_trend(self):
        """年数の起点は回帰に使った最新年。ocf_trendは点数に入らない"""
        for trend in ("ACCELERATING", "DETERIORATING", "UNKNOWN"):
            assert am._path_score_from_breakeven(_pp("PREDICTED", 2027, trend=trend), 2024)[0] == 65

    def test_too_far(self):
        assert am._path_score_from_breakeven(_pp("TOO_FAR", trend="ACCELERATING"), 2025) == (30, "OCF黒字化 5年超")

    @pytest.mark.parametrize("reason", ["NO_TREND", "NO_TREND:-17%→-84%"])
    def test_no_trend(self, reason):
        assert am._path_score_from_breakeven(_pp(reason, trend="ACCELERATING"), 2025) == (20, "OCFマージン 改善傾向なし")

    @pytest.mark.parametrize("trend, expected, ja", [
        ("ACCELERATING", 50, "加速中"),
        ("IMPROVING", 50, "改善中"),
        ("FLAT", 50, "横ばい"),
        ("DETERIORATING", 20, "悪化中"),
        ("UNKNOWN", 0, "不明"),
    ])
    def test_no_data_uses_trend_capped_at_50(self, trend, expected, ja):
        assert am._path_score_from_breakeven(_pp("NO_DATA", trend=trend), None) == (expected, f"推定不能: OCF{ja}（上限50）")

    def test_ocf_achieved_by_hidden_profit(self):
        pp = _pp("ACHIEVED", trend="IMPROVING", hidden=True)
        assert am._path_score_from_breakeven(pp, None) == (100, "OCF達成済")

    def test_ocf_achieved_by_reason_only(self):
        """回帰の最新年のマージンが0以上（_margin_breakevenがACHIEVED）でも100"""
        assert am._path_score_from_breakeven(_pp("ACHIEVED", 2025, trend="FLAT"), 2025) == (100, "OCF達成済")

    def test_constants_in_one_place(self):
        assert am.PATH_SCORE_OCF_ACHIEVED == 100
        assert am.PATH_SCORE_PREDICTED == ((1, 90), (2, 80), (3, 65), (5, 50))
        assert am.PATH_SCORE_TOO_FAR == 30
        assert am.PATH_SCORE_NO_TREND == 20
        assert am.PATH_SCORE_NO_DATA_CAP == 50


class TestMarginBreakevenBasisYear:
    def test_detail_returns_regression_latest_year(self):
        """売上が直近年の10%未満の年は回帰から外れるため、最新年はyears[-1]と違いうる"""
        years = [2022, 2023, 2024, 2025]
        ocf = {2022: -50.0, 2023: -40.0, 2024: -30.0, 2025: None}
        recs = {yr: {"pl": {"revenue_sanitized": 100.0}} for yr in years}
        year, reason, predicted, basis = am._margin_breakeven_detail(years, ocf, recs)
        assert basis == 2024
        assert (year, reason, predicted) == am._margin_breakeven(years, ocf, recs)

    def test_no_data_has_no_basis_year(self):
        years = [2025]
        assert am._margin_breakeven_detail(years, {2025: -10.0}, {2025: {"pl": {"revenue_sanitized": 100.0}}}) == (
            None, "NO_DATA", False, None)

    def test_analyze_profitability_path_records_basis_year(self):
        a = am.StonksAnalyzer()
        years = [2022, 2023, 2024, 2025]
        recs = {
            yr: {"pl": {"revenue_sanitized": 100.0, "net_income": -50.0}, "cf": {"operating_cash_flow": -40.0 + i * 10}}
            for i, yr in enumerate(years)
        }
        ocf = {yr: recs[yr]["cf"]["operating_cash_flow"] for yr in years}
        res = a._breakeven_estimate(years, recs, ocf, "IMPROVING")
        assert len(res) == 8
        assert res[1] == 2026 and res[4] == "PREDICTED" and res[7] == 2025

    def test_hidden_profit_has_no_basis_year(self):
        a = am.StonksAnalyzer()
        years = [2024, 2025]
        recs = {yr: {"pl": {"revenue_sanitized": 100.0, "net_income": -5.0}} for yr in years}
        res = a._breakeven_estimate(years, recs, {2024: -5.0, 2025: 5.0}, "IMPROVING")
        assert res[2] is True and res[4] == "ACHIEVED" and res[7] is None


class TestOverallWeightsAndThresholdsUnchanged:
    def _overall(self, dq_score, ra_verdict, pp):
        dq = SimpleNamespace(score=dq_score)
        ra = SimpleNamespace(verdict=ra_verdict, score=None)
        res = am.StonksAnalyzer()._overall(dq, ra, pp)
        return res, ra, pp

    def test_weights_40_30_30(self):
        pp = _pp("PREDICTED", 2028, trend="DETERIORATING", basis=2025)  # 65点
        (overall, _), ra, pp = self._overall(80.0, "WATCH", pp)
        assert overall == round(80 * 0.4 + 60 * 0.3 + 65 * 0.3, 1) == 69.5
        assert ra.score == 60.0 and pp.score == 65.0
        assert pp.path_score_basis == "OCF黒字化 2028年（3年後）"

    @pytest.mark.parametrize("dq_score, ra_verdict, path, expected_overall, expected_verdict", [
        # path=100（OCF達成済）。overall = dq*0.4 + ra*0.3 + 30
        (62.5, "SAFE", True, 85.0, "10x_CANDIDATE"),
        (37.5, "SAFE", True, 75.0, "10x_CANDIDATE"),   # 境界75
        (37.0, "SAFE", True, 74.8, "PROMISING"),
        (12.5, "WATCH", True, 53.0, "WATCH"),
        (25.0, "DANGER", True, 46.0, "WATCH"),
        (0.0, "DANGER", True, 36.0, "WATCH"),
    ])
    def test_verdict_thresholds(self, dq_score, ra_verdict, path, expected_overall, expected_verdict):
        pp = _pp("ACHIEVED", hidden=path)
        (overall, verdict), _, _ = self._overall(dq_score, ra_verdict, pp)
        assert (overall, verdict) == (expected_overall, expected_verdict)

    @pytest.mark.parametrize("overall_target, verdict", [(55.0, "PROMISING"), (54.9, "WATCH"), (35.0, "WATCH"), (34.9, "AVOID")])
    def test_lower_thresholds(self, overall_target, verdict):
        # path=20（NO_TREND）・ra=DANGER(20) → overall = dq*0.4 + 12
        dq = (overall_target - 12) / 0.4
        (overall, v), _, _ = self._overall(dq, "DANGER", _pp("NO_TREND"))
        assert overall == overall_target and v == verdict

    def test_summary_line_uses_basis(self):
        """総合判定の根拠の③の行は点数の根拠を出す（旧「明確な道筋」等の対応表は廃止）"""
        src = open(os.path.join(_STONKS_SRC, "analyzer.py"), encoding="utf-8").read()
        assert "明確な道筋" not in src
        assert "黒字化パス {pp_s}点　（{pp.path_score_basis or '—'}）" in src
        assert "• 営業CFトレンド（金額） {trend_ja}" in src
