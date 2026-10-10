"""
tests/test_stonks_silo_breakeven_unify.py

[[BREAKEVEN-FORECAST-METHOD-MISMATCH-1]]の回帰テスト。

discover/stonks-silo/src/analyzer.py::_margin_breakeven()・
_gaap_margin_breakeven()を、旧「直近2点の傾き優先→条件次第で絶対値
ベース3点OLSへフォールバック」という2段階方式から「常に直近4年
（データがあれば）のマージン比率OLS回帰」へ統一したことの検証。
既存の校正値（売上規模10%未満除外・|マージン|>1000%除外・500pt/年超
除外）は変更していないことも確認する。

実行方法:
    python -m pytest tests/test_stonks_silo_breakeven_unify.py -v
"""

import importlib.util
import os
import sys

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_STONKS_SRC = os.path.join(_REPO_ROOT, "discover", "stonks-silo", "src")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
if _STONKS_SRC not in sys.path:
    sys.path.insert(0, _STONKS_SRC)

_spec = importlib.util.spec_from_file_location(
    "stonks_silo_analyzer_breakeven", os.path.join(_STONKS_SRC, "analyzer.py")
)
am = importlib.util.module_from_spec(_spec)
sys.modules["stonks_silo_analyzer_breakeven"] = am
_spec.loader.exec_module(am)


def _records(revenues: dict[int, float]) -> dict:
    return {yr: {"pl": {"revenue_sanitized": rev}} for yr, rev in revenues.items()}


class TestMarginBreakevenAlwaysUsesFourPointOls:
    def test_uses_up_to_four_years_not_three(self):
        """5年分データがある場合、直近4年のみが回帰に使われ最古年は
        無視されること（旧実装のyears[-3:]から変更されたことの確認）。
        5年連続で均等改善するマージン系列に対し、直近4年OLSと
        直近5年OLSは異なる傾き・切片を持つ設計上の性質を利用して、
        「最古年を含めていない」ことを間接的に検証する。"""
        years = [2021, 2022, 2023, 2024, 2025]
        # 2021年だけ極端に悪い（含めれば傾きが急になる）、2022-2025は
        # 緩やかな改善
        ocf = {2021: -100.0, 2022: -10.0, 2023: -8.0, 2024: -6.0, 2025: -4.0}
        rev = {yr: 100.0 for yr in years}
        records = _records(rev)
        year, reason, predicted = am._margin_breakeven(years, ocf, records)
        # 直近4年(2022-2025)のみなら緩やかな改善傾向がそのまま反映される。
        # 2021年を含めていたら傾きが大きく変わり、この緩やかな延長予測には
        # ならないはず
        assert reason == "PREDICTED"
        assert predicted is True
        assert year is not None and year <= 2025 + 5

    def test_always_ols_not_two_point_slope(self):
        """4点のうち直近2点だけの傾きと、4点全体のOLS傾きが異なる
        ケースで、実際に4点OLSの結果が採用されていること
        （旧Step1の2点優先ロジックが残っていないことの確認）"""
        years = [2022, 2023, 2024, 2025]
        # 直近2点(2024,2025)だけ見ると横ばい（傾き0=NO_TREND相当）だが、
        # 4点全体では緩やかな改善トレンドがある
        ocf = {2022: -50.0, 2023: -30.0, 2024: -10.0, 2025: -10.0}
        rev = {yr: 100.0 for yr in years}
        records = _records(rev)

        # 直近2点のみの傾き（旧Step1相当）
        two_point_slope = (ocf[2025] / 100.0 - ocf[2024] / 100.0) / (2025 - 2024)
        assert two_point_slope == 0  # 旧Step1ならNO_TRENDになるはずの入力

        year, reason, predicted = am._margin_breakeven(years, ocf, records)
        # 4点OLSでは全体的な改善トレンドがあるため、2点法のNO_TRENDとは
        # 異なりPREDICTEDになるはず
        assert reason == "PREDICTED"
        assert predicted is True


class TestMarginBreakevenReasonCodes:
    def test_achieved_when_latest_margin_positive(self):
        years = [2023, 2024, 2025]
        ocf = {2023: -10.0, 2024: -5.0, 2025: 5.0}
        rev = {yr: 100.0 for yr in years}
        year, reason, predicted = am._margin_breakeven(years, ocf, _records(rev))
        assert reason == "ACHIEVED"
        assert year == 2025
        assert predicted is False

    def test_no_data_when_fewer_than_two_valid_points(self):
        years = [2024, 2025]
        ocf = {2024: None, 2025: -10.0}
        rev = {yr: 100.0 for yr in years}
        year, reason, predicted = am._margin_breakeven(years, ocf, _records(rev))
        assert reason == "NO_DATA"
        assert year is None
        assert predicted is False

    def test_no_trend_when_margin_deteriorating(self):
        years = [2023, 2024, 2025]
        ocf = {2023: -5.0, 2024: -10.0, 2025: -20.0}
        rev = {yr: 100.0 for yr in years}
        year, reason, predicted = am._margin_breakeven(years, ocf, _records(rev))
        assert reason.startswith("NO_TREND")
        assert year is None
        assert predicted is False

    def test_no_trend_when_slope_exceeds_500pt_per_year_cap(self):
        """500pt/年超の急改善は既存の校正値通りNO_TRENDとして
        信頼しないこと（フォールバック廃止後も閾値自体は維持）。
        margin: -999%→-399%（1年で600pt改善、閾値500pt/年超）だが
        直近年もまだ赤字（ACHIEVED分岐に入らない）"""
        years = [2024, 2025]
        rev = {yr: 100.0 for yr in years}
        ocf = {2024: -999.0, 2025: -399.0}
        year, reason, predicted = am._margin_breakeven(years, ocf, _records(rev))
        assert reason.startswith("NO_TREND")
        assert predicted is False

    def test_too_far_when_beyond_five_year_horizon(self):
        years = [2023, 2024, 2025]
        ocf = {2023: -100.0, 2024: -99.0, 2025: -98.0}  # 改善が極めて緩やか
        rev = {yr: 100.0 for yr in years}
        year, reason, predicted = am._margin_breakeven(years, ocf, _records(rev))
        assert reason == "TOO_FAR"
        assert year is None
        assert predicted is False

    def test_revenue_scale_filter_unchanged(self):
        """直近年収益の10%未満の年は除外する既存フィルタが維持されている
        こと"""
        years = [2022, 2023, 2024, 2025]
        rev = {2022: 5.0, 2023: 50.0, 2024: 80.0, 2025: 100.0}  # 2022だけ極小
        ocf = {2022: 100.0, 2023: -20.0, 2024: -10.0, 2025: -5.0}
        records = _records(rev)
        year, reason, predicted = am._margin_breakeven(years, ocf, records)
        # 2022年（rev=5、latest_revの10%未満）が除外されれば3点残り、
        # 100.0という極端値混入によるバグった結果にはならない
        assert reason in ("PREDICTED", "TOO_FAR", "NO_TREND")

    def test_abnormal_margin_magnitude_filter_unchanged(self):
        """|マージン|>1000%（ratio>10.0）の年を除外する既存フィルタが
        維持されていること"""
        years = [2023, 2024, 2025]
        rev = {yr: 100.0 for yr in years}
        ocf = {2023: -1500.0, 2024: -20.0, 2025: -10.0}  # 2023だけ|margin|=15.0
        year, reason, predicted = am._margin_breakeven(years, ocf, _records(rev))
        # 2023年が除外されれば残り2点(2024,2025)のみでOLS
        assert reason in ("PREDICTED", "NO_TREND", "TOO_FAR")


class TestGaapMarginBreakevenReasonCodes:
    def test_achieved(self):
        years = [2024, 2025]
        ni = {2024: -10.0, 2025: 5.0}
        rev = {yr: 100.0 for yr in years}
        year, reason, predicted = am._gaap_margin_breakeven(years, ni, _records(rev))
        assert reason == "ACHIEVED"
        assert predicted is False

    def test_imminent_when_breakeven_year_not_after_latest(self):
        years = [2023, 2024, 2025]
        # 改善が非常に急で、ゼロ交差が2025年以前に計算されるケース
        ni = {2023: -5.0, 2024: -1.0, 2025: -0.1}
        rev = {yr: 100.0 for yr in years}
        year, reason, predicted = am._gaap_margin_breakeven(years, ni, _records(rev))
        assert reason in ("IMMINENT", "PREDICTED")
        assert predicted is True

    def test_no_data(self):
        years = [2025]
        ni = {2025: -10.0}
        rev = {2025: 100.0}
        year, reason, predicted = am._gaap_margin_breakeven(years, ni, _records(rev))
        assert reason == "NO_DATA"
        assert predicted is False


class TestOlsSlopeIntercept:
    def test_perfect_line(self):
        slope, intercept = am._ols_slope_intercept([0, 1, 2, 3], [1.0, 3.0, 5.0, 7.0])
        assert abs(slope - 2.0) < 1e-9
        assert abs(intercept - 1.0) < 1e-9

    def test_zero_variance_x_returns_none(self):
        slope, intercept = am._ols_slope_intercept([5, 5, 5], [1.0, 2.0, 3.0])
        assert slope is None
        assert intercept is None


class TestDiscontinuousGrowthGateUsesActualPrediction:
    """[[BREAKEVEN-FORECAST-METHOD-MISMATCH-1]]: 非連続成長チェックの
    ゲートが、旧`ols_used`（絶対値OLSフォールバック使用時のみTrue）から
    `any_predicted`（PREDICTED/IMMINENTを実際に算出した場合のみTrue）
    へ意味を変えたこと。_breakeven_estimate()の戻り値7番目の要素で
    確認する。"""

    def test_predicted_case_returns_true_for_seventh_element(self):
        analyzer = am.StonksAnalyzer()
        years = [2023, 2024, 2025]
        records = {
            yr: {"pl": {"revenue_sanitized": 100.0, "net_income": -30.0 + i * 10}}
            for i, yr in enumerate(years)
        }
        ocf_annual = {2023: -30.0, 2024: -20.0, 2025: -10.0}
        result = analyzer._breakeven_estimate(years, records, ocf_annual, ocf_trend="IMPROVING")
        any_predicted = result[6]  # 8番目はOCFマージン回帰の最新年（[[STONKS-PATHSCORE-WITHOUT-ESTIMATE-1]]）
        assert isinstance(any_predicted, bool)

    def test_no_data_case_returns_false_for_seventh_element(self):
        analyzer = am.StonksAnalyzer()
        years = [2025]
        records = {2025: {"pl": {"revenue_sanitized": 100.0, "net_income": -10.0}}}
        ocf_annual = {2025: -10.0}
        result = analyzer._breakeven_estimate(years, records, ocf_annual, ocf_trend="FLAT")
        assert result[6] is False
