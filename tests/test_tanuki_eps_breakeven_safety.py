"""
tests/test_tanuki_eps_breakeven_safety.py

[[BREAKEVEN-FORECAST-METHOD-MISMATCH-1]]の回帰テスト。

TANUKI VALUATION（pipeline.py::compute_eps_breakeven()）にSTONKS SILO
（discover/stonks-silo/src/analyzer.py::_margin_breakeven()）と同種の
安全策一式（理由コード・傾き上限・異常値除外）を追加したことの検証。
回帰の方式自体（常に直近4点のOLS）はSTONKS SILO側と揃えたが、対象指標は
調整後EPSのまま維持している。

実行方法:
    python -m pytest tests/test_tanuki_eps_breakeven_safety.py -v
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

# [[TEST-SYSMODULES-MOCK-LEAK-1]]: 依存モジュールのスタブはpipelineのimportの間だけ入れて戻す
# （以前はsys.modules.setdefaultでMagicMockを入れたまま残し、収集の順番で結果が変わっていた）
from _tanuki_pipeline_stub import load_stubbed_pipeline  # noqa: E402

_pipeline = load_stubbed_pipeline()
compute_eps_breakeven = _pipeline.compute_eps_breakeven
EPS_MAGNITUDE_CAP = _pipeline.EPS_MAGNITUDE_CAP
EPS_SLOPE_CAP_PER_QUARTER = _pipeline.EPS_SLOPE_CAP_PER_QUARTER


def _q(adjusted_eps, flags=None):
    return {"adjusted_eps": adjusted_eps, "special_flags": flags or []}


CURRENT_YEAR = 2026


class TestAchievedAndNoData:
    def test_achieved_when_latest_quarter_non_negative(self):
        """STONKS SILO側の規約と同じく、達成済みはreasonのみで表し
        breakeven_estimateはNoneのまま（現在年を無理に埋めない）"""
        quarters = [_q(0.1), _q(-0.2), _q(-0.3), _q(-0.4)]
        est, reason = compute_eps_breakeven(quarters, CURRENT_YEAR)
        assert reason == "ACHIEVED"
        assert est is None

    def test_no_data_when_fewer_than_two_valid_points(self):
        quarters = [_q(-0.1), _q(None)]
        est, reason = compute_eps_breakeven(quarters, CURRENT_YEAR)
        assert reason == "NO_DATA"
        assert est is None

    def test_no_data_when_empty(self):
        est, reason = compute_eps_breakeven([], CURRENT_YEAR)
        assert reason == "NO_DATA"
        assert est is None


class TestPredictedUsesFourPointOls:
    def test_predicted_with_improving_trend(self):
        # oldest→newest (in list, newest is index 0): -0.5,-0.3,-0.2,-0.1
        quarters = [_q(-0.1), _q(-0.2), _q(-0.3), _q(-0.5)]
        est, reason = compute_eps_breakeven(quarters, CURRENT_YEAR)
        assert reason == "PREDICTED"
        assert est is not None
        assert est >= CURRENT_YEAR

    def test_uses_all_four_points_not_just_two(self):
        """直近2点だけなら横ばい(NO_TREND相当)だが、4点全体では改善
        トレンドがある場合にPREDICTEDになること（常に4点OLSであることの
        確認）"""
        # newest→oldest: -0.10, -0.10, -0.30, -0.50
        # 直近2点(-0.10,-0.10)だけなら傾き0でNO_TRENDになるはずの入力
        quarters = [_q(-0.10), _q(-0.10), _q(-0.30), _q(-0.50)]
        est, reason = compute_eps_breakeven(quarters, CURRENT_YEAR)
        assert reason == "PREDICTED"


class TestNoTrend:
    def test_no_trend_when_deteriorating(self):
        # newest→oldest: -0.5,-0.3,-0.2,-0.1 (悪化中)
        quarters = [_q(-0.5), _q(-0.3), _q(-0.2), _q(-0.1)]
        est, reason = compute_eps_breakeven(quarters, CURRENT_YEAR)
        assert reason == "NO_TREND"
        assert est is None

    def test_no_trend_when_slope_exceeds_cap(self):
        """LITE型の異常値混入シナリオ: 直近が突然の巨大な悪化を示す場合、
        傾き上限を超えるためNO_TRENDとして信頼しない
        （EPS_SLOPE_CAP_PER_QUARTER=2.0/四半期）"""
        quarters = [_q(-25.0), _q(2.0), _q(1.5), _q(1.0)]
        est, reason = compute_eps_breakeven(quarters, CURRENT_YEAR)
        assert reason == "NO_TREND"
        assert est is None


class TestTooFar:
    def test_too_far_when_beyond_five_year_horizon(self):
        # 極めて緩やかな改善（20四半期=5年を超える）
        quarters = [_q(-9.7), _q(-9.8), _q(-9.9), _q(-10.0)]
        est, reason = compute_eps_breakeven(quarters, CURRENT_YEAR)
        assert reason == "TOO_FAR"
        assert est is None


class TestMagnitudeOutlierExclusion:
    """LITE 2026Q4（adjusted_eps=-95.0、10-K通期実績の単一四半期
    誤抽出）のような、実データで確認された異常値を回帰対象から除外する
    こと（EPS_MAGNITUDE_CAP=30）。"""

    def test_lite_like_anomaly_is_excluded_from_regression(self):
        # 実際にLITEで観測された値に近いパターン: 直近が突然-95、
        # その前3四半期は堅調な黒字（0.93, 1.62, 2.12）
        quarters = [_q(-95.0), _q(2.12), _q(1.62), _q(0.93)]
        est, reason = compute_eps_breakeven(quarters, CURRENT_YEAR)
        # -95.0が除外されれば、残る直近有効値(2.12)は黒字のためACHIEVED
        assert reason == "ACHIEVED"
        assert est is None

    def test_value_within_cap_is_not_excluded(self):
        quarters = [_q(-EPS_MAGNITUDE_CAP + 1), _q(-1.0), _q(-1.0), _q(-1.0)]
        est, reason = compute_eps_breakeven(quarters, CURRENT_YEAR)
        # 除外されず、赤字継続のOLS回帰が行われること（ACHIEVEDにはならない）
        assert reason != "ACHIEVED"


class TestShareStructureMismatchExclusion:
    """[[EPS-LOAR-1]]で付与されるSHARE_STRUCTURE_MISMATCHフラグが
    立った四半期は、直近4四半期の中に含まれていても回帰対象から
    除外されること（IPO直後の別ティッカーへの防御的措置）。"""

    def test_flagged_quarter_excluded_leaves_fewer_valid_points(self):
        quarters = [
            _q(0.1),
            _q(-95.0, flags=["SHARE_STRUCTURE_MISMATCH"]),
            _q(-0.2),
            _q(-0.3),
        ]
        est, reason = compute_eps_breakeven(quarters, CURRENT_YEAR)
        # 直近(0.1)が黒字なのでACHIEVED。フラグ付き四半期の異常値が
        # 回帰・判定のいずれにも影響しないことを確認
        assert reason == "ACHIEVED"

    def test_flagged_quarters_can_reduce_below_minimum(self):
        quarters = [
            _q(-0.1, flags=["SHARE_STRUCTURE_MISMATCH"]),
            _q(-0.2, flags=["SHARE_STRUCTURE_MISMATCH"]),
            _q(-0.3),
            _q(None),
        ]
        est, reason = compute_eps_breakeven(quarters, CURRENT_YEAR)
        assert reason == "NO_DATA"
        assert est is None
