"""
tests/test_stonks_silo_yoy_period_validation.py

[[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]の回帰テスト。

financial_trend_calculator.py::_calc_yoy_change()は、fpラベル（Q1〜Q4）
の完全一致のみでYoY照合しており、期間長の妥当性チェックを持たなかった。
RCAT・CRWV等9銘柄で、比較列（10-Qの前四半期再掲）が申告元filingのfpを
そのまま引き継ぐ等のSEC/XBRL申告慣行により、1〜3月期エントリがfp="Q2"
と誤タグ付けされ、直近の真のQ2（4〜6月期）と隣接比較（日数差91日）される
実例が2026-09-12調査で確認された。

修正: 同一fp内の直近2件のend日付差が330〜400日の範囲内（真のYoYとして
妥当な期間長）であることを確認し、範囲外の場合はfpラベルの誤りを疑い
YoY計算をスキップ（None）する。

実行方法:
    python -m pytest tests/test_stonks_silo_yoy_period_validation.py -v
"""

import os
import sys

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_STONKS_SRC = os.path.join(_REPO_ROOT, "discover", "stonks-silo", "src")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
if _STONKS_SRC not in sys.path:
    sys.path.insert(0, _STONKS_SRC)

import financial_trend_calculator as ftc  # noqa: E402


def _entry(start, end, fp, val):
    return {"start": start, "end": end, "fp": fp, "val": val}


class TestFpLabelPeriodValidation:
    """RCAT/CRWV実例相当: 同一fpラベルの下に、実際には隣接する四半期
    （QoQ相当、約91日差）が混在するケースを検知しスキップすることを
    確認する"""

    def test_rcat_style_adjacent_quarters_mislabeled_same_fp_skipped(self):
        """CRWV(2026)実例相当: 1-3月期(fp=Q2、誤)と4-6月期(fp=Q2、正)が
        隣接（91日差）しているため、YoYとして扱わずNoneを返す"""
        entries = [
            _entry("2024-07-01", "2024-09-30", "Q3", -359807000),
            _entry("2024-09-30", "2024-12-31", "Q4", -50924000),
            _entry("2025-01-01", "2025-03-31", "Q2", -315000000),   # 誤タグ
            _entry("2025-04-01", "2025-06-30", "Q2", -290000000),   # 正しいQ2
            _entry("2025-07-01", "2025-09-30", "Q3", -110124000),
            _entry("2025-09-30", "2025-12-31", "Q4", -451876000),
            _entry("2026-01-01", "2026-03-31", "Q2", -740000000),   # 誤タグ
            _entry("2026-04-01", "2026-06-30", "Q2", -626000000),   # 正しいQ2（最新）
        ]
        result = ftc._calc_yoy_change(entries)
        assert result is None

    def test_genuine_yoy_pair_365_days_apart_still_works(self):
        """回帰確認: fpラベルが一致し、かつ真に約365日差の場合は
        従来通りYoY計算が成立する"""
        entries = [
            _entry("2024-01-01", "2024-03-31", "Q1", 100.0),
            _entry("2024-04-01", "2024-06-30", "Q2", 110.0),
            _entry("2024-07-01", "2024-09-30", "Q3", 120.0),
            _entry("2024-10-01", "2024-12-31", "Q4", 130.0),
            _entry("2025-01-01", "2025-03-31", "Q1", 150.0),
        ]
        result = ftc._calc_yoy_change(entries)
        assert result is not None
        assert result["change_pct"] == 50.0
        assert result["end_latest"] == "2025-03-31"
        assert result["end_prev"] == "2024-03-31"

    def test_slightly_irregular_but_valid_yoy_gap_still_works(self):
        """52/53週会計年度等で数日ずれるケース（330〜400日の許容範囲内）
        は引き続きYoY計算される（過剰検知でないことの確認）"""
        entries = [
            _entry("2023-12-25", "2024-03-30", "Q1", 100.0),
            _entry("2024-04-01", "2024-06-29", "Q2", 110.0),
            _entry("2024-07-01", "2024-09-28", "Q3", 120.0),
            _entry("2024-10-01", "2025-01-04", "Q4", 130.0),
            _entry("2024-12-30", "2025-04-05", "Q1", 150.0),  # 前年Q1(03-30)から371日
        ]
        result = ftc._calc_yoy_change(entries)
        assert result is not None
        assert result["change_pct"] == 50.0

    def test_gap_just_outside_tolerance_is_skipped(self):
        """境界値確認: 400日を超える場合はスキップする"""
        entries = [
            _entry("2023-01-01", "2023-03-31", "Q1", 100.0),
            _entry("2023-04-01", "2023-06-30", "Q2", 110.0),
            _entry("2023-07-01", "2023-09-30", "Q3", 120.0),
            _entry("2023-10-01", "2023-12-31", "Q4", 130.0),
            _entry("2024-06-01", "2024-08-31", "Q1", 150.0),  # 2023-03-31から518日
        ]
        result = ftc._calc_yoy_change(entries)
        assert result is None

    def test_gap_just_outside_tolerance_lower_bound_is_skipped(self):
        """境界値確認: 330日未満（91日等、隣接四半期相当）はスキップする"""
        entries = [
            _entry("2024-04-01", "2024-06-30", "Q1", -1.0),
            _entry("2024-07-01", "2024-09-30", "Q3", -1.0),
            _entry("2024-09-30", "2024-12-31", "Q4", -1.0),
            _entry("2025-01-01", "2025-03-31", "Q2", -315.0),
            _entry("2025-04-01", "2025-06-30", "Q2", -290.0),
        ]
        result = ftc._calc_yoy_change(entries)
        assert result is None
