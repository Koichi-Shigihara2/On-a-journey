"""
tests/test_stonks_silo_yoy_period_validation.py

STONKS SILOの財務トレンド（financial_trend_calculator.py）の前年同期比・QoQの回帰テスト。

経緯:
- [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（2026-09-12）: fpラベル（Q1〜Q4）の一致でYoYを
  照合し、同じfpの直近2件の日数差が330〜400日の範囲外ならNoneにしていた。RCAT・CRWV等で
  1〜3月期がfp="Q2"と付いており（fpは申告した書類の会計期間で、期間そのものの四半期ではない）、
  前年同期比が算出不能になっていた。
- [[STONKS-HEATMAP-FQ-LABEL-1]]（2026-10-10、上の方式を置き換え）: fpを判断に使わず、
  最新エントリのendから330〜400日前にendがあるエントリ（複数なら365日に最も近いもの）を
  前年同期とする。QoQは直前のエントリとの日数差が60〜120日のときだけ。画面のラベルも期末の
  yy/mm（fpは使わない）。

実行方法:
    python -m pytest tests/test_stonks_silo_yoy_period_validation.py -v
"""

import os
import re
import sys

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_STONKS_SRC = os.path.join(_REPO_ROOT, "discover", "stonks-silo", "src")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
if _STONKS_SRC not in sys.path:
    sys.path.insert(0, _STONKS_SRC)

import financial_trend_calculator as ftc  # noqa: E402

_INDEX_HTML = os.path.join(_REPO_ROOT, "docs", "value-monitor", "stonks-silo", "index.html")


def _entry(start, end, fp, val):
    return {"start": start, "end": end, "fp": fp, "val": val}


class TestYoyByEndDate:
    def test_mislabeled_fp_still_gets_yoy(self):
        """CRWV(2026)実例相当: 1-3月期がfp=Q2（誤）でも、期末日で前年同期（2025-06-30）を取る"""
        entries = [
            _entry("2024-07-01", "2024-09-30", "Q3", -359807000),
            _entry("2024-09-30", "2024-12-31", "Q4", -50924000),
            _entry("2025-01-01", "2025-03-31", "Q2", -315000000),   # 誤タグ
            _entry("2025-04-01", "2025-06-30", "Q2", -290000000),
            _entry("2025-07-01", "2025-09-30", "Q3", -110124000),
            _entry("2025-09-30", "2025-12-31", "Q4", -451876000),
            _entry("2026-01-01", "2026-03-31", "Q2", -740000000),   # 誤タグ
            _entry("2026-04-01", "2026-06-30", "Q2", -626000000),   # 最新
        ]
        result = ftc._calc_yoy_change(entries)
        assert result is not None
        assert result["end_latest"] == "2026-06-30"
        assert result["end_prev"] == "2025-06-30"
        assert round(result["change_pct"], 1) == round((-626 - -290) / 290 * 100, 1)
        assert result["fp"] == ""  # 互換のためキーは残すが値は入れない

    def test_fp_is_ignored_entirely(self):
        """fpが空・FY・でたらめでも、期末日だけで決まる"""
        entries = [
            _entry("", "2024-03-31", "", 100.0),
            _entry("", "2024-06-30", "FY", 110.0),
            _entry("", "2024-09-30", "Q1", 120.0),
            _entry("", "2024-12-31", "Q9", 130.0),
            _entry("", "2025-03-31", "", 150.0),
        ]
        result = ftc._calc_yoy_change(entries)
        assert result["end_prev"] == "2024-03-31" and result["change_pct"] == 50.0

    def test_genuine_yoy_pair_365_days_apart_still_works(self):
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
        """52/53週会計年度等で数日ずれるケース（330〜400日の許容範囲内）"""
        entries = [
            _entry("2023-12-25", "2024-03-30", "Q1", 100.0),
            _entry("2024-04-01", "2024-06-29", "Q2", 110.0),
            _entry("2024-07-01", "2024-09-28", "Q3", 120.0),
            _entry("2024-10-01", "2025-01-04", "Q4", 130.0),
            _entry("2024-12-30", "2025-04-05", "Q1", 150.0),  # 前年(03-30)から371日
        ]
        result = ftc._calc_yoy_change(entries)
        assert result is not None
        assert result["change_pct"] == 50.0

    def test_closest_to_365_when_multiple_candidates(self):
        """330〜400日に2件ある（決算期の変更等）なら365日に近い方"""
        entries = [
            _entry("", "2024-05-31", "", 80.0),    # 396日前
            _entry("", "2024-06-30", "", 100.0),   # 366日前 ← こちら
            _entry("", "2024-09-30", "", 1.0),
            _entry("", "2024-12-31", "", 1.0),
            _entry("", "2025-07-01", "", 150.0),
        ]
        result = ftc._calc_yoy_change(entries)
        assert result["end_prev"] == "2024-06-30" and result["change_pct"] == 50.0

    def test_none_when_no_entry_in_330_400_days(self):
        """330〜400日前にendが無ければNone（上限側: 前年の行が欠けている）"""
        entries = [
            _entry("", "2022-12-31", "Q4", 100.0),
            _entry("", "2023-03-31", "Q1", 100.0),
            _entry("", "2023-06-30", "Q2", 110.0),   # 428日前
            _entry("", "2024-06-30", "Q2", 120.0),   # 62日前
            _entry("", "2024-08-31", "Q1", 150.0),
        ]
        assert ftc._calc_yoy_change(entries) is None

    def test_none_when_only_closer_entries(self):
        """330日未満（隣接四半期相当）しか無ければNone（下限側）"""
        entries = [
            _entry("", "2024-09-30", "Q3", -1.0),    # 273日前
            _entry("", "2024-12-31", "Q4", -1.0),
            _entry("", "2025-03-31", "Q2", -315.0),
            _entry("", "2025-05-31", "Q2", -300.0),
            _entry("", "2025-06-30", "Q2", -290.0),
        ]
        assert ftc._calc_yoy_change(entries) is None

    def test_val_prev_zero_is_none(self):
        entries = [_entry("", e, "", v) for e, v in
                   [("2024-03-31", 0.0), ("2024-06-30", 1.0), ("2024-09-30", 1.0), ("2024-12-31", 1.0), ("2025-03-31", 5.0)]]
        assert ftc._calc_yoy_change(entries) is None


class TestQoqGapGuard:
    def test_adjacent_quarter_computed(self):
        entries = [_entry("", "2025-03-31", "Q2", 100.0), _entry("", "2025-06-30", "Q2", 150.0)]
        r = ftc._calc_qoq_change(entries)
        assert r["change_pct"] == 50.0 and r["end_prev"] == "2025-03-31"

    def test_missing_quarter_in_between_is_none(self):
        """直前の行が2四半期前（182日）なら比較しない"""
        entries = [_entry("", "2024-12-31", "Q4", 100.0), _entry("", "2025-06-30", "Q2", 150.0)]
        assert ftc._calc_qoq_change(entries) is None

    def test_fiscal_year_change_short_gap_is_none(self):
        """決算期の変更などで直前の行が60日未満なら比較しない"""
        entries = [_entry("", "2024-11-30", "Q2", 100.0), _entry("", "2024-12-31", "FY", 150.0)]  # 31日
        assert ftc._calc_qoq_change(entries) is None

    def test_boundaries(self):
        assert ftc._QOQ_GAP_DAYS_MIN == 60 and ftc._QOQ_GAP_DAYS_MAX == 120
        ok_lo = [_entry("", "2025-01-01", "", 100.0), _entry("", "2025-03-02", "", 110.0)]   # 60日
        ok_hi = [_entry("", "2025-01-01", "", 100.0), _entry("", "2025-05-01", "", 110.0)]   # 120日
        ng_hi = [_entry("", "2025-01-01", "", 100.0), _entry("", "2025-05-02", "", 110.0)]   # 121日
        assert ftc._calc_qoq_change(ok_lo) is not None
        assert ftc._calc_qoq_change(ok_hi) is not None
        assert ftc._calc_qoq_change(ng_hi) is None


class TestHtmlLabels:
    """画面のラベルは期末のyy/mm（fpは使わない）。JSは文字列で確認する"""

    def _html(self):
        with open(_INDEX_HTML, encoding="utf-8") as f:
            return f.read()

    def test_no_fp_based_labels(self):
        html = self._html()
        assert "fp.startsWith" not in html
        assert not re.search(r"\be\.fp\b", html)

    def test_end_label_function(self):
        html = self._html()
        m = re.search(r"function fvEndLabel\(end\) \{\s*return (.+?);\s*\}", html, re.S)
        assert m, "fvEndLabel()が無い"
        assert "end.slice(2, 4) + '/' + end.slice(5, 7)" in m.group(1)
        assert html.count("fvEndLabel(e.end)") == 2  # ヒートマップの列とチャートのラベル

    def test_qoq_thresholds_match_python(self):
        html = self._html()
        lo = int(re.search(r"const QOQ_GAP_DAYS_MIN = (\d+);", html).group(1))
        hi = int(re.search(r"const QOQ_GAP_DAYS_MAX = (\d+);", html).group(1))
        assert (lo, hi) == (ftc._QOQ_GAP_DAYS_MIN, ftc._QOQ_GAP_DAYS_MAX)
        assert "fvIsAdjacentQuarter(prevEnd, end)" in html
