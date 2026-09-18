"""
tests/test_dcf_validity_checker.py

common/screening/dcf_validity_checker.py::check_c_data_jump() の
section/fieldパラメータ化（[[DATA-JUMP-CHECK-GENERALIZE-1]]）のユニットテスト。
Revenue専用のハードコードから汎用化した際、Revenue呼び出し元の挙動が
一切変わらないこと・新規フィールド（売上総利益・CapEx）向けの非対称な
閾値（down_ratio明示指定）が正しく機能することを検証する。

実行方法:
    python -m pytest tests/test_dcf_validity_checker.py -v
"""

import json
import os

from common.screening.dcf_validity_checker import (
    check_b_source_label,
    check_c_data_jump,
    check_f_eps_analyzer_delta,
    check_g_rice_efficiency,
    check_h_analyst_vs_iv,
)


def _write_annual(sec_data_dir, ticker: str, year: int, section: str, field: str, value) -> None:
    ticker_dir = os.path.join(sec_data_dir, ticker)
    os.makedirs(ticker_dir, exist_ok=True)
    with open(os.path.join(ticker_dir, f"annual_{year}.json"), "w", encoding="utf-8") as f:
        json.dump({"period": year, section: {field: value}}, f)


class TestCheckBSourceLabelSegmentXbrl:
    """[[SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-IDEA-1]]案①（2026-09-18）:
    source="segment_xbrl"はgrowth.py側で表示ラベルと実態が常に一致する
    設計（[[GROWTH-SOURCE-LABEL-1]]と同種の食い違いを最初から作らない）
    ため、本チェックはflag=Falseの情報行としてのみ扱うことを検証する。
    """

    def test_segment_xbrl_source_is_not_flagged_and_is_explicitly_recognized(self):
        latest = {
            "growth": {"rate": 0.94, "source": "segment_xbrl"},
            "segment_configured": True,
            "growth_scenarios": {"segment": {"source": "segment_xbrl", "weighted_growth": 0.94}},
        }
        flag, note, detail = check_b_source_label("/repo", latest, "PLTR")
        assert flag is False
        assert detail["displayed_source"] == "segment_xbrl"
        assert detail["real_source_guess"] == "segment_xbrl"
        # 修正前は分岐自体が無く空文字列のnoteになっていた
        # （偶然flag=Falseになるだけで明示的な認識ではなかった）。
        # 修正後は専用の説明的なnoteが付くことを確認する。
        assert "XBRL" in note

    def test_existing_segment_weighted_unconfigured_mismatch_still_flagged(self):
        """既存の[[GROWTH-SOURCE-LABEL-1]]検知ロジック（segment_weighted
        なのにsegment_configured=False）は変更なく機能する"""
        latest = {
            "growth": {"rate": 0.20, "source": "segment_weighted"},
            "segment_configured": False,
            "growth_scenarios": {"segment": {"source": "segment_config"}},
            "growth_sanity": {"recommended_g": 0.20},
        }
        flag, note, detail = check_b_source_label("/repo", latest, "NEWCO")
        assert flag is True
        assert "recommended_g" in detail["real_source_guess"]


class TestCheckCDataJumpRevenueBackwardCompat:
    """デフォルト引数（section="pl", field="revenue"）がパラメータ化前と
    完全に同一の挙動をすることを確認する（後方互換の回帰テスト）"""

    def test_default_args_still_check_revenue_with_2x_threshold(self, tmp_path):
        repo_root = str(tmp_path)
        for year, val in [(2020, 100_000_000), (2021, 400_000_000)]:
            _write_annual(str(tmp_path / "common" / "sec_data" / "data"), "TESTCO", year, "pl", "revenue", val)
        flag, jumps, vals = check_c_data_jump(repo_root, "TESTCO")
        assert flag is True
        assert len(jumps) == 1
        assert "4.00x" in jumps[0]

    def test_default_args_no_flag_within_2x_band(self, tmp_path):
        repo_root = str(tmp_path)
        for year, val in [(2020, 100_000_000), (2021, 150_000_000)]:
            _write_annual(str(tmp_path / "common" / "sec_data" / "data"), "TESTCO", year, "pl", "revenue", val)
        flag, jumps, vals = check_c_data_jump(repo_root, "TESTCO")
        assert flag is False
        assert jumps == []

    def test_default_down_ratio_is_symmetric_half(self, tmp_path):
        """down_ratio省略時は1/jump_ratio（デフォルト2.0倍→0.5倍）で対称に判定されること"""
        repo_root = str(tmp_path)
        for year, val in [(2020, 100_000_000), (2021, 49_000_000)]:
            _write_annual(str(tmp_path / "common" / "sec_data" / "data"), "TESTCO", year, "pl", "revenue", val)
        flag, jumps, vals = check_c_data_jump(repo_root, "TESTCO")
        assert flag is True  # 0.49 <= 0.5


class TestCheckCDataJumpFieldParameterization:
    """section/field引数でRevenue以外のフィールド（売上総利益・CapEx）を
    チェックできることを確認する"""

    def test_gross_profit_field_with_custom_thresholds(self, tmp_path):
        repo_root = str(tmp_path)
        for year, val in [(2023, 10_000_000), (2024, 60_000_000)]:
            _write_annual(str(tmp_path / "common" / "sec_data" / "data"), "TESTCO", year, "pl", "gross_profit", val)
        flag, jumps, vals = check_c_data_jump(
            repo_root, "TESTCO", section="pl", field="gross_profit", jump_ratio=5.0, down_ratio=0.2,
        )
        assert flag is True
        assert "6.00x" in jumps[0]

    def test_gross_profit_not_flagged_by_revenue_default_args(self, tmp_path):
        """フィールド未指定（デフォルトrevenue）で呼ぶとgross_profitの変化は無視されること"""
        repo_root = str(tmp_path)
        for year, val in [(2023, 10_000_000), (2024, 60_000_000)]:
            _write_annual(str(tmp_path / "common" / "sec_data" / "data"), "TESTCO", year, "pl", "gross_profit", val)
        flag, jumps, vals = check_c_data_jump(repo_root, "TESTCO")
        assert flag is False

    def test_capex_field_asymmetric_down_ratio_not_reciprocal_of_up_ratio(self, tmp_path):
        """CapExの下振れ閾値0.15は上振れ8.0の逆数0.125とは異なる非対称な値であり、
        down_ratio引数で明示指定しないと正しく判定できないことを確認する"""
        repo_root = str(tmp_path)
        for year, val in [(2023, 100_000_000), (2024, 13_000_000)]:  # ratio=0.13 (< 0.15, > 0.125)
            _write_annual(str(tmp_path / "common" / "sec_data" / "data"), "TESTCO", year, "cf",
                           "capital_expenditure", val)
        flag_explicit, jumps_explicit, _ = check_c_data_jump(
            repo_root, "TESTCO", section="cf", field="capital_expenditure", jump_ratio=8.0, down_ratio=0.15,
        )
        assert flag_explicit is True  # 0.13 <= 0.15

        flag_reciprocal, jumps_reciprocal, _ = check_c_data_jump(
            repo_root, "TESTCO", section="cf", field="capital_expenditure", jump_ratio=8.0,
        )
        assert flag_reciprocal is False  # 0.13 > 0.125（down_ratio省略時の対称値では発火しない）

    def test_capex_upside_jump_detected(self, tmp_path):
        repo_root = str(tmp_path)
        for year, val in [(2023, 2_761_000), (2024, 34_245_000)]:  # ALAB実データ相当、比率約12.4x
            _write_annual(str(tmp_path / "common" / "sec_data" / "data"), "TESTCO", year, "cf",
                           "capital_expenditure", val)
        flag, jumps, _ = check_c_data_jump(
            repo_root, "TESTCO", section="cf", field="capital_expenditure", jump_ratio=8.0, down_ratio=0.15,
        )
        assert flag is True
        assert "12.4" in jumps[0]

    def test_negative_value_transition_is_flagged(self, tmp_path):
        """売上総利益がマイナスに転じるケース（RCAT実例相当）も比率が
        down_ratio以下になり検知されることを確認する"""
        repo_root = str(tmp_path)
        for year, val in [(2022, 925_515), (2023, -336_795)]:
            _write_annual(str(tmp_path / "common" / "sec_data" / "data"), "TESTCO", year, "pl", "gross_profit", val)
        flag, jumps, _ = check_c_data_jump(
            repo_root, "TESTCO", section="pl", field="gross_profit", jump_ratio=5.0, down_ratio=0.2,
        )
        assert flag is True

    def test_zero_denominator_skipped(self, tmp_path):
        repo_root = str(tmp_path)
        for year, val in [(2022, 0), (2023, 5_000_000)]:
            _write_annual(str(tmp_path / "common" / "sec_data" / "data"), "TESTCO", year, "cf",
                           "capital_expenditure", val)
        flag, jumps, _ = check_c_data_jump(
            repo_root, "TESTCO", section="cf", field="capital_expenditure", jump_ratio=8.0, down_ratio=0.15,
        )
        assert flag is False
        assert jumps == []


class TestCheckFEpsAnalyzerDelta:
    """F: components.per(GAAP PER)とcomponents.per_adjusted(調整後PER)の乖離チェック
    （[[SCREENING-SIGNAL-INTEGRATION-EPIC-1]]元EPS-ANALYZER-INTEGRATE-1）"""

    def test_flags_when_delta_at_least_10x(self):
        latest = {"components": {"per": 14.0, "per_adjusted": 3.5}}
        flag, reason, detail = check_f_eps_analyzer_delta(latest)
        assert flag is True
        assert "-10.5" in reason
        assert detail["delta"] == 3.5 - 14.0

    def test_no_flag_when_delta_below_10x(self):
        latest = {"components": {"per": 14.083194, "per_adjusted": 12.08}}
        flag, reason, detail = check_f_eps_analyzer_delta(latest)
        assert flag is False
        assert reason == ""

    def test_no_flag_when_per_missing(self):
        latest = {"components": {"per_adjusted": 12.08}}
        flag, reason, detail = check_f_eps_analyzer_delta(latest)
        assert flag is False
        assert detail["delta"] is None

    def test_no_flag_when_per_not_positive(self):
        latest = {"components": {"per": -5.0, "per_adjusted": 20.0}}
        flag, reason, detail = check_f_eps_analyzer_delta(latest)
        assert flag is False
        assert detail["delta"] is None

    def test_no_flag_when_components_missing(self):
        flag, reason, detail = check_f_eps_analyzer_delta({})
        assert flag is False
        assert detail == {"per_gaap": None, "per_adjusted": None, "delta": None}


class TestCheckGRiceEfficiency:
    """G: rice.base.rice<1.0(低効率)チェック。TANUKI SCORE=BUYとの組み合わせも記録
    （[[SCREENING-SIGNAL-INTEGRATION-EPIC-1]]元RICE-INTEGRATE-1）"""

    def test_flags_low_efficiency_below_1(self):
        latest = {"rice": {"base": {"rice": 0.652}, "bear": {"rice": 0.4}, "bull": {"rice": 0.9}},
                   "tanuki_score": "HOLD"}
        flag, reason, detail = check_g_rice_efficiency(latest)
        assert flag is True
        assert "0.652" in reason
        assert detail["buy_and_low_efficiency"] is False

    def test_flags_and_notes_buy_combination(self):
        """ADBE実データ相当（RICE=0.937<1.0かつTANUKI SCORE=BUY）"""
        latest = {"rice": {"base": {"rice": 0.937}, "bear": {"rice": 0.652}, "bull": {"rice": 1.124}},
                   "tanuki_score": "BUY"}
        flag, reason, detail = check_g_rice_efficiency(latest)
        assert flag is True
        assert detail["buy_and_low_efficiency"] is True
        assert "BUY" in reason

    def test_no_flag_when_rice_at_or_above_1(self):
        latest = {"rice": {"base": {"rice": 1.0}}, "tanuki_score": "BUY"}
        flag, reason, detail = check_g_rice_efficiency(latest)
        assert flag is False
        assert reason == ""

    def test_unavailable_rice_reports_reason_without_flag(self):
        latest = {"rice": {"available": False}, "tanuki_score": "BUY"}
        flag, reason, detail = check_g_rice_efficiency(latest)
        assert flag is False
        assert reason == "RICE計算不可（Revenue/CapExデータ不足等）"
        assert detail["rice_base"] is None

    def test_no_rice_key_at_all(self):
        flag, reason, detail = check_g_rice_efficiency({"tanuki_score": "BUY"})
        assert flag is False
        assert reason == "RICE計算不可（Revenue/CapExデータ不足等）"


class TestCheckHAnalystVsIv:
    """H: components.analyst_vs_ivの絶対値が50pt以上の乖離チェック
    （[[SCREENING-SIGNAL-INTEGRATION-EPIC-1]]元ANALYST-VS-IV-INTEGRATE-1）"""

    def test_flags_positive_divergence_as_tanuki_bearish(self):
        """analyst_target>IVの場合、TANUKIがアナリストより弱気と判定する
        （AAPL実データ相当: analyst_vs_iv=+166.5）"""
        latest = {"components": {"analyst_vs_iv": 166.5, "analyst_target_median": 335.0,
                                  "current_price": 250.0}}
        flag, reason, detail = check_h_analyst_vs_iv(latest)
        assert flag is True
        assert "弱気" in reason
        assert "+166.5" in reason

    def test_flags_negative_divergence_as_tanuki_bullish(self):
        """analyst_target<IVの場合、TANUKIがアナリストより強気と判定する
        （NVDA実データ相当: analyst_vs_iv=-51.5）"""
        latest = {"components": {"analyst_vs_iv": -51.5, "analyst_target_median": 315.0,
                                  "current_price": 300.0}}
        flag, reason, detail = check_h_analyst_vs_iv(latest)
        assert flag is True
        assert "強気" in reason

    def test_no_flag_below_threshold(self):
        latest = {"components": {"analyst_vs_iv": -39.6}}
        flag, reason, detail = check_h_analyst_vs_iv(latest)
        assert flag is False
        assert reason == ""

    def test_no_flag_when_analyst_vs_iv_missing(self):
        flag, reason, detail = check_h_analyst_vs_iv({"components": {}})
        assert flag is False
        assert detail["analyst_vs_iv"] is None
