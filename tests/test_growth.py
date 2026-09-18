"""
tests/test_growth.py

calculator/growth.py の get_segment_growth() 回帰テスト（GROWTH-SOURCE-LABEL-1）
と、FCFSeriesアクセサ導入（[[GROWTH-FCFSERIES-ACCESSOR-ADOPT-1]]）の
単体テスト。
"""

import logging
import sys
import os
from unittest.mock import patch

import pytest

_PIPELINE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation")
)
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

from calculator import growth as growth_module  # noqa: E402
from common.sec_data.contracts import ContractViolation  # noqa: E402


def test_segment_detail_source_reflects_growth_override():
    """recommended_g自動注入（growth_override）時はsourceがgrowth_overrideになる"""
    fake_config = {
        "enabled": True,
        "weighted_growth": 0.18,
        "source": "growth_override",
    }
    with patch.object(growth_module, "_get_segment_growth_from_config", return_value=fake_config):
        with patch.object(growth_module, "HAS_SEGMENT_CONFIG", True):
            result = growth_module.get_segment_growth("DUMMY")

    assert result is not None
    assert result.segment_detail["source"] == "growth_override"


def test_segment_detail_source_reflects_segment_config():
    """10-Kセグメント内訳が設定済みの場合はsourceがsegment_configになる"""
    fake_config = {
        "enabled": True,
        "weighted_growth": 0.12,
        "fiscal_year": 2025,
        "source": "segment_config",
    }
    with patch.object(growth_module, "_get_segment_growth_from_config", return_value=fake_config):
        with patch.object(growth_module, "HAS_SEGMENT_CONFIG", True):
            result = growth_module.get_segment_growth("DUMMY")

    assert result is not None
    assert result.segment_detail["source"] == "segment_config"


# ─────────────────────────────────────────────
# [[SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-IDEA-1]]案①（2026-09-18）
# XBRLセグメント決定論的算出が手動configより優先されること、
# 対応不可銘柄では既存の手動config経路へ正しくフォールバックすること
# を検証する。
# ─────────────────────────────────────────────

class TestXbrlSegmentGrowthPriority:
    def test_xbrl_result_takes_priority_over_manual_config(self):
        """XBRL側が算出可能な場合、手動config（segment_config.json）より
        優先され、source="segment_xbrl"になる"""
        fake_xbrl = {
            "segments": {"Government": {"weight": 0.5, "growth": 0.79},
                         "Commercial": {"weight": 0.5, "growth": 1.10}},
            "weighted_growth": 0.50,
            "raw_weighted_growth": 0.945,
            "clipped": True,
            "growth_floor": 0.15,
            "growth_cap": 0.50,
            "quarter": "2026Q2",
            "method": "segment_xbrl_yoy_smoothed",
        }
        fake_manual_config = {"enabled": True, "weighted_growth": 0.20, "source": "segment_config"}
        with patch.object(growth_module, "HAS_XBRL_SEGMENT_GROWTH", True), \
             patch.object(growth_module, "compute_xbrl_segment_growth", return_value=fake_xbrl), \
             patch.object(growth_module, "_get_segment_growth_from_config", return_value=fake_manual_config), \
             patch.object(growth_module, "HAS_SEGMENT_CONFIG", True):
            result = growth_module.get_segment_growth("PLTR")

        assert result is not None
        assert result.source == "segment_xbrl"
        assert result.rate == pytest.approx(0.50)  # クリップ後（DCFへ渡す値）
        assert result.segment_detail["source"] == "segment_xbrl"
        assert result.segment_detail["quarter"] == "2026Q2"
        assert result.segment_detail["raw_weighted_growth"] == pytest.approx(0.945)
        assert result.segment_detail["clipped"] is True

    def test_falls_back_to_manual_config_when_xbrl_returns_none(self):
        """XBRL側が対応不可（None）の銘柄は、既存の手動config経路
        （source="segment_config"）へ変更なくフォールバックする"""
        fake_manual_config = {"enabled": True, "weighted_growth": 0.12, "source": "segment_config"}
        with patch.object(growth_module, "HAS_XBRL_SEGMENT_GROWTH", True), \
             patch.object(growth_module, "compute_xbrl_segment_growth", return_value=None), \
             patch.object(growth_module, "_get_segment_growth_from_config", return_value=fake_manual_config), \
             patch.object(growth_module, "HAS_SEGMENT_CONFIG", True):
            result = growth_module.get_segment_growth("ADBE")

        assert result is not None
        assert result.source == "segment_weighted"
        assert result.segment_detail["source"] == "segment_config"

    def test_xbrl_unavailable_module_falls_back_unchanged(self):
        """segment_growth_xbrl.py自体がimportできない環境
        （HAS_XBRL_SEGMENT_GROWTH=False）でも既存動作を維持する"""
        fake_manual_config = {"enabled": True, "weighted_growth": 0.15, "source": "segment_config"}
        with patch.object(growth_module, "HAS_XBRL_SEGMENT_GROWTH", False), \
             patch.object(growth_module, "_get_segment_growth_from_config", return_value=fake_manual_config), \
             patch.object(growth_module, "HAS_SEGMENT_CONFIG", True):
            result = growth_module.get_segment_growth("SOFI")

        assert result is not None
        assert result.source == "segment_weighted"


# ─────────────────────────────────────────────
# [[GROWTH-FCFSERIES-ACCESSOR-ADOPT-1]]
# calculate_fcf_cagr()/determine_growth_rate()がFCFSeriesアクセサ
# （.newest/.oldest）を用いた順序検証を追加したことのテスト。
# ─────────────────────────────────────────────

# _filter_positive_with_dates: ペア整合性テスト

class TestFilterPositiveWithDates:
    def test_pair_integrity_when_middle_element_excluded(self):
        """5件中2件目が負の値で除外されるケースで、フィルタ後も
        残った値と日付が正しく対応していることを確認する
        （[[GROWTH-FCFSERIES-ACCESSOR-ADOPT-1]]依頼文の具体例）。"""
        values = [100.0, -50.0, 300.0, 400.0, 500.0]
        dates = [2025, 2024, 2023, 2022, 2021]

        filtered_values, filtered_dates = growth_module._filter_positive_with_dates(values, dates)

        assert filtered_values == [100.0, 300.0, 400.0, 500.0]
        # 2件目(-50.0, 2024)がペアで除外され、3件目以降の値と日付が
        # ズレずに対応していること
        assert filtered_dates == [2025, 2023, 2022, 2021]
        assert len(filtered_values) == len(filtered_dates)

    def test_none_dates_passthrough(self):
        """dates=Noneの場合、フィルタ後もdates側はNoneのまま
        （日付なし経路・後方互換）。"""
        values = [100.0, -50.0, 300.0]
        filtered_values, filtered_dates = growth_module._filter_positive_with_dates(values, None)
        assert filtered_values == [100.0, 300.0]
        assert filtered_dates is None

    def test_all_positive_no_exclusion(self):
        values = [500.0, 400.0, 300.0]
        dates = [2025, 2024, 2023]
        filtered_values, filtered_dates = growth_module._filter_positive_with_dates(values, dates)
        assert filtered_values == values
        assert filtered_dates == dates


# calculate_fcf_cagr: アクセサ経由 vs 直接インデックス方式の一致確認

class TestCalculateFcfCagrAccessorConsistency:
    def test_correct_order_matches_direct_index_result(self):
        """正しい新しい順のfcf_list+fcf_datesを渡した場合、
        .oldest/.newestアクセサ経由の計算結果が、fcf_dates=None時の
        従来の直接インデックス方式と完全一致すること。"""
        fcf_list = [500.0, 400.0, 300.0, 200.0, 100.0]
        fcf_dates = [2025, 2024, 2023, 2022, 2021]  # fcf_list[0]=直近と対応

        result_with_dates = growth_module.calculate_fcf_cagr(fcf_list, fcf_dates=fcf_dates)
        result_without_dates = growth_module.calculate_fcf_cagr(fcf_list)

        assert result_with_dates is not None
        assert result_without_dates is not None
        assert result_with_dates.rate == result_without_dates.rate
        assert result_with_dates.cagr_detail["start_value"] == result_without_dates.cagr_detail["start_value"]
        assert result_with_dates.cagr_detail["end_value"] == result_without_dates.cagr_detail["end_value"]
        # start=最古(100.0)・end=直近(500.0)であることも明示的に確認
        assert result_with_dates.cagr_detail["start_value"] == 100.0
        assert result_with_dates.cagr_detail["end_value"] == 500.0

    def test_string_dates_ttm_style_also_validated(self):
        """TTM経路のttm_end文字列（ISO日付）形式でも検証が機能すること。"""
        fcf_list = [300.0, 200.0, 100.0]
        fcf_dates = ["2026-03-31", "2025-03-31", "2024-03-31"]

        result = growth_module.calculate_fcf_cagr(fcf_list, fcf_dates=fcf_dates)
        assert result is not None
        assert result.cagr_detail["start_value"] == 100.0
        assert result.cagr_detail["end_value"] == 300.0

    def test_none_dates_backward_compatible(self):
        """fcf_dates未指定（デフォルトNone）の場合、検証をスキップして
        従来通り直接インデックスで計算すること（後方互換）。"""
        fcf_list = [500.0, 400.0, 300.0, 200.0, 100.0]
        result = growth_module.calculate_fcf_cagr(fcf_list)
        assert result is not None
        assert result.cagr_detail["start_value"] == 100.0
        assert result.cagr_detail["end_value"] == 500.0


class TestCalculateFcfCagrOrderViolationWarnFallback:
    def test_reversed_dates_logs_warn_and_falls_back_without_raising(self, caplog):
        """fcf_listとfcf_datesの順序が食い違う（fcf_list[0]が実は最古）
        場合、ContractViolationがWARNとしてログされ、例外を送出せず
        従来の直接インデックス方式にフォールバックして結果を返すこと。
        本番パイプラインをクラッシュさせない設計（CHECK-32〜40と同型）。"""
        fcf_list = [100.0, 200.0, 300.0, 400.0, 500.0]
        # datesが昇順（新しい順のはずのfcf_list[0]=100に最古の日付2021が
        # 対応してしまっている）→ 順序規約違反
        fcf_dates = [2021, 2022, 2023, 2024, 2025]

        with caplog.at_level(logging.WARNING):
            result = growth_module.calculate_fcf_cagr(fcf_list, fcf_dates=fcf_dates)

        # 例外を送出せず結果が返ること
        assert result is not None
        # WARNログが出力されていること
        assert "FCFSeries順序規約違反" in caplog.text
        # フォールバック値は従来の直接インデックス方式と一致する
        # （recent_fcfs[-1]=500.0, recent_fcfs[0]=100.0）
        assert result.cagr_detail["start_value"] == 500.0
        assert result.cagr_detail["end_value"] == 100.0

    def test_violation_does_not_raise_contractviolation(self):
        """呼び出し元にContractViolationが伝播しないこと（明示的確認）。"""
        fcf_list = [100.0, 200.0, 300.0]
        fcf_dates = [2021, 2022, 2023]  # 昇順 = 違反
        try:
            result = growth_module.calculate_fcf_cagr(fcf_list, fcf_dates=fcf_dates)
        except ContractViolation:
            pytest.fail("ContractViolationが呼び出し元に伝播してはならない（WARNのみで継続）")
        assert result is not None

    def test_mixed_type_dates_logs_warn_and_falls_back_without_raising(self, caplog):
        """年次経路のperiodはティッカーによりint/str型が混在しうる
        （実データ確認: LOARは"period"を文字列で保持、AAPLはint）。
        同一ティッカー内で型が揃わない場合のTypeErrorも、
        ContractViolationと同様にWARNのみで処理を継続すること。"""
        fcf_list = [500.0, 400.0, 300.0]
        fcf_dates = [2025, "2024", 2023]  # int/str混在 → 比較でTypeError

        with caplog.at_level(logging.WARNING):
            result = growth_module.calculate_fcf_cagr(fcf_list, fcf_dates=fcf_dates)

        assert result is not None
        assert "型エラー" in caplog.text
        # フォールバック値は従来の直接インデックス方式と一致する
        assert result.cagr_detail["start_value"] == 300.0
        assert result.cagr_detail["end_value"] == 500.0


# determine_growth_rate: fcf_datesの伝播確認

class TestDetermineGrowthRateFcfDatesPropagation:
    def test_fcf_dates_propagates_to_cagr_calc(self):
        """determine_growth_rate()に渡したfcf_datesがcalculate_fcf_cagr()
        まで正しく伝播すること（segment_config未設定のダミーティッカー
        で確認）。"""
        fcf_list = [500.0, 400.0, 300.0, 200.0, 100.0]
        fcf_dates = [2025, 2024, 2023, 2022, 2021]

        result = growth_module.determine_growth_rate(
            ticker="__NOT_A_REAL_TICKER__",
            fcf_list=fcf_list,
            fcf_dates=fcf_dates,
        )
        assert result.source == "fcf_cagr"
        assert result.cagr_detail["start_value"] == 100.0
        assert result.cagr_detail["end_value"] == 500.0

    def test_fcf_dates_none_default_unaffected(self):
        """fcf_dates未指定でも従来通りfcf_cagr計算が機能すること。"""
        fcf_list = [500.0, 400.0, 300.0, 200.0, 100.0]
        result = growth_module.determine_growth_rate(
            ticker="__NOT_A_REAL_TICKER__",
            fcf_list=fcf_list,
        )
        assert result.source == "fcf_cagr"
