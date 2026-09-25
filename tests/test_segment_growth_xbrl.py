"""
tests/test_segment_growth_xbrl.py

[[SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-IDEA-1]]案①（2026-09-18）:
calculator/segment_growth_xbrl.pyの回帰テスト。実ファイル・実SEC API
を使わずモックデータで、以下を検証する:
  - 複数セグメント銘柄（PLTR型）の加重成長率算出とweight合計=1.0
  - 単一セグメント銘柄（APP型）の全社売上YoYへのフォールバック
  - TSLA型（導出セグメント: 全社売上−個別セグメント）の特殊計算
  - 直近最大4四半期の平均によるYoY平滑化（単一四半期のブレを均す）
  - calculate_fcf_cagr()と同一のgrowth_floor 15%/growth_cap 50%
    クリップが最終加重平均値に適用されること（NVDA/APP/CRWVで
    実データ検証時に単一四半期採用が異常値検知anomaly_detection
    〈乖離率>1000%〉をFAILさせた実例の再発防止）
  - weight基準の複数四半期集計がscale mismatchを起こさないこと
  - データ欠損時にNoneを返す（部分採用しない）フェイルセーフ

実行方法:
    python -m pytest tests/test_segment_growth_xbrl.py -v
"""

import json
import os
import sys

import pytest

_CALC_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation", "calculator")
)
if _CALC_DIR not in sys.path:
    sys.path.insert(0, _CALC_DIR)

import segment_growth_xbrl as sgx  # noqa: E402


def _write_layer2(tmp_path, ticker, kpis):
    kpi_dir = tmp_path / "docs" / "portfolio" / "tail" / "data" / "kpi"
    kpi_dir.mkdir(parents=True, exist_ok=True)
    data = {
        "ticker": ticker,
        "kpis": {name: {"unit": "USD", "data": series} for name, series in kpis.items()},
    }
    (kpi_dir / f"{ticker}_layer2.json").write_text(json.dumps(data), encoding="utf-8")


def _series(pairs):
    return [{"quarter": q, "value": v, "filed": "2026-01-01"} for q, v in pairs]


class TestSmoothedGrowth:
    def test_averages_up_to_four_most_recent_yoy_ratios(self):
        """直近4件のYoY比率を単純平均する（新しい順に4件まで）"""
        series = {
            "2026Q2": 140, "2025Q2": 100,   # YoY +40%
            "2026Q1": 130, "2025Q1": 100,   # YoY +30%
            "2025Q3": 120, "2024Q3": 100,   # YoY +20%
        }
        g = sgx._smoothed_growth(series)
        assert abs(g - 0.30) < 1e-9

    def test_fewer_than_min_periods_returns_none(self):
        """有効なYoYペアが最低件数(2)未満ならNoneを返す"""
        series = {"2026Q2": 140, "2025Q2": 100}  # 有効ペア1件のみ
        assert sgx._smoothed_growth(series) is None

    def test_missing_quarter4_data_does_not_block_smoothing(self):
        """10-Qのみのデータで第4四半期が構造的に欠落していても、
        Q1-Q3のみで有効な平滑化ができる（TSLA実データで発生した状況）"""
        series = {
            "2026Q2": 110, "2025Q2": 100,
            "2026Q1": 108, "2025Q1": 100,
            "2025Q3": 105, "2024Q3": 100,
            # 2025Q4/2024Q4は存在しない
        }
        g = sgx._smoothed_growth(series)
        assert g is not None
        expected = ((110 - 100) / 100 + (108 - 100) / 100 + (105 - 100) / 100) / 3
        assert abs(g - expected) < 1e-9

    def test_ignores_quarters_without_prior_year_pair(self):
        """前年同期データが無い四半期はスキップし、有効なペアのみで
        平均する（前年同期の無い四半期が混在していても壊れない）"""
        series = {
            "2026Q2": 140, "2025Q2": 100,
            "2026Q1": 130, "2025Q1": 100,
            "2023Q4": 90,  # 対応する前年(2022Q4)がなくペア不成立、他とも無関係
        }
        g = sgx._smoothed_growth(series)
        assert abs(g - 0.35) < 1e-9  # (0.40+0.30)/2


class TestClipping:
    def test_within_range_not_clipped(self):
        assert sgx._clip_growth(0.30) == 0.30

    def test_above_cap_clipped_to_50pct(self):
        assert sgx._clip_growth(1.20) == 0.50

    def test_below_floor_clipped_to_15pct(self):
        assert sgx._clip_growth(0.05) == 0.15


class TestComputeMultiSegment:
    @pytest.fixture(autouse=True)
    def _no_layer3_reference(self, monkeypatch):
        """遅延判定の参照四半期（Layer3全社売上）を実データに依存させない
        （参照なし=遅延0扱い）。PLTRはTSLAと違い算出自体にLayer3を使わない"""
        monkeypatch.setattr(sgx, "_total_revenue_series", lambda ticker: {})

    def test_weighted_growth_and_weight_sum_to_one(self, tmp_path):
        """2セグメント銘柄（PLTR型）: weight合計=1.0、raw_weighted_growth
        がΣ(weight×smoothed_growth)と一致する（クリップ前）"""
        _write_layer2(tmp_path, "PLTR", {
            "Government売上": _series([
                ("2026Q2", 990032000), ("2025Q2", 553000000),
                ("2026Q1", 950000000), ("2025Q1", 520000000),
                ("2025Q3", 850000000), ("2024Q3", 700000000),
            ]),
            "Commercial売上": _series([
                ("2026Q2", 945432000), ("2025Q2", 450000000),
                ("2026Q1", 900000000), ("2025Q1", 420000000),
                ("2025Q3", 800000000), ("2024Q3", 650000000),
            ]),
        })
        result = sgx.compute_xbrl_segment_growth("PLTR", repo_root=str(tmp_path))
        assert result is not None
        segs = result["segments"]
        weight_sum = sum(v["weight"] for v in segs.values())
        assert abs(weight_sum - 1.0) < 1e-9
        expected_raw = sum(v["weight"] * v["growth"] for v in segs.values())
        assert abs(result["raw_weighted_growth"] - expected_raw) < 1e-9

    def test_extreme_growth_clipped_at_50pct_final_value(self, tmp_path):
        """実データで発見された事象の再現: 単一セグメントの実績成長率が
        非常に高くても、DCFへ渡すweighted_growthは50%でクリップされる
        （個別セグメントのgrowth自体はクリップしない）"""
        _write_layer2(tmp_path, "PLTR", {
            "Government売上": _series([
                ("2026Q2", 2000000000), ("2025Q2", 1000000000),
                ("2026Q1", 2000000000), ("2025Q1", 1000000000),
            ]),
            "Commercial売上": _series([
                ("2026Q2", 2000000000), ("2025Q2", 1000000000),
                ("2026Q1", 2000000000), ("2025Q1", 1000000000),
            ]),
        })
        result = sgx.compute_xbrl_segment_growth("PLTR", repo_root=str(tmp_path))
        assert result is not None
        assert result["raw_weighted_growth"] == 1.0  # 実績は+100%
        assert result["weighted_growth"] == 0.50      # DCFへはクリップ後の値
        assert result["clipped"] is True
        for seg in result["segments"].values():
            assert seg["growth"] == 1.0  # 表示用の個別セグメント値は非クリップ

    def test_missing_data_for_one_segment_returns_none(self, tmp_path):
        """一方のセグメントで平滑化に必要な最低件数のYoYが得られない場合、
        部分採用せずNoneを返す"""
        _write_layer2(tmp_path, "PLTR", {
            "Government売上": _series([("2026Q2", 990032000), ("2025Q2", 553000000)]),
            "Commercial売上": _series([("2026Q2", 945432000)]),  # 前年同期なし
        })
        result = sgx.compute_xbrl_segment_growth("PLTR", repo_root=str(tmp_path))
        assert result is None

    def test_layer2_file_missing_returns_none(self, tmp_path):
        result = sgx.compute_xbrl_segment_growth("PLTR", repo_root=str(tmp_path))
        assert result is None


class TestComputeSingleSegment:
    def test_single_segment_ticker_uses_smoothed_total_revenue_yoy(self, monkeypatch):
        """単一セグメント銘柄（APP型）は全社売上の平滑化YoYを使い、weight=1.0"""
        fake_series = {
            "2026Q2": 1923686000, "2025Q2": 1258754000,
            "2026Q1": 1842449000, "2025Q1": 1158974000,
        }
        monkeypatch.setattr(sgx, "_total_revenue_series", lambda ticker: fake_series)
        result = sgx.compute_xbrl_segment_growth("APP")
        assert result is not None
        assert result["method"] == "total_revenue_yoy_smoothed"
        seg = result["segments"]["Software Platform"]
        assert seg["weight"] == 1.0
        g1 = (1923686000 - 1258754000) / 1258754000
        g2 = (1842449000 - 1158974000) / 1158974000
        assert abs(seg["growth"] - (g1 + g2) / 2) < 1e-9

    def test_single_segment_insufficient_data_returns_none(self, monkeypatch):
        monkeypatch.setattr(sgx, "_total_revenue_series", lambda ticker: {"2026Q2": 100})
        result = sgx.compute_xbrl_segment_growth("APP")
        assert result is None


class TestComputeTslaDerivedSegment:
    def test_automotive_derived_by_subtraction_and_weights_sum_to_one(self, monkeypatch, tmp_path):
        """TSLA型: Automotive & Services and Otherは全社売上−Energyで
        導出され、weight合計は常に1.0になる"""
        _write_layer2(tmp_path, "TSLA", {
            "Energy Generation & Storage売上": _series([
                ("2026Q2", 3139000000), ("2025Q2", 2789000000),
                ("2026Q1", 2820000000), ("2025Q1", 2730000000),
                ("2025Q3", 2600000000), ("2024Q3", 2400000000),
            ]),
        })
        fake_total = {
            "2026Q2": 28236000000, "2025Q2": 22496000000,
            "2026Q1": 22387000000, "2025Q1": 19335000000,
            "2025Q3": 28095000000, "2024Q3": 25182000000,
        }
        monkeypatch.setattr(sgx, "_total_revenue_series", lambda ticker: fake_total)
        result = sgx.compute_xbrl_segment_growth("TSLA", repo_root=str(tmp_path))
        assert result is not None
        segs = result["segments"]
        assert set(segs.keys()) == {"Automotive & Services and Other", "Energy Generation and Storage"}
        weight_sum = sum(v["weight"] for v in segs.values())
        assert abs(weight_sum - 1.0) < 1e-9

    def test_energy_series_missing_returns_none(self, tmp_path):
        _write_layer2(tmp_path, "TSLA", {})
        result = sgx.compute_xbrl_segment_growth("TSLA", repo_root=str(tmp_path))
        assert result is None

    def test_total_revenue_unavailable_returns_none(self, monkeypatch, tmp_path):
        _write_layer2(tmp_path, "TSLA", {
            "Energy Generation & Storage売上": _series([
                ("2026Q2", 3139000000), ("2025Q2", 2789000000),
            ]),
        })
        monkeypatch.setattr(sgx, "_total_revenue_series", lambda ticker: {})
        result = sgx.compute_xbrl_segment_growth("TSLA", repo_root=str(tmp_path))
        assert result is None


class TestUnsupportedTicker:
    def test_ticker_not_in_any_mapping_returns_none(self, tmp_path):
        """ADBE/CELH等、対応表に無い銘柄はNoneを返す（既存の手動config
        経路へフォールバックさせるための正常な挙動）"""
        result = sgx.compute_xbrl_segment_growth("ADBE", repo_root=str(tmp_path))
        assert result is None

    def test_exception_inside_computation_is_swallowed(self, monkeypatch):
        """内部で予期しない例外が起きてもフェイルセーフでNoneを返す
        （パイプライン全体を止めない）"""
        def _raise(*a, **k):
            raise RuntimeError("simulated failure")
        monkeypatch.setattr(sgx, "_compute_single_segment", _raise)
        result = sgx.compute_xbrl_segment_growth("APP")
        assert result is None


class TestRecentWeightBases:
    def test_sums_recent_common_quarters(self):
        series_map = {
            "a": {"2026Q2": 10, "2026Q1": 10, "2025Q3": 10, "2025Q2": 10, "2024Q3": 99},
            "b": {"2026Q2": 5, "2026Q1": 5, "2025Q3": 5, "2025Q2": 5, "2024Q3": 99},
        }
        bases = sgx._recent_weight_bases(series_map)
        # 直近4件（2026Q2,2026Q1,2025Q3,2025Q2）のみ合計、2024Q3は含めない
        assert bases["a"] == 40
        assert bases["b"] == 20

    def test_fewer_than_max_common_quarters_uses_whats_available(self):
        series_map = {
            "a": {"2026Q2": 10, "2026Q1": 10},
            "b": {"2026Q2": 5, "2026Q1": 5},
        }
        bases = sgx._recent_weight_bases(series_map)
        assert bases["a"] == 20
        assert bases["b"] == 10


class TestXbrlSegmentStaleness:
    """2026-09-25: 基準四半期がLayer3全社売上の最新四半期から2四半期以上
    遅れている場合、compute_xbrl_segment_growth()はNone（次順位ソースへ
    フォールバック）を返し、get_xbrl_segment_status()は遅延四半期数を返す。
    比較は両者とも暦年ラベル（NVDAのlayer2 "2026Q3"は2026-07-26期末）"""

    _PLTR_SERIES = {
        "Government売上": [("2025Q4", 900), ("2024Q4", 600), ("2025Q3", 850), ("2024Q3", 700)],
        "Commercial売上": [("2025Q4", 800), ("2024Q4", 500), ("2025Q3", 750), ("2024Q3", 600)],
    }

    def _write(self, tmp_path):
        _write_layer2(tmp_path, "PLTR", {k: _series(v) for k, v in self._PLTR_SERIES.items()})

    def test_two_quarter_lag_is_stale_and_falls_back(self, tmp_path, monkeypatch):
        self._write(tmp_path)
        monkeypatch.setattr(sgx, "_total_revenue_series", lambda ticker: {"2026Q2": 1, "2026Q1": 1})
        status = sgx.get_xbrl_segment_status("PLTR", repo_root=str(tmp_path))
        assert status["quarter"] == "2025Q4"
        assert status["reference_quarter"] == "2026Q2"
        assert status["lag_quarters"] == 2
        assert status["stale"] is True
        assert sgx.compute_xbrl_segment_growth("PLTR", repo_root=str(tmp_path)) is None

    def test_one_quarter_lag_is_still_used(self, tmp_path, monkeypatch):
        self._write(tmp_path)
        monkeypatch.setattr(sgx, "_total_revenue_series", lambda ticker: {"2026Q1": 1})
        status = sgx.get_xbrl_segment_status("PLTR", repo_root=str(tmp_path))
        assert status["lag_quarters"] == 1
        assert status["stale"] is False
        assert sgx.compute_xbrl_segment_growth("PLTR", repo_root=str(tmp_path)) is not None

    def test_quarter_index_crosses_year_boundary(self):
        assert sgx._quarter_index("2026Q1") - sgx._quarter_index("2025Q4") == 1
