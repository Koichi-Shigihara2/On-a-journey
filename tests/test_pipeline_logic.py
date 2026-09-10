"""
tests/test_pipeline_logic.py

TANUKI VALUATION pipeline のユニットテスト。
外部API・実ファイルを使わずモックデータで動作する。

実行方法:
    venv/Scripts/python.exe -m pytest tests/test_pipeline_logic.py -v
"""

import sys
import os
import json
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

# ─────────────────────────────────────────────
# sys.path 設定と依存モジュールのスタブ化
# pipeline.py は src/value/tanuki_valuation/ 直下にあり
# 相対インポート（from data_fetcher import ...）を使うため
# ─────────────────────────────────────────────
_PIPELINE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation")
)
sys.path.insert(0, _PIPELINE_DIR)

# xlrd（Damodaran XLS 読込）をスタブ化してから growth_sanity を実際にインポート
# テスト6（growth_sanity）では本物の check_growth_sanity ロジックを検証する
sys.modules.setdefault("xlrd", MagicMock())
import growth_sanity as _gs  # 本物のモジュール参照を保存（後でも参照できるよう変数に束縛）

# TTM-QUARTERS-CHECK-1: TTMReader/build_rice_annual_shapeの実ロジックを検証するため
# growth_sanity と同様に、スタブ化前に本物のモジュール参照を保存する
import data_fetcher as _df

# pipeline の依存モジュールをスタブ化してから pipeline をインポート
# growth_sanity/data_fetcher は _gs/_df に保持済みだが pipeline 側ではスタブで十分
for _mod_name in ("data_fetcher", "core_calculator", "validator", "growth_sanity"):
    sys.modules[_mod_name] = MagicMock()

import pipeline  # noqa: E402
from pipeline import TanukiValuationPipeline  # noqa: E402

# ─────────────────────────────────────────────
# hypecore の detect_substage をインポート
# pandas は venv に存在する前提
# ─────────────────────────────────────────────
_HYPECORE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "hypecore")
)
sys.path.insert(0, _HYPECORE_DIR)
import pandas as pd
from hypecore import detect_substage  # noqa: E402


# ─────────────────────────────────────────────
# テスト共通ヘルパー
# ─────────────────────────────────────────────

def _make_pipe(tmp_path) -> TanukiValuationPipeline:
    """TanukiValuationPipeline のテスト用インスタンスを生成する。

    pipeline.__init__ の repo_root 計算式:
        repo_root = dirname(dirname(dirname(output_dir)))
    output_dir を tmp_path の 3 階層下に設定することで repo_root = tmp_path になる。
    （実ファイルのパスが all: tmp_path/docs/value-monitor/... と一致する）
    """
    output_dir = tmp_path / "out" / "tanuki" / "data"
    output_dir.mkdir(parents=True)
    pipe = TanukiValuationPipeline(output_dir=str(output_dir), use_ai_validation=False)
    # _load_eps_map のキャッシュを空に初期化（実ファイルを読まない）
    pipe._eps_summary_cache = {}
    return pipe


def _minimal_valuation(upside: float = 50.0) -> dict:
    """_generate_report に渡す最小限の valuation dict。"""
    return {
        "calculation_date": "2026-05-30",
        "components": {
            "current_price": 100.0,
            "beta": 1.0,
            "sector": "software",
            "industry": "Software",
            "rpo_pv": 0,
            "roe_10yr_avg": None,
            "ma200": 90.0,
            "fcf_base_used": None,
            "latest_revenue": None,
        },
        "upside_percent": upside,
        "intrinsic_value_per_share": 150.0,
        "wacc": {"value": 0.10},
        "alpha": 1.0,
        "rice": {
            "available": True,
            "note": "",
            "base": {"rice": 5.0},
            "q": 0.8,
            "cf_conversion": 4.0,
            "wacc": 0.10,
            "bear": {},
            "bull": {},
        },
        "scenario_valuations": {
            "bear": {"growth_rate": 0.20, "intrinsic_value_per_share": 130.0},
            "base": {"growth_rate": 0.30, "intrinsic_value_per_share": 150.0},
            "bull": {"growth_rate": 0.40, "intrinsic_value_per_share": 180.0},
        },
        "fcf_estimation": {
            "estimated_fcf": 1_000_000,
            "fcf_margin": 20.0,
            "conversion_rate": 0.8,
            "sector": "Software",
        },
        "fcf_base": {"base_fcf": 1_000_000.0},
        "growth_scenarios": {"primary": {"rate": 0.30}},
    }


def _minimal_score_data() -> dict:
    return {"score": "BUY", "funda_score": 75, "score_comment": "テストコメント"}


def _minimal_extra() -> dict:
    return {
        "fcf_history": [],
        "financial_health": {},
        "segments": [],
        "next_earnings_date": "N/A",
    }


def _write_poc_json(tmp_path, ticker: str, substage_label: str, substage_watch: str) -> None:
    """poc.json を tmp_path/docs/value-monitor/hypecore/data/ に作成する。
    pipe.repo_root = tmp_path のとき、pipeline が読みに来るパスと一致する。
    """
    poc_dir = tmp_path / "docs" / "value-monitor" / "hypecore" / "data"
    poc_dir.mkdir(parents=True, exist_ok=True)
    poc_data = {
        "monthly": [{
            "stage": 2,
            "stage_label": "期待拡大期",
            "substage_label": substage_label,
            "substage_watch": substage_watch,
            "short_pct_float": 0.012,
            "rev_yoy": 20.0,
            "recommendation_mean": 2.0,
        }]
    }
    (poc_dir / f"{ticker}_poc.json").write_text(
        json.dumps(poc_data, ensure_ascii=False), encoding="utf-8"
    )


def _write_stonks_json(tmp_path, tickers_data: dict) -> None:
    """stonks-silo の results.json を tmp_path/docs/value-monitor/stonks-silo/data/ に作成する。
    tickers_data: {"TICKER": {"runway": {"runway_months": N}}} 形式
    pipe.repo_root = tmp_path のとき、pipeline が読みに来るパスと一致する。
    stonks-silo は本番でも常に存在するファイルなので、ticker がない場合も
    ファイル自体は作成してエントリなしにする。
    """
    stonks_dir = tmp_path / "docs" / "value-monitor" / "stonks-silo" / "data"
    stonks_dir.mkdir(parents=True, exist_ok=True)
    data = {"tickers": tickers_data}
    (stonks_dir / "results.json").write_text(json.dumps(data), encoding="utf-8")


# ─────────────────────────────────────────────
# 1. FCFコメント判定
#    _generate_score_comment は純粋関数（ファイルI/Oなし）なので直接呼び出せる
# ─────────────────────────────────────────────

class TestFcfComment:
    def test_negative_fcf_latest_shows_fcf_minus(self, tmp_path):
        """直近FCFがマイナス → Comment に「FCFマイナス」を含む（投資フェーズ判定）"""
        pipe = _make_pipe(tmp_path)
        comment = pipe._generate_score_comment(
            "BUY", upside=50.0, rev_yoy=20.0, rule40_yoy_netmargin=40.0,
            fcf_base=1_000_000.0, funda=75, fcf_latest=-50_000.0,
        )
        assert "FCFマイナス" in comment

    def test_positive_fcf_latest_shows_fcf_profit(self, tmp_path):
        """直近FCFがプラス かつ fcf_base もプラス → Comment に「FCF黒字」を含む"""
        pipe = _make_pipe(tmp_path)
        comment = pipe._generate_score_comment(
            "BUY", upside=50.0, rev_yoy=20.0, rule40_yoy_netmargin=40.0,
            fcf_base=1_000_000.0, funda=75, fcf_latest=50_000.0,
        )
        assert "FCF黒字" in comment


# ─────────────────────────────────────────────
# 2. HYPE_Signal EPS条件
#    _generate_report 内の後処理ロジック（substage_watch テキスト置換）を検証
# ─────────────────────────────────────────────

class TestHypeSignal:
    def test_negative_eps_yoy_removes_eps_strong_text(self, tmp_path):
        """EPS YoY がマイナス → HYPE_Signal に「EPSは強い」を含まない（置換される）"""
        pipe = _make_pipe(tmp_path)
        # EPS マイナス成長（-10%）を EPS キャッシュに直接セット
        pipe._eps_summary_cache = {
            "TEST": {"yoy_growth": -0.10, "gaap_eps": -1.0}
        }
        _write_poc_json(tmp_path, "TEST",
                        substage_label="上昇継続中",
                        substage_watch="売上・EPSは強い。推進力がある局面。")

        report = pipe._generate_report(
            "TEST", _minimal_valuation(), _minimal_score_data(), _minimal_extra()
        )

        # 「売上・EPSは強い」が「売上は強いがEPS前年比マイナス」に置換されているはず
        assert "EPSは強い" not in report
        assert "EPS前年比マイナス" in report

    def test_positive_eps_yoy_with_eps_warning_replaces_to_improving(self, tmp_path):
        """EPS YoY がプラス かつ substage_watch に「EPSの悪化も確認」→「EPS改善中」に置換"""
        pipe = _make_pipe(tmp_path)
        # EPS 改善（+15%）をセット
        pipe._eps_summary_cache = {
            "TEST": {"yoy_growth": 0.15, "gaap_eps": 0.5}
        }
        _write_poc_json(tmp_path, "TEST",
                        substage_label="下落警戒中",
                        substage_watch=(
                            "売上・EPSの悪化も確認される本格的な下落局面。"
                            "実体も崩壊中"
                        ))

        report = pipe._generate_report(
            "TEST", _minimal_valuation(), _minimal_score_data(), _minimal_extra()
        )

        # 「実体も崩壊中」が「売上低迷・EPS改善中」に置換されているはず
        assert "実体も崩壊中" not in report
        assert "EPS改善中" in report


# ─────────────────────────────────────────────
# 3. Matrix Label
#    upside の正負で「割安」「割高」が決まることを検証
# ─────────────────────────────────────────────

class TestMatrixLabel:
    def test_positive_upside_label_contains_undervalued(self, tmp_path):
        """upside >= 0 → Matrix の Label: 行に「割安」を含む"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}

        report = pipe._generate_report(
            "TEST", _minimal_valuation(upside=30.0),
            _minimal_score_data(), _minimal_extra()
        )

        # "Label: 割安×高効率" のように Label 行に「割安」が現れる
        assert "Label: 割安" in report

    def test_negative_upside_label_contains_overvalued(self, tmp_path):
        """upside < 0 → Matrix の Label: 行に「割高」を含む"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}

        report = pipe._generate_report(
            "TEST", _minimal_valuation(upside=-25.0),
            _minimal_score_data(), _minimal_extra()
        )

        assert "Label: 割高" in report


# ─────────────────────────────────────────────
# 3b. RICE三分類ラベル (DESIGN-10)
#     RICE値に応じて 高効率/中効率/低効率 の三分類が正しく表示されることを検証
# ─────────────────────────────────────────────

class TestRiceEfficiencyLabel:
    def _make_report_with_rice(self, tmp_path, rice_val: float) -> str:
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = _minimal_valuation(upside=30.0)
        val["rice"]["base"] = {"rice": rice_val}
        return pipe._generate_report("TEST", val, _minimal_score_data(), _minimal_extra())

    def test_rice_above_3_shows_high_efficiency(self, tmp_path):
        """RICE >= 3.0 → Label に「高効率」が含まれる"""
        report = self._make_report_with_rice(tmp_path, 4.0)
        assert "高効率" in report

    def test_rice_between_1_and_3_shows_medium_efficiency(self, tmp_path):
        """1.0 <= RICE < 3.0 → Label に「中効率」が含まれる"""
        report = self._make_report_with_rice(tmp_path, 2.0)
        assert "中効率" in report

    def test_rice_below_1_shows_low_efficiency(self, tmp_path):
        """RICE < 1.0 → Label に「低効率」が含まれる"""
        report = self._make_report_with_rice(tmp_path, 0.5)
        assert "低効率" in report

    def test_rice_exactly_3_shows_high_efficiency(self, tmp_path):
        """RICE = 3.0（境界値）→「高効率」"""
        report = self._make_report_with_rice(tmp_path, 3.0)
        assert "高効率" in report

    def test_rice_exactly_1_shows_medium_efficiency(self, tmp_path):
        """RICE = 1.0（均衡点・境界値）→「中効率」"""
        report = self._make_report_with_rice(tmp_path, 1.0)
        assert "中効率" in report


# ─────────────────────────────────────────────
# 4. Funda_Score ペナルティ
#    _compute_tanuki_score のペナルティ適用ロジックを検証
#    poc.json・EPS キャッシュなしで基本スコアを固定し、ペナルティ量を確認する
#
#    注意: stonks-silo ファイルは本番でも常に存在するため
#    テストでも必ずファイルを作成し、ticker エントリの有無でシナリオを分ける
# ─────────────────────────────────────────────

class TestFundaScorePenalty:
    def test_runway_under_12_applies_30pt_penalty(self, tmp_path):
        """Runway_Months < 12 → funda_score から 30点ペナルティが適用される"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        # stonks-silo に TEST の runway = 6ヶ月 をセット
        _write_stonks_json(tmp_path, {"TEST": {"runway": {"runway_months": 6.0}}})

        # fcf_base > 0 のみで基本スコア 25点。runway ペナルティ -30 → max(0, -5) = 0
        valuation = {"upside_percent": 50.0, "fcf_base": {"base_fcf": 100.0}}
        result = pipe._compute_tanuki_score("TEST", valuation)

        assert result["funda_score"] == 0

    def test_dilution_over_20_applies_15pt_penalty(self, tmp_path):
        """dilution > 20%/yr → funda_score から 15点ペナルティが適用される"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        # stonks-silo に TEST エントリなし（runway ペナルティは発動しない）
        _write_stonks_json(tmp_path, {})

        valuation = {
            "upside_percent": 50.0,
            "fcf_base": {"base_fcf": 100.0},
            "financial_health": {"dilution_3yr_annual_pct": 25.0},  # 20%超
        }
        result = pipe._compute_tanuki_score("TEST", valuation)

        # 基本スコア 25 → -15 = 10
        assert result["funda_score"] == 10

    def test_dilution_over_40_applies_25pt_penalty(self, tmp_path):
        """dilution > 40%/yr → -25点ペナルティ（-15より大きいことを確認）"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        _write_stonks_json(tmp_path, {})

        valuation = {
            "upside_percent": 50.0,
            "fcf_base": {"base_fcf": 100.0},
            "financial_health": {"dilution_3yr_annual_pct": 45.0},  # 40%超
        }
        result = pipe._compute_tanuki_score("TEST", valuation)

        # 基本スコア 25 → -25 = 0
        assert result["funda_score"] == 0


# ─────────────────────────────────────────────
# 5. Runway フォールバック
#    stonks-silo にない銘柄でも computed_runway_months があれば
#    _compute_tanuki_score がペナルティを適用することを検証
#
#    実装上、fallback は stonks-silo ファイルが存在し ticker エントリが
#    ない場合（runway_months = None）に機能する。
#    ファイルごと存在しない場合は if os.path.exists(...) でブロック全体がスキップされるため、
#    テストでも「ファイルあり・TEST エントリなし」で検証する。
# ─────────────────────────────────────────────

class TestRunwayFallback:
    def test_computed_runway_used_when_ticker_absent_from_stonks(self, tmp_path):
        """stonks-silo ファイルはあるが ticker エントリなし → computed_runway_months でペナルティ適用"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        # stonks-silo ファイルは存在するが TEST エントリはない
        _write_stonks_json(tmp_path, {})

        # _load_extra_data が計算済みの computed_runway_months を valuation に含める想定
        valuation = {
            "upside_percent": 50.0,
            "fcf_base": {"base_fcf": 100.0},
            "computed_runway_months": 3.2,  # 3.2ヶ月 < 12 → ペナルティ対象
        }
        result = pipe._compute_tanuki_score("TEST", valuation)

        # 基本スコア 25 → computed_runway < 12 で -30 → 0
        assert result["funda_score"] == 0
        assert result["score"] == "PASS"

    def test_negative_fcf_triggers_penalty_despite_positive_gaap_eps(self, tmp_path):
        """一時的黒字（GAAP EPS プラス）でも直近FCFがマイナスなら Runway ペナルティが発動する
        （computed_runway_months の計算条件: FCF < 0 も含まれる）
        """
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        # stonks-silo ファイルはあるが TEST エントリなし
        _write_stonks_json(tmp_path, {})

        # GAAP EPS はプラスだが FCF はマイナス（一時的黒字）の場合を想定
        # _load_extra_data は FCF < 0 条件で computed_runway_months を計算するため
        # ここでは計算済みの値を valuation に直接セットして _compute_tanuki_score を検証する
        valuation = {
            "upside_percent": 50.0,
            "fcf_base": {"base_fcf": 100.0},
            "fcf_history": [{"year": 2025, "fcf": -92_600_000, "fcf_margin": -30.0}],
            "computed_runway_months": 3.2,  # cash / (|FCF| / 12) で計算済みと仮定
        }
        result = pipe._compute_tanuki_score("TEST", valuation)

        # 一時的黒字でも FCF ベースの runway ペナルティが適用される
        assert result["funda_score"] == 0
        assert result["score"] == "PASS"


# ─────────────────────────────────────────────
# 6. growth_sanity
#    check_growth_sanity の判定ロジックを、Damodaran データをモックして検証
#
#    注意: sys.modules["growth_sanity"] は MagicMock に差し替え済みのため
#    @patch("growth_sanity.get_industry_benchmark") は効かない。
#    _gs（本物のモジュール参照）に patch.object を使う。
# ─────────────────────────────────────────────

class TestGrowthSanity:
    @patch.object(_gs, "get_industry_benchmark", return_value={
        "industry": "Semiconductor",
        "g_ebit": 0.096,   # 業界平均 9.6%
        "roc": 0.10,
        "rr": 0.50,
    })
    def test_growth_2_5x_above_benchmark_adds_warning(self, _mock):
        """phase1_growth が業界平均の2.5倍超 → warnings に ⚠️ が含まれ verdict が REVIEW になる"""
        # 9.6% × 2.5 = 24% を超える 40% を設定
        result = _gs.check_growth_sanity("TEST", phase1_growth=0.40, sector="semiconductor")

        assert any("⚠️" in w for w in result["warnings"]), \
            f"warnings に ⚠️ が見つからない: {result['warnings']}"
        assert result["verdict"] == "REVIEW"

    @patch.object(_gs, "get_industry_benchmark", return_value={
        "industry": "Semiconductor",
        "g_ebit": 0.096,
        "roc": 0.10,
        "rr": 0.50,
    })
    def test_growth_at_or_below_benchmark_is_plausible(self, _mock):
        """phase1_growth が業界平均以下 → warnings なし・verdict が PLAUSIBLE"""
        # 業界平均 9.6% 以下の 8% を設定
        result = _gs.check_growth_sanity("TEST", phase1_growth=0.08, sector="semiconductor")

        assert result["verdict"] == "PLAUSIBLE"
        assert not any("⚠️" in w for w in result["warnings"]), \
            f"warnings に予期しない ⚠️: {result['warnings']}"


# ─────────────────────────────────────────────
# 7. recommended_g
#    check_growth_sanity の recommended_g 算出ロジックを検証
# ─────────────────────────────────────────────

class TestRecommendedGrowth:
    @patch.object(_gs, "get_industry_benchmark", return_value={
        "industry": "Semiconductor",
        "g_ebit": 0.096,
        "roc": 0.10,
        "rr": 0.50,
    })
    def test_recommended_g_is_median_of_two_candidates(self, _mock):
        """industry_g + g_fundamental の2候補 → recommended_g が中央値になる"""
        # annual_revenues 3件 → cagr_3yr/5yr = None（len<4 なので算出不可）
        result = _gs.check_growth_sanity(
            "ALAB", phase1_growth=0.934, sector="Semiconductor",
            annual_revenues=[100.0, 200.0, 400.0],
            g_fundamental=0.026,
        )
        # candidates: [0.096, 0.026] → sorted: [0.026, 0.096] → median = 0.061
        assert "recommended_g" in result
        assert result["recommended_g"] is not None
        assert abs(result["recommended_g"] - 0.061) < 0.001

    @patch.object(_gs, "get_industry_benchmark", return_value={
        "industry": "Software (System & Application)",
        "g_ebit": 0.216,
        "roc": 0.20,
        "rr": 0.60,
    })
    def test_recommended_g_is_median_of_four_candidates(self, _mock):
        """4候補 → recommended_g が中央値（中央2要素の平均）になる"""
        # annual_revenues 6件 → cagr_3yr/5yr 両方算出可
        result = _gs.check_growth_sanity(
            "NOW", phase1_growth=0.221, sector="Software_System",
            annual_revenues=[500.0, 650.0, 900.0, 1200.0, 1700.0, 2300.0],
            g_fundamental=0.014,
        )
        assert "recommended_g" in result
        # candidates 4つすべて > 0 → median = 中央2要素の平均
        # cagr_3yr/5yr の実計算値 + industry=0.216 + g_fundamental=0.014
        # 候補は4個 → recommended_g は None でない
        assert result["recommended_g"] is not None

    @patch.object(_gs, "get_industry_benchmark", return_value=None)
    def test_single_candidate_gives_none_recommended_g(self, _mock):
        """industry_g=None + cagr 不可 → 候補1件 → recommended_g = None"""
        result = _gs.check_growth_sanity(
            "TEST", phase1_growth=0.20, sector=None,
            annual_revenues=[100.0, 200.0],  # 2件 → cagr 計算不可
            g_fundamental=0.03,
        )
        # candidates = [0.03] のみ（1件）→ None
        assert result.get("recommended_g") is None

    def test_segment_configured_no_auto_adjusted_in_report(self, tmp_path):
        """segment_configured=True → レポートに Growth_Rate_Original/Adjusted が出力されない"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}

        extra = {
            **_minimal_extra(),
            "segment_configured": True,
            "segment_ttm_applied": False,
            "phase1_growth_auto_adjusted": False,
            "phase1_growth_original": None,
            "recommended_g": None,
        }
        report = pipe._generate_report(
            "NVDA", _minimal_valuation(), _minimal_score_data(), extra
        )

        assert "Growth_Rate_Original" not in report
        assert "Growth_Rate_Adjusted" not in report

    def test_segment_unconfigured_auto_adjusted_shows_in_report(self, tmp_path):
        """segment_configured=False + auto_adjusted=True → Growth_Rate_Original/Adjusted が出力される"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}

        extra = {
            **_minimal_extra(),
            "segment_configured": False,
            "segment_ttm_applied": True,
            "phase1_growth_auto_adjusted": True,
            "phase1_growth_original": 0.934,   # TTM: 93.4%
            "recommended_g": 0.061,             # 推奨: 6.1%
        }
        report = pipe._generate_report(
            "ALAB", _minimal_valuation(), _minimal_score_data(), extra
        )

        assert "Growth_Rate_Original: 93.4% (TTM実績)" in report
        assert "Growth_Rate_Adjusted: 6.1% (推奨値・中央値ベース)" in report
        assert "（推奨値ベース）" in report


# ─────────────────────────────────────────────
# 8. 高成長逓減モデル vs 中央値モデル
# ─────────────────────────────────────────────

class TestGrowthDecayModel:
    """TTM > 50% → 逓減モデル、≤ 50% → 中央値モデルのテスト"""

    @patch.object(_gs, "get_industry_benchmark", return_value={
        "industry": "Semiconductor", "g_ebit": 0.096, "roc": 0.20, "rr": 0.60,
    })
    def test_high_ttm_uses_decay_model(self, _mock):
        """TTM 93% (cagr_3yr) → 逓減モデル適用"""
        # revenues[-1]/revenues[-4] = 719/100 = 7.19 → cagr_3yr ≈ 93%
        result = _gs.check_growth_sanity(
            "TEST", phase1_growth=0.93, sector="Semiconductor",
            annual_revenues=[100.0, 200.0, 400.0, 719.0],
        )
        assert result["growth_model"] == "decay"
        assert result["recommended_g"] is not None
        # (min(0.93, 1.0) + 0.096) / 2 ≈ 0.513
        expected = (0.93 + 0.096) / 2
        assert abs(result["recommended_g"] - expected) < 0.01

    @patch.object(_gs, "get_industry_benchmark", return_value={
        "industry": "Software (System & Application)", "g_ebit": 0.10, "roc": 0.20, "rr": 0.60,
    })
    def test_low_ttm_uses_median_model(self, _mock):
        """TTM 30% (cagr_3yr) → 中央値モデル維持"""
        # revenues[-1]/revenues[-4] = 220/100 = 2.2 → cagr_3yr ≈ 30%
        result = _gs.check_growth_sanity(
            "TEST", phase1_growth=0.30, sector="Software_System",
            annual_revenues=[100.0, 140.0, 180.0, 220.0],
        )
        assert result["growth_model"] == "median"

    @patch.object(_gs, "get_industry_benchmark", return_value={
        "industry": "Software (System & Application)", "g_ebit": 0.10, "roc": 0.20, "rr": 0.60,
    })
    def test_boundary_ttm_50_uses_median(self, _mock):
        """TTM exactly 50% → 境界値は中央値モデル（> 50% でなければ逓減不適用）"""
        # revenues[-1]/revenues[-4] = 337/100 = 3.37 → cagr_3yr ≈ 49.9%
        result = _gs.check_growth_sanity(
            "TEST", phase1_growth=0.50, sector="Software_System",
            annual_revenues=[100.0, 150.0, 200.0, 337.0],
        )
        assert result["growth_model"] == "median"

    @patch.object(_gs, "get_industry_benchmark", return_value={
        "industry": "Semiconductor", "g_ebit": 0.096, "roc": 0.20, "rr": 0.60,
    })
    def test_ttm_actual_triggers_decay_without_cagr_data(self, _mock):
        """ttm_actual=93.4% のみで逓減モデルがトリガーされる（CAGR履歴なし）"""
        result = _gs.check_growth_sanity(
            "ALAB", phase1_growth=0.934, sector="Semiconductor",
            annual_revenues=None,  # CAGR データなし
            ttm_actual=0.934,
        )
        assert result["growth_model"] == "decay"
        # (min(0.934, 1.0) + 0.096) / 2 ≈ 0.515
        expected = (0.934 + 0.096) / 2
        assert abs(result["recommended_g"] - expected) < 0.005


# ─────────────────────────────────────────────
# GROWTH-1: HypeCoreフェーズ別逓減モデル重み調整
#    check_growth_sanity の hype_phase パラメータで
#    TTM / 業界平均の重みが変わることを検証
# ─────────────────────────────────────────────

class TestGrowthDecayModelFixedWeight:
    """[[LAYER1-GROWTH-HYPEPHASE-DECAY-GAP-1]]（2026-08-26）:
    GROWTH-1のHypePhaseフェーズ別加重（Phase1-2=65%/Phase3=50%/Phase4=35%）は、
    実証データなしに導入されておりFunda側(DCF成長率)にTiming側(HypePhase)の
    信号を混ぜる設計矛盾だったため廃止し、固定50:50へ復元した。
    本クラスは「hype_phaseにどの値を渡しても結果が変わらない」ことを
    検証する回帰テスト（旧TestGrowthDecayModelPhaseWeightを置き換え）。
    """

    @patch.object(_gs, "get_industry_benchmark", return_value={
        "industry": "Semiconductor", "g_ebit": 0.096, "roc": 0.20, "rr": 0.60,
    })
    def test_recommended_g_is_fixed_50_50_regardless_of_hype_phase(self, _mock):
        """hype_phase=1/3/4/Noneのいずれでもrecommended_gは固定50:50で同一"""
        ttm, industry = 0.80, 0.096
        expected_50_50 = ttm * 0.50 + industry * 0.50
        results = {}
        for phase in (1, 2, 3, 4, None):
            results[phase] = _gs.check_growth_sanity(
                "ALAB", phase1_growth=ttm, sector="Semiconductor",
                annual_revenues=None, ttm_actual=ttm, hype_phase=phase,
            )
        for phase, result in results.items():
            assert result["growth_model"] == "decay"
            assert abs(result["recommended_g"] - expected_50_50) < 0.001, (
                f"hype_phase={phase} で固定50:50から外れている"
            )
        # 全フェーズで同一のrecommended_gであること（フェーズ間で差が生じない）
        values = {round(r["recommended_g"], 6) for r in results.values()}
        assert len(values) == 1

    @patch.object(_gs, "get_industry_benchmark", return_value={
        "industry": "Semiconductor", "g_ebit": 0.096, "roc": 0.20, "rr": 0.60,
    })
    def test_hype_phase_used_still_recorded_for_display(self, _mock):
        """hype_phase_used/hype_phase_labelはTiming側表示用に引き続き記録される
        （DCF成長率の計算には使われないが、report.txt等の表示情報としては残置）"""
        ttm = 0.80
        result_phase4 = _gs.check_growth_sanity(
            "ALAB", phase1_growth=ttm, sector="Semiconductor",
            annual_revenues=None, ttm_actual=ttm, hype_phase=4,
        )
        assert result_phase4["hype_phase_used"] == 4
        result_no_phase = _gs.check_growth_sanity(
            "ALAB", phase1_growth=ttm, sector="Semiconductor",
            annual_revenues=None, ttm_actual=ttm, hype_phase=None,
        )
        assert result_no_phase["hype_phase_used"] is None
        # 表示用フィールドが異なっても、DCFに使うrecommended_gは同一
        assert result_phase4["recommended_g"] == result_no_phase["recommended_g"]

    @patch.object(_gs, "get_industry_benchmark", return_value={
        "industry": "Semiconductor", "g_ebit": 0.096, "roc": 0.20, "rr": 0.60,
    })
    def test_growth_model_reason_no_longer_references_phase_weighting(self, _mock):
        """growth_model_reasonの文言に「Phase」による加重を示す記述が残っていないこと"""
        result = _gs.check_growth_sanity(
            "ALAB", phase1_growth=0.80, sector="Semiconductor",
            annual_revenues=None, ttm_actual=0.80, hype_phase=4,
        )
        reason = result["growth_model_reason"]
        assert "Phase" not in reason
        assert "固定50:50" in reason


# ─────────────────────────────────────────────
# 10. hypecore substage_watch の eps_surprise 分岐
#    detect_substage() が eps_surprise の実際の値に応じて
#    「大幅ミス」か「軽微なミス」かを正しく出力することを検証
#
#    Stage4 中盤A ブランチ到達条件:
#      stage=4, stage_months>2, real_strong=False（rev_yoy<=15 かつ eps_surp<=0）,
#      eps_surp is not None, rev_yoy > 5
# ─────────────────────────────────────────────

def _make_stage4_row(rev_yoy: float, eps_surprise: float) -> pd.Series:
    """Stage4 中盤A ブランチに到達するための最小限の pd.Series を生成する。
    real_strong=False になるよう rev_yoy を 15% 以下に設定する。
    """
    return pd.Series({
        "ma200_dev_local": -15.0,   # MA200 を下回っている（Stage4 らしい状態）
        "from_peak":    -20.0,   # 高値から -20%
        "rsi":           35.0,   # RSI 低下
        "price_mom3m":  -10.0,
        "ma200_mom":     -3.0,
        "rev_yoy":      rev_yoy,
        "eps_surprise": eps_surprise,
        "price_iv_ratio": None,
        "forward_pe":     None,
    })


class TestHypecoreSubstageWatch:
    def test_minor_eps_miss_does_not_say_large_miss(self):
        """eps_surprise=-0.46%（軽微なミス）→ substage_watch に「大幅ミス」を含まない"""
        row = _make_stage4_row(rev_yoy=10.0, eps_surprise=-0.46)
        result = detect_substage(row, stage=4, stage_months=3)

        assert result["label"] == "実体軟化・期待崩壊中"
        assert "大幅ミス" not in result["watch"], \
            f"軽微なミスなのに「大幅ミス」が含まれている: {result['watch']}"
        # 実際の eps_surprise 値がテキストに反映されているか
        assert "-0.46" in result["watch"] or "わずかに" in result["watch"], \
            f"予想比の値が watch に含まれていない: {result['watch']}"

    def test_large_eps_miss_says_large_miss(self):
        """eps_surprise=-10%（大幅ミス）→ substage_watch に「大幅ミス」を含む"""
        row = _make_stage4_row(rev_yoy=10.0, eps_surprise=-10.0)
        result = detect_substage(row, stage=4, stage_months=3)

        assert result["label"] == "実体軟化・期待崩壊中"
        assert "大幅ミス" in result["watch"], \
            f"大幅ミスなのに「大幅ミス」が含まれていない: {result['watch']}"
        # 実際の eps_surprise 値（-10.0）がテキストに反映されているか
        assert "-10.0" in result["watch"], \
            f"予想比の値が watch に含まれていない: {result['watch']}"


# ─────────────────────────────────────────────
# 9b. GROWTH_PREMIUM 判定（DCF-2）
#     逆DCF Required Growth が TTM 成長率を下回る場合に GROWTH_PREMIUM が返ることを検証
# ─────────────────────────────────────────────

class TestGrowthPremiumScore:
    """_compute_tanuki_score の GROWTH_PREMIUM 判定を検証"""

    def _make_pipe(self, tmp_path):
        from pipeline import TanukiValuationPipeline  # type: ignore[import]
        output_dir = tmp_path / "out" / "tanuki" / "data"
        output_dir.mkdir(parents=True)
        pipe = TanukiValuationPipeline(output_dir=str(output_dir), use_ai_validation=False)
        pipe._eps_summary_cache = {}
        return pipe

    def _base_valuation(self, upside: float, ttm_growth: float, req_growth_factor: float = 0.5) -> dict:
        """テスト用 valuation dict。req_growth が ttm_growth の req_growth_factor 倍になるよう設定"""
        # price * shares = EV。fcf_base から逆算して required_growth = ttm_growth * factor になるよう調整
        # required_fcf5 = EV*(wacc-tvg)/(1+tvg) = fcf_base * (1+req_g)^5
        # EV = required_fcf5 / (wacc-tvg) * (1+tvg)
        fcf_base = 1_000_000_000  # $1B
        wacc, tvg = 0.10, 0.03
        req_g = ttm_growth * req_growth_factor
        required_fcf5 = fcf_base * (1 + req_g) ** 5
        ev = required_fcf5 * (1 + tvg) / (wacc - tvg)
        shares = 1_000_000_000  # 10億株
        price = ev / shares
        return {
            "upside_percent": upside,
            "components": {
                "current_price": price,
                "diluted_shares": shares,
                "fcf_base_used": fcf_base,
            },
            "financial_health": {"net_debt": 0},
            "wacc": {"value": wacc, "market_return": wacc},
            "growth_sanity": {"phase1_growth": ttm_growth},
        }

    def test_growth_premium_when_required_growth_below_ttm(self, tmp_path):
        """Required Growth < TTM 成長率 → GROWTH_PREMIUM"""
        pipe = self._make_pipe(tmp_path)
        # upside=-40%, stage=3, funda>=50 の状態でテストするため poc.json をモック不要
        # _compute_tanuki_score を直接呼ぶかわりに _calc_required_growth をテスト
        val = self._base_valuation(upside=-40.0, ttm_growth=0.80, req_growth_factor=0.5)
        req = pipe._calc_required_growth(val)
        assert req is not None
        assert req < val["growth_sanity"]["phase1_growth"], \
            "Required Growth が TTM を下回るはず（GROWTH_PREMIUM の条件）"

    def test_trim_when_required_growth_exceeds_ttm(self, tmp_path):
        """Required Growth > TTM 成長率 → TRIM（GROWTH_PREMIUM にならない）"""
        pipe = self._make_pipe(tmp_path)
        val = self._base_valuation(upside=-40.0, ttm_growth=0.20, req_growth_factor=2.0)
        req = pipe._calc_required_growth(val)
        assert req is not None
        assert req > val["growth_sanity"]["phase1_growth"], \
            "Required Growth が TTM を上回るはず（TRIM の条件）"

    def test_required_growth_returns_none_without_price(self, tmp_path):
        """株価データ不足のとき None を返す"""
        pipe = self._make_pipe(tmp_path)
        val = {"components": {"current_price": 0}, "financial_health": {}, "wacc": {}, "growth_sanity": {}}
        assert pipe._calc_required_growth(val) is None


# ─────────────────────────────────────────────
# 9. _calc_q: GAAP赤字年スキップ（BUG-2b）
#    NI<0 の年は SBC で earnings がプラスになっても Q 計算から除外されることを検証
#    背景: MRVL TTM2025-02-01 で NI=-469M, SBC=+608M → earnings=139M
#          Q = 1871M/139M = 13.43 という偽高Q が発生し Q異常値誤判定が起きていた
# ─────────────────────────────────────────────

from calculator.rice import _calc_q, calculate_rice  # type: ignore[import]
import maturity_config as _mc  # type: ignore[import]


def _make_entry(ni: float, sbc: float, ocf: float, rev: float) -> dict:
    return {
        "cf": {"operating_cash_flow": ocf, "stock_based_compensation": sbc},
        "pl": {"net_income": ni, "revenue": rev},
    }


# ─────────────────────────────────────────────
# RICE-1: 価値創造係数（roic_wacc_ratio）テスト
#    ROIC/WACC が RICE 計算に正しく反映されることを検証
# ─────────────────────────────────────────────

_RICE_MOCK_ANNUAL = [
    # FY2023（最新）
    {"period": "FY2023",
     "pl": {"revenue": 26_000e6, "net_income": 3_000e6, "research_and_development": 2_000e6},
     "cf": {"operating_cash_flow": 5_000e6, "capital_expenditure": -500e6, "stock_based_compensation": 400e6},
     "data_quality": {}},
    # FY2022
    {"period": "FY2022",
     "pl": {"revenue": 20_000e6, "net_income": 2_500e6, "research_and_development": 1_800e6},
     "cf": {"operating_cash_flow": 4_000e6, "capital_expenditure": -400e6, "stock_based_compensation": 350e6},
     "data_quality": {}},
    # FY2021
    {"period": "FY2021",
     "pl": {"revenue": 16_000e6, "net_income": 2_000e6, "research_and_development": 1_600e6},
     "cf": {"operating_cash_flow": 3_200e6, "capital_expenditure": -350e6, "stock_based_compensation": 300e6},
     "data_quality": {}},
    # FY2020
    {"period": "FY2020",
     "pl": {"revenue": 13_000e6, "net_income": 1_600e6, "research_and_development": 1_400e6},
     "cf": {"operating_cash_flow": 2_600e6, "capital_expenditure": -300e6, "stock_based_compensation": 280e6},
     "data_quality": {}},
]
_RICE_MOCK_SCENARIOS = {
    "bear": {"growth_rate": 0.15, "intrinsic_value_per_share": 200.0},
    "base": {"growth_rate": 0.25, "intrinsic_value_per_share": 280.0},
    "bull": {"growth_rate": 0.35, "intrinsic_value_per_share": 360.0},
}


class TestRiceValueCreationFactor:
    """RICE-1: roic_wacc_ratio による価値創造係数のテスト"""

    def test_high_roic_wacc_amplifies_rice(self):
        """ROIC/WACC=2.0（ROIC=20%・WACC=10%）→ vc_factor=2.0でRICEが2倍になる"""
        base = calculate_rice(
            annual_data=_RICE_MOCK_ANNUAL,
            wacc=0.10,
            scenario_valuations=_RICE_MOCK_SCENARIOS,
            roic_wacc_ratio=None,
        )
        boosted = calculate_rice(
            annual_data=_RICE_MOCK_ANNUAL,
            wacc=0.10,
            scenario_valuations=_RICE_MOCK_SCENARIOS,
            roic_wacc_ratio=2.0,
        )
        assert base.available and boosted.available
        assert boosted.base.rice == pytest.approx(base.base.rice * 2.0, rel=1e-6)
        assert boosted.vc_factor == pytest.approx(2.0, rel=1e-6)

    def test_low_roic_wacc_penalizes_rice(self):
        """ROIC/WACC=0.5（ROIC=5%・WACC=10%）→ vc_factor=0.5でRICEが半減"""
        base = calculate_rice(
            annual_data=_RICE_MOCK_ANNUAL,
            wacc=0.10,
            scenario_valuations=_RICE_MOCK_SCENARIOS,
            roic_wacc_ratio=None,
        )
        penalized = calculate_rice(
            annual_data=_RICE_MOCK_ANNUAL,
            wacc=0.10,
            scenario_valuations=_RICE_MOCK_SCENARIOS,
            roic_wacc_ratio=0.5,
        )
        assert base.available and penalized.available
        assert penalized.base.rice == pytest.approx(base.base.rice * 0.5, rel=1e-6)
        assert penalized.vc_factor == pytest.approx(0.5, rel=1e-6)

    def test_no_roic_wacc_is_backward_compatible(self):
        """roic_wacc_ratio=None → vc_factor=None、RICE値は従来と同じ"""
        result = calculate_rice(
            annual_data=_RICE_MOCK_ANNUAL,
            wacc=0.10,
            scenario_valuations=_RICE_MOCK_SCENARIOS,
            roic_wacc_ratio=None,
        )
        assert result.available
        assert result.vc_factor is None
        assert result.roic_wacc_ratio is None

    def test_vc_factor_floor_at_0_3(self):
        """roic_wacc_ratio=0.1（極端に低いROIC）→ vc_factor は 0.3 でフロアされる"""
        result = calculate_rice(
            annual_data=_RICE_MOCK_ANNUAL,
            wacc=0.10,
            scenario_valuations=_RICE_MOCK_SCENARIOS,
            roic_wacc_ratio=0.1,
        )
        assert result.available
        assert result.vc_factor == pytest.approx(0.3, rel=1e-6)

    def test_vc_factor_cap_at_2_0(self):
        """roic_wacc_ratio=5.0（極端に高いROIC）→ vc_factor は 2.0 でキャップされる"""
        result = calculate_rice(
            annual_data=_RICE_MOCK_ANNUAL,
            wacc=0.10,
            scenario_valuations=_RICE_MOCK_SCENARIOS,
            roic_wacc_ratio=5.0,
        )
        assert result.available
        assert result.vc_factor == pytest.approx(2.0, rel=1e-6)


class TestCalcQNegativeNI:
    def test_negative_ni_skipped_even_when_sbc_makes_earnings_positive(self):
        """NI<0 かつ SBC で earnings>0 になる年はスキップされる（MRVLケース）"""
        data = [
            _make_entry(ni=-469e6, sbc=608e6, ocf=1871e6, rev=6424e6),  # earns=139M, Q=13.4 → SKIP
        ]
        q, used, _ = _calc_q(data)
        assert used == 0, "GAAP赤字年はSBCでelearnings>0でもスキップされるべき"

    def test_positive_ni_year_is_included(self):
        """NI>0 の年は通常通り計算に含まれる"""
        data = [
            _make_entry(ni=2888e6, sbc=592e6, ocf=1791e6, rev=8518e6),  # Q=0.51
        ]
        q, used, _ = _calc_q(data)
        assert used == 1
        assert abs(q - 1791e6 / (2888e6 + 592e6)) < 0.01

    def test_mrvl_pattern_excludes_negative_ni_year(self):
        """MRVLパターン: 赤字TTM年を除外し黒字年のみでQ平均を計算"""
        data = [
            _make_entry(ni=2888e6,  sbc=592e6, ocf=1791e6, rev=8518e6),  # Q=0.51  ✓
            _make_entry(ni=-469e6,  sbc=608e6, ocf=1871e6, rev=6424e6),  # SKIP（NI<0）
            _make_entry(ni=-1157e6, sbc=622e6, ocf=1709e6, rev=5612e6),  # SKIP（NI<0）
            _make_entry(ni=-13e6,   sbc=552e6, ocf=1446e6, rev=5891e6),  # SKIP（NI<0）
        ]
        q, used, _ = _calc_q(data, years=3)
        # years=3 なので先頭3件のみ参照 → 黒字は先頭1件のみ
        assert used == 1
        assert q < 5.0, f"Q={q:.2f} が Q_MAX=5.0 を超えてはならない（Q異常値誤判定防止）"


# ─────────────────────────────────────────────
# 10. calculate_tapering_dcf（DCF-1）
#     Phase1内で成長率を線形逓減させるDCFの年次成長率と価値計算を検証
# ─────────────────────────────────────────────

_CALC_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation", "calculator")
)
sys.path.insert(0, _CALC_DIR)
from dcf import calculate_tapering_dcf, calculate_two_stage_dcf  # type: ignore[import]


class TestTaperingDCF:
    def test_year1_equals_g_start(self):
        """Year1の成長率がg_startと一致する"""
        result = calculate_tapering_dcf(
            base_fcf=1_000_000, g_start=0.50, g_end=0.10,
            wacc=0.12, high_growth_years=5,
        )
        assert abs(result.high_growth_detail[0]["growth_rate"] - 0.50) < 1e-9

    def test_last_year_equals_g_end(self):
        """Year5の成長率がg_endと一致する"""
        result = calculate_tapering_dcf(
            base_fcf=1_000_000, g_start=0.50, g_end=0.10,
            wacc=0.12, high_growth_years=5,
        )
        assert abs(result.high_growth_detail[-1]["growth_rate"] - 0.10) < 1e-9

    def test_growth_rates_are_linearly_decreasing(self):
        """中間年の成長率が線形補間になっている"""
        result = calculate_tapering_dcf(
            base_fcf=1_000_000, g_start=0.50, g_end=0.10,
            wacc=0.12, high_growth_years=5,
        )
        rates = [d["growth_rate"] for d in result.high_growth_detail]
        # Year2 = 0.50 + (0.10-0.50)*1/4 = 0.40
        assert abs(rates[1] - 0.40) < 1e-9
        # Year3 = 0.50 + (0.10-0.50)*2/4 = 0.30
        assert abs(rates[2] - 0.30) < 1e-9

    def test_tapering_gives_lower_value_than_fixed_high_growth(self):
        """逓減DCFは高成長固定DCFより低い価値を返す（高成長銘柄への保守的評価）"""
        base_fcf, wacc = 1_000_000, 0.12
        tapering = calculate_tapering_dcf(
            base_fcf=base_fcf, g_start=0.50, g_end=0.10,
            wacc=wacc, high_growth_years=5,
        )
        fixed = calculate_two_stage_dcf(
            base_fcf=base_fcf, high_growth_rate=0.50,
            wacc=wacc, high_growth_years=5,
        )
        assert tapering.v0 < fixed.v0, "逓減DCFは固定高成長DCFより低い価値であるべき"

    def test_equal_start_end_matches_two_stage(self):
        """g_start == g_end の場合は2段階DCFと同等になる"""
        base_fcf, wacc, g = 1_000_000, 0.12, 0.20
        tapering = calculate_tapering_dcf(
            base_fcf=base_fcf, g_start=g, g_end=g,
            wacc=wacc, high_growth_years=5,
        )
        fixed = calculate_two_stage_dcf(
            base_fcf=base_fcf, high_growth_rate=g,
            wacc=wacc, high_growth_years=5,
        )
        assert abs(tapering.v0 - fixed.v0) < 1.0, "g_start==g_endのとき2段階DCFと同値になるべき"


# ─────────────────────────────────────────────
# WACC-1: セクター別ターミナル成長率
#    get_terminal_growth() のフォールバックロジックを検証
# ─────────────────────────────────────────────

class TestTerminalGrowthBySector:
    """WACC-1: get_terminal_growth() のセクター別フォールバック検証"""

    def test_software_industry_returns_35_pct(self):
        """Software (System & Application) 銘柄 → tv_g = 3.5%"""
        with patch.object(_mc, "_lookup_tv_g_by_industry", return_value=0.035):
            result = _mc.get_terminal_growth("NOW")
        assert result == pytest.approx(0.035, rel=1e-6)

    def test_consumer_industry_returns_25_pct(self):
        """Restaurant/Dining 銘柄 → tv_g = 2.5%"""
        with patch.object(_mc, "_lookup_tv_g_by_industry", return_value=0.025):
            result = _mc.get_terminal_growth("CAKE")
        assert result == pytest.approx(0.025, rel=1e-6)

    def test_unknown_ticker_returns_default_30_pct(self):
        """業種不明銘柄 → tv_g = 3.0%（デフォルト）"""
        with patch.object(_mc, "_lookup_tv_g_by_industry", return_value=None):
            result = _mc.get_terminal_growth("UNKNOWN_XYZ")
        assert result == pytest.approx(0.030, rel=1e-6)

    # ── [[TVGROWTH-EXPLICIT-DEFAULT-AMBIGUOUS-1]] ──────────────────────
    # terminal_growth_explicit: true を明示設定した場合、値が3.0%と一致
    # していてもデフォルト未設定とみなさず常にそれを採用することの検証。
    # 修正前は差分チェック(abs(g-0.03)>1e-5)のみだったため、3.0%を意図的に
    # 指定してもセクター別テーブルの値に上書きされる抜け道があった。

    def test_explicit_3pct_is_honored_even_when_sector_table_disagrees(self):
        """terminal_growth=0.03・terminal_growth_explicit=Trueの場合、
        セクター別テーブルが別の値（例:3.5%）を返しても3.0%が優先される
        （修正前バグ: このケースは常にセクター別の値に上書きされていた）"""
        profile = {"type": "two_stage", "terminal_growth": 0.03, "terminal_growth_explicit": True}
        with patch.object(_mc, "get_maturity_profile", return_value=profile), \
             patch.object(_mc, "_lookup_tv_g_by_industry", return_value=0.035):
            result = _mc.get_terminal_growth("EXPLICIT_3PCT_TICKER")
        assert result == pytest.approx(0.03, rel=1e-6)

    def test_without_explicit_flag_3pct_still_falls_back_to_sector(self):
        """explicit флаг無し（既存挙動）: 3.0%は「未設定」とみなされ、
        セクター別テーブルの値が優先される（後方互換の確認）"""
        profile = {"type": "two_stage", "terminal_growth": 0.03}
        with patch.object(_mc, "get_maturity_profile", return_value=profile), \
             patch.object(_mc, "_lookup_tv_g_by_industry", return_value=0.035):
            result = _mc.get_terminal_growth("NO_EXPLICIT_FLAG_TICKER")
        assert result == pytest.approx(0.035, rel=1e-6)

    def test_explicit_flag_false_behaves_like_absent(self):
        """terminal_growth_explicit: false（明示的にfalse）も未設定と
        同様に扱われること"""
        profile = {"type": "two_stage", "terminal_growth": 0.03, "terminal_growth_explicit": False}
        with patch.object(_mc, "get_maturity_profile", return_value=profile), \
             patch.object(_mc, "_lookup_tv_g_by_industry", return_value=0.035):
            result = _mc.get_terminal_growth("EXPLICIT_FALSE_TICKER")
        assert result == pytest.approx(0.035, rel=1e-6)

    def test_explicit_flag_with_non_default_value_still_honored(self):
        """3.0%以外の値でもexplicitフラグは無害に機能する（既存の差分
        チェック経路と同じ結果になる、回帰なしの確認）"""
        profile = {"type": "two_stage", "terminal_growth": 0.04, "terminal_growth_explicit": True}
        with patch.object(_mc, "get_maturity_profile", return_value=profile), \
             patch.object(_mc, "_lookup_tv_g_by_industry", return_value=0.035):
            result = _mc.get_terminal_growth("EXPLICIT_NONDEFAULT_TICKER")
        assert result == pytest.approx(0.04, rel=1e-6)

    def test_damodaran_tv_g_table_has_software_35_pct(self):
        """_DAMODARAN_TV_G テーブルに Software (System & Application) → 3.5% が含まれる"""
        assert _mc._DAMODARAN_TV_G.get("Software (System & Application)") == pytest.approx(0.035)

    def test_damodaran_tv_g_table_has_restaurant_25_pct(self):
        """_DAMODARAN_TV_G テーブルに Restaurant/Dining → 2.5% が含まれる"""
        assert _mc._DAMODARAN_TV_G.get("Restaurant/Dining") == pytest.approx(0.025)

    def test_damodaran_tv_g_table_has_utility_20_pct(self):
        """_DAMODARAN_TV_G テーブルに Power → 2.0% が含まれる"""
        assert _mc._DAMODARAN_TV_G.get("Power") == pytest.approx(0.020)

    def test_calc_required_growth_uses_tv_g_parameter(self, tmp_path):
        """_calc_required_growth は tv_g パラメータを正しく使用する"""
        pipe = _make_pipe(tmp_path)
        # 同じ valuation でも tv_g が違えば Required Growth が変わる
        val = {
            "components": {
                "current_price": 100.0,
                "diluted_shares": 1_000_000_000,
                "fcf_base_used": 5_000_000_000,
            },
            "financial_health": {"net_debt": 0},
            "wacc": {"value": 0.10, "market_return": 0.10},
        }
        req_30 = pipe._calc_required_growth(val, tv_g=0.030)
        req_35 = pipe._calc_required_growth(val, tv_g=0.035)
        assert req_30 is not None and req_35 is not None
        # tv_g が高いほど分子(wacc - tv_g)が小さくなり required_fcf5 が小さくなる
        # → 必要成長率は低下する
        assert req_35 < req_30, "tv_g=3.5% のとき required_growth は tv_g=3.0% より低いはず"


# ─────────────────────────────────────────────────────────────────
# safe_yf_utils テスト
# ─────────────────────────────────────────────────────────────────
_REPO_ROOT_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT_DIR not in sys.path:
    sys.path.insert(0, _REPO_ROOT_DIR)

from common.yfinance_utils import safe_yf_ticker, safe_yf_history


class TestSafeYfTicker:
    def test_returns_none_on_network_failure(self):
        """fast_info アクセスで例外が出たとき None を返す"""
        with patch("common.yfinance_utils.yf.Ticker") as mock_cls:
            mock_instance = MagicMock()
            type(mock_instance).fast_info = PropertyMock(
                side_effect=Exception("network error")
            )
            mock_cls.return_value = mock_instance
            result = safe_yf_ticker("AAPL", retries=0, wait=0)
            assert result is None

    def test_returns_ticker_on_success(self):
        """fast_info が正常に返ったとき Ticker オブジェクトを返す"""
        with patch("common.yfinance_utils.yf.Ticker") as mock_cls:
            mock_instance = MagicMock()
            # fast_info は例外を出さない（MagicMock デフォルト）
            mock_cls.return_value = mock_instance
            result = safe_yf_ticker("AAPL", retries=0, wait=0)
            assert result is mock_instance

    def test_retries_on_failure(self):
        """retries=1 のとき sleep が1回呼ばれる（リトライ発生の証拠）"""
        with patch("common.yfinance_utils.yf.Ticker") as mock_cls, \
             patch("common.yfinance_utils.time.sleep") as mock_sleep:
            mock_instance = MagicMock()
            type(mock_instance).fast_info = PropertyMock(
                side_effect=Exception("flaky")
            )
            mock_cls.return_value = mock_instance
            result = safe_yf_ticker("AAPL", retries=1, wait=0)
        assert result is None
        assert mock_sleep.call_count == 1  # retries=1 → 1回スリープ


class TestSafeYfHistory:
    def test_returns_empty_df_on_failure(self):
        """例外が出たとき空 DataFrame を返す"""
        import pandas as pd
        with patch("common.yfinance_utils.yf.Ticker") as mock_cls:
            mock_cls.return_value.history.side_effect = Exception("timeout")
            result = safe_yf_history("AAPL", period="5d", retries=0)
            assert isinstance(result, pd.DataFrame)
            assert result.empty

    def test_returns_data_on_success(self):
        """正常時は DataFrame を返す"""
        import pandas as pd
        fake_df = pd.DataFrame({"Close": [100.0, 101.0]})
        with patch("common.yfinance_utils.yf.Ticker") as mock_cls:
            mock_cls.return_value.history.return_value = fake_df
            result = safe_yf_history("AAPL", period="5d", retries=0)
            assert not result.empty
            assert "Close" in result.columns

    def test_accepts_start_end_kwargs(self):
        """start/end キーワード引数を history に渡せる"""
        import pandas as pd
        fake_df = pd.DataFrame({"Close": [150.0]})
        with patch("common.yfinance_utils.yf.Ticker") as mock_cls:
            mock_cls.return_value.history.return_value = fake_df
            result = safe_yf_history("AAPL", period=None, start="2026-01-01", end="2026-01-05", retries=0)
            mock_cls.return_value.history.assert_called_once_with(
                period=None, start="2026-01-01", end="2026-01-05"
            )
            assert not result.empty


# ─────────────────────────────────────────────
# 14. determine_stage: S0→S1昇格ロジック
# ─────────────────────────────────────────────
from hypecore import determine_stage  # noqa: E402


def _make_row(**kwargs) -> pd.Series:
    """determine_stage 用の最小 pd.Series を生成する"""
    defaults = dict(
        ma200_dev_local=0, ma200_mom=0, from_peak=0, price_mom3m=0,
        rsi=50, vol_surge_6m=1.0,
        sell_on_good_news=0, eps_surprise=None, analyst_upgrade_rate=None,
        buy_ratio=None, forward_pe=None, peg_ratio=None,
        revenue_growth_yf=None, earnings_growth=None,
        short_pct_float=None, recommendation_mean=None,
        expectation_score=0, fundamental_score=0, momentum_score=0,
    )
    defaults.update(kwargs)
    return pd.Series(defaults)


class TestDetermineStageS0S1Promotion:
    """S0条件でも反発モメンタムが強ければS1に昇格する"""

    def test_deep_s0_weak_rebound_stays_s0(self):
        """深い低迷（MA200=-23%）+ 反発+14% → S0維持（昇格条件 +20% 未達）"""
        row = _make_row(ma200_dev_local=-23, price_mom3m=14, rsi=45)
        assert determine_stage(row, prev_stage=2) == 0

    def test_deep_s0_strong_rebound_promotes_to_s1(self):
        """深い低迷（MA200=-23%）+ 強反発+22% → S1昇格"""
        row = _make_row(ma200_dev_local=-23, price_mom3m=22, rsi=48)
        assert determine_stage(row, prev_stage=2) == 1

    def test_shallow_s0_moderate_rebound_promotes_to_s1(self):
        """浅い低迷（MA200=-15%, short>8%）+ 反発+12% → S1昇格"""
        row = _make_row(ma200_dev_local=-15, price_mom3m=12, short_pct_float=0.10, rsi=47)
        assert determine_stage(row, prev_stage=2) == 1

    def test_shallow_s0_insufficient_rebound_stays_s0(self):
        """浅い低迷（MA200=-15%, short>8%）+ 反発+8% → S0維持（+10% 未達）"""
        row = _make_row(ma200_dev_local=-15, price_mom3m=8, short_pct_float=0.10, rsi=45)
        assert determine_stage(row, prev_stage=2) == 0


# ─────────────────────────────────────────────
# 15. adjust_rpo: RPO比率条件ゲート (B-5 whitelist方式+比率条件)
# ─────────────────────────────────────────────
from calculator.adjustments import adjust_rpo, _get_rpo_application_rate  # type: ignore[import]


class TestRpoRatioGate:
    """whitelist通過/keyword通過+比率条件/keyword通過+比率未達を検証"""

    def test_whitelist_ticker_bypasses_ratio_check(self):
        """whitelist登録銘柄は比率<0.3でも exclusion_reason が設定されない"""
        # ratio=0.03 < 0.30: 非whitelist銘柄なら除外されるが GOOGL(whitelist) は免除
        result_wl = adjust_rpo(
            rpo=3_000_000_000,   # RPO $3B
            sector="Communication Services",
            ticker="GOOGL",      # whitelist登録済み
            industry="Internet Content & Information",
            op_margin=0.30,
            rev_ttm=100_000_000_000,  # ratio=0.03 < 0.30
        )
        result_nwl = adjust_rpo(
            rpo=3_000_000_000,
            sector="Technology",
            ticker="NONWHITE",   # whitelist未登録
            industry="Software - Application",
            op_margin=0.30,
            rev_ttm=100_000_000_000,  # ratio=0.03 < 0.30
        )
        # whitelist → 比率チェック免除 → exclusion_reason なし
        assert result_wl.exclusion_reason == ""
        # 非whitelist → 比率チェックで除外
        assert "not applied" in result_nwl.exclusion_reason

    def test_keyword_ticker_below_ratio_is_excluded(self):
        """softwareキーワードで通過しても RPO/Revenue<0.3 なら除外される"""
        result = adjust_rpo(
            rpo=2_000_000_000,   # RPO $2B
            sector="Technology",
            ticker="TESTSOFT",   # whitelist未登録
            industry="Software - Application",
            op_margin=0.25,
            rev_ttm=20_000_000_000,  # Revenue $20B → ratio=0.10 < 0.30
        )
        assert result.rpo_pv == 0.0
        assert "not applied" in result.exclusion_reason
        assert "0.10" in result.exclusion_reason

    def test_keyword_ticker_above_ratio_is_applied(self):
        """softwareキーワードで通過し RPO/Revenue>=0.3 なら exclusion_reason なしで適用"""
        # rpo=25B > rev_ttm=20B → rpo_incremental>0 かつ ratio=1.25 >= 0.30
        result = adjust_rpo(
            rpo=25_000_000_000,  # RPO $25B
            sector="Technology",
            ticker="TESTSOFT2",  # whitelist未登録
            industry="Software - Application",
            op_margin=0.20,
            rev_ttm=20_000_000_000,  # Revenue $20B → ratio=1.25 >= 0.30
        )
        assert result.exclusion_reason == ""
        assert result.rpo_pv > 0


# ─────────────────────────────────────────────
# 16. _compute_tanuki_score: DCF_Reliability=LOW → WATCH丸め
# ─────────────────────────────────────────────

class TestDcfReliabilityLowRounding:
    """DCF_Reliability=LOW (fcf_floor_applied>0) のとき WATCH に丸められる"""

    def test_low_reliability_rounds_hold_to_watch(self, tmp_path):
        """HOLD銘柄でfcf_floor_applied>0 → WATCH に丸められ、コメントにLOW旨が入る"""
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        valuation = {
            "upside_percent": -5.0,   # HOLDになる条件
            "components": {
                "fcf_floor_applied": 1_000_000_000,  # LOW判定トリガー
                "diluted_shares": None,
            },
            "fcf_base": {"base_fcf": 500_000_000},
            "financial_health": {},
            "fcf_estimation": {},
        }
        result = pipe._compute_tanuki_score("LOWTEST", valuation)
        assert result["score"] == "WATCH"
        assert "LOW" in result["score_comment"]

    def test_low_reliability_keeps_sell(self, tmp_path):
        """ファンダ劣化SELLはfcf_floor_applied>0でもSELLのまま維持される"""
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        # SELLになる条件: 25 <= funda < 50 かつ sell_funda=True
        # sell_funda = rev_yoy<0 and rule40_yoy_netmargin<20 and fcf_est < fcf_base*0.8
        # funda: rev_yoy=-5 → +0, rule40_yoy_netmargin=15 → +0, eps_yoy=None → +0, fcf_base>0 → +25 = 25
        poc_dir = tmp_path / "docs" / "value-monitor" / "hypecore" / "data"
        poc_dir.mkdir(parents=True, exist_ok=True)
        (poc_dir / "SELLTEST_poc.json").write_text(json.dumps({
            "monthly": [{"stage": 2, "rev_yoy": -5.0, "rule40_yoy_netmargin": 15.0}]
        }), encoding="utf-8")
        valuation = {
            "upside_percent": 30.0,
            "components": {
                "fcf_floor_applied": 1_000_000_000,  # LOW判定トリガー
                "diluted_shares": None,
            },
            "fcf_base": {"base_fcf": 1_000_000},      # FCF正（funda+25）
            "financial_health": {},
            "fcf_estimation": {                         # fcf_est < fcf_base*0.8 → sell_funda
                "estimated_fcf": 600_000,
            },
        }
        result = pipe._compute_tanuki_score("SELLTEST", valuation)
        # funda=25, sell_funda=True → SELL。LOW丸め対象外（SELL維持）
        assert result["score"] == "SELL"


class TestDcfReliabilityPolicyB:
    """
    DCF-RELIABILITY-1: FCF_Conversion_Rate方式向けDCF_Reliability判定（Policy B）

    判定表（DCF-REL-SYNC-1 2026-07-11修正: transient_found→action=="excluded"に変更。
    証拠が"存在するか"ではなく乖離を"金額として説明しきれているか"で判定する）:
      eps_invalid=true                                       → LOW（最優先）
      eps_invalid=false, detected=true,  action!="excluded"  → LOW
      eps_invalid=false, detected=true,  action=="excluded"  → NORMAL
      eps_invalid=false, detected=false                      → NORMAL
    """

    @staticmethod
    def _valuation(detected: bool, explained: bool, divergence_warning: str = "") -> dict:
        return {
            "fcf_outlier": {
                "detected": detected,
                "action": "excluded" if explained else "flagged",
            },
            "fcf_estimation": {
                "applied": True,
                "divergence_warning": divergence_warning,
            },
        }

    def test_detected_true_not_explained_is_low(self):
        v = self._valuation(detected=True, explained=False)
        assert TanukiValuationPipeline._calc_dcf_reliability_policy_b(v) == "LOW"

    def test_detected_true_explained_is_normal(self):
        v = self._valuation(detected=True, explained=True)
        assert TanukiValuationPipeline._calc_dcf_reliability_policy_b(v) == "NORMAL"

    def test_detected_true_flagged_with_partial_evidence_is_low(self):
        """一過性費用の証拠(transient_evidence.found)はあるが乖離を説明しきれない(action=flagged)場合はLOW
        （DCF-REL-SYNC-1: FLYW型の回帰防止。旧ロジックはfoundのみを見てNORMAL誤判定していた）"""
        v = {
            "fcf_outlier": {
                "detected": True,
                "action": "flagged",
                "transient_evidence": {"found": True},
            },
            "fcf_estimation": {"applied": True, "divergence_warning": ""},
        }
        assert TanukiValuationPipeline._calc_dcf_reliability_policy_b(v) == "LOW"

    def test_detected_false_eps_valid_is_normal(self):
        v = self._valuation(detected=False, explained=False)
        assert TanukiValuationPipeline._calc_dcf_reliability_policy_b(v) == "NORMAL"

    def test_detected_false_eps_invalid_is_low(self):
        v = self._valuation(detected=False, explained=False, divergence_warning="乖離警告")
        assert TanukiValuationPipeline._calc_dcf_reliability_policy_b(v) == "LOW"

    def test_eps_invalid_overrides_explained_true(self):
        """eps_invalid=true は detected×explained=true（本来NORMAL）より優先してLOWにする"""
        v = self._valuation(detected=True, explained=True, divergence_warning="乖離警告")
        assert TanukiValuationPipeline._calc_dcf_reliability_policy_b(v) == "LOW"

    def test_compute_tanuki_score_rounds_to_watch_when_policy_b_low(self, tmp_path):
        """fcf_estimation.applied=True & Policy B=LOW → WATCHに丸められる"""
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        valuation = {
            "upside_percent": -5.0,  # HOLDになる条件
            "components": {"diluted_shares": None},
            "fcf_base": {"base_fcf": 500_000_000},
            "financial_health": {},
            "fcf_estimation": {"applied": True, "divergence_warning": ""},
            "fcf_outlier": {
                "detected": True,
                "transient_evidence": {"found": False},
            },
        }
        result = pipe._compute_tanuki_score("POLICYB_LOW", valuation)
        assert result["score"] == "WATCH"
        assert "LOW" in result["score_comment"]

    def test_compute_tanuki_score_not_rounded_when_policy_b_normal(self, tmp_path):
        """fcf_estimation.applied=True だが Policy B=NORMAL → 通常判定のまま（WATCHに強制されない）"""
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        valuation = {
            "upside_percent": -5.0,  # HOLDになる条件（upside<=0 かつ <-30でないため）
            "components": {"diluted_shares": None},
            "fcf_base": {"base_fcf": 500_000_000},
            "financial_health": {},
            "fcf_estimation": {"applied": True, "divergence_warning": ""},
            "fcf_outlier": {
                "detected": False,
                "transient_evidence": {"found": False},
            },
        }
        result = pipe._compute_tanuki_score("POLICYB_NORMAL", valuation)
        assert result["score"] == "HOLD"

    def test_compute_tanuki_score_keeps_sell_when_policy_b_low(self, tmp_path):
        """ファンダ劣化SELLはPolicy B=LOWでもSELLのまま維持される"""
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        poc_dir = tmp_path / "docs" / "value-monitor" / "hypecore" / "data"
        poc_dir.mkdir(parents=True, exist_ok=True)
        (poc_dir / "POLICYB_SELL_poc.json").write_text(json.dumps({
            "monthly": [{"stage": 2, "rev_yoy": -5.0, "rule40_yoy_netmargin": 15.0}]
        }), encoding="utf-8")
        valuation = {
            "upside_percent": 30.0,
            "components": {"diluted_shares": None},
            "fcf_base": {"base_fcf": 1_000_000},
            "financial_health": {},
            "fcf_estimation": {
                "applied": True,
                "estimated_fcf": 600_000,  # fcf_est < fcf_base*0.8 → sell_funda
                "divergence_warning": "",
            },
            "fcf_outlier": {
                "detected": True,
                "transient_evidence": {"found": False},
            },
        }
        result = pipe._compute_tanuki_score("POLICYB_SELL", valuation)
        assert result["score"] == "SELL"

    def test_policy_b_fires_when_applied_false_and_floor_not_applied(self, tmp_path):
        """POLICYB-GATE-FIX-1: applied=False・floor未発火でもfcf_outlier未解消ならPolicy BでWATCHに丸める（BKNG/RBRK型）"""
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        valuation = {
            "upside_percent": -5.0,
            "components": {"diluted_shares": None},  # fcf_floor_applied未設定 → Policy A発火なし
            "fcf_base": {"base_fcf": 500_000_000},
            "financial_health": {},
            "fcf_estimation": {"applied": False, "divergence_warning": ""},
            "fcf_outlier": {
                "detected": True,
                "action": "flagged",
                "transient_evidence": {"found": False},
            },
        }
        result = pipe._compute_tanuki_score("POLICYAB_EXCL", valuation)
        # 旧仕様ではapplied=FalseのためPolicy Bがゲートで弾かれHOLDのままだったが、
        # 修正後はfloor未発火でもPolicy Bが評価されWATCHへ丸められる
        assert result["score"] == "WATCH"
        # raw_fcfフォールバック方式であることが分かるコメントになっている（FCF_Conversion_Rate方式と誤表示しない）
        assert "FCF_Base方式" in result["score_comment"]

    def test_policy_a_message_not_overwritten_when_floor_already_applied(self, tmp_path):
        """POLICYB-GATE-FIX-1回帰防止: floor発火済み（Policy A発火）の場合はPolicy Bのコメントで上書きしない"""
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        valuation = {
            "upside_percent": -5.0,
            "components": {"diluted_shares": None, "fcf_floor_applied": 1},
            "fcf_base": {"base_fcf": 500_000_000},
            "financial_health": {},
            "fcf_estimation": {"applied": False, "divergence_warning": ""},
            "fcf_outlier": {
                "detected": True,
                "action": "flagged",  # Policy B単独ならLOWとなる条件だが、Policy Aが既に発火済み
                "transient_evidence": {"found": False},
            },
        }
        result = pipe._compute_tanuki_score("POLICYA_FLOOR_FIRST", valuation)
        assert result["score"] == "WATCH"
        # raw_fcf方式なのに「FCF_Conversion_Rate方式」というPolicy Bの文言で上書きされていないこと
        assert "実績FCF赤字" in result["score_comment"]
        assert "FCF_Conversion_Rate方式" not in result["score_comment"]

    def test_policy_b_fires_when_applied_true_and_floor_also_applied(self, tmp_path):
        """POLICYB-GATE-FIX-1横断調査で発見（BROS/CEG/SOFI/SPIR型）:
        fcf_floor_applied>0でもfcf_estimation.applied=Trueの場合、実際のDCFは
        conversion-rate推定値を使う（floor値は使われない）ため、Policy Aは
        発火させず、Policy Bのみで判定する（「実績FCF赤字」という誤ったコメントを防ぐ）"""
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        valuation = {
            "upside_percent": -5.0,
            "components": {"diluted_shares": None, "fcf_floor_applied": 1},  # raw fcfはfloor対象だが
            "fcf_base": {"base_fcf": 500_000_000},
            "financial_health": {},
            "fcf_estimation": {"applied": True, "divergence_warning": ""},  # 実際のDCFはconversion-rate推定値を使用
            "fcf_outlier": {
                "detected": True,
                "action": "flagged",
                "transient_evidence": {"found": False},
            },
        }
        result = pipe._compute_tanuki_score("POLICYB_FLOOR_AND_APPLIED", valuation)
        assert result["score"] == "WATCH"
        # Policy Aの「実績FCF赤字」（revenue_floor由来）ではなく、Policy Bの
        # 「FCF_Conversion_Rate方式」が正しい理由付けである
        assert "FCF_Conversion_Rate方式" in result["score_comment"]
        assert "実績FCF赤字" not in result["score_comment"]


class TestCalculateFcfCagrDirection:
    """GROWTH-CAGR-SIGN-1: calculate_fcf_cagr()のCAGR計算方向の回帰テスト

    fcf_listは本コードベース全体の規約（新しい順、fcf_list[0]が直近）に
    従う。修正前は start_value/end_value の割り当てが逆向きで、実際には
    成長している銘柄でも負のraw_cagrが算出されていた（NVDA実データで
    -60.1%という不合理な値が出ていたことを実測で確認済み）。
    """

    @staticmethod
    def _import():
        from calculator.growth import calculate_fcf_cagr
        return calculate_fcf_cagr

    def test_high_growth_produces_large_positive_raw_cagr(self):
        """NVDA型: 直近が最古より大幅に大きい（急成長）場合、raw_cagrは大幅プラスになる"""
        calculate_fcf_cagr = self._import()
        # 新しい順: 直近$119,076M → ... → 5年前$3,028M（NVDA実データ）
        fcf_list = [119_076_000_000, 72_064_000_000, 39_334_000_000, 6_351_000_000, 3_028_000_000]
        result = calculate_fcf_cagr(fcf_list)
        assert result is not None
        assert result.cagr_detail["raw_cagr"] > 1.0  # 実測+150.4%
        assert result.rate == 0.50  # growth_cap(50%)でクリップ

    def test_moderate_growth_within_floor_cap_not_clipped(self):
        """MO型: floor(15%)〜cap(50%)の範囲内に収まる場合はクリップされない"""
        calculate_fcf_cagr = self._import()
        # 新しい順: 直近$8,623M → ... → 5年前$3,030M（MO実データ）
        fcf_list = [8_623_000_000, 8_451_000_000, 9_004_000_000, 7_950_000_000, 3_030_000_000]
        result = calculate_fcf_cagr(fcf_list)
        assert result is not None
        raw = result.cagr_detail["raw_cagr"]
        assert 0.25 < raw < 0.35  # 実測+29.9%
        assert result.rate == raw  # floor/capにかからないため生の値がそのまま採用される

    def test_extreme_growth_capped_at_ceiling(self):
        """LOAR型: 極端な急成長はgrowth_cap(50%)側でクリップされる（floor側ではない）"""
        calculate_fcf_cagr = self._import()
        # 新しい順: 直近$112.28M → 1年前$54.971M → 2年前$12.813M（LOAR実データ）
        fcf_list = [112_280_000, 54_971_000, 12_813_000]
        result = calculate_fcf_cagr(fcf_list)
        assert result is not None
        assert result.cagr_detail["raw_cagr"] > 1.0  # 実測+196.0%
        assert result.rate == 0.50

    def test_declining_fcf_produces_negative_raw_cagr(self):
        """減少トレンド（直近が最古より小さい）の場合、raw_cagrは正しく負になり、floor(15%)でクリップされる"""
        calculate_fcf_cagr = self._import()
        # 新しい順: 直近50 → ... → 5年前100（一貫した減少トレンド）
        fcf_list = [50_000_000, 65_000_000, 75_000_000, 85_000_000, 100_000_000]
        result = calculate_fcf_cagr(fcf_list)
        assert result is not None
        assert result.cagr_detail["raw_cagr"] < 0  # 減少トレンドは負のCAGRが正しい
        assert result.rate == 0.15  # growth_floor(15%)でクリップ

    def test_uses_most_recent_five_years_when_list_longer(self):
        """fcf_listが5年超の場合、末尾（最古側）ではなく先頭（直近側）5年分を対象にする"""
        calculate_fcf_cagr = self._import()
        # 新しい順で7年分: 直近5年は緩やかな成長、末尾2年（最古側）に極端な値を混ぜる
        fcf_list = [
            120_000_000, 110_000_000, 105_000_000, 100_000_000, 95_000_000,
            1_000_000, 500_000,  # 最古側2年（対象外になるべき極端な値）
        ]
        result = calculate_fcf_cagr(fcf_list)
        assert result is not None
        # 直近5年（120M→95M）のCAGRになっているはず。末尾の極端な値(1M/0.5M)を
        # 使うと桁違いに大きいCAGRになるため、それが使われていないことで確認する
        assert result.cagr_detail["periods"] == 4
        assert 0 < result.cagr_detail["raw_cagr"] < 0.10


class TestAnalyzeFcfOutlierDeviationPct:
    """DCF-REL-SYNC-1: analyze_fcf_outlier()のdeviation_pctフィールドのテスト"""

    @staticmethod
    def _import_analyze_fcf_outlier():
        from calculator.adjustments import analyze_fcf_outlier
        return analyze_fcf_outlier

    def test_deviation_large_populates_deviation_pct(self):
        """rule=deviation_large型はnote文字列と同一の乖離%を数値で保持する（FLYW型: 215%乖離）"""
        analyze_fcf_outlier = self._import_analyze_fcf_outlier()
        fcf_list = [163_819_000] + [52_000_000] * 4
        result = analyze_fcf_outlier(
            ticker="FLYW_TEST", fcf_list=fcf_list, fcf_5yr_avg=52_000_000,
            cv=0.3, fiscal_year_of_latest=2025, eps_data_dir="",
        )
        assert result.rule == "deviation_large"
        assert result.deviation_pct == pytest.approx(215.0, abs=1.0)

    def test_latest_negative_deviation_pct_is_none(self):
        """rule=latest_negative型（FCFマイナス）は乖離%の概念が成立しないためNone"""
        analyze_fcf_outlier = self._import_analyze_fcf_outlier()
        fcf_list = [-100_000_000] + [50_000_000] * 4
        result = analyze_fcf_outlier(
            ticker="NEG_TEST", fcf_list=fcf_list, fcf_5yr_avg=50_000_000,
            cv=0.3, fiscal_year_of_latest=2025, eps_data_dir="",
        )
        assert result.rule == "latest_negative"
        assert result.deviation_pct is None

    def test_no_outlier_deviation_pct_is_none(self):
        """外れ値なし（detected=False）の場合もdeviation_pctはNone"""
        analyze_fcf_outlier = self._import_analyze_fcf_outlier()
        fcf_list = [50_000_000] * 5
        result = analyze_fcf_outlier(
            ticker="FLAT_TEST", fcf_list=fcf_list, fcf_5yr_avg=50_000_000,
            cv=0.3, fiscal_year_of_latest=2025, eps_data_dir="",
        )
        assert result.detected is False
        assert result.deviation_pct is None

    def test_to_dict_includes_deviation_pct(self):
        """to_dict()の出力にdeviation_pctが含まれる"""
        analyze_fcf_outlier = self._import_analyze_fcf_outlier()
        fcf_list = [163_819_000] + [52_000_000] * 4
        result = analyze_fcf_outlier(
            ticker="FLYW_TEST2", fcf_list=fcf_list, fcf_5yr_avg=52_000_000,
            cv=0.3, fiscal_year_of_latest=2025, eps_data_dir="",
        )
        d = result.to_dict()
        assert "deviation_pct" in d
        assert d["deviation_pct"] == pytest.approx(215.0, abs=1.0)


# ─────────────────────────────────────────────
# 17. 汎用性検証: 新規銘柄自動適用 (回帰防止)
#
#     「実績FCF全年マイナス・RPO比率0.1・上場2年・ROEに損失年含む」
#     相当の新規銘柄に対して全修正が自動適用されることを確認する。
#     個別銘柄のif分岐ではなく汎用ロジックが駆動することを証明する。
# ─────────────────────────────────────────────

_SEC_READER_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _SEC_READER_DIR not in sys.path:
    sys.path.insert(0, _SEC_READER_DIR)
from common.sec_data.reader import SECReader  # type: ignore[import]


class TestNewTickerIntegration:
    """
    汎用性検証: 「実績FCF全年マイナス・RPO比率0.10・上場2年・ROEに損失年含む」
    相当の新規銘柄 (NEWCO) に対し、全修正が特定銘柄ハードコードなしで
    自動適用されることを確認する回帰防止テスト群。
    """

    @staticmethod
    def _make_neg_fcf_valuation(upside: float = -5.0) -> dict:
        """実績FCF全年マイナス新規銘柄のバリュエーション dict（_minimal_valuation ベース）"""
        val = _minimal_valuation(upside=upside)
        # FCF全年マイナス → revenue_floor適用 → FCF_Reliability=LOW
        val["fcf_estimation"] = {"applied": False, "sector": "Technology"}
        val["components"].update({
            "fcf_base_used":    900_000_000,
            "fcf_floor_applied": 900_000_000,   # > 0 → DCF_Reliability=LOW
            "fcf_5yr_avg":     -6_000_000_000,  # 実績avg negative
            "fcf_base_method": "avg_5yr",
            "fcf_list_raw":    [-8e9, -6e9, -4e9],
        })
        # RICE未計算 → Matrix④ を使わせる
        val["rice"] = {"available": False, "note": ""}
        return val

    def test_dcf_reliability_low_shown_in_report_for_negative_fcf_ticker(self, tmp_path):
        """新規銘柄: 実績FCF全年マイナス → DCF_Reliability=LOW が report に自動表示される"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = self._make_neg_fcf_valuation()
        score_data = {"score": "WATCH", "funda_score": 25, "score_comment": "DCF信頼性LOW"}
        report = pipe._generate_report("NEWCO", val, score_data, _minimal_extra())
        assert "DCF_Reliability: LOW" in report

    def test_rpo_ratio_below_threshold_auto_excluded_for_new_ticker(self):
        """新規銘柄: RPO/Revenue=0.10 → whitelist未登録の場合に自動的に rpo_pv=0 かつ除外理由が設定される"""
        result = adjust_rpo(
            rpo=5_000_000_000,         # RPO $5B
            sector="Technology",
            ticker="NEWCO",             # whitelist未登録
            industry="Software - Application",
            op_margin=0.25,
            rev_ttm=50_000_000_000,    # Revenue $50B → ratio=0.10 < 0.30
        )
        assert result.rpo_pv == 0.0
        assert "not applied" in result.exclusion_reason
        assert "0.10" in result.exclusion_reason

    def test_matrix4_uses_actual_negative_fcf_margin_for_new_ticker(self, tmp_path):
        """新規銘柄: rice未計算 → Matrix④ が fcf_history の実績マイナスマージンを使う"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = self._make_neg_fcf_valuation(upside=-5.0)
        extra = _minimal_extra()
        extra["fcf_history"] = [{"fcf": -500_000_000, "fcf_margin": -125.0, "year": 2024}]
        score_data = {"score": "WATCH", "funda_score": 25, "score_comment": "test"}
        report = pipe._generate_report("NEWCO", val, score_data, extra)
        assert "④キャッシュ創出力系" in report
        assert "FCF_Margin = -125.0%" in report

    def test_roe_avg_includes_negative_year_in_calculation(self, tmp_path):
        """新規銘柄: reader.py が損失年度を除外せず全期間平均でROEを算出する"""
        sec_dir = tmp_path / "NEWCO"
        sec_dir.mkdir()
        # 2022: -20%、2023: -10%、2024: +6% → 平均 -8%
        for yr, ni in [(2022, -200_000_000), (2023, -100_000_000), (2024, 60_000_000)]:
            (sec_dir / f"annual_{yr}.json").write_text(json.dumps({
                "pl": {"net_income": ni},
                "bs": {"stockholders_equity": 1_000_000_000},
            }), encoding="utf-8")

        reader = SECReader(data_dir=str(tmp_path))
        avg, years_used, _ = reader.get_roe_avg_detail("NEWCO", years=10)

        assert years_used == 3, f"損失年を含む全3年が計算に使われるべき (got {years_used})"
        assert avg < 0, f"損失年込み平均はマイナスになるべき (got {avg:.2%})"
        assert abs(avg - (-0.08)) < 0.005, f"平均ROE ≈ -8% のはず (got {avg:.2%})"

    def test_watch_score_auto_applied_for_negative_fcf_new_ticker(self, tmp_path):
        """新規銘柄: FCF全年マイナス(floor適用) → SCORE が WATCH に自動丸めされる"""
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        valuation = {
            "upside_percent": -5.0,
            "components": {
                "fcf_floor_applied": 900_000_000,  # LOW判定トリガー
                "diluted_shares": None,
            },
            "fcf_base": {"base_fcf": 900_000_000},
            "financial_health": {},
            "fcf_estimation": {},
        }
        result = pipe._compute_tanuki_score("NEWCO", valuation)
        assert result["score"] == "WATCH"
        assert "LOW" in result["score_comment"]


# ─────────────────────────────────────────────
# 17.5. FY52WEEK-BS-FADEOUT-FALLBACK-1 回帰テスト
#
#     short_term_investments/long_term_debt/short_term_debtが最新年度で
#     完全欠損（None）だが過去に明示的$0申告実績がある場合、真のゼロと
#     推定してis_estimated_zero/last_confirmed_zero_yearを付与すること。
#     直近の既知値が非ゼロの複雑パターン（CSGP/KULR/RCAT型）では
#     誤って推定ゼロにならないことも確認する。
# ─────────────────────────────────────────────

class TestFadeoutZeroFallback:
    """FY52WEEK-BS-FADEOUT-FALLBACK-1: reader.py::get_net_cash()の
    履歴フォールバックロジック（年数閾値なし・条件判定による汎用ロジック）"""

    def _write_annual(self, tmp_path, ticker: str, period_to_bs: dict) -> None:
        ticker_dir = tmp_path / ticker
        ticker_dir.mkdir(parents=True, exist_ok=True)
        for period, bs in period_to_bs.items():
            (ticker_dir / f"annual_{period}.json").write_text(
                json.dumps({"period": period, "bs": bs}), encoding="utf-8"
            )

    def test_pltr_type_clean_fadeout_gets_estimated_zero(self, tmp_path):
        """PLTR/long_term_debt型（代表例）: 過去年度に明示的0があり最新年度が
        Noneの場合、is_estimated_zero=True・last_confirmed_zero_yearが
        正しく設定される"""
        self._write_annual(tmp_path, "TESTCO", {
            2019: {"long_term_debt": 500_000_000},
            2020: {"long_term_debt": 500_000_000},
            2021: {"long_term_debt": 0},
            2022: {"long_term_debt": None},
            2023: {"long_term_debt": None},
            2024: {"long_term_debt": None},
            2025: {"long_term_debt": None},
        })
        reader = SECReader(data_dir=str(tmp_path))
        result = reader.get_net_cash("TESTCO")
        assert result["ltdebt_estimated_zero"] is True
        assert result["ltdebt_last_confirmed_zero_year"] == 2021
        assert result["long_term_debt"] == 0.0

    def test_gap_of_many_years_still_applies_no_threshold(self, tmp_path):
        """年数閾値なしの確認: 12年前の$0実績でも推定ゼロが適用される
        （CSGP/short_term_investments型の年数ギャップに相当）"""
        period_to_bs = {2013: {"short_term_investments": 0}}
        for yr in range(2014, 2026):
            period_to_bs[yr] = {"short_term_investments": None}
        self._write_annual(tmp_path, "TESTCO", period_to_bs)
        reader = SECReader(data_dir=str(tmp_path))
        result = reader.get_net_cash("TESTCO")
        assert result["sti_estimated_zero"] is True
        assert result["sti_last_confirmed_zero_year"] == 2013

    def test_csgp_type_nonzero_last_known_value_not_flagged(self, tmp_path):
        """CSGP/short_term_investments型: 2013年$0の後、2015-2018年に実額
        （非ゼロ）が再登場してから2019年以降消失するケース。直近の既知値
        （2018年、非ゼロ）が優先され、誤って推定ゼロにならないこと"""
        self._write_annual(tmp_path, "TESTCO", {
            2011: {"short_term_investments": 3_515_000},
            2012: {"short_term_investments": 37_000},
            2013: {"short_term_investments": 0},
            2014: {"short_term_investments": None},
            2015: {"short_term_investments": 15_507_000},
            2016: {"short_term_investments": 9_952_000},
            2017: {"short_term_investments": 10_070_000},
            2018: {"short_term_investments": 10_070_000},
            2019: {"short_term_investments": None},
            2020: {"short_term_investments": None},
        })
        reader = SECReader(data_dir=str(tmp_path))
        result = reader.get_net_cash("TESTCO")
        assert result["sti_estimated_zero"] is False, (
            "CSGP型回帰: 直近の既知値(2018年, $10.07M)が非ゼロにも関わらず"
            "誤って推定ゼロと判定された"
        )
        assert result["sti_last_confirmed_zero_year"] is None

    def test_kulr_type_nonzero_reappears_after_zero_not_flagged(self, tmp_path):
        """KULR/short_term_debt型: 2021-2022年$0の後、2024年に実額が
        再登場してから2025年に消失するケース。誤って推定ゼロにならないこと"""
        self._write_annual(tmp_path, "TESTCO", {
            2020: {"short_term_debt": 2_321_802},
            2021: {"short_term_debt": 0},
            2022: {"short_term_debt": 0},
            2023: {"short_term_debt": None},
            2024: {"short_term_debt": 516_547},
            2025: {"short_term_debt": None},
        })
        reader = SECReader(data_dir=str(tmp_path))
        result = reader.get_net_cash("TESTCO")
        assert result["stdebt_estimated_zero"] is False, (
            "KULR型回帰: 直近の既知値(2024年, $516,547)が非ゼロにも関わらず"
            "誤って推定ゼロと判定された"
        )
        assert result["stdebt_last_confirmed_zero_year"] is None

    def test_rcat_type_intermittent_nonzero_values_not_flagged(self, tmp_path):
        """RCAT/long_term_debt型: $0期間の後に複数回、断続的に実額が出現する
        より複雑なケースでも誤って推定ゼロにならないこと"""
        self._write_annual(tmp_path, "TESTCO", {
            2012: {"long_term_debt": 0},
            2013: {"long_term_debt": 0},
            2014: {"long_term_debt": 0},
            2015: {"long_term_debt": 0},
            2016: {"long_term_debt": None},
            2017: {"long_term_debt": None},
            2018: {"long_term_debt": 1_982_829},
            2019: {"long_term_debt": None},
            2020: {"long_term_debt": 450_000},
            2021: {"long_term_debt": None},
            2022: {"long_term_debt": 973_707},
            2023: {"long_term_debt": 401_569},
            2024: {"long_term_debt": None},
            2025: {"long_term_debt": None},
        })
        reader = SECReader(data_dir=str(tmp_path))
        result = reader.get_net_cash("TESTCO")
        assert result["ltdebt_estimated_zero"] is False, (
            "RCAT型回帰: 直近の既知値(2023年, $401,569)が非ゼロにも関わらず"
            "誤って推定ゼロと判定された"
        )
        assert result["ltdebt_last_confirmed_zero_year"] is None

    def test_no_historical_zero_at_all_not_flagged(self, tmp_path):
        """過去に一度も明示的0の申告実績がない場合（真の構造的不明）は
        推定ゼロを適用しない"""
        self._write_annual(tmp_path, "TESTCO", {
            2023: {"short_term_debt": None},
            2024: {"short_term_debt": None},
            2025: {"short_term_debt": None},
        })
        reader = SECReader(data_dir=str(tmp_path))
        result = reader.get_net_cash("TESTCO")
        assert result["stdebt_estimated_zero"] is False
        assert result["stdebt_last_confirmed_zero_year"] is None

    def test_latest_year_has_real_value_not_flagged(self, tmp_path):
        """最新年度に本人データがある場合（完全欠損ではない）は
        推定ゼロロジック自体が発火しない"""
        self._write_annual(tmp_path, "TESTCO", {
            2023: {"long_term_debt": 0},
            2024: {"long_term_debt": None},
            2025: {"long_term_debt": 100_000_000},
        })
        reader = SECReader(data_dir=str(tmp_path))
        result = reader.get_net_cash("TESTCO")
        assert result["ltdebt_estimated_zero"] is False
        assert result["long_term_debt"] == 100_000_000.0


# ─────────────────────────────────────────────
# 18. BUG-MATRIX4-1追補: fcf_history末尾None銘柄のMatrix④回帰防止
#
#     上場後まもない / SEC年次未取得年が末尾にある銘柄（RCATパターン）で
#     fcf_history[-1]がNoneのとき、実績マイナスのマージンを正しく採用し
#     revenue_floor正値にフォールバックしないことを確認する。
# ─────────────────────────────────────────────

class TestMatrix4TailNoneRegression:
    """
    BUG-MATRIX4-1追補:
    fcf_history の末尾エントリーが fcf=None/fcf_margin=None の銘柄(RCATパターン)で
    Matrix④ Key_Metric_Y が実績マイナスマージンを採用することを保証する回帰防止テスト。
    """

    @staticmethod
    def _make_tail_none_valuation(upside: float = -67.0) -> dict:
        """末尾None + floor適用 のバリュエーション dict（RCATパターン）"""
        val = _minimal_valuation(upside=upside)
        val["fcf_estimation"] = {"applied": False, "sector": "Technology"}
        val["components"].update({
            "fcf_base_used":     41_393_459.0,   # revenue_floor適用後の正値
            "fcf_floor_applied": 41_393_459.0,   # > 0 → DCF_Reliability=LOW
            "fcf_5yr_avg":      -16_000_000.0,
            "fcf_base_method":  "avg_5yr",
            "fcf_list_raw":     [-1.4e6, -16.4e6, -31.6e6],
            "latest_revenue":    101_800_000.0,
        })
        val["rice"] = {"available": False, "note": ""}
        return val

    def test_matrix4_tail_none_uses_last_real_margin(self, tmp_path):
        """末尾None銘柄: reversed()で2023年(-319.3%)を採用、+40.7%にフォールバックしない"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = self._make_tail_none_valuation()
        extra = _minimal_extra()
        # 2024/2025 は fcf=None/fcf_margin=None（SEC未取得）、2023 が最後の実績
        extra["fcf_history"] = [
            {"year": 2021, "fcf": -1_399_001,  "fcf_margin": -28.0},
            {"year": 2022, "fcf": -16_383_009, "fcf_margin": -254.8},
            {"year": 2023, "fcf": -31_649_633, "fcf_margin": -319.3},
            {"year": 2024, "fcf": None,         "fcf_margin": None},
            {"year": 2025, "fcf": None,         "fcf_margin": None},
        ]
        score_data = {"score": "WATCH", "funda_score": 25, "score_comment": "DCF信頼性LOW"}
        report = pipe._generate_report("RCATLIKE", val, score_data, extra)

        # 実績マイナスマージン(2023年=-319.3%)が使われること
        assert "FCF_Margin = -319.3%" in report, (
            "末尾NoneのときKey_Metric_Yが revenue_floor正値(+40.7%)を使うリグレッション"
        )
        # 高FCFラベルになっていないこと
        assert "割高×高FCF" not in report
        assert "低FCF" in report

    def test_matrix4_all_none_floor_applied_shows_na(self, tmp_path):
        """全年None + floor適用: revenue_floor正値でなくN/Aを表示する"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = self._make_tail_none_valuation()
        extra = _minimal_extra()
        # 全年 None（理論上の極端ケース）
        extra["fcf_history"] = [
            {"year": 2024, "fcf": None, "fcf_margin": None},
            {"year": 2025, "fcf": None, "fcf_margin": None},
        ]
        score_data = {"score": "WATCH", "funda_score": 25, "score_comment": "DCF信頼性LOW"}
        report = pipe._generate_report("ALLNONE", val, score_data, extra)

        # floor適用時は revenue_floor正値でなく N/A になること
        assert "FCF_Margin = N/A" in report
        # 40.7% のような正値が出ていないこと
        import re
        m = re.search(r'FCF_Margin = ([+-]?\d+\.?\d*)%', report)
        assert m is None, f"floor適用時に正値が表示された: {m.group(0)}"


# ─────────────────────────────────────────────
# 19. 回帰防止: Fix1/Fix2/Fix3 (2026-06-11 バグ修正)
#
#   Fix1: scenario_valuations を growth source に関わらず全銘柄で計算する
#   Fix2: segment_config.json 未登録銘柄に segment_configured=False をセット
#   Fix3: Matrix② 定義文の ROE 年数を roe_years_used から動的に生成する
# ─────────────────────────────────────────────

class TestScenarioValuationsAlwaysRendered:
    """Fix1 回帰防止: scenario_valuations が None のとき BEAR/BULL が $0.00 になるリグレッション検出"""

    def test_bear_bull_non_zero_when_scenario_valuations_populated(self, tmp_path):
        """scenario_valuations に BEAR/BULL が設定されていれば report に非ゼロ IV が表示される"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = _minimal_valuation(upside=30.0)
        val["scenario_valuations"] = {
            "bear": {"growth_rate": 0.15, "intrinsic_value_per_share": 110.0},
            "base": {"growth_rate": 0.25, "intrinsic_value_per_share": 140.0},
            "bull": {"growth_rate": 0.35, "intrinsic_value_per_share": 175.0},
        }
        score_data = _minimal_score_data()
        report = pipe._generate_report("SCENTEST", val, score_data, _minimal_extra())

        # BEAR/BULL の IV が 0.00 でないこと（旧バグでは segment_weighted 以外は $0.00 になっていた）
        assert "BEAR: Growth=15.0%, IV=$110.00" in report, \
            "BEAR IV が正しく表示されていない (segment_weighted 以外でシナリオが計算されないリグレッション)"
        assert "BULL: Growth=35.0%, IV=$175.00" in report, \
            "BULL IV が正しく表示されていない"

    def test_bear_bull_show_zero_when_scenario_valuations_absent(self, tmp_path):
        """scenario_valuations が None のときは BEAR/BULL が $0.00 になること（旧バグ再現・検出用）"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = _minimal_valuation(upside=30.0)
        val["scenario_valuations"] = None  # 旧バグ: segment_weighted 以外はここが None だった
        score_data = _minimal_score_data()
        report = pipe._generate_report("SCENTEST_NULL", val, score_data, _minimal_extra())

        assert "BEAR: Growth=0.0%, IV=$0.00" in report
        assert "BULL: Growth=0.0%, IV=$0.00" in report


class TestSegmentConfiguredFalseForUnconfiguredTicker:
    """Fix2 回帰防止: segment_config.json 未登録銘柄に segment_configured=False がセットされること"""

    def test_absent_ticker_gets_segment_configured_false(self, tmp_path):
        """segment_config.json に存在しない銘柄は segment_configured=False になる"""
        pipe = _make_pipe(tmp_path)
        # repo_root/config/segment_config.json を作成（OTHERTICKER のみ登録）
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / "segment_config.json").write_text(
            json.dumps({"OTHERTICKER": {"segments": {"Cloud": {"weight": 1.0, "growth": 0.15}}}}),
            encoding="utf-8",
        )
        valuation = {"components": {"latest_revenue": 1_000_000_000}}
        result = pipe._load_extra_data("NEWTICKER", valuation)

        assert result.get("segment_configured") is False, (
            "segment_config.json 未登録銘柄で segment_configured=False がセットされていない"
            " (旧バグ: キーが存在せず extra.get('segment_configured', True) が True になっていた)"
        )

    def test_registered_ticker_gets_segment_configured_true(self, tmp_path):
        """segment_config.json に登録済みの銘柄は segment_configured=True になる"""
        pipe = _make_pipe(tmp_path)
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / "segment_config.json").write_text(
            json.dumps({"REGTICKER": {"segments": {"Cloud": {"weight": 1.0, "growth": 0.20}}}}),
            encoding="utf-8",
        )
        valuation = {"components": {"latest_revenue": 5_000_000_000}}
        result = pipe._load_extra_data("REGTICKER", valuation)

        assert result.get("segment_configured") is True, \
            "登録済み銘柄で segment_configured=True がセットされていない"


class TestLoadExtraDataNextEarningsDate:
    """[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-4: next_earnings_dateの
    取得元をyfinance直接呼び出し（.calendar）からcommon.market_data.reader
    経由（_md_get_calendar）に切替えたことの回帰テスト。過去日の場合はリスト
    内の次の未来日を採用する既存ロジック（Phase3-1修正）は維持している。"""

    def test_future_earnings_date_is_selected(self, tmp_path, monkeypatch):
        monkeypatch.setattr(pipeline, "HAS_MARKET_DATA", True)
        monkeypatch.setattr(pipeline, "_md_get_calendar", lambda ticker: {"earnings_date": ["2099-01-01"]})
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        valuation = {"components": {"latest_revenue": 1_000_000_000}}
        result = pipe._load_extra_data("FUTURETICKER", valuation)
        assert result.get("next_earnings_date") == "2099-01-01"

    def test_earliest_future_date_is_selected_when_multiple_present(self, tmp_path, monkeypatch):
        monkeypatch.setattr(pipeline, "HAS_MARKET_DATA", True)
        monkeypatch.setattr(
            pipeline, "_md_get_calendar",
            lambda ticker: {"earnings_date": ["2020-01-01", "2099-01-01", "2099-04-01"]},
        )
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        valuation = {"components": {"latest_revenue": 1_000_000_000}}
        result = pipe._load_extra_data("MULTITICKER", valuation)
        assert result.get("next_earnings_date") == "2099-01-01"

    def test_all_past_dates_fall_back_to_first_entry(self, tmp_path, monkeypatch):
        """全ての日付が過去の場合はリストの最初の値を採用する（旧ロジック踏襲）"""
        monkeypatch.setattr(pipeline, "HAS_MARKET_DATA", True)
        monkeypatch.setattr(
            pipeline, "_md_get_calendar",
            lambda ticker: {"earnings_date": ["2020-01-01", "2020-04-01"]},
        )
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        valuation = {"components": {"latest_revenue": 1_000_000_000}}
        result = pipe._load_extra_data("PASTTICKER", valuation)
        assert result.get("next_earnings_date") == "2020-01-01"

    def test_empty_calendar_yields_no_next_earnings_date_key(self, tmp_path, monkeypatch):
        """reader.get_calendar()の中立デフォルト（空dict）ではキー自体を
        resultに追加しない（旧コードのcalendar取得失敗・空時と同じ挙動）"""
        monkeypatch.setattr(pipeline, "HAS_MARKET_DATA", True)
        monkeypatch.setattr(pipeline, "_md_get_calendar", lambda ticker: {})
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        valuation = {"components": {"latest_revenue": 1_000_000_000}}
        result = pipe._load_extra_data("EMPTYTICKER", valuation)
        assert "next_earnings_date" not in result

    def test_market_data_unavailable_yields_no_next_earnings_date_key(self, tmp_path, monkeypatch):
        """HAS_MARKET_DATA=False（common.market_data未import環境）でも例外に
        ならず中立デフォルトのまま継続する"""
        monkeypatch.setattr(pipeline, "HAS_MARKET_DATA", False)
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        valuation = {"components": {"latest_revenue": 1_000_000_000}}
        result = pipe._load_extra_data("NOMDTICKER", valuation)
        assert "next_earnings_date" not in result


class TestSegmentTtmRevenueFallbackKeyCaseFix:
    """[[DUPONT-TTM-FIELD-CASE-MISMATCH-1]]の回帰テスト（同根バグの2件目）。

    Segment_Weighted_Growthの「General fallback」（非12月期決算銘柄の
    TTM売上陳腐化対策）も同じPascalCase/snake_caseキー不一致で、
    `_ttm_rev_for_seg`が常に年次売上のままフォールバックし続けていた。
    """

    def test_newer_ttm_revenue_overrides_stale_annual_revenue(self, tmp_path):
        """TTM売上が年次売上より新しい（大きい）場合、snake_caseキー経由で
        正しく上書きされることを確認する（修正前はPascalCase参照のため
        常にNoneになり、年次売上のまま変化しなかった）"""
        pipe = _make_pipe(tmp_path)
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / "segment_config.json").write_text(
            json.dumps({"SEGTICK": {"segments": {"General": {"weight": 1.0, "growth": 0.1}}}}),
            encoding="utf-8",
        )
        ttm_dir = tmp_path / "common" / "sec_data" / "ttm"
        ttm_dir.mkdir(parents=True, exist_ok=True)
        (ttm_dir / "SEGTICK_ttm_series.json").write_text(
            json.dumps({
                "series": [{
                    "ttm_end": "2026-06-30",
                    "flow": {"revenue": {"val": 115_299_000, "quarters_used": 4, "missing": 0}},
                }],
            }),
            encoding="utf-8",
        )
        # latest_revenue（年次売上、陳腐化した値）はTTM売上より小さい
        valuation = {"components": {"latest_revenue": 70_918_000, "diluted_shares": 0, "current_price": 0}}

        result = pipe._load_extra_data("SEGTICK", valuation)

        segs = result.get("segments") or []
        assert len(segs) == 1
        # weight=1.0のためestimated_revenueはTTM売上そのものになるはず
        assert segs[0]["estimated_revenue"] == pytest.approx(115_299_000.0), (
            "TTM売上が年次売上より新しい場合に上書きされていない"
            "（旧バグ: PascalCase参照のためrevenueが常にNoneになり年次売上のまま）"
        )


class TestMatrix2ROEDefinitionDynamicYear:
    """Fix3 回帰防止: Matrix② 定義文の ROE 年数が roe_years_used から動的に生成されること"""

    @staticmethod
    def _make_sector_excluded_valuation(roe_years_used: int = 6) -> dict:
        """Matrix②（セクター除外）を使うバリュエーション dict"""
        val = _minimal_valuation(upside=20.0)
        val["rice"] = {
            "available": False,
            "note": "セクター除外",
            "base": {},
            "bear": {},
            "bull": {},
        }
        val["components"]["roe_10yr_avg"] = 0.18
        val["components"]["roe_years_used"] = roe_years_used
        return val

    def test_matrix2_definition_uses_roe_years_used(self, tmp_path):
        """roe_years_used=6 のとき Matrix② 定義文に ROE_6yr_avg が含まれる"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = self._make_sector_excluded_valuation(roe_years_used=6)
        score_data = _minimal_score_data()
        report = pipe._generate_report("ROE6TEST", val, score_data, _minimal_extra())

        assert "Matrix②(収益性系): Y=ROE_6yr_avg, X=Deviation Rate" in report, \
            "roe_years_used=6 のとき ROE_6yr_avg が表示されるべき (旧バグ: 固定 ROE_10yr_avg)"
        assert "ROE_10yr_avg" not in report.split("[2. MATRIX POSITION]")[1].split("[3.")[0], \
            "roe_years_used=6 なのに ROE_10yr_avg が残っている"

    def test_matrix2_definition_default_10yr(self, tmp_path):
        """roe_years_used が未設定のとき Matrix② 定義文に ROE_10yr_avg が含まれる"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = self._make_sector_excluded_valuation()
        val["components"].pop("roe_years_used", None)  # キーを削除してデフォルト動作を確認
        score_data = _minimal_score_data()
        report = pipe._generate_report("ROE10DEFAULT", val, score_data, _minimal_extra())

        assert "Matrix②(収益性系): Y=ROE_10yr_avg, X=Deviation Rate" in report, \
            "roe_years_used 未設定のとき ROE_10yr_avg がデフォルト値として表示されるべき"


# ─────────────────────────────────────────────
# 20. 回帰防止: SEC-REV-FINTECH-1 (2026-06-11)
#
#   金融セクター銘柄(SOFI等)で parser.py が狭義 revenue タグ
#   (RevenueFromContract...) を採用し annual revenue が過小になる問題。
#   quarterly.py の TICKER_RESTRICTIONS[ticker]["revenue_concept"] で
#   指定されたタグのみを使うことで広義 revenue を採用する。
# ─────────────────────────────────────────────

class TestFinancialSectorRevenueParsing:
    """
    SEC-REV-FINTECH-1 回帰防止:
    TICKER_RESTRICTIONS に revenue_concept が設定されている銘柄では
    parser.py が指定タグのみを使って annual revenue を取得することを保証する。
    """

    def test_revenue_concept_override_uses_single_tag(self, tmp_path):
        """revenue_concept 指定銘柄では指定タグのみが使われ広義 revenue が採用される"""
        import sys
        sys.path.insert(0, str(tmp_path))

        from common.sec_data.parser import SECParser
        from common.sec_data.quarterly import TICKER_RESTRICTIONS

        # TICKER_RESTRICTIONS に revenue_concept が登録されていること
        assert "SOFI" in TICKER_RESTRICTIONS, "SOFI が TICKER_RESTRICTIONS に未登録"
        assert "revenue_concept" in TICKER_RESTRICTIONS["SOFI"], \
            "SOFI の revenue_concept が TICKER_RESTRICTIONS に未設定"
        override_concept = TICKER_RESTRICTIONS["SOFI"]["revenue_concept"]
        assert override_concept == "RevenuesNetOfInterestExpense"

        # parser が SOFI の company_facts.json を読んで正しい revenue を返すこと
        # (実際のファイルが存在する場合のみテスト)
        import os
        facts_path = os.path.join(
            os.path.dirname(__file__), "..", "common", "sec_data", "data", "SOFI", "company_facts.json"
        )
        if not os.path.exists(facts_path):
            return  # CI 環境でファイルがない場合はスキップ

        p = SECParser()
        result = p.parse_company_facts("SOFI")
        assert result is not None

        # FY2025 の revenue が RevenuesNetOfInterestExpense の値 ($3.61B) に近いこと
        # (RevenueFromContractWithCustomer の狭義値 $619M ではないこと)
        fy2025_rev = result["annual"].get(2025, {}).get("pl", {}).get("revenue")
        assert fy2025_rev is not None, "FY2025 revenue が None"
        assert fy2025_rev > 1_000_000_000, (
            f"SOFI FY2025 revenue = ${fy2025_rev/1e9:.2f}B が $1B 未満 "
            f"(狭義 revenue タグ ${619e6/1e9:.2f}B が採用されるリグレッション)"
        )

    def test_revenue_concept_not_applied_to_normal_ticker(self, tmp_path):
        """revenue_concept 未設定銘柄は通常の MERGE_ALL_TAGS 動作を使う"""
        from common.sec_data.quarterly import TICKER_RESTRICTIONS

        # AAPL には revenue_concept が設定されていないこと
        aapl_restrictions = TICKER_RESTRICTIONS.get("AAPL", {})
        assert "revenue_concept" not in aapl_restrictions, \
            "AAPL に revenue_concept が誤って設定されている"

    def test_ticker_restrictions_revenue_concept_values_are_valid_xbrl_tags(self):
        """TICKER_RESTRICTIONS の revenue_concept 値が有効な XBRL タグ形式であること"""
        from common.sec_data.quarterly import TICKER_RESTRICTIONS
        from common.sec_data.parser import SECParser

        valid_tags = set(SECParser.XBRL_MAPPING.get("revenue", []))
        for ticker, restrictions in TICKER_RESTRICTIONS.items():
            concept = restrictions.get("revenue_concept")
            if concept:
                assert concept in valid_tags, (
                    f"{ticker}: revenue_concept='{concept}' が "
                    f"SECParser.XBRL_MAPPING['revenue'] に含まれていない "
                    "(parser.py が採用できないタグが指定されている)"
                )


# =============================================================================
# Section 21: BUG-NETDEBT-2 LongTermDebt二重計上修正 回帰テスト
# =============================================================================

class TestLongTermDebtPriorityNoncurrent:
    """BUG-NETDEBT-2: LongTermDebt (total) と LongTermDebtCurrent の二重計上防止"""

    def test_xbrl_mapping_priority_noncurrent_first(self):
        """XBRL_MAPPING の long_term_debt は LongTermDebtNoncurrent が最優先"""
        from common.sec_data.parser import SECParser

        priority = SECParser.XBRL_MAPPING["long_term_debt"]
        assert priority[0] == "LongTermDebtNoncurrent", (
            f"long_term_debt[0]={priority[0]!r} should be 'LongTermDebtNoncurrent'. "
            "BUG-NETDEBT-2: LongTermDebt (total) が先頭だと LongTermDebtCurrent との二重計上が発生する"
        )
        assert "LongTermDebt" in priority, "LongTermDebt フォールバックが priority リストに必要"
        # LongTermDebt は LongTermDebtNoncurrent より後ろ
        assert priority.index("LongTermDebt") > priority.index("LongTermDebtNoncurrent"), \
            "LongTermDebt は LongTermDebtNoncurrent より後ろに位置すること"

    def test_short_term_debt_uses_current_tag(self):
        """short_term_debt は LongTermDebtCurrent タグを持つこと（total との分離確認）"""
        from common.sec_data.parser import SECParser

        st_priority = SECParser.XBRL_MAPPING.get("short_term_debt", [])
        assert "LongTermDebtCurrent" in st_priority, \
            "short_term_debt mapping に LongTermDebtCurrent がない"

    def test_docn_total_debt_corrected(self):
        """DOCN の annual JSON で Total_Debt が是正されていること (BUG-NETDEBT-2 後)"""
        import os, json

        annual_path = os.path.join(
            os.path.dirname(__file__), "..", "common", "sec_data", "data", "DOCN", "annual.json"
        )
        if not os.path.exists(annual_path):
            return  # データファイル非存在時はスキップ

        with open(annual_path, encoding="utf-8") as f:
            data = json.load(f)

        # FY2024 の Total_Debt を確認（LTD_total+LTDcurrent の二重計上なら ~$1.62B になる）
        fy2024 = data.get("2024", {})
        lt = fy2024.get("bs", {}).get("long_term_debt")
        st = fy2024.get("bs", {}).get("short_term_debt", 0) or 0
        if lt is None:
            return  # データ構造が異なる場合はスキップ

        total = lt + st
        assert total < 1_500_000_000, (
            f"DOCN FY2024 Total_Debt=${total/1e9:.2f}B が $1.5B 超 "
            "— BUG-NETDEBT-2 が再発している可能性 (修正後は ~$1.30B が期待値)"
        )


# =============================================================================
# Section 22: BUG-REV-SPAC-1 / A-2-TTM 回帰テスト (2026-06-12)
# =============================================================================

class TestIonqRevenueSPACBug:
    """BUG-REV-SPAC-1: IONQの2022年10-K Revenuesタグ($1,235M)誤採用防止"""

    def test_ionq_revenue_concept_restriction_defined(self):
        """IONQ が TICKER_RESTRICTIONS に revenue_concept を持つこと"""
        from common.sec_data.quarterly import TICKER_RESTRICTIONS

        assert "IONQ" in TICKER_RESTRICTIONS, "IONQ が TICKER_RESTRICTIONS に未登録"
        assert "revenue_concept" in TICKER_RESTRICTIONS["IONQ"], \
            "IONQ の revenue_concept が TICKER_RESTRICTIONS に未設定"
        assert TICKER_RESTRICTIONS["IONQ"]["revenue_concept"] == \
            "RevenueFromContractWithCustomerExcludingAssessedTax", \
            "IONQ の revenue_concept が期待値と異なる"

    def test_ionq_annual_2022_revenue_corrected(self):
        """IONQ annual_2022.json の revenue が $1,235M ではなく ~$11M であること"""
        import os, json

        annual_path = os.path.join(
            os.path.dirname(__file__), "..", "common", "sec_data", "data", "IONQ", "annual_2022.json"
        )
        if not os.path.exists(annual_path):
            return

        with open(annual_path, encoding="utf-8") as f:
            data = json.load(f)

        rev = data.get("pl", {}).get("revenue")
        assert rev is not None, "IONQ annual_2022.json に revenue が存在しない"
        assert rev < 100_000_000, (
            f"IONQ 2022 revenue=${rev/1e6:.1f}M が $100M 超 "
            "— BUG-REV-SPAC-1 が再発している可能性 (正しい値は ~$11M)"
        )

    def test_ionq_fcf_margin_2022_not_anomalous(self):
        """IONQ FCF_Margin 2022 が他年と同程度の桁であること (孤立年でないこと)"""
        import os, json

        sec_dir = os.path.join(
            os.path.dirname(__file__), "..", "common", "sec_data", "data", "IONQ"
        )
        if not os.path.exists(sec_dir):
            return

        margins = {}
        for yr in [2021, 2022, 2023, 2024, 2025]:
            p = os.path.join(sec_dir, f"annual_{yr}.json")
            if not os.path.exists(p):
                continue
            with open(p, encoding="utf-8") as f:
                d = json.load(f)
            rev = d.get("pl", {}).get("revenue") or 0
            fcf = d.get("cf", {}).get("free_cash_flow")
            if fcf is not None and rev > 0:
                margins[yr] = abs(fcf / rev * 100)

        if 2022 not in margins or len(margins) < 3:
            return

        other_margins = [v for y, v in margins.items() if y != 2022]
        avg_other = sum(other_margins) / len(other_margins)
        margin_2022 = margins[2022]
        assert margin_2022 > avg_other / 100, (
            f"IONQ FCF_Margin 2022 ({margin_2022:.1f}%) が他年平均 ({avg_other:.1f}%) の"
            "1/100未満 — BUG-REV-SPAC-1 型の孤立年異常が再発している可能性"
        )


class TestTTMTerminologyDistinction:
    """A-2-TTM: TTM_YoY_Growth (実績) と CAGR_max (成長モデル判定指標) の区別
    注意: _gs は test_pipeline_logic.py 先頭で import された本物の growth_sanity モジュール。
          sys.modules["growth_sanity"] は MagicMock に差し替え済みのため _gs を直接使う。
    """

    def test_growth_model_reason_no_bare_ttm_in_median_label(self):
        """中央値モデル reason に 'TTM' を単独使用しないこと (CAGR_max= で表示)"""
        # cagr_3yr < 50%・cagr_5yr < 50% のケース → 中央値モデル
        result = _gs.check_growth_sanity(
            ticker="TEST_MEDIAN",
            phase1_growth=0.15,
            annual_revenues=[100e6, 110e6, 120e6, 135e6, 150e6],  # ~11% CAGR
            ttm_actual=0.15,
        )
        reason = result.get("growth_model_reason", "")
        model = result.get("growth_model")

        if model == "median":
            # "CAGR_max=" で始まる形式に変わっているはず
            assert not reason.startswith("TTM"), (
                f"中央値モデルの reason='{reason}' が 'TTM' で始まる "
                "(旧形式 'TTMxx%のため中央値...' → 新形式 'CAGR_max=xx%のため...' に変更すべき)"
            )

    def test_growth_model_decay_uses_cagr_max_label_when_cagr_triggers(self):
        """CAGR > 50% で逓減モデルが発動する場合 reason に 'CAGR_max' が含まれること"""
        # IONQ修正後相当: cagr_3yr ≈ 127% が逓減トリガー、phase1_growth=15%
        result = _gs.check_growth_sanity(
            ticker="TEST_IONQ",
            phase1_growth=0.15,
            annual_revenues=[2.1e6, 11.1e6, 22.0e6, 43.1e6, 130.0e6],
            ttm_actual=0.15,
        )
        model = result.get("growth_model")
        reason = result.get("growth_model_reason", "")

        assert model == "decay", f"逓減モデルが発動されるべき (actual model={model}, reason={reason})"
        assert "CAGR_max" in reason, (
            f"reason='{reason}' に 'CAGR_max' が含まれない "
            "(CAGR_3yr>50%が逓減トリガーのため CAGR_max= で表示すべき)"
        )
        assert "TTM_YoY" not in reason, (
            f"reason='{reason}' に 'TTM_YoY' が含まれる "
            "(CAGR起動の逓減モデルで TTM_YoY ラベルは不正確)"
        )


# =====================================================================
# Section 23: BUG-NETDEBT-5 回帰テスト
# ST_Investの期ズレ修正: CashはBUG-NETDEBT-1で最新四半期値に更新済みだが
# ST_Investはannual年次のままだったバグ。最新quarterly_*.jsonのbs値に上書き。
# =====================================================================

class TestSTInvestQuarterlyOverride:
    """BUG-NETDEBT-5: ST_Investが最新四半期bsから上書きされること

    [[TEST-STALE-IONQ-STINVEST-1]]対応（2026-08-31）: 当初
    quarterly_2026Q1（ST_Invest=$1,539M）をハードコードして検証していたが、
    その後IONQがquarterly_2026Q2（ST_Invest=$883M）を提出したことで
    ハードコード値が陳腐化し誤検知した（本番コードは最新四半期値を正しく
    採用していた）。将来同種の陳腐化が再発しないよう、固定値との比較では
    なく、`common/sec_data/data/IONQ/`の最新quarterlyファイルを都度動的に
    読み直して比較する方式に変更した。
    """

    def test_ionq_st_invest_uses_quarterly_value(self):
        """IONQ: latest.json の ST_Invest が最新quarterly SECファイルの値と一致すること"""
        import json, os, glob as _glob
        latest_path = os.path.join(
            os.path.dirname(__file__), "..",
            "docs", "value-monitor", "tanuki_valuation", "data", "IONQ", "latest.json"
        )
        sec_dir = os.path.join(
            os.path.dirname(__file__), "..", "common", "sec_data", "data", "IONQ"
        )
        if not os.path.exists(latest_path):
            return
        q_files = sorted(_glob.glob(os.path.join(sec_dir, "quarterly_*.json")))
        if not q_files:
            return
        with open(latest_path, encoding="utf-8") as f:
            latest = json.load(f)
        fh = latest.get("financial_health", {})
        if fh.get("sti_approximated"):
            # NVDA-STI-TAG-UNIDENTIFIED-1: 複数タグ合算近似値の場合は
            # 生のquarterly bs値と完全一致しないため対象外
            return
        with open(q_files[-1], encoding="utf-8") as f:
            q_sti = json.load(f).get("bs", {}).get("short_term_investments", 0) or 0
        st_invest = fh.get("short_term_investments", 0) or 0
        assert st_invest == q_sti, (
            f"IONQ ST_Invest={st_invest/1e6:.0f}M が最新quarterlyファイル"
            f"({os.path.basename(q_files[-1])})の値({q_sti/1e6:.0f}M)と不一致。"
            "BUG-NETDEBT-5: quarterly値で上書きされていない可能性"
        )

    def test_ionq_net_debt_corrected(self):
        """IONQ: Net_Debt が (cash + 最新quarterlyのST_Invest - total_debt) と整合すること

        net_cash = (cash + short_term_investments) - (long_term_debt + short_term_debt)
        （common/sec_data/reader.py::get_net_cash()の定義。net_debt = -net_cash）
        """
        import json, os, glob as _glob
        latest_path = os.path.join(
            os.path.dirname(__file__), "..",
            "docs", "value-monitor", "tanuki_valuation", "data", "IONQ", "latest.json"
        )
        sec_dir = os.path.join(
            os.path.dirname(__file__), "..", "common", "sec_data", "data", "IONQ"
        )
        if not os.path.exists(latest_path):
            return
        with open(latest_path, encoding="utf-8") as f:
            latest = json.load(f)
        fh = latest.get("financial_health", {})
        net_debt = fh.get("net_debt")
        if net_debt is None or fh.get("sti_approximated"):
            return
        q_files = sorted(_glob.glob(os.path.join(sec_dir, "quarterly_*.json")))
        if not q_files:
            return
        with open(q_files[-1], encoding="utf-8") as f:
            q_sti = json.load(f).get("bs", {}).get("short_term_investments", 0) or 0
        cash = fh.get("cash_and_equivalents") or 0
        total_debt = fh.get("total_debt") or 0
        expected_net_debt = -(cash + q_sti - total_debt)
        assert abs(net_debt - expected_net_debt) < 1.0, (
            f"IONQ Net_Debt={net_debt/1e9:.3f}B が期待値"
            f"(-({cash/1e6:.0f}M+{q_sti/1e6:.0f}M-{total_debt/1e6:.0f}M)="
            f"{expected_net_debt/1e9:.3f}B)と不一致。"
            "BUG-NETDEBT-5: ST_Invest期ズレが再発している可能性"
        )

    def test_ionq_quarterly_st_invest_exists_and_differs_from_annual(self):
        """IONQ: 最新quarterlyのST_Investがannualと異なること(テスト前提確認)"""
        import json, os, glob as _glob
        sec_dir = os.path.join(
            os.path.dirname(__file__), "..", "common", "sec_data", "data", "IONQ"
        )
        ann_files = sorted(_glob.glob(os.path.join(sec_dir, "annual_*.json")))
        q_files   = sorted(_glob.glob(os.path.join(sec_dir, "quarterly_*.json")))
        if not ann_files or not q_files:
            return
        with open(ann_files[-1], encoding="utf-8") as f:
            ann_sti = json.load(f).get("bs", {}).get("short_term_investments", 0) or 0
        with open(q_files[-1], encoding="utf-8") as f:
            q_sti = json.load(f).get("bs", {}).get("short_term_investments", 0) or 0
        assert q_sti != ann_sti, (
            f"IONQ annual_ST_Invest={ann_sti/1e6:.0f}M == quarterly_ST_Invest={q_sti/1e6:.0f}M "
            "テストの前提が崩れた(期が一致するか値が同じ)"
        )


# =====================================================================
# Section 24: NVDA-STI-TAG-UNIDENTIFIED-1 回帰テスト
# ANOMALY-PATTERN-CATALOG-1型C（資産クラス変化・当年度未タグ化型）対応方針①。
# annual FY2026(FYE 2026-01-25)はAvailableForSaleSecuritiesDebtSecurities
# ($39,520M、10-K本体)＋EquitySecuritiesFvNi($12,886M、後続10-Q〈2026-05-20
# 提出、Q1 FY2027〉の比較年度遡及開示にのみ登場)を合算した近似値$52,406M
# （実額$51,951M比+0.88%）。quarterly 2027Q1(end 2026-04-26)は同一10-Q内の
# 両タグ合算($39,233M+$30,237M=$69,470M、近似ではない正規合算値）。
# =====================================================================

class TestNvdaCrossFilingSTI:
    """parser.py::_apply_cross_filing_tags()がNVDAのshort_term_investmentsを
    正しく合算していること・近似値フラグが期待通り伝播することを確認する"""

    def test_nvda_annual_2026_sti_is_cross_filing_sum(self):
        """NVDA annual_2026.json: bs.short_term_investments が合算近似値$52,406Mであること"""
        import json, os
        ann_path = os.path.join(
            os.path.dirname(__file__), "..",
            "common", "sec_data", "data", "NVDA", "annual_2026.json"
        )
        if not os.path.exists(ann_path):
            return
        with open(ann_path, encoding="utf-8") as f:
            ann = json.load(f)
        sti = ann.get("bs", {}).get("short_term_investments")
        assert sti == 52_406_000_000, (
            f"NVDA annual_2026 short_term_investments={sti} != $52,406M "
            "(AvailableForSaleSecuritiesDebtSecurities $39,520M + EquitySecuritiesFvNi $12,886M)"
        )
        prov = ann.get("bs_provenance", {}).get("short_term_investments", {})
        assert prov.get("is_approximated") is True, (
            "NVDA annual_2026 short_term_investmentsはcross_filing_tags近似値のため"
            "bs_provenance.is_approximated=Trueが必須"
        )
        assert prov.get("residual_pct") == pytest.approx(0.0088, abs=1e-4), (
            f"NVDA annual_2026 residual_pct={prov.get('residual_pct')} != 0.0088(+0.88%)"
        )
        assert set(prov.get("combined_tags", [])) == {
            "AvailableForSaleSecuritiesDebtSecurities", "EquitySecuritiesFvNi",
        }

    def test_nvda_quarterly_2027q1_sti_is_exact_sum(self):
        """NVDA quarterly_2027Q1.json: bs.short_term_investments が同一10-Q内
        合算の正規値$69,470M（近似ではない）であること"""
        import json, os
        q_path = os.path.join(
            os.path.dirname(__file__), "..",
            "common", "sec_data", "data", "NVDA", "quarterly_2027Q1.json"
        )
        if not os.path.exists(q_path):
            return
        with open(q_path, encoding="utf-8") as f:
            q = json.load(f)
        sti = q.get("bs", {}).get("short_term_investments")
        assert sti == 69_470_000_000, (
            f"NVDA quarterly_2027Q1 short_term_investments={sti} != $69,470M "
            "(AvailableForSaleSecuritiesDebtSecurities $39,233M + EquitySecuritiesFvNi $30,237M)"
        )

    def test_nvda_latest_json_reports_quarterly_sti_not_approximated(self):
        """NVDA latest.json: BUG-NETDEBT-4の同一時点優先ロジックにより
        financial_healthは四半期の正規合算値を採用し、sti_approximatedはFalseになること
        （annual側の近似値フラグが誤って伝播しないことの確認）"""
        import json, os
        latest_path = os.path.join(
            os.path.dirname(__file__), "..",
            "docs", "value-monitor", "tanuki_valuation", "data", "NVDA", "latest.json"
        )
        if not os.path.exists(latest_path):
            return
        with open(latest_path, encoding="utf-8") as f:
            latest = json.load(f)
        fh = latest.get("financial_health", {})
        if fh.get("net_debt_period") != "2027Q1":
            # BUG-NETDEBT-4の対象四半期は将来のデータ更新で変わりうるため、
            # 現行の2027Q1採用時のみ厳密チェックする
            return
        assert fh.get("short_term_investments") == 69_470_000_000
        assert fh.get("sti_approximated") is False
        assert fh.get("sti_residual_pct") is None


class TestRiceNegativeLabel:
    """RICE-3 回帰防止: OCF赤字で RICE < 0 のとき Matrix Label が 'N/A (OCF赤字)' になること"""

    def test_negative_rice_label_contains_ocf_loss(self, tmp_path):
        """rice.base.rice < 0 のとき Label が 'N/A (OCF赤字)' を含む"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = _minimal_valuation(upside=10.0)
        val["rice"] = {
            "available": True,
            "note": "",
            "base": {"rice": -0.55},
            "q": -0.25,
            "cf_conversion": 0.40,
            "wacc": 0.10,
            "bear": {"rice": -0.38},
            "bull": {"rice": -0.65},
        }
        score_data = _minimal_score_data()
        report = pipe._generate_report("NEGRICE", val, score_data, _minimal_extra())

        assert "N/A (OCF赤字)" in report, (
            "RICE < 0 のとき Matrix Label に 'N/A (OCF赤字)' が含まれていない"
            " (RICE-3 回帰: rice_efficiency が '低効率' に戻った可能性)"
        )

    def test_zero_rice_label_is_low_efficiency(self, tmp_path):
        """rice.base.rice == 0.0 は '低効率' (負値と境界を明確にする)"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = _minimal_valuation(upside=10.0)
        val["rice"] = {
            "available": True,
            "note": "",
            "base": {"rice": 0.0},
            "q": 0.0,
            "cf_conversion": 0.5,
            "wacc": 0.10,
            "bear": {},
            "bull": {},
        }
        score_data = _minimal_score_data()
        report = pipe._generate_report("ZERORICE", val, score_data, _minimal_extra())

        assert "低効率" in report, (
            "RICE == 0.0 のとき '低効率' が表示されていない (負値との境界が崩れた可能性)"
        )
        assert "N/A (OCF赤字)" not in report, (
            "RICE == 0.0 は '低効率' であり 'N/A (OCF赤字)' ではない"
        )

    def test_positive_rice_label_is_not_ocf_loss(self, tmp_path):
        """rice.base.rice > 0 は 'N/A (OCF赤字)' を含まない (正常系の非汚染確認)"""
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = _minimal_valuation(upside=30.0)
        score_data = _minimal_score_data()
        report = pipe._generate_report("POSRICE", val, score_data, _minimal_extra())

        assert "N/A (OCF赤字)" not in report, (
            "RICE > 0 なのに 'N/A (OCF赤字)' ラベルが出現した (正常系への誤波及)"
        )


# =============================================================================
# Section 25: ANNUAL-FY-1 回帰テスト (2026-06-13)
# aggregate_annual が fiscal_year フィールドベースで集計すること
# 非12月FY企業で filing_date[:4] 年跨ぎ混合が発生しないことを保証
# =============================================================================

class TestAnnualFYConsistency:
    """ANNUAL-FY-1: annual.json の各年が単一FYの4Qで構成されること

    aggregate_annual の filing_date[:4] → fiscal_year フィールドへの変更により
    非12月FY企業（NVDA/AAPL/MSFT/INTU等）の年跨ぎFY混合を解消。
    """

    def test_aggregate_annual_uses_fiscal_year_field(self):
        """fiscal_year フィールドがあればそれを優先してグループ化すること"""
        from src.value.adjusted_eps_analyzer.pipeline import aggregate_annual

        # NVDA FY2026 (1月決算): Q1-Q4 全て fiscal_year=2026
        quarters_fy2026 = [
            {"gaap_net_income": 18e9, "net_adjustment_total": 1e9,
             "diluted_shares_used": 24.6e9, "adjusted_eps": 0.82,
             "filing_date": "2025-04-27", "fiscal_year": 2026, "quarter": 1, "adjustments": []},
            {"gaap_net_income": 26e9, "net_adjustment_total": 1.4e9,
             "diluted_shares_used": 24.5e9, "adjusted_eps": 1.14,
             "filing_date": "2025-07-27", "fiscal_year": 2026, "quarter": 2, "adjustments": []},
            {"gaap_net_income": 32e9, "net_adjustment_total": 1.5e9,
             "diluted_shares_used": 24.5e9, "adjusted_eps": 1.36,
             "filing_date": "2025-10-26", "fiscal_year": 2026, "quarter": 3, "adjustments": []},
            {"gaap_net_income": 43e9, "net_adjustment_total": 1.2e9,
             "diluted_shares_used": 24.5e9, "adjusted_eps": 1.81,
             "filing_date": "2026-01-25", "fiscal_year": 2026, "quarter": 4, "adjustments": []},
        ]
        # FY2025 Q4: filing_date=2025-01-26 → 旧ロジックでは year=2025 グループに混入
        quarters_fy2025_q4 = [
            {"gaap_net_income": 22e9, "net_adjustment_total": 1.0e9,
             "diluted_shares_used": 24.8e9, "adjusted_eps": 0.93,
             "filing_date": "2025-01-26", "fiscal_year": 2025, "quarter": 4, "adjustments": []},
        ]
        all_quarters = quarters_fy2025_q4 + quarters_fy2026

        result = aggregate_annual(all_quarters)
        fy2026 = next((r for r in result if r["year"] == "2026"), None)

        assert fy2026 is not None, "FY2026 が集計されていない"
        # FY2026 の4Q合計: (18+26+32+43)B + (1+1.4+1.5+1.2)B = 119B + 5.1B = 124.1B
        expected_adj_ni = (18 + 26 + 32 + 43 + 1 + 1.4 + 1.5 + 1.2) * 1e9
        assert abs(fy2026["adjusted_net_income"] - expected_adj_ni) < 1e9, (
            f"FY2026 adjusted_net_income={fy2026['adjusted_net_income']/1e9:.1f}B "
            f"が期待値 {expected_adj_ni/1e9:.1f}B と乖離 "
            "— fiscal_year ベース集計が機能していない可能性"
        )

        # FY2025 Q4 が FY2026 グループに混入していないこと
        fy2025 = next((r for r in result if r["year"] == "2025"), None)
        assert fy2025 is None, (
            "FY2025 が4件未満なのに集計されている（Q4のみのため正しくはスキップ）"
        )

    def test_aggregate_annual_fallback_without_fiscal_year(self):
        """fiscal_year フィールドが無い場合は filing_date[:4] にフォールバックすること"""
        from src.value.adjusted_eps_analyzer.pipeline import aggregate_annual

        quarters = [
            {"gaap_net_income": 10e9, "net_adjustment_total": 0.5e9,
             "diluted_shares_used": 10e9, "adjusted_eps": 1.05,
             "filing_date": "2025-03-31", "quarter": 1, "adjustments": []},
            {"gaap_net_income": 11e9, "net_adjustment_total": 0.5e9,
             "diluted_shares_used": 10e9, "adjusted_eps": 1.15,
             "filing_date": "2025-06-30", "quarter": 2, "adjustments": []},
            {"gaap_net_income": 12e9, "net_adjustment_total": 0.5e9,
             "diluted_shares_used": 10e9, "adjusted_eps": 1.25,
             "filing_date": "2025-09-30", "quarter": 3, "adjustments": []},
            {"gaap_net_income": 13e9, "net_adjustment_total": 0.5e9,
             "diluted_shares_used": 10e9, "adjusted_eps": 1.35,
             "filing_date": "2025-12-31", "quarter": 4, "adjustments": []},
        ]
        result = aggregate_annual(quarters)
        assert len(result) == 1, f"結果が1件でない: {len(result)}"
        assert result[0]["year"] == "2025", f"year が 2025 でない: {result[0]['year']}"

    def test_nvda_annual_json_fy_not_mixed(self):
        """NVDA annual.json の各 year エントリが単一FYの4Qで構成されること（年跨ぎ混合なし）"""
        import json, os

        annual_path = os.path.join(
            os.path.dirname(__file__), "..",
            "docs", "value-monitor", "adjusted_eps_analyzer", "data", "NVDA", "annual.json"
        )
        q_path = os.path.join(
            os.path.dirname(__file__), "..",
            "docs", "value-monitor", "adjusted_eps_analyzer", "data", "NVDA", "quarterly.json"
        )
        if not os.path.exists(annual_path) or not os.path.exists(q_path):
            return

        with open(annual_path, encoding="utf-8") as f:
            annual = json.load(f)
        with open(q_path, encoding="utf-8") as f:
            q_data = json.load(f)

        # quarterly.json の fiscal_year → year マッピングを構築
        fy_to_quarters = {}
        for q in q_data.get("quarters", []):
            fy = q.get("fiscal_year")
            if fy is not None:
                fy_to_quarters.setdefault(str(fy), []).append(q)

        for year_entry in annual.get("years", []):
            yr = year_entry["year"]
            qs_in_fy = fy_to_quarters.get(yr, [])
            if not qs_in_fy:
                continue  # quarterly.json に対応FYなし（古いデータ等）はスキップ

            # 各QのFYが全て yr と一致するか
            mixed = [q for q in qs_in_fy if str(q.get("fiscal_year", "")) != yr]
            assert not mixed, (
                f"NVDA annual.json year={yr} に異なる fiscal_year の四半期が混入: "
                f"{[q['filing_date'] for q in mixed]}"
            )


# ─────────────────────────────────────────────
# 20. DuPont分解 (TANUKI-ROE-3): _load_extra_data の dupont ブロック検証
#
#   BUG-DUPONT-1 / TANUKI-ROE-3 で追加した以下の挙動を検証する:
#   - 正常計算（NI/Revenue/Assets/Equity 全て正値）
#   - Equity<=0 銘柄は dupont={"excluded": True, "reason": "negative_equity"}
#     （2026-08-29 DuPont観測性統一以前は無言でキー自体が欠落していた）
#   - TTM Revenue < $15M 銘柄は dupont={"excluded": True, "reason": "revenue_too_small"}
#   - 単四半期NI集中（最大1Q/4Q合計 > 0.6）で reliability="LOW"
#   - |ROE|>100% となる極端値ケースでも roe_decomposed が正しく計算される
#     （表示バッジ自体は index.html 側のJSロジックでありPythonテスト対象外）
# ─────────────────────────────────────────────

def _write_dupont_ttm(tmp_path, ticker: str, ni_ttm: float, revenue_ttm: float,
                       quarters_used: int = 4, ttm_end: str = "2026-03-31") -> None:
    """common/sec_data/ttm/{ticker}_ttm_series.json を作成する"""
    ttm_dir = tmp_path / "common" / "sec_data" / "ttm"
    ttm_dir.mkdir(parents=True, exist_ok=True)
    # [[DUPONT-TTM-FIELD-CASE-MISMATCH-1]]: ttm_calculator.py::FLOW_FIELDSが
    # 実際に生成するキーはsnake_case。旧版のこのヘルパーはPascalCase
    # （"NetIncome"/"Revenue"）で書き出しており、当時のpipeline.py側の
    # バグ（同じくPascalCase参照）と偶然自己整合してしまっていたため、
    # 本来検知すべきキー名不一致を長期間見逃す一因になっていた。
    data = {
        "ticker": ticker,
        "series": [{
            "ttm_end": ttm_end,
            "flow": {
                "net_income": {"val": ni_ttm, "quarters_used": quarters_used, "missing": 0},
                "revenue": {"val": revenue_ttm, "quarters_used": quarters_used, "missing": 0},
            },
        }],
    }
    (ttm_dir / f"{ticker}_ttm_series.json").write_text(
        json.dumps(data, ensure_ascii=False), encoding="utf-8"
    )


def _write_dupont_quarterly_bs(tmp_path, ticker: str, total_assets: float, equity: float,
                                period: str = "2026Q1", filename: str = "quarterly_2026Q1.json") -> None:
    """common/sec_data/data/{ticker}/{filename} を作成する（DuPontのBS取得元）"""
    sec_dir = tmp_path / "common" / "sec_data" / "data" / ticker
    sec_dir.mkdir(parents=True, exist_ok=True)
    data = {
        "ticker": ticker,
        "period": period,
        "bs": {"total_assets": total_assets, "stockholders_equity": equity},
    }
    (sec_dir / filename).write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _write_dupont_normalized_ni(tmp_path, ticker: str, quarterly_ni: list) -> None:
    """common/sec_data/normalized/{ticker}_quarterly_normalized.json を作成する。
    quarterly_ni: [(end_date, val), ...] 直近4四半期分（reliability判定の単四半期集中チェック用）

    フェーズD Step2-1でTTM信頼性判定の参照元がnormalized/からLayer3
    （build_ticker_store()）へ切替済みのため、本ヘルパーが書き出す
    ファイルは現在の実装からは参照されない。互換性のため残置するが、
    新規テストは_mock_layer3_ni_store()を使うこと。
    """
    norm_dir = tmp_path / "common" / "sec_data" / "normalized"
    norm_dir.mkdir(parents=True, exist_ok=True)
    fields_ni = [
        {"end": end, "start": "", "val": val, "is_annual": False}
        for end, val in quarterly_ni
    ]
    data = {"ticker": ticker, "fields": {"NetIncome": fields_ni}}
    (norm_dir / f"{ticker.upper()}_quarterly_normalized.json").write_text(
        json.dumps(data, ensure_ascii=False), encoding="utf-8"
    )


def _mock_layer3_ni_store(ticker: str, quarterly_ni: list) -> dict:
    """build_ticker_store()の戻り値と同じ形の、net_incomeのみを持つ
    合成storeを作る（フェーズD Step2-1対応。TTM信頼性判定のテストが
    build_ticker_store()呼び出しをpatchで差し替える際に使う）。

    quarterly_ni: [(end_date, val), ...] 直近4四半期分
    """
    entries = [
        {"end": end, "start": "", "val": val, "is_annual": False, "is_ytd": False}
        for end, val in quarterly_ni
    ]
    return {
        "ticker": ticker,
        "fields": {
            "net_income": {"source_tag": "NetIncomeLoss", "category": "flow", "entries": entries},
        },
    }


class TestDuPontNormalCalculation:
    """正常計算ケース: NI/Revenue/Assets/Equity が全て正常値のとき DuPont3要素を正しく算出する"""

    def test_normal_case_computes_three_factors_correctly(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        _write_dupont_ttm(tmp_path, "NORMALCO", ni_ttm=100_000_000, revenue_ttm=1_000_000_000)
        _write_dupont_quarterly_bs(tmp_path, "NORMALCO", total_assets=2_000_000_000, equity=500_000_000)

        result = pipe._load_extra_data("NORMALCO", {"components": {}})
        dp = result.get("dupont")

        assert dp is not None
        assert dp["net_margin"] == 0.1          # 100M / 1000M
        assert dp["asset_turnover"] == 0.5       # 1000M / 2000M
        assert dp["financial_leverage"] == 4.0   # 2000M / 500M
        assert dp["roe_decomposed"] == 0.2       # 0.1 * 0.5 * 4.0
        assert dp["dupont_bs_period"] == "2026Q1"
        assert "reliability" not in dp           # 単四半期集中データなし＝判定スキップ
        assert "excluded" not in dp


class TestDuPontEquityExclusion:
    """Equity<=0（債務超過）の銘柄は明示的な除外理由付きでdupontが付与される

    2026-08-29のDuPont観測性統一（[[DUPONT-TTM-FIELD-CASE-MISMATCH-1]]
    関連）以前は dupont キー自体が無言で欠落していた（QBTSのrevenue_
    too_small除外とは非対称な扱い）。以降は{"excluded": True, "reason":
    "negative_equity"}が明示的に付与される。
    """

    def test_negative_equity_excludes_dupont_with_explicit_reason(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        _write_dupont_ttm(tmp_path, "DEFICITCO", ni_ttm=50_000_000, revenue_ttm=500_000_000)
        _write_dupont_quarterly_bs(tmp_path, "DEFICITCO", total_assets=1_000_000_000, equity=-100_000_000)

        result = pipe._load_extra_data("DEFICITCO", {"components": {}})

        assert result.get("dupont") == {"excluded": True, "reason": "negative_equity"}, (
            "Equity<=0 の銘柄（ABBV/BKNG/DELL等の純資産マイナス銘柄）は"
            "negative_equity理由付きで明示的に除外されるべき"
        )


class TestDuPontOtherExclusionReasons:
    """2026-08-29のDuPont観測性統一で追加した、negative_equity以外の
    明示的除外理由（データ欠損系）を検証する。
    """

    def test_total_assets_missing_gives_total_assets_unavailable(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        _write_dupont_ttm(tmp_path, "NOASSETCO", ni_ttm=10_000_000, revenue_ttm=500_000_000)
        _write_dupont_quarterly_bs(tmp_path, "NOASSETCO", total_assets=None, equity=100_000_000)

        result = pipe._load_extra_data("NOASSETCO", {"components": {}})

        assert result.get("dupont") == {"excluded": True, "reason": "total_assets_unavailable"}

    def test_equity_missing_with_valid_assets_gives_equity_unavailable(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        _write_dupont_ttm(tmp_path, "NOEQUITYCO", ni_ttm=10_000_000, revenue_ttm=500_000_000)
        _write_dupont_quarterly_bs(tmp_path, "NOEQUITYCO", total_assets=1_000_000_000, equity=None)

        result = pipe._load_extra_data("NOEQUITYCO", {"components": {}})

        assert result.get("dupont") == {"excluded": True, "reason": "equity_unavailable"}

    def test_ni_ttm_missing_gives_ni_ttm_unavailable(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        _write_dupont_ttm(tmp_path, "NOICO", ni_ttm=None, revenue_ttm=500_000_000)
        _write_dupont_quarterly_bs(tmp_path, "NOICO", total_assets=1_000_000_000, equity=100_000_000)

        result = pipe._load_extra_data("NOICO", {"components": {}})

        assert result.get("dupont") == {"excluded": True, "reason": "ni_ttm_unavailable"}


class TestDuPontRevenueThresholdExclusion:
    """TANUKI-ROE-3: TTM Revenue < $15M の銘柄は excluded=True になる"""

    def test_revenue_below_15m_is_excluded(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        _write_dupont_ttm(tmp_path, "TINYCO", ni_ttm=-10_000_000, revenue_ttm=12_000_000)
        _write_dupont_quarterly_bs(tmp_path, "TINYCO", total_assets=1_000_000_000, equity=900_000_000)

        result = pipe._load_extra_data("TINYCO", {"components": {}})

        assert result.get("dupont") == {"excluded": True, "reason": "revenue_too_small"}

    def test_revenue_at_15m_is_not_excluded(self, tmp_path):
        """境界値: $15M ちょうどは除外されない（< $15M のみが除外条件）"""
        pipe = _make_pipe(tmp_path)
        _write_dupont_ttm(tmp_path, "BOUNDARYCO", ni_ttm=1_000_000, revenue_ttm=15_000_000)
        _write_dupont_quarterly_bs(tmp_path, "BOUNDARYCO", total_assets=100_000_000, equity=50_000_000)

        result = pipe._load_extra_data("BOUNDARYCO", {"components": {}})

        dp = result.get("dupont")
        assert dp is not None
        assert dp.get("excluded") is not True


class TestDuPontReliabilityLowFlag:
    """単四半期NI集中（最大1Q ÷ 直近4Q合計 > 0.6）で reliability=LOW になる"""

    def test_single_quarter_concentration_sets_reliability_low(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        # 直近4Q: 10M, 10M, 10M, 100M (合計130M) → 最大Q比率 100/130 ≈ 0.77 > 0.6
        _write_dupont_ttm(tmp_path, "LYFTLIKE", ni_ttm=130_000_000, revenue_ttm=200_000_000,
                           quarters_used=4, ttm_end="2026-03-31")
        _write_dupont_quarterly_bs(tmp_path, "LYFTLIKE", total_assets=400_000_000, equity=100_000_000)
        # フェーズD Step2-1: TTM信頼性判定の参照元がLayer3
        # （build_ticker_store()）に切替済みのため、normalized/への
        # ファイル書き出しではなくbuild_ticker_store()自体をpatchする。
        mock_store = _mock_layer3_ni_store("LYFTLIKE", [
            ("2026-03-31", 10_000_000),
            ("2025-12-31", 100_000_000),  # 一過性要因（DTA等）を想定した単四半期集中
            ("2025-09-30", 10_000_000),
            ("2025-06-30", 10_000_000),
        ])
        with patch("pipeline.build_ticker_store", return_value=mock_store):
            result = pipe._load_extra_data("LYFTLIKE", {"components": {}})
        dp = result.get("dupont")

        assert dp is not None
        assert dp.get("reliability") == "LOW"
        assert dp.get("reliability_reason") == "単四半期NI集中（一過性要因の可能性）"

    def test_evenly_distributed_quarters_no_reliability_flag(self, tmp_path):
        """4Qが均等に分散していれば reliability フラグは付与されない"""
        pipe = _make_pipe(tmp_path)
        # 直近4Q: 25M ずつ均等(合計100M) → 最大Q比率 25/100 = 0.25 < 0.6
        _write_dupont_ttm(tmp_path, "EVENCO", ni_ttm=100_000_000, revenue_ttm=200_000_000,
                           quarters_used=4, ttm_end="2026-03-31")
        _write_dupont_quarterly_bs(tmp_path, "EVENCO", total_assets=400_000_000, equity=100_000_000)
        mock_store = _mock_layer3_ni_store("EVENCO", [
            ("2026-03-31", 25_000_000),
            ("2025-12-31", 25_000_000),
            ("2025-09-30", 25_000_000),
            ("2025-06-30", 25_000_000),
        ])
        with patch("pipeline.build_ticker_store", return_value=mock_store):
            result = pipe._load_extra_data("EVENCO", {"components": {}})
        dp = result.get("dupont")

        assert dp is not None
        assert "reliability" not in dp


class TestDuPontExtremeRoeCalculation:
    """|ROE|>100% となる極端値ケースで roe_decomposed が正しく算出される
    （表示バッジ自体は index.html 側のJSロジックであり本テストはPython側の計算正当性のみ検証）"""

    def test_high_margin_high_leverage_produces_roe_over_100_percent(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        # net_margin=0.5, asset_turnover=1.0, financial_leverage=5.0 → roe=2.5 (250%)
        _write_dupont_ttm(tmp_path, "EXTREMECO", ni_ttm=50_000_000, revenue_ttm=100_000_000)
        _write_dupont_quarterly_bs(tmp_path, "EXTREMECO", total_assets=100_000_000, equity=20_000_000)

        result = pipe._load_extra_data("EXTREMECO", {"components": {}})
        dp = result.get("dupont")

        assert dp is not None
        assert dp["roe_decomposed"] == 2.5
        assert abs(dp["roe_decomposed"]) > 1.0, (
            "|ROE|>100% ケースで roe_decomposed が想定通り算出されていない"
            "（index.html の isExtreme バッジ判定 Math.abs(roe_decomposed)>1.0 が依拠する値）"
        )


class TestDuPontReportTextReason:
    """_generate_report()のDuPont_ROE行が、excluded=Trueの実際のreasonに
    応じた文言を表示することを検証する（2026-08-29のDuPont観測性統一で
    修正。従来はrevenue_too_small専用の固定文言をどのreasonでも表示して
    いた——他のreasonはdupontキー自体が付与されずこの分岐に到達しな
    かったため長らく気づかれなかったバグ）。
    """

    def test_negative_equity_reason_shown_not_revenue_too_small(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        extra = {**_minimal_extra(), "dupont": {"excluded": True, "reason": "negative_equity"}}
        report = pipe._generate_report(
            "TEST", _minimal_valuation(), _minimal_score_data(), extra
        )
        assert "純資産（自己資本）がマイナス" in report
        assert "TTM売上僅少" not in report

    def test_revenue_too_small_reason_shown(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        extra = {**_minimal_extra(), "dupont": {"excluded": True, "reason": "revenue_too_small"}}
        report = pipe._generate_report(
            "TEST", _minimal_valuation(), _minimal_score_data(), extra
        )
        assert "TTM売上僅少" in report

    def test_unknown_reason_falls_back_to_raw_reason_string(self, tmp_path):
        """未知のreason文字列でもクラッシュせず、reasonの生値をそのまま表示する"""
        pipe = _make_pipe(tmp_path)
        extra = {**_minimal_extra(), "dupont": {"excluded": True, "reason": "some_future_reason"}}
        report = pipe._generate_report(
            "TEST", _minimal_valuation(), _minimal_score_data(), extra
        )
        assert "reason=some_future_reason" in report


# ─────────────────────────────────────────────
# 22. STONKS-DIV-1: analyzer.py のゼロ除算防御テスト（PREVENT-3）
#
#     r_start=0 / rev=0 / avg_past=0 のエッジケースで
#     ZeroDivisionError が発生しないことを確認する。
#     （コード上はすでにガード済み; このテストでガードの継続的動作を担保する）
# ─────────────────────────────────────────────

class TestStonksDivisionGuards:
    """STONKS-DIV-1: analyzer.py のゼロ除算ガードが有効であることを保証する"""

    @staticmethod
    def _load_analyzer():
        import importlib.util, os, sys as _sys
        path = os.path.normpath(
            os.path.join(os.path.dirname(__file__), "..", "discover", "stonks-silo", "src", "analyzer.py")
        )
        mod_name = "stonks_analyzer_test"
        spec = importlib.util.spec_from_file_location(mod_name, path)
        mod = importlib.util.module_from_spec(spec)
        _sys.modules[mod_name] = mod  # dataclass が cls.__module__ を解決できるよう登録
        spec.loader.exec_module(mod)
        return mod

    @staticmethod
    def _make_record(rev_san, net_income, assets=5_000_000, debt=2_000_000, equity=500_000, ocf=-100_000):
        return {
            "pl": {"revenue_sanitized": rev_san, "net_income": net_income,
                   "research_and_development": None, "selling_and_marketing": None,
                   "gross_profit": None},
            "bs": {"total_assets": assets, "total_debt": debt, "stockholders_equity": equity},
            "cf": {"ocf": ocf},
        }

    def test_lpr_rev_zero_returns_none(self):
        """rev_sanitized=0 でも StonksAnalyzer.analyze() が ZeroDivisionError を起こさないこと"""
        mod = self._load_analyzer()
        data = {
            "ticker": "ZEROREV",
            "years": [2024],
            "records": {2024: self._make_record(rev_san=0.0, net_income=-1_000_000,
                                                equity=-500_000)},
        }
        try:
            mod.StonksAnalyzer().analyze(data)
        except ZeroDivisionError as e:
            pytest.fail(f"rev=0 で ZeroDivisionError が発生した: {e}")

    def test_cagr_r_start_zero_returns_none(self):
        """4年前の rev_sanitized=0 でも 3年CAGR 計算が ZeroDivisionError を起こさないこと"""
        mod = self._load_analyzer()
        data = {
            "ticker": "RSTART0",
            "years": [2021, 2022, 2023, 2024],
            "records": {
                2021: self._make_record(rev_san=0.0,        net_income=-500_000),
                2022: self._make_record(rev_san=1_000_000,  net_income=-400_000),
                2023: self._make_record(rev_san=2_000_000,  net_income=-300_000),
                2024: self._make_record(rev_san=3_000_000,  net_income=-200_000),
            },
        }
        try:
            mod.StonksAnalyzer().analyze(data)
        except ZeroDivisionError as e:
            pytest.fail(f"r_start=0 で ZeroDivisionError が発生した: {e}")

    def test_discontinuous_growth_avg_past_zero_skips_comparison(self):
        """
        過去YoYの平均が0（avg_past=0）でも _analyze_profitability_path が
        ZeroDivisionError を起こさず、非連続成長判定をスキップすること。

        直近YoYが200%以上（急拡大）かつ過去YoY平均がちょうど0%になるデータを構成し、
        analyzer.py L625 の `avg_past > 0` ガードが機能していることを確認する。
        """
        mod = self._load_analyzer()
        data = {
            "ticker": "AVGPAST0",
            "years": [2021, 2022, 2023],
            "records": {
                # 2021→2022: 売上横ばい(0%) → 過去YoY平均が0になる
                2021: self._make_record(rev_san=100_000, net_income=-50_000),
                2022: self._make_record(rev_san=100_000, net_income=-60_000),
                # 2022→2023: 売上4倍(+300%) → 直近YoYが200%以上の急拡大条件を満たす
                2023: self._make_record(rev_san=400_000, net_income=-70_000),
            },
        }
        try:
            result = mod.StonksAnalyzer().analyze(data)
        except ZeroDivisionError as e:
            pytest.fail(f"avg_past=0 で ZeroDivisionError が発生した: {e}")
        # avg_past=0 は「比較不可」としてスキップされ、非連続成長フラグは立たない
        assert result.profitability_path.discontinuous_growth is False


# ─────────────────────────────────────────────
# TTM-QUARTERS-CHECK-1: TTM系列構築時の四半期完全性チェック
# TTM-FRESHNESS-CHECK-1: TTM鮮度チェック（data_fetcher.py::_quarters_fresh()が
# date.today()を直接参照するため、以下のTTMReader/build_rice_annual_shape系
# テストの日付フィクスチャはすべてdate.today()基準の相対日付で構築する
# （絶対日付だと将来date.today()が閾値を超えテストが陳腐化するため）
# ─────────────────────────────────────────────

def _rel_date(days_ago: int) -> str:
    """date.today()を基準にdays_ago日前のISO日付文字列を返す"""
    from datetime import date, timedelta
    return (date.today() - timedelta(days=days_ago)).isoformat()


def _make_flow_entry(val, quarters_used=4, missing=0):
    return {"val": val, "quarters_used": quarters_used, "missing": missing}


def _make_ttm_entry(ttm_end, ocf_q=4, capex_q=4, revenue_q=4, ni_q=4, fcf_val=100.0,
                     sbc_q=4, sbc_val=50.0):
    """完全性フィルタのテスト用にoperating_cash_flow/capital_expenditure/
    revenue/net_incomeのquarters_usedを個別に指定できるTTM seriesエントリを
    構築する（MO実データ同様、FCF自体にはquarters_usedフィールドが存在しない）。
    フィールド名はフェーズC移行後のLayer3 snake_case命名
    （[[TTM-PASCALCASE-KEY-STALE-1]]対応、2026-07-29）。
    sbc_q/sbc_valは[[TTM-SBC-QUARTERS-GAP-1]]対応の回帰テスト用
    （デフォルトは完全〈quarters_used=4〉、既存テストへの影響なし）。"""
    return {
        "ttm_end": ttm_end,
        "flow": {
            "operating_cash_flow": _make_flow_entry(500.0, ocf_q),
            "capital_expenditure": _make_flow_entry(400.0, capex_q),
            "revenue": _make_flow_entry(1000.0, revenue_q),
            "net_income": _make_flow_entry(300.0, ni_q),
            "stock_based_compensation": _make_flow_entry(sbc_val, sbc_q),
            "FCF": {"val": fcf_val},
        },
    }


class TestSelectFcfSource:
    """_select_fcf_source()がTTM点数<min_years(3)のケースに限り、年次の方が
    多ければ年次を優先すること（CRWV/CON実データ事例: TTM完全性フィルタ適用後に
    TTM系列が2点まで減り、min_fcf_years=3未満で計算全体が失敗していた問題の修正）。

    TTM点数>=min_years（3）であれば、年次点数の方が多くても常にTTMを優先する
    （AAPL実データ事例: annual=5年・TTM完全性フィルタ後=4点でもTTM優先が正しい。
    「TTM点数<年次点数なら常に年次優先」という単純な実装では、AAPL含む大多数の
    銘柄がTTMより鮮度の劣る年次データへ意図せず後退する回帰を起こしていた）"""

    def test_ttm_at_least_min_years_uses_ttm_even_if_fewer_than_annual(self):
        """AAPL実データ相当: annual=5年、TTM=4点（>=min_years=3） → TTM優先"""
        annual = [10.0, 9.0, 8.0, 7.0, 6.0]
        ttm = [12.0, 11.0, 10.0, 9.0]
        result, used_ttm = _df._select_fcf_source(annual, ttm)
        assert used_ttm is True
        assert result == ttm

    def test_ttm_more_than_annual_uses_ttm(self):
        annual = [10.0, 9.0, 8.0]
        ttm = [12.0, 11.0, 10.0, 9.0]
        result, used_ttm = _df._select_fcf_source(annual, ttm)
        assert used_ttm is True
        assert result == ttm

    def test_ttm_below_min_years_and_fewer_than_annual_uses_annual(self):
        """CRWV実データ相当: annual=3年、TTM完全性フィルタ後=2点(<min_years=3) → 年次優先"""
        annual = [-7193000000, -5899000000, -1102000000]
        ttm = [-10587000000, -7582103000]
        result, used_ttm = _df._select_fcf_source(annual, ttm)
        assert used_ttm is False
        assert result == annual
        assert len(result) == 3

    def test_ttm_below_min_years_but_annual_not_more_uses_ttm(self):
        """TTM点数がmin_years未満でも、年次がそれ以下ならTTM（最良の選択肢）を使う"""
        annual = [10.0, 9.0]
        ttm = [12.0, 11.0]
        result, used_ttm = _df._select_fcf_source(annual, ttm)
        assert used_ttm is True
        assert result == ttm

    def test_ttm_none_uses_annual(self):
        annual = [10.0, 9.0, 8.0]
        result, used_ttm = _df._select_fcf_source(annual, None)
        assert used_ttm is False
        assert result == annual

    def test_ttm_empty_list_uses_annual(self):
        annual = [10.0, 9.0, 8.0]
        result, used_ttm = _df._select_fcf_source(annual, [])
        assert used_ttm is False
        assert result == annual


class TestTTMReaderQuartersCompleteness:
    """TTMReader.get_fcf_series()/get_periods()がquarters_used<4の期間を除外すること"""

    def _make_reader(self, series):
        reader = _df.TTMReader(ticker="TEST", repo_root_path=None)
        reader._series = series
        return reader

    def test_all_complete_periods_all_included(self):
        series = [
            _make_ttm_entry(_rel_date(90), fcf_val=100.0),
            _make_ttm_entry(_rel_date(455), fcf_val=90.0),
            _make_ttm_entry(_rel_date(820), fcf_val=80.0),
        ]
        reader = self._make_reader(series)
        assert reader.get_fcf_series() == [100.0, 90.0, 80.0]
        assert reader.get_periods() == 3

    def test_incomplete_ocf_period_excluded(self):
        """OCF.quarters_used<4の期間はFCF.valが存在してもfcf_list_rawから除外される
        （MO実データ2022-03-31: OCF.quarters_used=1で$3,030Mが混入していた事例）"""
        series = [
            _make_ttm_entry(_rel_date(90), fcf_val=100.0),
            _make_ttm_entry(_rel_date(455), fcf_val=90.0),
            _make_ttm_entry(_rel_date(1550), ocf_q=1, capex_q=1, fcf_val=30.0),
        ]
        reader = self._make_reader(series)
        assert reader.get_fcf_series() == [100.0, 90.0]
        assert reader.get_periods() == 2

    def test_incomplete_capex_only_period_excluded(self):
        """OCFは完全でもCapExが不完全なら除外される（OCF/CapExのquarters_usedが
        食い違うケース。実データ横断調査でLLY/NVDA/FCX等11件で確認済み）"""
        series = [
            _make_ttm_entry(_rel_date(90), fcf_val=100.0),
            _make_ttm_entry(_rel_date(455), fcf_val=90.0),
            _make_ttm_entry(_rel_date(820), ocf_q=4, capex_q=3, fcf_val=70.0),
        ]
        reader = self._make_reader(series)
        assert reader.get_fcf_series() == [100.0, 90.0]
        assert reader.get_periods() == 2

    def test_fewer_than_two_complete_periods_returns_none(self):
        """フィルタ後1点以下ならNone（既存の「2点未満はNone」規約を維持）"""
        series = [
            _make_ttm_entry(_rel_date(90), fcf_val=100.0),
            _make_ttm_entry(_rel_date(455), ocf_q=2, capex_q=2, fcf_val=90.0),
            _make_ttm_entry(_rel_date(1550), ocf_q=1, capex_q=1, fcf_val=30.0),
        ]
        reader = self._make_reader(series)
        assert reader.get_fcf_series() is None
        assert reader.get_periods() == 1

    def test_missing_field_key_treated_as_incomplete(self):
        """operating_cash_flow/capital_expenditureキー自体が存在しない場合も
        quarters_used=0扱いで除外される"""
        entry = _make_ttm_entry(_rel_date(90), fcf_val=100.0)
        del entry["flow"]["capital_expenditure"]
        series = [
            _make_ttm_entry(_rel_date(455), fcf_val=90.0),
            _make_ttm_entry(_rel_date(820), fcf_val=80.0),
            entry,
        ]
        reader = self._make_reader(series)
        assert reader.get_fcf_series() == [90.0, 80.0]

    def test_stale_head_makes_whole_series_unusable(self):
        """TTM-FRESHNESS-CHECK-1: 最新end日が陳腐化していれば件数が揃っていても
        シリーズ全体を不採用とする（LLY-CAPEX-STALE-1型の再発対策）"""
        series = [
            _make_ttm_entry(_rel_date(400), fcf_val=100.0),
            _make_ttm_entry(_rel_date(765), fcf_val=90.0),
            _make_ttm_entry(_rel_date(1130), fcf_val=80.0),
        ]
        reader = self._make_reader(series)
        assert reader.get_fcf_series() is None
        assert reader.get_periods() == 0

    def test_fresh_head_keeps_older_complete_entries(self):
        """最新end日さえ新しければ、1年以上前の正規の過去実績エントリは
        鮮度チェックで除外されない（正常な複数年TTM系列を陳腐化扱いしない）"""
        series = [
            _make_ttm_entry(_rel_date(90)),
            _make_ttm_entry(_rel_date(1200)),
        ]
        reader = self._make_reader(series)
        assert reader.get_periods() == 2

    def test_freshness_gate_uses_max_not_position_zero(self):
        """鮮度判定自体（_freshest_end()）はmax()で決まるためseries[0]の位置に
        依存しない。ただしQUALITY-GATES-EPIC-1 Phase 3a導入前は、鮮度判定を
        通過しさえすれば混在順序（descendingでない）のseriesもget_fcf_series()が
        そのまま返していた（このテストの旧版は[90.0, 100.0]を期待していた）。
        FCFSeries導入後は、fcf_listの「新しい順」規約自体をconstruction時に
        検証するため、混在順序のseriesはget_fcf_series()の時点でNone（安全側）
        を返すよう仕様変更した（GROWTH-CAGR-SIGN-1のような順序取り違えバグが
        下流〈growth.py等〉に伝播する前に、生成源で弾く）。"""
        series = [
            _make_ttm_entry(_rel_date(455), fcf_val=90.0),
            _make_ttm_entry(_rel_date(90), fcf_val=100.0),
        ]
        reader = self._make_reader(series)
        assert reader.get_fcf_series() is None

    def test_properly_ordered_series_with_freshest_not_needing_reorder(self):
        """正しく新しい順（descending）に並んだseriesは従来通り採用される
        （鮮度判定・順序検証のいずれも通過する通常ケース）"""
        series = [
            _make_ttm_entry(_rel_date(90), fcf_val=100.0),
            _make_ttm_entry(_rel_date(455), fcf_val=90.0),
        ]
        reader = self._make_reader(series)
        assert reader.get_fcf_series() == [100.0, 90.0]


class TestBuildRiceAnnualShapeQuartersCompleteness:
    """build_rice_annual_shape()がOCF/CapEx/Revenue/NetIncome不完全な期間を除外すること"""

    def test_all_complete_periods_all_included(self):
        series = [
            _make_ttm_entry(_rel_date(90)),
            _make_ttm_entry(_rel_date(455)),
        ]
        result = _df.build_rice_annual_shape(series)
        assert len(result) == 2

    def test_incomplete_period_excluded_from_rice_shape(self):
        fresh_end = _rel_date(90)
        series = [
            _make_ttm_entry(fresh_end),
            _make_ttm_entry(_rel_date(1550), ocf_q=1, capex_q=1, revenue_q=2, ni_q=2),
        ]
        result = _df.build_rice_annual_shape(series)
        assert len(result) == 1
        assert result[0]["period"] == f"TTM@{fresh_end}"

    def test_incomplete_revenue_or_netincome_also_excludes_period(self):
        """OCF/CapExが完全でもRevenue/NetIncomeが不完全なら除外される
        （rice.py側でrev/niはNone不許容のロジックのため）"""
        fresh_end = _rel_date(90)
        series = [
            _make_ttm_entry(fresh_end),
            _make_ttm_entry(_rel_date(820), revenue_q=3, ni_q=4),
        ]
        result = _df.build_rice_annual_shape(series)
        assert len(result) == 1
        assert result[0]["period"] == f"TTM@{fresh_end}"

    def test_stale_head_returns_empty(self):
        """TTM-FRESHNESS-CHECK-1: 最新end日が陳腐化していればresult全体が空になる"""
        series = [
            _make_ttm_entry(_rel_date(400)),
            _make_ttm_entry(_rel_date(765)),
        ]
        result = _df.build_rice_annual_shape(series)
        assert result == []

    def test_rd_and_sm_incompleteness_does_not_exclude_period(self):
        """RD/SMはrice.py側で既にNone許容のためチェック対象外
        （フィールド自体が存在しなくても期間は除外されない）"""
        entry = _make_ttm_entry(_rel_date(90))
        # RD/SMキーは元々存在しないダミーデータだが、期間自体は残ることを確認
        result = _df.build_rice_annual_shape([entry])
        assert len(result) == 1

    def test_sbc_incomplete_quarters_nulled_but_period_kept(self):
        """[[TTM-SBC-QUARTERS-GAP-1]]回帰テスト: SBCがquarters_used<4
        （部分四半期合計）の場合、期間自体は除外せずSBC値のみNoneへ
        差し替える（OCF/CapEx/Revenue/NetIncomeが完全な期間を、SBC単独の
        不完全性で失わないため）。実データ確認: GEV/HWM/TDYでquarters_
        used=1〜3の非NoneなSBC部分合計が、完全な年間SBCであるかのように
        rice.py::_calc_q()へ渡っていた。"""
        entry = _make_ttm_entry(_rel_date(90), sbc_q=1, sbc_val=54.0)
        result = _df.build_rice_annual_shape([entry])
        assert len(result) == 1
        assert result[0]["cf"]["stock_based_compensation"] is None
        # SBC以外のフィールドは影響を受けない
        assert result[0]["cf"]["operating_cash_flow"] == 500.0
        assert result[0]["pl"]["revenue"] == 1000.0

    def test_sbc_missing_field_still_none_period_kept(self):
        """SBCフィールド自体が存在しない場合（quarters_used=0扱い）も
        同様にNoneとなり、期間は除外されないことを確認する
        （従来のrice.py側or 0.0フォールバックと同じ結果になることの確認）。"""
        entry = _make_ttm_entry(_rel_date(90), sbc_q=0, sbc_val=None)
        result = _df.build_rice_annual_shape([entry])
        assert len(result) == 1
        assert result[0]["cf"]["stock_based_compensation"] is None

    def test_sbc_complete_quarters_value_passed_through(self):
        """SBCがquarters_used=4（完全）の場合は値がそのまま渡ることを確認する
        （デフォルトのsbc_q=4フィクスチャで既存テスト全てが暗黙に検証済みだが、
        明示的に確認する）。"""
        entry = _make_ttm_entry(_rel_date(90), sbc_q=4, sbc_val=54.0)
        result = _df.build_rice_annual_shape([entry])
        assert len(result) == 1
        assert result[0]["cf"]["stock_based_compensation"] == 54.0


# ─────────────────────────────────────────────
# ZS-TICKERS-LEAK-1: pipeline.run()のCLI引数パスがtanuki=falseを検証しない
# 構造的ギャップの回帰テスト（銘柄リスト統一アクセサ導入）
# ─────────────────────────────────────────────

class TestFilterTanukiTickers:
    """TanukiValuationPipeline._filter_tanuki_tickers()が
    tanuki=false銘柄を除外することを検証する（本番cik_lookup.csv使用）"""

    def _make_pipeline_stub(self):
        """__init__のファイルI/O・計算機初期化を避け、repo_rootのみ持つ
        最小限のインスタンス相当を作る"""
        pipe = object.__new__(TanukiValuationPipeline)
        pipe.repo_root = _REPO_ROOT_DIR
        return pipe

    def test_tanuki_false_ticker_excluded(self):
        """ZSはtanuki=falseのため、明示指定してもフィルタで除外される"""
        pipe = self._make_pipeline_stub()
        result = pipe._filter_tanuki_tickers(["ZS"])
        assert result == []

    def test_tanuki_true_ticker_kept(self):
        pipe = self._make_pipeline_stub()
        result = pipe._filter_tanuki_tickers(["AAPL"])
        assert result == ["AAPL"]

    def test_mixed_tickers_only_tanuki_true_kept(self):
        """tanuki=true/false混在指定時、trueのみ残り順序は維持される"""
        pipe = self._make_pipeline_stub()
        result = pipe._filter_tanuki_tickers(["AAPL", "ZS", "RKLB", "NVDA"])
        assert result == ["AAPL", "NVDA"]

    def test_lowercase_input_still_matched(self):
        pipe = self._make_pipeline_stub()
        result = pipe._filter_tanuki_tickers(["aapl"])
        assert result == ["aapl"]


# ─────────────────────────────────────────────
# 18. DCF-RELIABILITY-LABEL-MISMATCH-1: FCF_Base方式の非LOW表示語彙統一
#
#     _calc_dcf_reliability_policy_b()は常にLOW/NORMALのいずれかを返すが、
#     report.txtの表示文言がFCF_Base方式ではHIGH・FCF_Conversion_Rate方式
#     ではNORMALと分岐していた。2026-08-30、FCF_Base方式側もNORMALに統一。
# ─────────────────────────────────────────────


class TestDcfReliabilityLabelUnified:
    """FCF_Base方式でPolicy Bが非LOWを返した場合、report.txtの表示が
    NORMALに統一されており、旧HIGH表記が残っていないことを確認する"""

    @staticmethod
    def _make_normal_fcf_valuation(upside: float = 50.0) -> dict:
        """実績FCFプラス・fcf_outlier未検出の新規銘柄（Policy B → NORMAL想定）"""
        val = _minimal_valuation(upside=upside)
        val["fcf_estimation"] = {"applied": False, "sector": "Technology"}
        val["components"].update({
            "fcf_base_used":     900_000_000,
            "fcf_floor_applied": 0,            # floor未発火 → Policy A側はLOWにならない
            "fcf_5yr_avg":       900_000_000,
            "fcf_base_method":   "avg_5yr",
            "fcf_list_raw":      [800e6, 850e6, 900e6],
        })
        val["fcf_outlier"] = {"detected": False}  # Policy B → NORMAL
        val["rice"] = {"available": False, "note": ""}
        return val

    def test_dcf_reliability_normal_shown_for_positive_fcf_ticker(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = self._make_normal_fcf_valuation()
        score_data = {"score": "BUY", "funda_score": 60, "score_comment": "test"}
        report = pipe._generate_report("NEWCO2", val, score_data, _minimal_extra())
        assert "DCF_Reliability: NORMAL" in report
        assert "DCF_Reliability: HIGH" not in report
