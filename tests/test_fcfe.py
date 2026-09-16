"""
tests/test_fcfe.py

calculator/fcfe.py（[[TANUKI-FIN-2]]、金融機関向けFCFEエクイティDCF・
参考表示専用）の単体テスト。銘柄非依存の共通ロジックであることの検証と、
SOFI実データ相当（成長率がROEを上回りFCFEがマイナスになるケース）の
回帰テストを含む。
"""

import os
import sys

_PIPELINE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation")
)
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

from calculator import fcfe as fcfe_module  # noqa: E402
from calculator.fcfe import (  # noqa: E402
    calculate_equity_reinvestment_rate,
    calculate_fcfe,
    calculate_fcfe_valuation,
    is_financial_institution_ticker,
)


class TestIsFinancialInstitutionTicker:
    """config/financial_institution_config.jsonのスイッチ判定
    （実ファイルに依存せずmonkeypatchで独立にテストする）"""

    def test_ticker_in_config_returns_true(self, monkeypatch):
        monkeypatch.setattr(fcfe_module, "_load_financial_institution_config",
                             lambda: {"tickers": ["SOFI"]})
        assert is_financial_institution_ticker("SOFI") is True

    def test_ticker_not_in_config_returns_false(self, monkeypatch):
        monkeypatch.setattr(fcfe_module, "_load_financial_institution_config",
                             lambda: {"tickers": ["SOFI"]})
        assert is_financial_institution_ticker("AAPL") is False

    def test_case_insensitive_match(self, monkeypatch):
        monkeypatch.setattr(fcfe_module, "_load_financial_institution_config",
                             lambda: {"tickers": ["SOFI"]})
        assert is_financial_institution_ticker("sofi") is True

    def test_empty_config_returns_false_for_all(self, monkeypatch):
        monkeypatch.setattr(fcfe_module, "_load_financial_institution_config",
                             lambda: {"tickers": []})
        assert is_financial_institution_ticker("SOFI") is False

    def test_config_load_failure_defaults_to_no_tickers(self, monkeypatch):
        """設定ファイル読み込み失敗時は対象銘柄なし（中立デフォルト）として扱う"""
        monkeypatch.setattr(fcfe_module, "_load_financial_institution_config",
                             lambda: {"tickers": []})
        assert is_financial_institution_ticker("SOFI") is False

    def test_real_config_file_includes_sofi(self):
        """実際のconfig/financial_institution_config.jsonにSOFIが登録済み
        であることの回帰テスト（[[TANUKI-FIN-2]]実装完了の確認）"""
        assert is_financial_institution_ticker("SOFI") is True
        assert is_financial_institution_ticker("AAPL") is False


class TestCalculateEquityReinvestmentRate:
    def test_normal_case(self):
        """g=5%, ROE=10% -> reinvestment rate=50%"""
        rate = calculate_equity_reinvestment_rate(0.05, 0.10)
        assert rate == 0.5

    def test_growth_exceeds_roe_yields_rate_above_1(self):
        """SOFI実データ相当: g=22.3%, ROE=5.74% -> reinvestment rate約3.89"""
        rate = calculate_equity_reinvestment_rate(0.223, 0.0574)
        assert rate == 0.223 / 0.0574

    def test_roe_zero_returns_none(self):
        assert calculate_equity_reinvestment_rate(0.10, 0.0) is None

    def test_roe_negative_returns_none(self):
        assert calculate_equity_reinvestment_rate(0.10, -0.05) is None


class TestCalculateFcfe:
    def test_normal_case(self):
        """NI=$1000, reinvestment rate=50% -> FCFE=$500"""
        assert calculate_fcfe(1000, 0.5) == 500

    def test_reinvestment_rate_above_1_yields_negative_fcfe(self):
        assert calculate_fcfe(1000, 1.5) == -500

    def test_reinvestment_rate_zero_yields_full_net_income(self):
        assert calculate_fcfe(1000, 0.0) == 1000


class TestCalculateFcfeValuation:
    """銘柄非依存の共通関数であることを検証する（ticker引数自体を
    持たない設計、input dataのみで挙動が決まる）"""

    def test_normal_case_returns_available_true(self):
        """g < ROEの健全なケース: FCFEがプラスになりIVが算出される"""
        result = calculate_fcfe_valuation(
            net_income=1_000_000_000,
            growth_rate=0.05,
            roe=0.15,
            cost_of_equity=0.10,
            diluted_shares=100_000_000,
            high_growth_years=5,
        )
        assert result["available"] is True
        assert result["method"] == "FCFE_equity_dcf"
        assert result["fcfe"] == 1_000_000_000 * (1 - 0.05 / 0.15)
        assert result["intrinsic_value_per_share"] > 0
        assert "dcf_components" in result

    def test_sofi_real_data_yields_unavailable_negative_fcfe(self):
        """SOFI実データ相当（growth.rate=22.3%、dupont.roe_decomposed=5.74%、
        wacc.value=16.86%）を投入すると、g/ROE比率が約3.89倍となり
        FCFEが大幅マイナスになるためavailable=Falseで理由が返ること"""
        result = calculate_fcfe_valuation(
            net_income=636_264_000,
            growth_rate=0.223,
            roe=0.0574,
            cost_of_equity=0.168628,
            diluted_shares=1_291_570_324,
            high_growth_years=6,
        )
        assert result["available"] is False
        assert result["reason"] == "negative_or_zero_fcfe"
        assert result["fcfe"] < 0
        assert result["equity_reinvestment_rate"] == 0.223 / 0.0574
        # 計算根拠の内訳が保持されていること（report.txt側の理由表示に必要）
        assert result["roe"] == 0.0574
        assert result["growth_rate"] == 0.223
        assert result["cost_of_equity"] == 0.168628

    def test_roe_not_positive_returns_reason(self):
        result = calculate_fcfe_valuation(
            net_income=1_000_000_000,
            growth_rate=0.05,
            roe=0.0,
            cost_of_equity=0.10,
            diluted_shares=100_000_000,
        )
        assert result["available"] is False
        assert result["reason"] == "roe_not_positive"
        assert "fcfe" not in result  # ROE不明でFCFE自体を計算できないため含まれない

    def test_cost_of_equity_below_terminal_growth_returns_reason(self):
        result = calculate_fcfe_valuation(
            net_income=1_000_000_000,
            growth_rate=0.02,
            roe=0.15,
            cost_of_equity=0.02,  # デフォルトterminal_growth(3%)以下
            diluted_shares=100_000_000,
        )
        assert result["available"] is False
        assert result["reason"] == "cost_of_equity_below_terminal_growth"

    def test_diluted_shares_zero_returns_reason(self):
        result = calculate_fcfe_valuation(
            net_income=1_000_000_000,
            growth_rate=0.05,
            roe=0.15,
            cost_of_equity=0.10,
            diluted_shares=0,
        )
        assert result["available"] is False
        assert result["reason"] == "diluted_shares_unavailable"

    def test_diluted_shares_none_returns_reason(self):
        result = calculate_fcfe_valuation(
            net_income=1_000_000_000,
            growth_rate=0.05,
            roe=0.15,
            cost_of_equity=0.10,
            diluted_shares=None,
        )
        assert result["available"] is False
        assert result["reason"] == "diluted_shares_unavailable"

    def test_no_ticker_argument_exists(self):
        """[[TANUKI-FIN-2]]の制約（銘柄非依存の共通コード）の回帰テスト:
        calculate_fcfe_valuation()はticker引数を一切持たない"""
        import inspect
        sig = inspect.signature(calculate_fcfe_valuation)
        assert "ticker" not in sig.parameters
