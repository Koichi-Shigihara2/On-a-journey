"""
tests/test_valuation_fetcher_market_data_switch.py

[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-3（valuation_fetcher.py切替）の
回帰テスト。fetch_valuation()のyfinance直接呼び出しがcommon.market_data.
reader経由に置き換わったこと・reader側がNoneを返す場合の中立デフォルト
劣化（選択肢A）を検証する。

src/value/stonks_silo/valuation_fetcher.pyはパッケージ経由でimport
できない構成のため、importlib.util経由でファイルパス指定してモジュール
単体を読む（beta_fetcher.py/data_fetcher.pyのsys.path追加パターンとは
異なり、本モジュールはトップレベルパッケージに属さない独立ディレクトリの
ため、より確実なimportlib方式を用いる）。

実行方法:
    python -m pytest tests/test_valuation_fetcher_market_data_switch.py -v
"""

import importlib.util
import os

import pytest

_MODULE_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "stonks_silo", "valuation_fetcher.py")
)


def _load_module():
    spec = importlib.util.spec_from_file_location("valuation_fetcher", _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def vf():
    return _load_module()


def _patch_market_data(monkeypatch, module, latest_price=None, attrs=None, has_market_data=True):
    monkeypatch.setattr(module, "HAS_MARKET_DATA", has_market_data)
    monkeypatch.setattr(module, "_md_get_latest_price", lambda ticker: latest_price)
    monkeypatch.setattr(module, "_md_get_attributes", lambda ticker: attrs)


class TestFetchValuationNormalCase:
    def test_all_fields_correctly_mapped(self, vf, monkeypatch):
        _patch_market_data(
            monkeypatch, vf,
            latest_price={"date": "2026-08-10", "close": 250.5},
            attrs={
                "market_cap": 1_000_000_000, "enterprise_value": 1_050_000_000, "total_debt": 500_000_000,
                "sector": "Technology", "industry": "Software",
            },
        )
        result = vf.fetch_valuation("XYZ")
        assert result["current_price"] == 250.5
        assert result["market_cap"] == 1_000_000_000
        assert result["enterprise_value"] == 1_050_000_000
        assert result["total_debt"] == 500_000_000
        assert result["sector"] == "Technology"
        assert result["industry"] == "Software"
        assert result["error"] is None
        assert result["fetched_at"] is not None

    def test_return_keys_include_sector_industry_for_netcash_dual_calc(self, vf, monkeypatch):
        """戻り値のキーは旧yfinance版のmarket_cap/current_price/
        enterprise_value/total_debt/fetched_at/errorに加え、
        [[NETCASH-DUAL-CALC-1]]でSECReader.get_net_cash()のセクター
        ガード引数用に追加したsector/industryを含む
        （pipeline.py側のval["..."]アクセスを壊さない範囲での拡張）"""
        _patch_market_data(
            monkeypatch, vf,
            latest_price={"date": "2026-08-10", "close": 100.0},
            attrs={"market_cap": 1.0, "enterprise_value": 2.0, "total_debt": 3.0, "sector": "Technology", "industry": "Software"},
        )
        result = vf.fetch_valuation("XYZ")
        assert set(result.keys()) == {
            "market_cap", "current_price", "enterprise_value", "total_debt",
            "sector", "industry", "fetched_at", "error",
        }


class TestFetchValuationNeutralDefaults:
    def test_missing_daily_price_falls_back_to_none_current_price(self, vf, monkeypatch):
        """daily/未取得（reader.get_latest_price()がNone）でも例外にならず
        current_price=Noneの中立デフォルトに倒れる（errorは設定されない、
        旧コードの「.infoの値がたまたまNoneだった」場合と同じ扱い）"""
        _patch_market_data(
            monkeypatch, vf, latest_price=None,
            attrs={"market_cap": 1.0, "enterprise_value": 2.0, "total_debt": 3.0},
        )
        result = vf.fetch_valuation("XYZ")
        assert result["current_price"] is None
        assert result["market_cap"] == 1.0
        assert result["error"] is None

    def test_missing_attributes_falls_back_to_none_for_all_attribute_fields(self, vf, monkeypatch):
        """attributes/未取得（reader.get_attributes()がNone）の場合、
        market_cap/enterprise_value/total_debtが全てNoneに倒れる。
        sector/industryはSECReader.get_net_cash()の_is_insurance_local()が
        文字列比較を行う前提のため、Noneではなく"default"/""に倒れる
        （[[NETCASH-DUAL-CALC-1]]）"""
        _patch_market_data(
            monkeypatch, vf,
            latest_price={"date": "2026-08-10", "close": 100.0}, attrs=None,
        )
        result = vf.fetch_valuation("XYZ")
        assert result["market_cap"] is None
        assert result["enterprise_value"] is None
        assert result["total_debt"] is None
        assert result["current_price"] == 100.0
        assert result["sector"] == "default"
        assert result["industry"] == ""
        assert result["error"] is None

    def test_both_missing_all_fields_none_no_error(self, vf, monkeypatch):
        _patch_market_data(monkeypatch, vf, latest_price=None, attrs=None)
        result = vf.fetch_valuation("XYZ")
        assert result["market_cap"] is None
        assert result["current_price"] is None
        assert result["enterprise_value"] is None
        assert result["total_debt"] is None
        assert result["sector"] == "default"
        assert result["industry"] == ""
        assert result["error"] is None

    def test_market_data_module_unavailable_sets_error(self, vf, monkeypatch):
        """common.market_data.readerがimportできない場合（HAS_MARKET_DATA=
        False）、旧来の完全失敗時except節と同様にerrorメッセージを設定する"""
        _patch_market_data(monkeypatch, vf, has_market_data=False)
        result = vf.fetch_valuation("XYZ")
        assert result["market_cap"] is None
        assert result["current_price"] is None
        assert result["enterprise_value"] is None
        assert result["total_debt"] is None
        assert result["sector"] == "default"
        assert result["industry"] == ""
        assert result["error"] is not None

    def test_unexpected_exception_sets_error_and_neutral_defaults(self, vf, monkeypatch):
        def _raise(ticker):
            raise RuntimeError("simulated failure")
        monkeypatch.setattr(vf, "HAS_MARKET_DATA", True)
        monkeypatch.setattr(vf, "_md_get_latest_price", _raise)
        result = vf.fetch_valuation("XYZ")
        assert result["error"] == "simulated failure"
        assert result["market_cap"] is None
        assert result["current_price"] is None
        assert result["sector"] == "default"
        assert result["industry"] == ""
