"""
tests/test_data_fetcher_market_data_switch.py

[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-2（data_fetcher.py切替）の
回帰テスト。get_financials()のyfinance直接呼び出し部分がcommon.market_data.
reader経由に置き換わったこと・reader側がNoneを返す場合の中立デフォルト
劣化（選択肢A）・ma200の代数的逆算ロジックを検証する。

data_fetcher.pyはsrc.value.tanuki_valuationパッケージ経由（__init__.pyの
wacc未解決import）ではimportできないため、tests/test_growth.py・
tests/test_beta_fetcher.pyと同じパターン（対象ディレクトリ自体をsys.path
へ追加）でモジュール単体を読む。SEC依存部分（sec_reader）はNoneに強制
差し替えてmarket_data関連ロジックのみを単離してテストする。

実行方法:
    python -m pytest tests/test_data_fetcher_market_data_switch.py -v
"""

import os
import sys

import pytest

_PIPELINE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation")
)
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

import data_fetcher as df  # noqa: E402


@pytest.fixture
def fetcher(monkeypatch):
    """SEC依存部分を無効化したTanukiDataFetcherを返す（market_data関連
    ロジックのみを単離してテストするため）。beta_config.jsonは実ファイルを
    読む可能性があるため、overridesを空にして_determine_beta()の分岐に
    影響しないようにする。"""
    monkeypatch.setattr(df, "HAS_SEC", False)
    f = df.TanukiDataFetcher()
    f.sec_reader = None
    f._beta_overrides = {}
    return f


def _patch_market_data(monkeypatch, latest_price=None, attrs=None, ma200_dev=None):
    monkeypatch.setattr(df, "HAS_MARKET_DATA", True)
    monkeypatch.setattr(df, "_md_get_latest_price", lambda ticker: latest_price)
    monkeypatch.setattr(df, "_md_get_attributes", lambda ticker: attrs)
    monkeypatch.setattr(df, "_md_get_ma_deviation", lambda ticker, window=200: ma200_dev)


_FULL_ATTRS = {
    "beta": 1.5, "sector": "Technology", "industry": "Software",
    "trailing_pe": 20.0, "forward_pe": 18.0,
    "peg_ratio": 1.5, "price_to_sales": 5.0, "ev_to_ebitda": 12.0,
    "forward_eps": 5.0, "dividend_yield": 0.02, "payout_ratio": 0.3,
    "shares_outstanding": 1_000_000_000, "implied_shares_outstanding": 1_010_000_000,
    "target_mean_price": 110.0, "target_median_price": 112.0,
    "target_low_price": 90.0, "target_high_price": 130.0,
    "analyst_count": 20, "analyst_recommendation_key": "buy",
}


class TestPriceFromMarketData:
    def test_current_price_from_daily_close(self, fetcher, monkeypatch):
        _patch_market_data(monkeypatch, latest_price={"date": "2026-08-10", "close": 250.5}, attrs=_FULL_ATTRS)
        result = fetcher.get_financials("XYZ")
        assert result["current_price"] == 250.5

    def test_missing_daily_data_gives_none_not_zero(self, fetcher, monkeypatch):
        """daily/に有効な終値が無い（reader.get_latest_price()がNone）場合、
        current_price=Noneのまま返す（例外にならない）。0.0を入れるとupside=0・
        分類が中立値で埋まるため（MARKETDATA-DAILY-CLOSE-NONE-PERMANENT-1、2026-09-26）"""
        _patch_market_data(monkeypatch, latest_price=None, attrs=_FULL_ATTRS)
        result = fetcher.get_financials("XYZ")
        assert result["current_price"] is None

    def test_market_data_unavailable_entirely(self, fetcher, monkeypatch):
        """HAS_MARKET_DATA=False（import失敗相当）でも例外を出さず
        中立デフォルトで継続する"""
        monkeypatch.setattr(df, "HAS_MARKET_DATA", False)
        result = fetcher.get_financials("XYZ")
        assert result["current_price"] is None  # 2026-09-26: 0.0ではなくNone
        assert result["beta"] is not None  # _determine_beta()のフォールバックで何らかの値になる


class TestAttributesFromMarketData:
    def test_all_fields_correctly_mapped(self, fetcher, monkeypatch):
        _patch_market_data(monkeypatch, latest_price={"date": "2026-08-10", "close": 100.0}, attrs=_FULL_ATTRS)
        result = fetcher.get_financials("XYZ")
        assert result["beta_yf_raw"] == 1.5
        assert result["sector"] == "Technology"
        assert result["industry"] == "Software"
        assert result["per"] == 20.0
        assert result["peg"] == 1.5
        assert result["ps"] == 5.0
        assert result["ev_ebitda"] == 12.0
        assert result["forward_eps"] == 5.0
        assert result["dividend_yield"] == 0.02
        assert result["payout_ratio"] == 0.3
        assert result["analyst_target_median"] == 112.0
        assert result["analyst_target_mean"] == 110.0
        assert result["analyst_target_low"] == 90.0
        assert result["analyst_target_high"] == 130.0
        assert result["analyst_count"] == 20
        assert result["analyst_rec_key"] == "buy"

    def test_forward_pe_used_when_trailing_pe_missing(self, fetcher, monkeypatch):
        attrs = dict(_FULL_ATTRS, trailing_pe=None)
        _patch_market_data(monkeypatch, latest_price={"date": "2026-08-10", "close": 100.0}, attrs=attrs)
        result = fetcher.get_financials("XYZ")
        assert result["per"] == 18.0  # forward_peにフォールバック
        assert result["per_is_forward"] is True

    def test_missing_attributes_falls_back_to_neutral_defaults(self, fetcher, monkeypatch):
        """attributes/未取得（reader.get_attributes()がNone）の場合、
        beta/sector/per等が全て中立デフォルトに倒れる（既存except節と同じ値）"""
        _patch_market_data(monkeypatch, latest_price={"date": "2026-08-10", "close": 100.0}, attrs=None)
        result = fetcher.get_financials("XYZ")
        assert result["beta_yf_raw"] is None
        assert result["sector"] == "default"
        assert result["industry"] == ""
        assert result["per"] is None
        assert result["peg"] is None
        assert result["ps"] is None
        assert result["ev_ebitda"] is None
        assert result["forward_eps"] is None
        assert result["dividend_yield"] == 0.0
        assert result["payout_ratio"] == 0.0
        assert result["analyst_target_median"] is None
        assert result["analyst_rec_key"] == ""
        assert result["_shares_source"] == "none"  # implied/outstanding/secいずれも0のため


class TestMa200Roundtrip:
    def test_ma200_is_algebraically_derived_from_deviation(self, fetcher, monkeypatch):
        """ma200はreader.get_ma_deviation()の値から代数的に逆算される。
        pipeline.py側の既存計算式 (current_price/ma200-1)*100 に通すと
        元のma200_devに一致すること（往復の正しさ）を確認する。"""
        current_price = 308.26
        ma200_dev = 10.234512142081599
        _patch_market_data(
            monkeypatch,
            latest_price={"date": "2026-08-10", "close": current_price},
            attrs=_FULL_ATTRS, ma200_dev=ma200_dev,
        )
        result = fetcher.get_financials("XYZ")
        ma200 = result["ma200"]
        recomputed_dev = (current_price / ma200 - 1) * 100
        assert recomputed_dev == pytest.approx(ma200_dev, abs=1e-6)

    def test_ma200_is_none_when_deviation_unavailable(self, fetcher, monkeypatch):
        """get_ma_deviation()がNone（200日分データ不足、CWAN型）の場合、
        ma200もNoneになる"""
        _patch_market_data(
            monkeypatch, latest_price={"date": "2026-08-10", "close": 100.0},
            attrs=_FULL_ATTRS, ma200_dev=None,
        )
        result = fetcher.get_financials("XYZ")
        assert result["ma200"] is None

    def test_ma200_is_none_when_price_is_zero(self, fetcher, monkeypatch):
        """current_price=0.0（daily/未取得）の場合、ma200_devが値を
        持っていても逆算不能なためma200=None"""
        _patch_market_data(monkeypatch, latest_price=None, attrs=_FULL_ATTRS, ma200_dev=5.0)
        result = fetcher.get_financials("XYZ")
        assert result["ma200"] is None


class TestSharesFromMarketData:
    def test_implied_shares_preferred_over_outstanding(self, fetcher, monkeypatch):
        _patch_market_data(monkeypatch, latest_price={"date": "2026-08-10", "close": 100.0}, attrs=_FULL_ATTRS)
        result = fetcher.get_financials("XYZ")
        assert result["diluted_shares"] == 1_010_000_000
        assert result["_shares_source"] == "yf_implied"
