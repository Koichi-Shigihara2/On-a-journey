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


_MULT_NONE = {"market_cap": None, "market_cap_source": None, "market_cap_yf_gap": None, "per": None, "ps": None, "ttm_end": None}


def _patch_market_data(monkeypatch, latest_price=None, attrs=None, ma200_dev=None):
    # 倍率の部品（common/multiples.py）は実ファイルを読まず、既定は FULL_ATTRS と同じ値（PER 20・PSR 5）を返す
    monkeypatch.setattr(df, "_multiples_current", lambda t, px, mc=None: dict(_MULT_NONE, per=20.0, ps=5.0) if attrs else _MULT_NONE)
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
        assert result["per_gaap_ttm_sec"] == 20.0     # 部品の値（_patch_market_dataが差し替え）
        assert result["per_forward_yf"] == 18.0
        assert result["peg"] == 1.5
        assert result["ps_ttm_sec"] == 5.0
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

    def test_forward_pe_is_never_used_as_gaap_per(self, fetcher, monkeypatch):
        """[[MULTIPLES-ADOPT-1]] 決定1（2026-11-13）: GAAP PER（部品）がNoneでもforward_peには落とさない。
        forward_peは別の列 per_forward_yf（per_is_forward は廃止）"""
        _patch_market_data(monkeypatch, latest_price={"date": "2026-08-10", "close": 100.0}, attrs=_FULL_ATTRS)
        monkeypatch.setattr(df, "_multiples_current", lambda t, px, mc=None: _MULT_NONE)
        result = fetcher.get_financials("XYZ")
        assert result["per_gaap_ttm_sec"] is None
        assert result["per_forward_yf"] == 18.0
        assert "per_is_forward" not in result and "per" not in result

    def test_negative_forward_pe_stays_in_forward_column(self, fetcher, monkeypatch):
        """負のforward_pe（QBTS型）もGAAP PERの列には入らない"""
        attrs = dict(_FULL_ATTRS, trailing_pe=None, forward_pe=-41.758244)
        _patch_market_data(monkeypatch, latest_price={"date": "2026-08-10", "close": 100.0}, attrs=attrs)
        monkeypatch.setattr(df, "_multiples_current", lambda t, px, mc=None: _MULT_NONE)
        result = fetcher.get_financials("XYZ")
        assert result["per_gaap_ttm_sec"] is None
        assert result["per_forward_yf"] == -41.758244

    def test_yfinance_trailing_pe_and_ps_are_ignored(self, fetcher, monkeypatch):
        """yfinanceのtrailing_pe・price_to_salesは読まない（部品の値だけ）"""
        attrs = dict(_FULL_ATTRS, trailing_pe=-12.5, price_to_sales=99.0)
        _patch_market_data(monkeypatch, latest_price={"date": "2026-08-10", "close": 100.0}, attrs=attrs)
        monkeypatch.setattr(df, "_multiples_current", lambda t, px, mc=None: dict(_MULT_NONE, per=33.0, ps=7.0))
        result = fetcher.get_financials("XYZ")
        assert result["per_gaap_ttm_sec"] == 33.0 and result["ps_ttm_sec"] == 7.0

    def test_yf_market_cap_is_only_passed_as_fallback_argument(self, fetcher, monkeypatch):
        seen = {}
        def fake(t, px, mc=None):
            seen["px"], seen["mc"] = px, mc
            return dict(_MULT_NONE, market_cap=px * 10, market_cap_source="sec")
        _patch_market_data(monkeypatch, latest_price={"date": "2026-08-10", "close": 100.0},
                           attrs=dict(_FULL_ATTRS, market_cap=555.0))
        monkeypatch.setattr(df, "_multiples_current", fake)
        result = fetcher.get_financials("XYZ")
        assert seen == {"px": 100.0, "mc": 555.0}
        assert result["market_cap_sec"] == 1000.0 and result["market_cap_source"] == "sec"

    def test_missing_attributes_falls_back_to_neutral_defaults(self, fetcher, monkeypatch):
        """attributes/未取得（reader.get_attributes()がNone）の場合、
        beta/sector/per等が全て中立デフォルトに倒れる（既存except節と同じ値）"""
        _patch_market_data(monkeypatch, latest_price={"date": "2026-08-10", "close": 100.0}, attrs=None)
        result = fetcher.get_financials("XYZ")
        assert result["beta_yf_raw"] is None
        assert result["sector"] == "default"
        assert result["industry"] == ""
        assert result["per_gaap_ttm_sec"] is None
        assert result["per_forward_yf"] is None
        assert result["peg"] is None
        assert result["ps_ttm_sec"] is None
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
        # 倍率の部品に株数が無い銘柄（V型）は yfinance implied を印つきで使う（[[TANUKI-SHARES-SINGLE-SOURCE-1]]）
        assert result["_shares_source"] == "yf_implied_fallback"
