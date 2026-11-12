"""
tests/test_hypecore_market_data_switch.py

[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8（hypecore.py本体切替）の
回帰テスト。fetch_price_data()・fetch_info_snapshot()・
fetch_analyst_history()のyfinance直接呼び出しがcommon.market_data.
reader経由に置き換わったことを検証する。

src/value/hypecore/hypecore.pyはtests/test_pipeline_logic.pyと同じ
sys.path追加による直接importで読む。

実行方法:
    python -m pytest tests/test_hypecore_market_data_switch.py -v
"""

import os
import sys

import pandas as pd
import pytest

_HYPECORE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "hypecore")
)
if _HYPECORE_DIR not in sys.path:
    sys.path.insert(0, _HYPECORE_DIR)

import hypecore  # noqa: E402


def _make_price_series(closes, gap_at=None):
    series = []
    for i, c in enumerate(closes):
        date = f"2024-{(i // 28) % 12 + 1:02d}-{(i % 28) + 1:02d}"
        if gap_at is not None and i in gap_at:
            series.append({"date": date, "_gap": True})
        else:
            series.append({"date": date, "close": c, "volume": 1_000_000 + i, "_gap": False})
    return series


class TestFetchPriceData:
    def test_normal_case_computes_technical_indicators(self, monkeypatch):
        monkeypatch.setattr(hypecore, "HAS_MARKET_DATA", True)
        series = _make_price_series([100.0 + i * 0.1 for i in range(250)])
        monkeypatch.setattr(hypecore, "_md_get_price_series", lambda ticker, days: series)
        df = hypecore.fetch_price_data("XYZ")
        assert not df.empty
        assert "ma50_dev" in df.columns
        assert "ma200_dev_local" in df.columns
        assert "rsi" in df.columns
        assert "volume_ratio_20d" in df.columns

    def test_empty_series_raises_value_error(self, monkeypatch):
        """対象銘柄のデータが完全に存在しない場合のみValueErrorを送出する
        （前回投資調査の改善案: 単発の営業日欠損では発火しない）"""
        monkeypatch.setattr(hypecore, "HAS_MARKET_DATA", True)
        monkeypatch.setattr(hypecore, "_md_get_price_series", lambda ticker, days: [])
        with pytest.raises(ValueError):
            hypecore.fetch_price_data("XYZ")

    def test_all_gap_series_raises_value_error(self, monkeypatch):
        monkeypatch.setattr(hypecore, "HAS_MARKET_DATA", True)
        series = _make_price_series([100.0] * 10, gap_at=set(range(10)))
        monkeypatch.setattr(hypecore, "_md_get_price_series", lambda ticker, days: series)
        with pytest.raises(ValueError):
            hypecore.fetch_price_data("XYZ")

    def test_partial_gaps_do_not_raise(self, monkeypatch):
        """一部の営業日欠損（_gap: True混在）だけでは例外にならない"""
        monkeypatch.setattr(hypecore, "HAS_MARKET_DATA", True)
        series = _make_price_series([100.0 + i * 0.1 for i in range(250)], gap_at={5, 10, 15})
        monkeypatch.setattr(hypecore, "_md_get_price_series", lambda ticker, days: series)
        df = hypecore.fetch_price_data("XYZ")
        assert not df.empty

    def test_market_data_unavailable_raises_value_error(self, monkeypatch):
        monkeypatch.setattr(hypecore, "HAS_MARKET_DATA", False)
        with pytest.raises(ValueError):
            hypecore.fetch_price_data("XYZ")


class TestFetchInfoSnapshot:
    _ATTRS = {
        "forward_pe": 18.0, "trailing_pe": 99.0, "price_to_sales": 77.0, "peg_ratio": 1.5,   # trailing_pe・price_to_salesは読まない（部品の値）
        "revenue_growth": 0.164, "earnings_growth": 0.287, "gross_margins": 0.48653,
        "recommendation_mean": 2.08696, "analyst_count": 41, "short_pct_float": 0.01,
        "short_ratio": 2.28, "market_cap": 1_000_000_000, "shares_outstanding": 1_000_000_000,
        "ev_to_ebitda": 12.0, "average_volume": 56_619_253,
    }

    def _patch(self, monkeypatch, attrs=None, latest_price=None, has_market_data=True):
        monkeypatch.setattr(hypecore, "HAS_MARKET_DATA", has_market_data)
        monkeypatch.setattr(hypecore, "_md_get_attributes", lambda ticker: attrs)
        monkeypatch.setattr(hypecore, "_md_get_latest_price", lambda ticker: latest_price)
        # [[MULTIPLES-ADOPT-1]]: GAAP PER・PSR・時価総額は倍率の部品（実ファイルは読まない）
        monkeypatch.setattr("common.multiples.current_multiples",
                            lambda t, px, mc=None: {"per": 20.0, "ps": 5.0, "market_cap": 1_000_000_000} if attrs else {"per": None, "ps": None, "market_cap": None})

    def test_normal_case_maps_all_fields(self, monkeypatch):
        self._patch(monkeypatch, attrs=dict(self._ATTRS), latest_price={"volume": 40_000_000})
        result = hypecore.fetch_info_snapshot("XYZ")
        assert result["forward_pe"] == 18.0           # stageの入力。yfinanceのまま
        assert result["trailing_pe"] == 20.0 and result["psr"] == 5.0   # 部品の値
        assert result["revenue_growth"] == 0.164
        assert result["earnings_growth"] == 0.287
        assert result["gross_margins"] == 0.48653
        assert result["recommendation_mean"] == 2.08696
        assert result["short_pct_float"] == 0.01
        assert result["short_ratio"] == 2.28
        assert result["num_analysts"] == 41
        assert result["shares"] == 1_000_000_000
        assert result["market_cap"] == 1_000_000_000
        assert result["ev_ebitda"] == 12.0

    def test_volume_vs_avg_uses_daily_layer_current_volume(self, monkeypatch):
        self._patch(monkeypatch, attrs=dict(self._ATTRS), latest_price={"volume": 40_000_000})
        result = hypecore.fetch_info_snapshot("XYZ")
        assert result["volume_vs_avg"] == pytest.approx(40_000_000 / 56_619_253)

    def test_volume_vs_avg_falls_back_to_avg_when_no_latest_price(self, monkeypatch):
        """daily/未取得銘柄（get_latest_price()がNone）ではcur_vol=avg_volに
        フォールバックする（旧コードのinfo.get("volume") or avg_volと同型）"""
        self._patch(monkeypatch, attrs=dict(self._ATTRS), latest_price=None)
        result = hypecore.fetch_info_snapshot("XYZ")
        assert result["volume_vs_avg"] == 1.0

    def test_missing_attributes_returns_neutral_defaults(self, monkeypatch):
        self._patch(monkeypatch, attrs=None, latest_price=None)
        result = hypecore.fetch_info_snapshot("XYZ")
        for k in ("forward_pe", "revenue_growth", "recommendation_mean", "market_cap"):
            assert result[k] is None

    def test_market_data_unavailable_returns_neutral_defaults(self, monkeypatch):
        self._patch(monkeypatch, has_market_data=False)
        result = hypecore.fetch_info_snapshot("XYZ")
        assert result["forward_pe"] is None
        assert result["volume_vs_avg"] is None
        assert len(result) == 15


class TestFetchAnalystHistory:
    def _patch(self, monkeypatch, events=None, earnings_history=None,
               recommendation=None, attrs=None, has_market_data=True):
        monkeypatch.setattr(hypecore, "HAS_MARKET_DATA", has_market_data)
        monkeypatch.setattr(hypecore, "_md_get_analyst_events", lambda ticker: events or [])
        monkeypatch.setattr(hypecore, "_md_get_earnings_history", lambda ticker: earnings_history or [])
        monkeypatch.setattr(
            hypecore, "_md_get_recommendations_history",
            lambda ticker, latest_only=True: recommendation or {},
        )
        monkeypatch.setattr(hypecore, "_md_get_attributes", lambda ticker: attrs)

    def test_upgrades_downgrades_mapped_to_monthly_rates(self, monkeypatch):
        events = [
            {"date": "2026-08-01", "firm": "UBS", "to_grade": "Buy", "from_grade": "Hold", "action": "up"},
            {"date": "2026-08-05", "firm": "Jefferies", "to_grade": "Sell", "from_grade": "Hold", "action": "down"},
        ]
        self._patch(monkeypatch, events=events)
        result = hypecore.fetch_analyst_history("XYZ")
        assert not result.empty
        assert "analyst_upgrade_rate" in result.columns
        # 2026-08は1件Buy(上方)・1件Sell(下方) → upgrade_rate=0.5
        latest = result.iloc[-1]
        assert latest["analyst_upgrade_rate"] == pytest.approx(0.5, abs=0.01)

    def test_eps_surprise_from_earnings_history_stored_as_percent(self, monkeypatch):
        """market_dataのsurprise_percentは生値（小数）保存のため、
        hypecore.py側で*100変換する（旧コードと同じ変換責務）"""
        earnings_history = [
            {"quarter": "2026-06-30", "eps_actual": 2.02, "eps_estimate": 1.89,
             "eps_difference": 0.13, "surprise_percent": 0.0674},
        ]
        self._patch(monkeypatch, earnings_history=earnings_history)
        result = hypecore.fetch_analyst_history("XYZ")
        assert not result.empty
        assert result["eps_surprise"].iloc[-1] == pytest.approx(6.74, abs=0.01)

    def test_eps_surprise_falls_back_to_attributes_earnings_growth(self, monkeypatch):
        """earnings_historyが空の場合、attributes/のearnings_growthに
        フォールバックする（fetcher.py側でのフォールバック合成はしない
        設計、reader.get_attributes()を直接参照）"""
        events = [{"date": "2026-08-01", "firm": "UBS", "to_grade": "Buy",
                   "from_grade": "Hold", "action": "up"}]
        self._patch(monkeypatch, events=events, earnings_history=[], attrs={"earnings_growth": 0.20})
        result = hypecore.fetch_analyst_history("XYZ")
        assert "eps_surprise" in result.columns
        assert result["eps_surprise"].iloc[-1] == pytest.approx(20.0, abs=0.01)

    def test_buy_ratio_from_recommendations_history(self, monkeypatch):
        """[[HYPECORE-MISC-NAMING-GAPS-1]]①: fetcher.py側の生データキー名
        （analyst_history/{SYMBOL}.jsonの蓄積履歴）は引き続きbuy_hold_ratio
        のままだが、hypecore.py側の出力列名はbuy_ratioへリネームされている
        （holdが実際の計算式の分子に含まれない誤称の是正）。"""
        from datetime import date
        events = [{"date": "2026-08-01", "firm": "UBS", "to_grade": "Buy",
                   "from_grade": "Hold", "action": "up"}]
        self._patch(
            monkeypatch, events=events,
            recommendation={"date": str(date.today()), "strong_buy": 6, "buy": 21, "hold": 15,
                             "sell": 2, "strong_sell": 2, "buy_hold_ratio": 0.587},
        )
        result = hypecore.fetch_analyst_history("XYZ")
        assert "buy_ratio" in result.columns
        today_ts = pd.Timestamp(date.today()).to_period("M").to_timestamp()
        if today_ts in result.index:
            assert result.loc[today_ts, "buy_ratio"] == 0.587

    def test_all_sources_empty_returns_empty_dataframe(self, monkeypatch):
        self._patch(monkeypatch)
        result = hypecore.fetch_analyst_history("XYZ")
        assert result.empty

    def test_market_data_unavailable_returns_empty_dataframe(self, monkeypatch):
        self._patch(monkeypatch, has_market_data=False)
        result = hypecore.fetch_analyst_history("XYZ")
        assert result.empty


class TestBuildMonthRecord:
    """[[HYPECORE-MISC-NAMING-GAPS-1]]④の回帰テスト。

    ma200_momはdetermine_stage()のS3慣性・S4転落判定等で使われる内部値
    だが、run_poc()のJSON保存ループが従来ma200_momを出力に含めておらず、
    判定根拠を事後検証できなかった。_build_month_record()（run_poc()の
    JSON保存ループから抽出した本番の出力構築関数そのもの）が実際に
    ma200_momを出力に含めることを検証する。
    """

    def _make_row(self, **overrides):
        base = {
            "price": 123.45,
            "stage": 2,
            "ma200_dev_local": 12.3,
            "ma200_mom": -4.5,
            "ma50_dev": 5.0,
            "from_peak": -10.0,
            "rsi": 55.0,
            "volume_ratio_20d": 1.2,
            "vol_surge_6m": 1.1,
            "rev_yoy": 20.0,
            "ni_yoy": 15.0,
            "rule40_yoy_netmargin": 30.0,
            "ocf_yield_q": 0.02,
            "forward_pe": 25.0,
            "peg_ratio": 1.5,
            "psr": 8.0,
            "revenue_growth_yf": 0.2,
            "earnings_growth": 0.15,
            "recommendation_mean": 2.0,
            "short_pct_float": 0.03,
            "eps_surprise": 5.0,
            "analyst_upgrade_rate": 0.4,
            "analyst_downgrade_rate": 0.1,
            "sell_on_good_news": 0,
            "buy_ratio": 0.6,
            "substage": {"phase": "mid", "label": "中盤", "watch": "watch", "next": "next"},
            "expectation_score": 0.3,
            "fundamental_score": 0.2,
            "momentum_score": 0.1,
            "price_iv_ratio": 1.1,
            "ev_ebitda": 18.0,
            "low_base_effect": False,
        }
        base.update(overrides)
        return pd.Series(base)

    def test_ma200_mom_included_in_output(self):
        idx = pd.Timestamp("2026-03-01")
        row = self._make_row(ma200_mom=-7.891234)
        record = hypecore._build_month_record(idx, row)
        assert "ma200_mom" in record
        assert record["ma200_mom"] == -7.891

    def test_ma200_mom_nan_becomes_null(self):
        idx = pd.Timestamp("2026-03-01")
        row = self._make_row(ma200_mom=float("nan"))
        record = hypecore._build_month_record(idx, row)
        assert record["ma200_mom"] is None

    def test_other_fields_unaffected_by_extraction(self):
        idx = pd.Timestamp("2026-03-01")
        row = self._make_row()
        record = hypecore._build_month_record(idx, row)
        assert record["month"] == "2026-03"
        assert record["stage_label"] == hypecore.STAGE_LABELS[2]
        assert record["ma200_dev_local"] == 12.3
        assert record["substage_phase"] == "mid"
        assert record["low_base_effect"] is False
