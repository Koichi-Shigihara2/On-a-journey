"""
tests/test_breadth_calculator_market_data_switch.py

[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-7（breadth_calculator.py
切替）の回帰テスト。compute_breadth()・fetch_rsp_spy_divergence()の
yfinance一括ダウンロード（yf.download()）がcommon.market_data.reader
経由に置き換わったことを検証する。get_sp500_tickers()は意図的独立実装
のため対象外（本テストでは扱わない）。

src/market/market_pulse/breadth_calculator.pyは他の切替済みファイルと
同じくトップレベルパッケージに属さない独立ディレクトリのスクリプトの
ため、sys.path追加による直接importで読む。

実行方法:
    python -m pytest tests/test_breadth_calculator_market_data_switch.py -v
"""

import os
import sys

import pytest

_MARKET_PULSE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "market", "market_pulse")
)
if _MARKET_PULSE_DIR not in sys.path:
    sys.path.insert(0, _MARKET_PULSE_DIR)

import breadth_calculator as bc  # noqa: E402


_ANCHOR = "2026-09-25"
_DAYS_CACHE = {}


def _trading_days(anchor, n):
    """anchorまでの直近n営業日（NYSE、古い順）。実装側の関数に依存しない独立計算"""
    if (anchor, n) not in _DAYS_CACHE:
        import pandas_market_calendars as mcal
        from datetime import date, timedelta
        a = date.fromisoformat(anchor)
        days = mcal.get_calendar("NYSE").valid_days(start_date=(a - timedelta(days=n * 3 + 15)).isoformat(),
                                                    end_date=anchor)
        _DAYS_CACHE[(anchor, n)] = [d.strftime("%Y-%m-%d") for d in days[-n:]]
    return _DAYS_CACHE[(anchor, n)]


def _make_series(closes, gap_at=None, anchor=_ANCHOR):
    """dateはanchorを最終日とするNYSE営業日の連番、closeは引数のリストから生成する。
    gap_atで指定したインデックスは_gap: Trueのプレースホルダーにする。
    （2026-09-26: compute_breadth()が基準日と前営業日で銘柄をそろえるようになったため、
    合成の連番日付〈d0000…〉から実際の営業日に変更）"""
    days = _trading_days(anchor, len(closes))
    series = []
    for i, c in enumerate(closes):
        if gap_at is not None and i in gap_at:
            series.append({"date": days[i], "_gap": True})
        else:
            series.append({"date": days[i], "close": c, "_gap": False})
    return series


def _patch_price_series(monkeypatch, series_map, has_market_data=True):
    monkeypatch.setattr(bc, "HAS_MARKET_DATA", has_market_data)
    monkeypatch.setattr(bc, "_md_get_price_series", lambda ticker, days: series_map.get(ticker, []))


class TestComputeBreadthBasics:
    def test_market_data_unavailable_returns_none(self, monkeypatch):
        monkeypatch.setattr(bc, "HAS_MARKET_DATA", False)
        assert bc.compute_breadth(["AAPL"]) is None

    def test_insufficient_valid_tickers_returns_none(self, monkeypatch):
        """有効銘柄が100未満の場合はNoneを返す（データ品質ガード）"""
        # 50銘柄のみ、いずれも十分なデータを持つ
        series_map = {
            f"T{i}": _make_series([100.0 + j for j in range(60)]) for i in range(50)
        }
        _patch_price_series(monkeypatch, series_map)
        assert bc.compute_breadth(list(series_map.keys())) is None

    def test_ticker_with_recent_gaps_is_excluded(self, monkeypatch):
        """直近5営業日で実データが3日未満の銘柄は除外される
        （旧ロジックのrecent_nan<3条件と同義）"""
        good = {f"G{i}": _make_series([100.0 + j for j in range(260)]) for i in range(150)}
        # 直近5日中4日がgap（実データ1日のみ）→ 除外対象
        sparse = {"SPARSE": _make_series([100.0 + j for j in range(260)], gap_at={256, 257, 258, 259})}
        series_map = {**good, **sparse}
        _patch_price_series(monkeypatch, series_map)
        result = bc.compute_breadth(list(series_map.keys()))
        assert result is not None
        assert result["total_stocks"] == 150

    def test_missing_ticker_data_skips_gracefully(self, monkeypatch):
        """個別銘柄取得失敗（空リスト）でも例外にならず他銘柄の集計は継続する"""
        good = {f"G{i}": _make_series([100.0 + j for j in range(260)]) for i in range(150)}
        series_map = {**good, "MISSING": []}
        _patch_price_series(monkeypatch, series_map)
        result = bc.compute_breadth(list(series_map.keys()))
        assert result is not None
        assert result["total_stocks"] == 150


class TestComputeBreadthCalculations:
    def _base_series_map(self, n=150):
        """n銘柄、260日分の単調増加系列（全銘柄が上昇トレンド=新高値・
        50/200MA超過の判定を検証しやすくする）"""
        return {f"T{i}": _make_series([100.0 + j * 0.1 for j in range(260)]) for i in range(n)}

    def test_advances_declines_counted_correctly(self, monkeypatch):
        series_map = self._base_series_map(100)
        # 追加で下落銘柄を50件作る
        down = {f"D{i}": _make_series([200.0 - j * 0.1 for j in range(260)]) for i in range(50)}
        series_map.update(down)
        _patch_price_series(monkeypatch, series_map)
        result = bc.compute_breadth(list(series_map.keys()))
        assert result["advances"] == 100  # 単調増加系列は最終日も上昇
        assert result["declines"] == 50   # 単調減少系列は最終日も下落

    def test_new_highs_52w_detected_for_monotonic_increase(self, monkeypatch):
        """単調増加系列は最終日が52週内の最高値 → 新高値としてカウントされる"""
        series_map = self._base_series_map(150)
        _patch_price_series(monkeypatch, series_map)
        result = bc.compute_breadth(list(series_map.keys()))
        assert result["new_highs_52w"] == 150

    def test_pct_above_50ma_and_200ma_for_uptrend(self, monkeypatch):
        """単調増加系列は直近終値が50日/200日移動平均を上回る"""
        series_map = self._base_series_map(150)
        _patch_price_series(monkeypatch, series_map)
        result = bc.compute_breadth(list(series_map.keys()))
        assert result["pct_above_50ma"] == 100.0
        assert result["pct_above_200ma"] == 100.0

    def test_date_reflects_latest_real_date(self, monkeypatch):
        series_map = self._base_series_map(150)
        _patch_price_series(monkeypatch, series_map)
        result = bc.compute_breadth(list(series_map.keys()))
        assert result["date"] == _ANCHOR


class TestFetchRspSpyDivergence:
    def test_normal_case_computed_correctly(self, monkeypatch):
        rsp = _make_series([100.0 + i * 0.1 for i in range(25)])
        spy = _make_series([200.0 + i * 0.05 for i in range(25)])
        _patch_price_series(monkeypatch, {"RSP": rsp, "SPY": spy})
        result = bc.fetch_rsp_spy_divergence()
        assert result is not None
        assert "rsp_return_1d" in result
        assert "rsp_spy_divergence_1d" in result
        assert "rsp_spy_divergence_20d_avg" in result

    def test_rsp_outperforms_spy_yields_positive_divergence(self, monkeypatch):
        """RSPの騰落率がSPYを上回る場合、divergence_1dは正になる
        （プラス=RSP優勢=広範な上昇、docstring記載の符号と一致）"""
        rsp = _make_series([100.0, 100.0, 105.0])  # +5%
        spy = _make_series([200.0, 200.0, 202.0])  # +1%
        _patch_price_series(monkeypatch, {"RSP": rsp, "SPY": spy})
        result = bc.fetch_rsp_spy_divergence()
        assert result["rsp_spy_divergence_1d"] > 0

    def test_market_data_unavailable_returns_none(self, monkeypatch):
        _patch_price_series(monkeypatch, {}, has_market_data=False)
        assert bc.fetch_rsp_spy_divergence() is None

    def test_insufficient_data_returns_none(self, monkeypatch):
        _patch_price_series(monkeypatch, {"RSP": _make_series([100.0]), "SPY": _make_series([200.0])})
        assert bc.fetch_rsp_spy_divergence() is None

    def test_unequal_length_series_truncated_to_common_tail(self, monkeypatch):
        """RSP/SPYの実データ件数が異なる場合、末尾の共通件数に揃える"""
        rsp = _make_series([100.0 + i * 0.1 for i in range(25)])
        spy = _make_series([200.0 + i * 0.05 for i in range(20)])
        _patch_price_series(monkeypatch, {"RSP": rsp, "SPY": spy})
        result = bc.fetch_rsp_spy_divergence()
        assert result is not None

    def test_unexpected_exception_returns_none(self, monkeypatch):
        def _raise(ticker, days):
            raise RuntimeError("simulated failure")
        monkeypatch.setattr(bc, "HAS_MARKET_DATA", True)
        monkeypatch.setattr(bc, "_md_get_price_series", _raise)
        assert bc.fetch_rsp_spy_divergence() is None


class TestBreadthSameDateOnly:
    """MARKETPULSE-BREADTH-MIXED-DATES-1（2026-09-26）: 基準日の終値があり、直前の終値が
    前営業日である銘柄だけを集計する（2026-09-26型: 133銘柄が09-25、370銘柄が09-24の比較）"""

    def test_stale_tickers_excluded_and_counted(self, monkeypatch):
        up = {f"U{i}": _make_series([100.0 + j for j in range(260)]) for i in range(120)}
        # 09-25の終値が欠け、最新が09-24の銘柄（下落系列）→ 除外され、下落として数えない
        stale = {f"S{i}": _make_series([400.0 - j for j in range(259)] + [None], gap_at={259}) for i in range(50)}
        _patch_price_series(monkeypatch, {**up, **stale})
        r = bc.compute_breadth(list(up) + list(stale))
        assert r["date"] == _ANCHOR
        assert r["total_stocks"] == 120 and r["stocks_excluded_date_mismatch"] == 50
        assert r["advances"] == 120 and r["declines"] == 0

    def test_hole_before_latest_is_excluded(self, monkeypatch):
        """前営業日の終値が欠けている銘柄は2営業日分の変化になるため除外する"""
        up = {f"U{i}": _make_series([100.0 + j for j in range(260)]) for i in range(120)}
        hole = {"H": _make_series([100.0 + j for j in range(260)], gap_at={258})}
        _patch_price_series(monkeypatch, {**up, **hole})
        r = bc.compute_breadth(list(up) + ["H"])
        assert r["total_stocks"] == 120 and r["stocks_excluded_date_mismatch"] == 1


class TestRspSpyDateAligned:
    def test_missing_spy_close_does_not_pair_different_days(self, monkeypatch):
        """SPYの最新日の終値が欠けても、RSPの最新日とSPYの前日を組み合わせない"""
        rsp = _make_series([100.0 + j for j in range(25)])
        spy = _make_series([100.0] * 24 + [None], gap_at={24})
        _patch_price_series(monkeypatch, {"RSP": rsp, "SPY": spy})
        r = bc.fetch_rsp_spy_divergence()
        # 共通の最新日は前営業日。その日のRSP騰落率は(123/122-1)、SPYは0
        assert r["spy_return_1d"] == 0.0
        assert r["rsp_return_1d"] == round((123.0 / 122.0 - 1) * 100, 3)
