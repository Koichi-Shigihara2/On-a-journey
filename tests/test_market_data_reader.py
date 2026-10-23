"""
tests/test_market_data_reader.py

BACKLOG [[MARKETDATA-LAYER-CONSTRUCTION-1]] common/market_data/reader.py の
単体テスト。fetcher.pyが書き込むdaily/・attributes/・analyst_history/を
読み取るAPI群（get_latest_price・get_price_series・get_price_series_as_of・
get_price_on_or_after・get_ma_deviation・get_attributes・get_analyst_events・
get_calendar・get_index_series・get_sp500_constituents_prices）の正常系・
欠損系・エラー耐性を検証する。ネットワークアクセス（yfinance呼び出し）は
行わない。

実行方法:
    python -m pytest tests/test_market_data_reader.py -v
"""

import os

from common.market_data import calendar_universe, fetcher, reader

# 合成データの基準日（NYSE通常営業日、月曜）。「今日」の日付に依存させず
# テストを決定的にするため固定日を使う。
_ANCHOR = "2026-08-10"


def _make_price_record(date: str, close: float, warnings=None) -> dict:
    return {
        "date": date, "open": close - 0.5, "high": close + 1.0, "low": close - 1.0,
        "close": close, "volume": 1000, "_validation_warnings": warnings or [],
    }


def _seed_no_gap_series(base_dir: str, symbol: str, window: int, anchor: str = _ANCHOR):
    """anchorを最終日とするwindow営業日分の連続データ（欠損なし）を書き込み、
    実際に書き込んだ日付リスト（昇順）を返す。"""
    dates = calendar_universe.nyse_trading_days_ending(anchor, window)
    for i, d in enumerate(dates):
        fetcher._append_daily_record(symbol, _make_price_record(d, 100.0 + i), base_dir=base_dir)
    return dates


class TestGetLatestPrice:
    def test_missing_symbol_returns_none(self, tmp_path):
        assert reader.get_latest_price("NOPE", base_dir=str(tmp_path)) is None

    def test_single_record_returned(self, tmp_path):
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record(_ANCHOR, 200.0), base_dir=base)
        latest = reader.get_latest_price("AAPL", base_dir=base)
        assert latest["date"] == _ANCHOR
        assert latest["close"] == 200.0
        assert latest["symbol"] == "AAPL"

    def test_picks_most_recent_of_multiple_records(self, tmp_path):
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record("2026-08-07", 190.0), base_dir=base)
        fetcher._append_daily_record("AAPL", _make_price_record(_ANCHOR, 200.0), base_dir=base)
        latest = reader.get_latest_price("AAPL", base_dir=base)
        assert latest["date"] == _ANCHOR
        assert latest["close"] == 200.0

    def test_lowercase_symbol_is_normalized(self, tmp_path):
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record(_ANCHOR, 200.0), base_dir=base)
        assert reader.get_latest_price("aapl", base_dir=base) is not None


class TestGetPriceSeries:
    def test_missing_symbol_returns_empty_list(self, tmp_path):
        assert reader.get_price_series("NOPE", days=10, base_dir=str(tmp_path)) == []

    def test_zero_or_negative_days_returns_empty_list(self, tmp_path):
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record(_ANCHOR, 200.0), base_dir=base)
        assert reader.get_price_series("AAPL", days=0, base_dir=base) == []
        assert reader.get_price_series("AAPL", days=-5, base_dir=base) == []

    def test_no_gap_series_matches_expected_trading_days(self, tmp_path):
        base = str(tmp_path)
        expected_dates = _seed_no_gap_series(base, "AAPL", 30)
        series = reader.get_price_series("AAPL", days=30, base_dir=base)
        assert len(series) == 30
        assert [r["date"] for r in series] == expected_dates
        assert all(r["_gap"] is False for r in series)

    def test_missing_trading_day_is_flagged_as_gap(self, tmp_path):
        base = str(tmp_path)
        expected_dates = _seed_no_gap_series(base, "AAPL", 30)
        removed_date = expected_dates[15]

        # 該当日のレコードを削除して欠損を意図的に作る
        path = os.path.join(base, "daily", "AAPL.json")
        payload = fetcher._load_json(path, default={"records": []})
        payload["records"] = [r for r in payload["records"] if r["date"] != removed_date]
        fetcher._atomic_write_json(path, payload)

        series = reader.get_price_series("AAPL", days=30, base_dir=base)
        assert len(series) == 30
        gap_dates = [r["date"] for r in series if r["_gap"]]
        assert gap_dates == [removed_date]
        gap_row = [r for r in series if r["date"] == removed_date][0]
        assert gap_row.get("close") is None

    def test_only_latest_available_date_used_as_anchor(self, tmp_path):
        """複数日の記録があっても、直近保存日を末尾とする窓で切り出すこと"""
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record("2026-07-01", 100.0), base_dir=base)
        fetcher._append_daily_record("AAPL", _make_price_record(_ANCHOR, 200.0), base_dir=base)
        series = reader.get_price_series("AAPL", days=5, base_dir=base)
        assert series[-1]["date"] == _ANCHOR
        assert series[-1]["close"] == 200.0


class TestGetPriceSeriesAsOf:
    """[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序6-2（backfill_tech_pulse.py
    切替の前提作業）で新設したget_price_series_as_of()の回帰テスト。
    get_price_series()の「終点＝daily/の最新保存日」を任意の過去日
    （as_of_date）に一般化した版。"""

    _PAST_ANCHOR = "2026-06-03"  # _ANCHOR（2026-08-10）より過去の基準日

    def test_missing_symbol_returns_empty_list(self, tmp_path):
        assert reader.get_price_series_as_of("NOPE", self._PAST_ANCHOR, days=10, base_dir=str(tmp_path)) == []

    def test_zero_or_negative_days_returns_empty_list(self, tmp_path):
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record(_ANCHOR, 200.0), base_dir=base)
        assert reader.get_price_series_as_of("AAPL", self._PAST_ANCHOR, days=0, base_dir=base) == []
        assert reader.get_price_series_as_of("AAPL", self._PAST_ANCHOR, days=-5, base_dir=base) == []

    def test_anchors_at_as_of_date_not_latest_saved_date(self, tmp_path):
        """daily/に最新保存日（_ANCHOR）より後のレコードがあっても、
        as_of_dateを終点とする窓で切り出すこと（get_price_series()との
        違いそのもの）"""
        base = str(tmp_path)
        # 過去基準日を末尾とする30営業日分（欠損なし）を用意
        past_dates = _seed_no_gap_series(base, "AAPL", 30, anchor=self._PAST_ANCHOR)
        # さらに未来（_ANCHOR）のレコードも書き込む（本来のget_price_series()
        # ならこちらが終点になってしまうはずの罠データ）
        fetcher._append_daily_record("AAPL", _make_price_record(_ANCHOR, 999.0), base_dir=base)

        series = reader.get_price_series_as_of("AAPL", self._PAST_ANCHOR, days=30, base_dir=base)
        assert len(series) == 30
        assert [r["date"] for r in series] == past_dates
        assert series[-1]["date"] == self._PAST_ANCHOR
        assert series[-1]["close"] != 999.0
        assert all(r["_gap"] is False for r in series)

    def test_missing_trading_day_is_flagged_as_gap(self, tmp_path):
        base = str(tmp_path)
        expected_dates = _seed_no_gap_series(base, "AAPL", 30, anchor=self._PAST_ANCHOR)
        removed_date = expected_dates[15]

        path = os.path.join(base, "daily", "AAPL.json")
        payload = fetcher._load_json(path, default={"records": []})
        payload["records"] = [r for r in payload["records"] if r["date"] != removed_date]
        fetcher._atomic_write_json(path, payload)

        series = reader.get_price_series_as_of("AAPL", self._PAST_ANCHOR, days=30, base_dir=base)
        assert len(series) == 30
        gap_dates = [r["date"] for r in series if r["_gap"]]
        assert gap_dates == [removed_date]
        gap_row = [r for r in series if r["date"] == removed_date][0]
        assert gap_row.get("close") is None

    def test_date_object_accepted(self, tmp_path):
        import datetime as _dt
        base = str(tmp_path)
        _seed_no_gap_series(base, "AAPL", 30, anchor=self._PAST_ANCHOR)
        series = reader.get_price_series_as_of("AAPL", _dt.date(2026, 6, 3), days=30, base_dir=base)
        assert len(series) == 30
        assert series[-1]["date"] == self._PAST_ANCHOR

    def test_lowercase_symbol_is_normalized(self, tmp_path):
        base = str(tmp_path)
        _seed_no_gap_series(base, "AAPL", 30, anchor=self._PAST_ANCHOR)
        series = reader.get_price_series_as_of("aapl", self._PAST_ANCHOR, days=30, base_dir=base)
        assert len(series) == 30

    def test_as_of_date_beyond_latest_saved_data_returns_gaps(self, tmp_path):
        """daily/の最新保存日より未来のas_of_dateを渡した場合、未保存分は
        例外を発生させずgapとして返ること（エラー耐性）"""
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record(_ANCHOR, 200.0), base_dir=base)
        series = reader.get_price_series_as_of("AAPL", "2099-01-10", days=10, base_dir=base)
        assert len(series) == 10
        assert all(r["_gap"] for r in series)


class TestGetPriceOnOrAfter:
    """[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序5-2（score_verifier.py
    切替の前提作業）で新設したget_price_on_or_after()の回帰テスト。"""

    def test_missing_symbol_returns_none(self, tmp_path):
        assert reader.get_price_on_or_after("NOPE", "2026-06-03", base_dir=str(tmp_path)) is None

    def test_exact_date_match_returned(self, tmp_path):
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record("2026-06-03", 200.0), base_dir=base)
        record = reader.get_price_on_or_after("AAPL", "2026-06-03", base_dir=base)
        assert record["date"] == "2026-06-03"
        assert record["close"] == 200.0
        assert record["symbol"] == "AAPL"

    def test_target_date_is_non_trading_day_picks_next_available(self, tmp_path):
        """targetが休場日（土日等）で当日レコードが無い場合、
        ウィンドウ内で最も早い次の取引日レコードを返すこと"""
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record("2026-06-06", 210.0), base_dir=base)
        fetcher._append_daily_record("AAPL", _make_price_record("2026-06-08", 211.0), base_dir=base)
        # 2026-06-04（木）を起点にした場合、それより後の最初のレコードは06-06
        record = reader.get_price_on_or_after("AAPL", "2026-06-04", base_dir=base)
        assert record["date"] == "2026-06-06"
        assert record["close"] == 210.0

    def test_no_data_within_5day_window_returns_none(self, tmp_path):
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record("2026-06-10", 220.0), base_dir=base)
        record = reader.get_price_on_or_after("AAPL", "2026-06-03", base_dir=base)
        assert record is None

    def test_data_exactly_at_window_boundary_returned(self, tmp_path):
        """date+5日ちょうどのレコードは境界内として採用されること"""
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record("2026-06-08", 230.0), base_dir=base)
        record = reader.get_price_on_or_after("AAPL", "2026-06-03", base_dir=base)
        assert record["date"] == "2026-06-08"

    def test_data_one_day_past_window_returns_none(self, tmp_path):
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record("2026-06-09", 230.0), base_dir=base)
        record = reader.get_price_on_or_after("AAPL", "2026-06-03", base_dir=base)
        assert record is None

    def test_earliest_candidate_picked_among_multiple(self, tmp_path):
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record("2026-06-05", 205.0), base_dir=base)
        fetcher._append_daily_record("AAPL", _make_price_record("2026-06-04", 204.0), base_dir=base)
        record = reader.get_price_on_or_after("AAPL", "2026-06-03", base_dir=base)
        assert record["date"] == "2026-06-04"
        assert record["close"] == 204.0

    def test_lowercase_symbol_is_normalized(self, tmp_path):
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record("2026-06-03", 200.0), base_dir=base)
        assert reader.get_price_on_or_after("aapl", "2026-06-03", base_dir=base) is not None

    def test_date_object_accepted(self, tmp_path):
        import datetime as _dt
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record("2026-06-03", 200.0), base_dir=base)
        record = reader.get_price_on_or_after("AAPL", _dt.date(2026, 6, 3), base_dir=base)
        assert record["date"] == "2026-06-03"


class TestGetMaDeviation:
    def test_missing_symbol_returns_none(self, tmp_path):
        assert reader.get_ma_deviation("NOPE", window=50, base_dir=str(tmp_path)) is None

    def test_insufficient_history_returns_none(self, tmp_path):
        base = str(tmp_path)
        _seed_no_gap_series(base, "AAPL", 10)
        assert reader.get_ma_deviation("AAPL", window=50, base_dir=base) is None

    def test_correct_value_when_window_fully_satisfied(self, tmp_path):
        base = str(tmp_path)
        _seed_no_gap_series(base, "AAPL", 30)
        deviation = reader.get_ma_deviation("AAPL", window=30, base_dir=base)
        closes = [100.0 + i for i in range(30)]
        expected_ma = sum(closes) / 30
        expected_deviation = (closes[-1] - expected_ma) / expected_ma * 100.0
        assert deviation == expected_deviation

    def test_gap_within_window_returns_none(self, tmp_path):
        base = str(tmp_path)
        expected_dates = _seed_no_gap_series(base, "AAPL", 30)
        removed_date = expected_dates[10]
        path = os.path.join(base, "daily", "AAPL.json")
        payload = fetcher._load_json(path, default={"records": []})
        payload["records"] = [r for r in payload["records"] if r["date"] != removed_date]
        fetcher._atomic_write_json(path, payload)
        assert reader.get_ma_deviation("AAPL", window=30, base_dir=base) is None


class TestGetAttributes:
    def test_missing_symbol_returns_none(self, tmp_path):
        assert reader.get_attributes("NOPE", base_dir=str(tmp_path)) is None

    def test_present_record_returned_with_symbol(self, tmp_path):
        base = str(tmp_path)
        path = os.path.join(base, "attributes", "AAPL.json")
        fetcher._atomic_write_json(path, {
            "fetched_at": "2026-08-10T00:00:00+00:00", "trailing_pe": 30.0, "beta": 1.1,
        })
        attrs = reader.get_attributes("AAPL", base_dir=base)
        assert attrs["symbol"] == "AAPL"
        assert attrs["trailing_pe"] == 30.0


class TestGetCalendar:
    def test_missing_symbol_returns_empty_dict(self, tmp_path):
        assert reader.get_calendar("NOPE", base_dir=str(tmp_path)) == {}

    def test_attributes_without_calendar_key_returns_empty_dict(self, tmp_path):
        """[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-4（2026-08-11）で
        fetcher.pyがcalendarフィールドを取得するようになったが、常に
        {"earnings_date": [...]}形式のdict（取得失敗・空の場合は空dict）を
        書き込む設計のため、calendarキー自体が欠落したレコードは実運用では
        発生しない。本テストは移行前の旧レコード・想定外の書き込み欠落を
        想定した後方互換の確認（中立デフォルトへのフォールバック）"""
        base = str(tmp_path)
        path = os.path.join(base, "attributes", "AAPL.json")
        fetcher._atomic_write_json(path, {"fetched_at": "2026-08-10T00:00:00+00:00"})
        assert reader.get_calendar("AAPL", base_dir=base) == {}

    def test_calendar_key_is_surfaced_when_present(self, tmp_path):
        """fetcher.py::fetch_weekly_attributes()が実際に書き込むスキーマ
        （{"earnings_date": [ISO8601日付文字列, ...]}）で検証する
        （[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-4、2026-08-11）"""
        base = str(tmp_path)
        path = os.path.join(base, "attributes", "AAPL.json")
        fetcher._atomic_write_json(path, {
            "fetched_at": "2026-08-10T00:00:00+00:00",
            "calendar": {"earnings_date": ["2026-10-30"]},
        })
        assert reader.get_calendar("AAPL", base_dir=base) == {"earnings_date": ["2026-10-30"]}


class TestGetAnalystEvents:
    def test_missing_symbol_returns_empty_list(self, tmp_path):
        assert reader.get_analyst_events("NOPE", base_dir=str(tmp_path)) == []

    def test_all_events_returned_without_since(self, tmp_path):
        base = str(tmp_path)
        path = os.path.join(base, "analyst_history", "AAPL.json")
        fetcher._atomic_write_json(path, {"events": [
            {"date": "2026-08-01", "firm": "A"}, {"date": "2026-07-01", "firm": "B"},
        ]})
        events = reader.get_analyst_events("AAPL", base_dir=base)
        assert len(events) == 2

    def test_since_filters_older_events(self, tmp_path):
        base = str(tmp_path)
        path = os.path.join(base, "analyst_history", "AAPL.json")
        fetcher._atomic_write_json(path, {"events": [
            {"date": "2026-08-01", "firm": "A"}, {"date": "2026-07-01", "firm": "B"},
        ]})
        events = reader.get_analyst_events("AAPL", since="2026-07-15", base_dir=base)
        assert len(events) == 1
        assert events[0]["firm"] == "A"

    def test_since_accepts_date_object(self, tmp_path):
        import datetime
        base = str(tmp_path)
        path = os.path.join(base, "analyst_history", "AAPL.json")
        fetcher._atomic_write_json(path, {"events": [
            {"date": "2026-08-01", "firm": "A"}, {"date": "2026-07-01", "firm": "B"},
        ]})
        events = reader.get_analyst_events("AAPL", since=datetime.date(2026, 7, 15), base_dir=base)
        assert len(events) == 1


class TestGetEarningsHistory:
    """[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8（hypecore.py本体
    切替）で新設したget_earnings_history()の回帰テスト。"""

    def test_missing_symbol_returns_empty_list(self, tmp_path):
        assert reader.get_earnings_history("NOPE", base_dir=str(tmp_path)) == []

    def test_returns_earnings_history_as_stored(self, tmp_path):
        base = str(tmp_path)
        path = os.path.join(base, "analyst_history", "AAPL.json")
        fetcher._atomic_write_json(path, {"events": [], "earnings_history": [
            {"quarter": "2026-06-30", "eps_actual": 2.02, "eps_estimate": 1.89243,
             "eps_difference": 0.13, "surprise_percent": 0.0674},
            {"quarter": "2026-03-31", "eps_actual": 2.01, "eps_estimate": 1.94275,
             "eps_difference": 0.07, "surprise_percent": 0.0346},
        ]})
        result = reader.get_earnings_history("AAPL", base_dir=base)
        assert len(result) == 2
        assert result[0]["quarter"] == "2026-06-30"
        assert result[0]["surprise_percent"] == 0.0674  # 生値のまま（%変換なし）

    def test_attributes_without_earnings_history_key_returns_empty_list(self, tmp_path):
        """analyst_history/{SYMBOL}.jsonにearnings_historyキー自体が
        存在しない旧レコード（移行前データ）でも例外にならず空リストを返す"""
        base = str(tmp_path)
        path = os.path.join(base, "analyst_history", "AAPL.json")
        fetcher._atomic_write_json(path, {"events": []})
        assert reader.get_earnings_history("AAPL", base_dir=base) == []


class TestGetRecommendationsHistory:
    """[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8（hypecore.py本体
    切替）で新設したget_recommendations_history()の回帰テスト。"""

    def test_missing_symbol_latest_only_returns_empty_dict(self, tmp_path):
        assert reader.get_recommendations_history("NOPE", base_dir=str(tmp_path)) == {}

    def test_missing_symbol_full_history_returns_empty_list(self, tmp_path):
        assert reader.get_recommendations_history("NOPE", latest_only=False, base_dir=str(tmp_path)) == []

    def test_latest_only_returns_most_recent_snapshot(self, tmp_path):
        base = str(tmp_path)
        path = os.path.join(base, "analyst_history", "AAPL.json")
        fetcher._atomic_write_json(path, {"events": [], "recommendations_history": [
            {"date": "2026-08-11", "strong_buy": 6, "buy": 21, "hold": 15,
             "sell": 2, "strong_sell": 2, "buy_hold_ratio": 0.587},
            {"date": "2026-08-04", "strong_buy": 5, "buy": 20, "hold": 16,
             "sell": 2, "strong_sell": 2, "buy_hold_ratio": 0.5556},
        ]})
        result = reader.get_recommendations_history("AAPL", base_dir=base)
        assert isinstance(result, dict)
        assert result["date"] == "2026-08-11"
        assert result["buy_hold_ratio"] == 0.587

    def test_full_history_returns_all_snapshots(self, tmp_path):
        base = str(tmp_path)
        path = os.path.join(base, "analyst_history", "AAPL.json")
        fetcher._atomic_write_json(path, {"events": [], "recommendations_history": [
            {"date": "2026-08-11", "buy_hold_ratio": 0.587},
            {"date": "2026-08-04", "buy_hold_ratio": 0.5556},
        ]})
        result = reader.get_recommendations_history("AAPL", latest_only=False, base_dir=base)
        assert isinstance(result, list)
        assert len(result) == 2

    def test_attributes_without_recommendations_history_key_returns_neutral_default(self, tmp_path):
        """analyst_history/{SYMBOL}.jsonにrecommendations_historyキー
        自体が存在しない旧レコードでも例外にならない"""
        base = str(tmp_path)
        path = os.path.join(base, "analyst_history", "AAPL.json")
        fetcher._atomic_write_json(path, {"events": []})
        assert reader.get_recommendations_history("AAPL", base_dir=base) == {}
        assert reader.get_recommendations_history("AAPL", latest_only=False, base_dir=base) == []


class TestGetIndexSeries:
    def test_aliases_get_price_series(self, tmp_path):
        base = str(tmp_path)
        _seed_no_gap_series(base, "^GSPC", 10)
        assert reader.get_index_series("^GSPC", days=10, base_dir=base) == \
            reader.get_price_series("^GSPC", days=10, base_dir=base)

    def test_missing_symbol_returns_empty_list(self, tmp_path):
        assert reader.get_index_series("^NOPE", days=10, base_dir=str(tmp_path)) == []


class TestGetSp500ConstituentsPrices:
    def test_only_symbols_with_data_are_included(self, tmp_path):
        base = str(tmp_path)
        fetcher._append_daily_record("AAPL", _make_price_record(_ANCHOR, 200.0), base_dir=base)
        result = reader.get_sp500_constituents_prices(symbols=["AAPL", "MSFT"], base_dir=base)
        assert list(result.keys()) == ["AAPL"]
        assert result["AAPL"]["close"] == 200.0

    def test_empty_symbol_list_returns_empty_dict(self, tmp_path):
        assert reader.get_sp500_constituents_prices(symbols=[], base_dir=str(tmp_path)) == {}


class TestErrorResilienceForNeverFetchedSymbol:
    """fetcher.py未実行の銘柄に対する全API呼び出しが例外を投げないことを確認"""

    def test_all_apis_return_empty_values_not_raise(self, tmp_path):
        base = str(tmp_path)
        assert reader.get_latest_price("GHOST", base_dir=base) is None
        assert reader.get_price_series("GHOST", days=30, base_dir=base) == []
        assert reader.get_price_on_or_after("GHOST", "2026-06-03", base_dir=base) is None
        assert reader.get_ma_deviation("GHOST", window=50, base_dir=base) is None
        assert reader.get_attributes("GHOST", base_dir=base) is None
        assert reader.get_analyst_events("GHOST", base_dir=base) == []
        assert reader.get_calendar("GHOST", base_dir=base) == {}
        assert reader.get_index_series("GHOST", days=30, base_dir=base) == []
        assert reader.get_sp500_constituents_prices(symbols=["GHOST"], base_dir=base) == {}
