"""
tests/test_close_missing_price.py

[[MARKETDATA-DAILY-CLOSE-NONE-PERMANENT-1]]（2026-09-26）の回帰テスト。
yfinanceが終値の無い足を返した日に、それが確定値としてdaily/へ保存され、
TANUKI VALUATIONがcurrent_price=0.0で計算を続けた不具合（2026-09-22に99銘柄・
09-26に78銘柄の分類が誤って変化）の再発防止。

- fetcher: 終値の無い足を保存しない・既存の終値の無い行を取り直す
- reader: get_latest_price()/get_price_on_or_after()は有効な終値を持つ行を返す
- score_watcher: UNDETERMINED（判定不能）を通知せず前回状態を維持する
- daily_pick: UNDETERMINEDを候補にせず、前日UNDETERMINEDを「分類変化」に数えない

ネットワークアクセスは行わない（_download_historical_barsを差し替える）。
"""

import json
import os
import sys

from common.market_data import fetcher, reader

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _bar(date, close=100.0, volume=1000):
    return {"date": date, "open": 99.0, "high": 101.0, "low": 98.0, "close": close, "volume": volume}


def _write_daily(base, symbol, records):
    os.makedirs(os.path.join(base, "daily"), exist_ok=True)
    with open(os.path.join(base, "daily", f"{symbol}.json"), "w", encoding="utf-8") as f:
        json.dump({"symbol": symbol, "records": records}, f)


def _read_daily(base, symbol):
    with open(os.path.join(base, "daily", f"{symbol}.json"), encoding="utf-8") as f:
        return json.load(f)["records"]


class TestFetchDailyPricesSkipsNoClose:
    def test_latest_bar_without_close_is_not_saved(self, tmp_path, monkeypatch):
        """最新の足に終値が無い（2026-09-25のSPY型）→ 保存せず、終値のある直前の足を保存する"""
        base = str(tmp_path)
        _write_daily(base, "SPY", [_bar("2026-09-23", 767.81)])
        monkeypatch.setattr(fetcher, "_download_historical_bars", lambda syms, period="5d", start=None: {
            "SPY": [_bar("2026-09-23", 767.81), _bar("2026-09-24", 767.18), _bar("2026-09-25", None, 35_238_956)]})
        fetcher.fetch_daily_prices(["SPY"], base_dir=base)
        recs = _read_daily(base, "SPY")
        assert [r["date"] for r in recs] == ["2026-09-23", "2026-09-24"]
        assert all(r["close"] is not None for r in recs)

    def test_existing_close_missing_row_is_repaired_on_next_run(self, tmp_path, monkeypatch):
        """前回保存された終値の無い行（2026-09-21型）が、次回の5日窓で終値つきで取れたら置き換わる"""
        base = str(tmp_path)
        bad = dict(_bar("2026-09-21", None, 49_311_754), _validation_warnings=["close must be > 0 (got None)"])
        _write_daily(base, "SPY", [_bar("2026-09-18", 761.69), bad])
        monkeypatch.setattr(fetcher, "_download_historical_bars", lambda syms, period="5d", start=None: {
            "SPY": [_bar("2026-09-21", 773.50, 50_484_700), _bar("2026-09-22", 773.38)]})
        fetcher.fetch_daily_prices(["SPY"], base_dir=base)
        recs = {r["date"]: r for r in _read_daily(base, "SPY")}
        assert recs["2026-09-21"]["close"] == 773.50
        assert recs["2026-09-22"]["close"] == 773.38
        assert recs["2026-09-18"]["close"] == 761.69  # 終値のある過去行は変更しない

    def test_existing_valid_rows_are_not_overwritten(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        _write_daily(base, "SPY", [_bar("2026-09-18", 761.69, volume=111)])
        monkeypatch.setattr(fetcher, "_download_historical_bars", lambda syms, period="5d", start=None: {
            "SPY": [_bar("2026-09-18", 761.69, volume=999), _bar("2026-09-21", 773.50)]})
        fetcher.fetch_daily_prices(["SPY"], base_dir=base)
        recs = {r["date"]: r for r in _read_daily(base, "SPY")}
        assert recs["2026-09-18"]["volume"] == 111

    def test_repair_close_missing_records_only_touches_missing_rows(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        _write_daily(base, "AAPL", [_bar("2026-09-18", 336.13, volume=5), _bar("2026-09-21", None),
                                    _bar("2026-09-24", 335.92, volume=7), _bar("2026-09-25", None)])
        calls = []

        def fake(syms, period="5d", start=None):
            calls.append((tuple(syms), start))
            return {"AAPL": [_bar("2026-09-18", 1.0, volume=1), _bar("2026-09-21", 338.98),
                             _bar("2026-09-24", 1.0, volume=1), _bar("2026-09-25", 341.07)]}
        monkeypatch.setattr(fetcher, "_download_historical_bars", fake)
        result = fetcher.repair_close_missing_records(["AAPL"], base_dir=base)
        recs = {r["date"]: r for r in _read_daily(base, "AAPL")}
        assert result == {"AAPL": 2}
        assert calls == [(("AAPL",), "2026-09-21")]
        assert recs["2026-09-21"]["close"] == 338.98 and recs["2026-09-25"]["close"] == 341.07
        assert recs["2026-09-18"]["close"] == 336.13 and recs["2026-09-24"]["volume"] == 7


class TestReaderReturnsLatestValidClose:
    def test_get_latest_price_skips_close_none_row(self, tmp_path):
        base = str(tmp_path)
        _write_daily(base, "AAPL", [_bar("2026-09-24", 335.92), _bar("2026-09-25", None)])
        p = reader.get_latest_price("AAPL", base_dir=base)
        assert p["close"] == 335.92 and p["date"] == "2026-09-24"

    def test_get_latest_price_none_when_no_valid_close(self, tmp_path):
        base = str(tmp_path)
        _write_daily(base, "AAPL", [_bar("2026-09-25", None)])
        assert reader.get_latest_price("AAPL", base_dir=base) is None

    def test_get_price_on_or_after_skips_close_none_row(self, tmp_path):
        base = str(tmp_path)
        _write_daily(base, "AAPL", [_bar("2026-09-21", None), _bar("2026-09-22", 339.75)])
        p = reader.get_price_on_or_after("AAPL", "2026-09-21", base_dir=base)
        assert p["date"] == "2026-09-22" and p["close"] == 339.75


def _import_score_watcher():
    path = os.path.join(_REPO_ROOT, "src", "value", "tanuki_valuation")
    if path not in sys.path:
        sys.path.insert(0, path)
    import score_watcher
    return score_watcher


class TestScoreWatcherIgnoresUndetermined:
    def test_undetermined_not_notified_and_state_kept(self, monkeypatch):
        sw = _import_score_watcher()
        monkeypatch.setattr(sw, "load_latest", lambda t: {"tanuki_score": "UNDETERMINED", "funda_score": 75,
                                                          "upside_percent": None})
        monkeypatch.setattr(sw, "load_hype_stage", lambda t: (2, "x"))
        prev = {"tanuki_score": "BUY", "funda_score": 75, "upside_percent": 150.0}
        changes, state = sw.detect_changes("ADBE", prev)
        assert changes == [] and state == prev


class TestDailyPickIgnoresUndetermined:
    def _import(self):
        path = os.path.join(_REPO_ROOT, "src", "value", "tanuki_score")
        if path not in sys.path:
            sys.path.insert(0, path)
        import daily_pick
        return daily_pick

    def test_previous_undetermined_is_not_a_category_change(self, monkeypatch):
        dp = self._import()
        monkeypatch.setattr(dp, "XAI_API_KEY", "")
        stocks = [{"ticker": "ADBE", "company": "A", "funda": 75, "timing": 60, "category": "BUY"},
                  {"ticker": "KO", "company": "K", "funda": 80, "timing": 40, "category": "HOLD"}]
        history = [{"date": "2026-09-26", "ticker": "X", "all_categories": {"ADBE": "UNDETERMINED", "KO": "HOLD"}}]
        pick, reason = dp.select_ticker(stocks, history, "2026-09-27")
        assert not reason.startswith("分類変化")

    def test_undetermined_never_picked_in_fallback(self, monkeypatch):
        dp = self._import()
        monkeypatch.setattr(dp, "XAI_API_KEY", "")
        stocks = [{"ticker": "ADBE", "company": "A", "funda": 75, "timing": None, "category": "UNDETERMINED"},
                  {"ticker": "LOAR", "company": "L", "funda": 10, "timing": 20, "category": "PASS"}]
        pick, _ = dp.select_ticker(stocks, [], "2026-09-27")
        assert pick["ticker"] != "ADBE"


class TestMissingTradingDays:
    """MARKETPULSE-TECHPULSE-QQQ-NULL-1（2026-09-26）: 行ごと抜けた取引日の検知と実データでの取り直し"""

    def test_find_missing_trading_days_nyse(self, tmp_path):
        base = str(tmp_path)
        _write_daily(base, "QQQ", [_bar("2026-08-24", 1.0), _bar("2026-08-26", 1.0)])
        assert fetcher.find_missing_trading_days("QQQ", base_dir=base) == ["2026-08-25"]

    def test_n225_uses_japan_calendar(self, tmp_path):
        """2026-09-21〜23は日本の祝日（休場）のため抜けとして扱わない"""
        base = str(tmp_path)
        _write_daily(base, "^N225", [_bar("2026-09-18", 1.0), _bar("2026-09-24", 1.0)])
        assert fetcher.find_missing_trading_days("^N225", base_dir=base) == []

    def test_repair_fills_only_missing_days_with_real_bars(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        _write_daily(base, "QQQ", [_bar("2026-08-24", 700.0, volume=5), _bar("2026-08-26", 702.0, volume=6)])
        monkeypatch.setattr(fetcher, "_download_historical_bars", lambda syms, period="5d", start=None: {
            "QQQ": [_bar("2026-08-24", 1.0), _bar("2026-08-25", 701.0), _bar("2026-08-26", 1.0)]})
        res = fetcher.repair_missing_trading_days(["QQQ"], base_dir=base)
        recs = {r["date"]: r for r in _read_daily(base, "QQQ")}
        assert res == {"QQQ": {"filled": ["2026-08-25"], "unresolved": []}}
        assert recs["2026-08-25"]["close"] == 701.0
        assert recs["2026-08-24"]["close"] == 700.0 and recs["2026-08-26"]["volume"] == 6

    def test_unavailable_day_is_reported_not_guessed(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        _write_daily(base, "FISV", [_bar("2025-11-11", 64.26), _bar("2025-11-13", 64.53)])
        monkeypatch.setattr(fetcher, "_download_historical_bars", lambda syms, period="5d", start=None: {
            "FISV": [_bar("2025-11-11", 64.26), _bar("2025-11-13", 64.53)]})
        res = fetcher.repair_missing_trading_days(["FISV"], base_dir=base)
        assert res == {"FISV": {"filled": [], "unresolved": ["2025-11-12"]}}
        assert [r["date"] for r in _read_daily(base, "FISV")] == ["2025-11-11", "2025-11-13"]

    def test_check57_reports_each_missing_day(self, tmp_path):
        sys.path.insert(0, os.path.join(_REPO_ROOT, "common", "sec_data"))
        import report_consistency_check as rcc
        base = str(tmp_path)
        _write_daily(base, "QQQ", [_bar("2026-08-24", 1.0), _bar("2026-08-27", 1.0)])
        out = rcc._check_daily_missing_trading_days(base_dir=base)
        assert [(s, "2026-08-25" in m or "2026-08-26" in m) for s, m in out] == [("QQQ", True), ("QQQ", True)]
        assert all("[WARN-57" in m for _, m in out)


class TestDailyPickPackagePrice:
    """DAILYPICK-TANUKI-CURRENT-PRICE-KEY-1（2026-09-26）: daily pickのtanuki.current_priceは
    latest.jsonのcomponentsから取り、株価が無い場合はGrokへ渡すパッケージに入れない"""

    def _import(self):
        path = os.path.join(_REPO_ROOT, "src", "value", "tanuki_score")
        if path not in sys.path:
            sys.path.insert(0, path)
        import daily_pick
        return daily_pick

    def _stock(self):
        return {"ticker": "ADBE", "company": "Adobe", "funda": 70, "timing": 65, "category": "BUY"}

    def test_price_taken_from_components(self, monkeypatch):
        dp = self._import()
        monkeypatch.setattr(dp, "load_tanuki", lambda t: {"components": {"current_price": 235.47},
                                                          "intrinsic_value_per_share": 608.2, "upside_percent": 158.3})
        monkeypatch.setattr(dp, "load_hype", lambda t: {})
        monkeypatch.setattr(dp, "load_eps_annual_latest", lambda t: {})
        pkg = dp.build_data_package(self._stock(), {})
        assert pkg["tanuki"]["current_price"] == 235.47
        assert "deviation_rate" not in pkg["tanuki"]

    def test_missing_price_not_passed_to_prompt(self, monkeypatch):
        dp = self._import()
        monkeypatch.setattr(dp, "load_tanuki", lambda t: {"components": {"current_price": None},
                                                          "intrinsic_value_per_share": 608.2})
        monkeypatch.setattr(dp, "load_hype", lambda t: {})
        monkeypatch.setattr(dp, "load_eps_annual_latest", lambda t: {})
        pkg = dp.build_data_package(self._stock(), {})
        assert "current_price" not in pkg["tanuki"]
        assert '"current_price": null' not in json.dumps(pkg)


class TestGetTtmRevenue:
    """指示書㉑ STEP C（2026-09-26）: 社内PS比率（表示用）の分母に使うTTM売上"""

    def _patch(self, monkeypatch, series):
        import common.sec_data.ttm_calculator as tc
        monkeypatch.setattr(tc, "calc_ttm_series", lambda t, store, n_periods=1: series)

    def test_four_quarters_returns_value_and_end(self, monkeypatch):
        from common.sec_data.reader import get_ttm_revenue
        self._patch(monkeypatch, [{"ttm_end": "2026-06-30", "flow": {"revenue": {"val": 174104000, "quarters_used": 4, "missing": 0}}}])
        assert get_ttm_revenue("ONDS", store={}) == {"val": 174104000, "ttm_end": "2026-06-30"}

    def test_incomplete_quarters_returns_none(self, monkeypatch):
        from common.sec_data.reader import get_ttm_revenue
        self._patch(monkeypatch, [{"ttm_end": "2026-06-30", "flow": {"revenue": {"val": 90000000, "quarters_used": 3, "missing": 1}}}])
        assert get_ttm_revenue("X", store={}) is None

    def test_no_series_returns_none(self, monkeypatch):
        from common.sec_data.reader import get_ttm_revenue
        self._patch(monkeypatch, [])
        assert get_ttm_revenue("X", store={}) is None


class TestCloseRetryInSameRun:
    """[[MARKETDATA-DAILY-CLOSE-NONE-RECUR-1]]（2026-09-30）: 同じ実行の中で終値の無い銘柄だけを取り直し、
    それでも取れなければ別経路（Ticker.history(start, end)）を試す。推測では埋めない。"""

    def test_retry_download_recovers_only_missing_symbols(self, monkeypatch):
        calls = []

        def fake(syms, period="5d", start=None):
            calls.append(list(syms))
            return {"AAA": [_bar("2026-09-25", 10.0), _bar("2026-09-28", 11.0)]}
        monkeypatch.setattr(fetcher, "_download_historical_bars", fake)
        history = {"AAA": [_bar("2026-09-25", 10.0), _bar("2026-09-28", None)],
                   "BBB": [_bar("2026-09-25", 20.0), _bar("2026-09-28", 21.0)]}
        slept = []
        out = fetcher._retry_missing_closes(history, attempts=3, wait_sec=5, sleep=slept.append)
        assert calls == [["AAA"]] and slept == [5]
        assert out["recovered_download"] == ["AAA"] and out["still_missing"] == []
        assert history["AAA"][-1]["close"] == 11.0

    def test_history_fallback_then_still_missing(self, monkeypatch):
        monkeypatch.setattr(fetcher, "_download_historical_bars", lambda syms, period="5d", start=None: {
            s: [_bar("2026-09-28", None)] for s in syms})
        monkeypatch.setattr(fetcher, "_history_bars_single", lambda s, start: (
            [_bar("2026-09-25", 10.0), _bar("2026-09-28", 12.0), _bar("2026-09-29", 13.0)] if s == "AAA" else
            [_bar("2026-09-25", 10.0)]))
        history = {"AAA": [_bar("2026-09-25", 10.0), _bar("2026-09-28", None)],
                   "CCC": [_bar("2026-09-25", 30.0), _bar("2026-09-28", None)]}
        out = fetcher._retry_missing_closes(history, attempts=2, wait_sec=0, sleep=lambda s: None)
        assert out["recovered_history"] == ["AAA"] and out["still_missing"] == ["CCC"]
        # 終値の無かった日（09-28）までに切り詰める。取れなかった銘柄は元のまま（推測で埋めない）
        assert [b["date"] for b in history["AAA"]] == ["2026-09-25", "2026-09-28"]
        assert history["CCC"][-1]["close"] is None

    def test_missing_earlier_day_in_window_is_added(self, tmp_path, monkeypatch):
        """前回09-28の足に終値が無く、次の実行で09-28・09-29が取れた場合、09-28も追加する（既存行は上書きしない）"""
        base = str(tmp_path)
        _write_daily(base, "SPY", [_bar("2026-09-24", 1.0), _bar("2026-09-25", 2.0)])
        monkeypatch.setattr(fetcher, "_download_historical_bars", lambda syms, period="5d", start=None: {
            "SPY": [_bar("2026-09-24", 9.0), _bar("2026-09-25", 9.0), _bar("2026-09-28", 3.0), _bar("2026-09-29", 4.0)]})
        fetcher.fetch_daily_prices(["SPY"], base_dir=base)
        recs = _read_daily(base, "SPY")
        assert [(r["date"], r["close"]) for r in recs] == [
            ("2026-09-24", 1.0), ("2026-09-25", 2.0), ("2026-09-28", 3.0), ("2026-09-29", 4.0)]


class TestProvisionalRows:
    """[[MARKETDATA-DAILY-PROVISIONAL-ROWS-1]]（2026-09-30）: 日足の確定前に保存した行に暫定の印を付け、確定した足で置き換える"""

    def test_bar_final_times(self):
        from datetime import datetime, timezone
        assert fetcher.bar_final_at("AAPL", "2026-09-29") == datetime(2026, 9, 29, 20, 0, tzinfo=timezone.utc)
        assert fetcher.bar_final_at("^N225", "2026-09-29") == datetime(2026, 9, 29, 6, 30, tzinfo=timezone.utc)
        assert fetcher.bar_final_at("JPY=X", "2026-09-29") == datetime(2026, 9, 29, 23, 0, tzinfo=timezone.utc)
        assert fetcher.bar_final_at("JPY=X", "2026-11-02") == datetime(2026, 11, 3, 0, 0, tzinfo=timezone.utc)
        assert fetcher.bar_final_at("CL=F", "2026-09-29") >= datetime(2026, 9, 30, 4, 0, tzinfo=timezone.utc)
        assert fetcher.bar_final_at("AAPL", "2026-09-27") is None   # 日曜

    def test_n225_volume_zero_is_provisional(self):
        from datetime import datetime, timezone
        late = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
        assert fetcher.is_provisional_bar("^N225", _bar("2026-09-29", 65358.4, 0), late)
        assert not fetcher.is_provisional_bar("^N225", _bar("2026-09-29", 65209.4, 150_000_000), late)

    def test_provisional_saved_then_replaced_by_final(self, tmp_path, monkeypatch):
        from datetime import datetime, timezone
        base = str(tmp_path)
        _write_daily(base, "CL=F", [_bar("2026-09-25", 92.41)])
        early = datetime(2026, 9, 29, 1, 6, tzinfo=timezone.utc)    # 09-28の足は確定前（NYの翌日0時より前）
        rec = fetcher._mark_provisional("CL=F", _bar("2026-09-28", 93.33), early)
        assert rec["_provisional"] is True
        fetcher._append_daily_record("CL=F", rec, base_dir=base)
        later = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
        n = fetcher._replace_provisional_records("CL=F", [_bar("2026-09-25", 1.0), _bar("2026-09-28", 92.60)], base, later)
        recs = _read_daily(base, "CL=F")
        assert n == 1
        assert [(r["date"], r["close"], r.get("_provisional")) for r in recs] == [
            ("2026-09-25", 92.41, None), ("2026-09-28", 92.60, None)]   # 確定済みの09-25は上書きしない

    def test_n225_not_retried(self, monkeypatch):
        calls = []
        monkeypatch.setattr(fetcher, "_download_historical_bars", lambda syms, period="5d", start=None: calls.append(syms) or {})
        history = {"^N225": [_bar("2026-09-28", 65877.6), _bar("2026-09-29", None, 0)]}
        out = fetcher._retry_missing_closes(history, attempts=3, wait_sec=0, sleep=lambda s: None)
        assert out["initial"] == [] and calls == []


class TestResetWindowSkip:
    """指示書㉕ STEP A: 1回目の取得で半数以上の銘柄に終値が無い（Yahooの日足の作り直しの時間帯）場合は、
    取り直さず何も保存せずに終了する。20銘柄未満の手動実行は対象外"""

    def test_half_or_more_no_close_skips_without_saving(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        syms = [f"S{i}" for i in range(20)]
        for s in syms:
            _write_daily(base, s, [_bar("2026-09-28", 10.0)])
        monkeypatch.setattr(fetcher, "_download_historical_bars", lambda ss, period="5d", start=None: {
            s: [_bar("2026-09-28", 10.0), _bar("2026-09-29", None if i < 10 else 11.0)] for i, s in enumerate(ss)})
        called = []
        monkeypatch.setattr(fetcher, "_retry_missing_closes", lambda h, **k: called.append(1))
        out = fetcher.fetch_daily_prices(syms, base_dir=base)
        assert out["status"] == "reset_window" and out["no_close"] == 10 and called == []
        assert all(len(_read_daily(base, s)) == 1 for s in syms)

    def test_fewer_missing_proceeds(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        syms = [f"S{i}" for i in range(20)]
        for s in syms:
            _write_daily(base, s, [_bar("2026-09-28", 10.0)])
        monkeypatch.setattr(fetcher, "_download_historical_bars", lambda ss, period="5d", start=None: {
            s: [_bar("2026-09-28", 10.0), _bar("2026-09-29", None if i < 9 else 11.0)] for i, s in enumerate(ss)})
        out = fetcher.fetch_daily_prices(syms, base_dir=base)
        assert out["status"] == "fetched"
        assert sum(len(_read_daily(base, s)) == 2 for s in syms) == 11


class TestDailyGuard:
    """指示書㉕ STEP A: Market Data Daily Updateの起動ガード"""

    def _decide(self, iso, base):
        from datetime import datetime
        from common.market_data import daily_guard
        return daily_guard.decide(datetime.fromisoformat(iso), base)

    def test_before_close_plus_20min(self, tmp_path):
        assert self._decide("2026-09-29T20:19:00+00:00", str(tmp_path))["run"] == "false"   # 夏時間: 引け20:00 UTC
        assert self._decide("2026-11-02T21:05:00+00:00", str(tmp_path))["run"] == "false"   # 冬時間: 引け21:00 UTC

    def test_holiday_and_early_close(self, tmp_path):
        assert "休場" in self._decide("2026-12-25T21:00:00+00:00", str(tmp_path))["reason"]
        # 感謝祭の翌日は13:00 ET（18:00 UTC）に引け。18:25 UTCには判定に進む
        assert self._decide("2026-11-27T18:25:00+00:00", str(tmp_path))["run"] == "true"

    def test_already_complete(self, tmp_path):
        d = os.path.join(str(tmp_path), "daily")
        for i in range(20):
            _write_daily(str(tmp_path), f"S{i}", [_bar("2026-09-28", 1.0), _bar("2026-09-29", 1.0 if i < 19 else None)])
        r = self._decide("2026-09-29T21:00:00+00:00", d)
        assert r["run"] == "false" and "19/20" in r["reason"]
        for i in range(20):
            _write_daily(str(tmp_path), f"S{i}", [_bar("2026-09-28", 1.0), _bar("2026-09-29", 1.0 if i < 18 else None)])
        assert self._decide("2026-09-29T21:00:00+00:00", d)["run"] == "true"   # 18/20=90% < 95%
