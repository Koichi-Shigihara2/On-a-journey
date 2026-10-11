"""
tests/test_market_data_stale_provisional.py

[[MARKETDATA-DAILY-PROVISIONAL-ROWS-1]]・[[MARKETDATA-FUTURES-BACKFILL-ROWS-1]]（2026-10-11）の回帰テスト。
確定時刻を過ぎても`_provisional`が残る行（CL=F・GC=Fの09-07、JPY=Xの08-15・10-03、ES=F・NQ=F・NIY=Fの09-29・09-30）の再発防止。

- a: 夜間の取得で、確定時刻を過ぎた暫定の行の最古日から取り直す（period="5d"の窓の外も対象。確定済みの行は上書きしない）
- b: Yahooに足が無いまま確定時刻から7日を過ぎた暫定の行は、消さずに`_no_yahoo_bar`の印に変え、読む側が除外する
- c: 保存の前に、その銘柄の取引日かを確かめる（JPY=Xは土日、先物はCMEの休場日〈CME_TradeDate〉、^N225はJPXの休場日）
- e: report_consistency_check.pyのCHECK-62（確定時刻を過ぎて残る暫定の行のWARN）

ネットワークアクセスは行わない（_download_historical_barsと先物の限月の取得を差し替える）。
"""

import json
import os
import sys
from datetime import datetime, timezone

from common.market_data import fetcher, reader

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_NOW = datetime(2026, 10, 11, 4, 30, tzinfo=timezone.utc)   # 日曜。10-11の調査の時刻


def _bar(date, close=100.0, volume=1000):
    return {"date": date, "open": 99.0, "high": 101.0, "low": 98.0, "close": close, "volume": volume}


def _prov(date, close=100.0, volume=1000):
    return dict(_bar(date, close, volume), _provisional=True)


def _write_daily(base, symbol, records):
    os.makedirs(os.path.join(base, "daily"), exist_ok=True)
    with open(os.path.join(base, "daily", f"{symbol}.json"), "w", encoding="utf-8") as f:
        json.dump({"symbol": symbol, "records": records}, f)


def _read_daily(base, symbol):
    with open(os.path.join(base, "daily", f"{symbol}.json"), encoding="utf-8") as f:
        return {r["date"]: r for r in json.load(f)["records"]}


def _fake_download(window, by_start, calls):
    """period="5d"ならwindow、start指定ならby_start（start以降の足だけ）を返す。呼び出しを記録する。"""
    def dl(syms, period="5d", start=None):
        calls.append((tuple(syms), start))
        src = window if start is None else by_start
        return {s: [b for b in src.get(s, []) if start is None or b["date"] >= start] for s in syms if s in src}
    return dl


_WINDOW_JPY = {"JPY=X": [_bar(d, 150.0) for d in ("2026-10-05", "2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09")]}


class TestRefetchStaleProvisional:
    """a: 確定時刻を過ぎた暫定の行の最古日から取り直す"""

    def test_row_outside_5d_window_is_replaced(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        _write_daily(base, "JPY=X", [_bar("2026-09-09", 147.0, 5), _prov("2026-09-10", 147.5), _bar("2026-10-02", 149.0)])
        calls = []
        by_start = {"JPY=X": [_bar("2026-09-09", 999.0), _bar("2026-09-10", 147.9), _bar("2026-10-02", 999.0)]
                    + _WINDOW_JPY["JPY=X"]}
        monkeypatch.setattr(fetcher, "_download_historical_bars", _fake_download(_WINDOW_JPY, by_start, calls))
        fetcher.fetch_daily_prices(["JPY=X"], base_dir=base, now=_NOW)
        recs = _read_daily(base, "JPY=X")
        assert (("JPY=X",), "2026-09-10") in calls                  # 最古の暫定の行の日から取り直す
        assert recs["2026-09-10"]["close"] == 147.9 and "_provisional" not in recs["2026-09-10"]
        assert recs["2026-09-09"]["close"] == 147.0 and recs["2026-09-09"]["volume"] == 5   # 確定済みは上書きしない
        assert recs["2026-10-02"]["close"] == 149.0

    def test_no_refetch_when_no_stale_rows(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        _write_daily(base, "JPY=X", [_bar("2026-10-02", 149.0)])
        calls = []
        monkeypatch.setattr(fetcher, "_download_historical_bars", _fake_download(_WINDOW_JPY, {}, calls))
        fetcher.fetch_daily_prices(["JPY=X"], base_dir=base, now=_NOW)
        assert all(start is None for _, start in calls)

    def test_refetch_range_is_capped(self, tmp_path, monkeypatch):
        """取り直しは最大PROVISIONAL_REFETCH_MAX_DAYS日まで。それより古い行はそのまま（CHECK-62で検知する）"""
        base = str(tmp_path)
        _write_daily(base, "JPY=X", [_prov("2026-01-05", 157.0), _prov("2026-09-10", 147.5)])
        calls = []
        by_start = {"JPY=X": [_bar("2026-01-05", 158.0), _bar("2026-09-10", 147.9)] + _WINDOW_JPY["JPY=X"]}
        monkeypatch.setattr(fetcher, "_download_historical_bars", _fake_download(_WINDOW_JPY, by_start, calls))
        fetcher.fetch_daily_prices(["JPY=X"], base_dir=base, now=_NOW)
        assert fetcher.PROVISIONAL_REFETCH_MAX_DAYS == 90
        assert (("JPY=X",), "2026-07-13") in calls                  # 10-11の90日前
        recs = _read_daily(base, "JPY=X")
        assert recs["2026-01-05"]["close"] == 157.0 and recs["2026-01-05"]["_provisional"] is True
        assert recs["2026-09-10"]["close"] == 147.9 and "_provisional" not in recs["2026-09-10"]


class TestNoYahooBarMark:
    """b: Yahooに足が無いまま確定時刻から7日を過ぎた暫定の行は、消さずに足なしの印に変える"""

    def test_saturday_row_without_bar_gets_mark_after_7_days(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        # 10-03（土）: 確定時刻10-03 23:00 UTC＋7日＝10-10 23:00 < 10-11 04:30。08-15（土）も同じ
        _write_daily(base, "JPY=X", [_prov("2026-08-15", 159.3, 0), _bar("2026-10-02", 149.0), _prov("2026-10-03", 157.83, 0)])
        calls = []
        by_start = {"JPY=X": [_bar("2026-08-14", 1.0), _bar("2026-08-17", 1.0), _bar("2026-10-02", 1.0)] + _WINDOW_JPY["JPY=X"]}
        monkeypatch.setattr(fetcher, "_download_historical_bars", _fake_download(_WINDOW_JPY, by_start, calls))
        fetcher.fetch_daily_prices(["JPY=X"], base_dir=base, now=_NOW)
        recs = _read_daily(base, "JPY=X")
        for d in ("2026-08-15", "2026-10-03"):
            assert d in recs                                          # 削除しない
            assert recs[d].get("_no_yahoo_bar") is True and "_provisional" not in recs[d]
        assert recs["2026-10-03"]["close"] == 157.83                  # 値はそのまま
        assert "_no_yahoo_bar" not in recs["2026-10-02"]

    def test_within_7_days_stays_provisional(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        # 10-06の足: 確定時刻10-06 23:00 UTC＋7日＝10-13 23:00 > 10-11 04:30（Yahooの窓に無くても、まだ印を変えない）
        window = {"JPY=X": [_bar(d, 150.0) for d in ("2026-10-05", "2026-10-07", "2026-10-08", "2026-10-09")]}
        _write_daily(base, "JPY=X", [_prov("2026-10-06", 150.0)])
        monkeypatch.setattr(fetcher, "_download_historical_bars", _fake_download(window, window, []))
        fetcher.fetch_daily_prices(["JPY=X"], base_dir=base, now=_NOW)
        rec = _read_daily(base, "JPY=X")["2026-10-06"]
        assert rec["_provisional"] is True and "_no_yahoo_bar" not in rec

    def test_not_marked_when_download_failed(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        _write_daily(base, "JPY=X", [_prov("2026-10-03", 157.83, 0)])
        monkeypatch.setattr(fetcher, "_download_historical_bars", lambda syms, period="5d", start=None: {})
        fetcher.fetch_daily_prices(["JPY=X"], base_dir=base, now=_NOW)
        rec = _read_daily(base, "JPY=X")["2026-10-03"]
        assert rec["_provisional"] is True and "_no_yahoo_bar" not in rec

    def test_reader_excludes_marked_rows(self, tmp_path):
        base = str(tmp_path)
        _write_daily(base, "CL=F", [_bar("2026-09-04", 90.0), dict(_bar("2026-09-07", 92.59), _no_yahoo_bar=True)])
        assert reader.get_latest_price("CL=F", base_dir=base)["date"] == "2026-09-04"
        assert [r["date"] for r in reader.get_price_series("CL=F", days=5, base_dir=base) if r.get("close")] == ["2026-09-04"]
        assert reader.get_price_on_or_after("CL=F", "2026-09-05", base_dir=base) is None

    def test_rotce_prices_exclude_marked_rows(self, tmp_path):
        sys.path.insert(0, os.path.join(_REPO_ROOT, "common", "sec_data"))
        import rotce
        root = str(tmp_path)
        _write_daily(os.path.join(root, "common", "market_data"), "AAA",
                     [_bar("2026-09-04", 90.0), dict(_bar("2026-09-07", 92.59), _no_yahoo_bar=True)])
        assert [r["date"] for r in rotce._load_prices(root, "AAA")] == ["2026-09-04"]

    def test_daily_guard_ignores_marked_rows(self, tmp_path):
        from common.market_data import daily_guard
        base = str(tmp_path)
        _write_daily(base, "AAA", [_bar("2026-10-08", 1.0), dict(_bar("2026-10-09", 1.0), _no_yahoo_bar=True)])
        assert daily_guard.completeness("2026-10-09", base=os.path.join(base, "daily")) == (0, 1)


class TestTradingDayCheckBeforeSave:
    """c: 保存の前に、その日がその銘柄の取引日かを確かめる"""

    def test_jpy_saturday_bar_not_saved(self, tmp_path, monkeypatch):
        """10-03 02:06 UTCの実行が保存したO=H=L=C・出来高0の土曜の足（run 37087907001）"""
        base = str(tmp_path)
        _write_daily(base, "JPY=X", [_bar("2026-10-01", 148.0)])
        window = {"JPY=X": [_bar("2026-10-01", 148.0), _bar("2026-10-02", 149.0), _bar("2026-10-03", 157.83, 0)]}
        monkeypatch.setattr(fetcher, "_download_historical_bars", _fake_download(window, window, []))
        fetcher.fetch_daily_prices(["JPY=X"], base_dir=base, now=datetime(2026, 10, 3, 2, 6, tzinfo=timezone.utc))
        recs = _read_daily(base, "JPY=X")
        assert "2026-10-03" not in recs and recs["2026-10-02"]["close"] == 149.0

    def test_futures_labor_day_bar_not_saved(self, tmp_path, monkeypatch):
        """09-07（Labor Day、CMEの清算なし）の足を09-07 23:38 UTCの取得で保存していた（bbcdccb205）"""
        base = str(tmp_path)
        _write_daily(base, "CL=F", [_bar("2026-09-03", 90.0)])
        window = {"CL=F": [_bar("2026-09-03", 90.0), _bar("2026-09-04", 91.0), _bar("2026-09-07", 92.59)]}
        monkeypatch.setattr(fetcher, "_download_historical_bars", _fake_download(window, window, []))
        monkeypatch.setattr(fetcher, "_futures_contract", lambda s: None)
        fetcher.fetch_daily_prices(["CL=F"], base_dir=base, now=datetime(2026, 9, 7, 23, 38, tzinfo=timezone.utc))
        recs = _read_daily(base, "CL=F")
        assert "2026-09-07" not in recs and "2026-09-04" in recs

    def test_n225_japan_holiday_bar_not_saved(self, tmp_path, monkeypatch):
        """2026-09-21〜23は日本の祝日（JPX休場）"""
        base = str(tmp_path)
        _write_daily(base, "^N225", [_bar("2026-09-17", 1.0, 10)])
        window = {"^N225": [_bar("2026-09-17", 1.0, 10), _bar("2026-09-18", 2.0, 10), _bar("2026-09-21", 3.0, 10)]}
        monkeypatch.setattr(fetcher, "_download_historical_bars", _fake_download(window, window, []))
        fetcher.fetch_daily_prices(["^N225"], base_dir=base, now=datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc))
        recs = _read_daily(base, "^N225")
        assert "2026-09-21" not in recs and "2026-09-18" in recs

    def test_trading_day_rules(self):
        assert fetcher.is_symbol_trading_day("JPY=X", "2026-10-02") is True
        assert fetcher.is_symbol_trading_day("JPY=X", "2026-10-03") is False
        assert fetcher.is_symbol_trading_day("GC=F", "2026-09-07") is False
        assert fetcher.is_symbol_trading_day("ES=F", "2026-09-08") is True
        assert fetcher.is_symbol_trading_day("^N225", "2026-09-22") is False
        assert fetcher.is_symbol_trading_day("AAPL", "2026-09-07") is True   # 対象外の銘柄は判定しない


class TestCheck62StaleProvisional:
    """e: 確定時刻を過ぎて残る暫定の行を数えるWARN（足なしの印の行は数えない）"""

    def _rcc(self):
        sys.path.insert(0, os.path.join(_REPO_ROOT, "common", "sec_data"))
        import report_consistency_check as rcc
        return rcc

    def test_counts_stale_rows_and_skips_marked_and_recent(self, tmp_path):
        rcc = self._rcc()
        base = str(tmp_path)
        _write_daily(base, "CL=F", [_prov("2026-09-29"), dict(_bar("2026-09-07"), _no_yahoo_bar=True), _prov("2026-10-08")])
        _write_daily(base, "JPY=X", [_prov("2026-10-03"), _bar("2026-10-02")])
        out = rcc._check_stale_provisional_rows(base_dir=base, now=datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc))
        assert [(s, m.split()[2], m.split()[3].rstrip(":")) for s, m in out] == [
            ("CL=F", "CL=F", "2026-09-29"), ("JPY=X", "JPY=X", "2026-10-03")]
        assert all("[WARN-62" in m for _, m in out)

    def test_margin_is_11_days_after_final(self, tmp_path):
        """bの7日＋夜間の取得が無い最長の間4日。先物の平らな足の行は7日後に足なしの印になるのが正常のため、その間はWARNにしない"""
        rcc = self._rcc()
        base = str(tmp_path)
        _write_daily(base, "JPY=X", [_prov("2026-10-09")])   # 確定時刻10-09 23:00 UTC
        assert rcc.STALE_PROVISIONAL_WARN_DAYS == fetcher.NO_BAR_MARK_DAYS + 4 == 11
        assert rcc._check_stale_provisional_rows(base_dir=base, now=datetime(2026, 10, 20, 22, 59, tzinfo=timezone.utc)) == []
        assert len(rcc._check_stale_provisional_rows(base_dir=base, now=datetime(2026, 10, 20, 23, 0, tzinfo=timezone.utc))) == 1


def _flat(date, price, volume=0):
    return {"date": date, "open": price, "high": price, "low": price, "close": price, "volume": volume}


class TestFlatFuturesBarIsNotFinal:
    """2026-10-11（Koichi判断）: 先物のO=H=L=Cかつ出来高0の足は、暫定の行を確定にする根拠として扱わない。
    NIY=Fの09-30（取引中の値 66995・出来高5,911）がYahooの今の平らな足（66810・出来高0）に置き換わるところだった"""

    def _setup(self, monkeypatch, window, by_start=None):
        monkeypatch.setattr(fetcher, "_download_historical_bars", _fake_download(window, by_start or window, []))
        monkeypatch.setattr(fetcher, "_futures_contract", lambda s: None)

    def test_flat_bar_does_not_replace_provisional_row(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        _write_daily(base, "NIY=F", [_bar("2026-09-29", 66230.0), _prov("2026-09-30", 66995.0, 5911)])
        self._setup(monkeypatch, {"NIY=F": [_bar("2026-09-29", 66230.0), _flat("2026-09-30", 66810.0), _bar("2026-10-01", 68270.0)]})
        fetcher.fetch_daily_prices(["NIY=F"], base_dir=base, now=datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc))
        rec = _read_daily(base, "NIY=F")["2026-09-30"]
        assert rec["close"] == 66995.0 and rec["volume"] == 5911 and rec["_provisional"] is True

    def test_flat_bar_counts_as_no_bar_after_7_days(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        _write_daily(base, "NIY=F", [_bar("2026-09-29", 66230.0), _prov("2026-09-30", 66995.0, 5911)])
        window = {"NIY=F": [_bar(d, 69000.0) for d in ("2026-10-05", "2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09")]}
        by_start = {"NIY=F": [_flat("2026-09-30", 66810.0)] + window["NIY=F"]}
        self._setup(monkeypatch, window, by_start)
        fetcher.fetch_daily_prices(["NIY=F"], base_dir=base, now=_NOW)
        rec = _read_daily(base, "NIY=F")["2026-09-30"]
        assert rec.get("_no_yahoo_bar") is True and "_provisional" not in rec
        assert rec["close"] == 66995.0 and rec["volume"] == 5911          # 値は既存のまま

    def test_normal_bar_still_confirms(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        _write_daily(base, "NIY=F", [_bar("2026-09-29", 66230.0), _prov("2026-09-30", 66995.0, 5911)])
        self._setup(monkeypatch, {"NIY=F": [_bar("2026-09-29", 66230.0), _bar("2026-09-30", 66900.0, 8000), _bar("2026-10-01", 68270.0)]})
        fetcher.fetch_daily_prices(["NIY=F"], base_dir=base, now=datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc))
        rec = _read_daily(base, "NIY=F")["2026-09-30"]
        assert rec["close"] == 66900.0 and "_provisional" not in rec

    def test_new_flat_row_is_saved_as_provisional(self, tmp_path, monkeypatch):
        """新しい行の平らな足は、確定時刻を過ぎていても暫定として保存する（^N225の出来高0の足と同じ作り）"""
        base = str(tmp_path)
        _write_daily(base, "NIY=F", [_bar("2026-10-06", 70885.0)])
        self._setup(monkeypatch, {"NIY=F": [_bar("2026-10-06", 70885.0), _flat("2026-10-07", 68137.0), _flat("2026-10-08", 68137.0)]})
        fetcher.fetch_daily_prices(["NIY=F"], base_dir=base, now=_NOW)
        recs = _read_daily(base, "NIY=F")
        assert recs["2026-10-07"]["_provisional"] is True and recs["2026-10-08"]["_provisional"] is True

    def test_flat_latest_bar_does_not_overwrite_existing_row(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        _write_daily(base, "NIY=F", [_bar("2026-10-08", 68000.0), _prov("2026-10-09", 69075.0, 18302)])
        self._setup(monkeypatch, {"NIY=F": [_bar("2026-10-08", 68000.0), _flat("2026-10-09", 69100.0)]})
        fetcher.fetch_daily_prices(["NIY=F"], base_dir=base, now=datetime(2026, 10, 10, 1, 0, tzinfo=timezone.utc))
        rec = _read_daily(base, "NIY=F")["2026-10-09"]
        assert rec["close"] == 69075.0 and rec["volume"] == 18302 and rec["_provisional"] is True

    def test_non_futures_zero_volume_symbols_unaffected(self, tmp_path, monkeypatch):
        """出来高が常に0の銘柄（^TNXの債券市場の休日の平らな足など）は対象外: 従来どおり確定する"""
        base = str(tmp_path)
        _write_daily(base, "^TNX", [_bar("2025-10-10", 4.0, 0)])
        monkeypatch.setattr(fetcher, "_download_historical_bars", _fake_download(
            {"^TNX": [_bar("2025-10-10", 4.0, 0), _flat("2025-10-13", 4.03)]}, {}, []))
        fetcher.fetch_daily_prices(["^TNX"], base_dir=base, now=datetime(2025, 10, 14, 12, 0, tzinfo=timezone.utc))
        assert "_provisional" not in _read_daily(base, "^TNX")["2025-10-13"]


def test_check57_does_not_count_no_bar_rows_as_missing(tmp_path):
    """足なしの印の行は「行がある」と数える（CHECK-57の抜けにしない。取り直しても埋まらないため）"""
    base = str(tmp_path)
    _write_daily(base, "NIY=F", [_bar("2026-09-28", 1.0), dict(_flat("2026-09-29", 66230.0), _no_yahoo_bar=True), _bar("2026-09-30", 1.0)])
    assert fetcher.find_missing_trading_days("NIY=F", base_dir=base) == []
