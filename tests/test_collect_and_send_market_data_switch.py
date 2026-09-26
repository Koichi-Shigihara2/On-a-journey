"""
tests/test_collect_and_send_market_data_switch.py

[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-6（collect_and_send.py切替）
の回帰テスト。fetch_hist()（yfinance直接呼び出し）のcommon.market_data.
reader経由への置き換え（fetch_recent_records()新設・format_line()の入力
形式変更・_get_sp500_ma_deviation()/fetch_qqq_tech_data()の再実装）を
検証する。

src/market/market_pulse/collect_and_send.pyは他の切替済みファイル
（beta_fetcher.py等）と同じくトップレベルパッケージに属さない独立
ディレクトリのスクリプトのため、sys.path追加による直接importで読む
（pipeline.py用テストと同型のパターン）。

実行方法:
    python -m pytest tests/test_collect_and_send_market_data_switch.py -v
"""

import os
import sys

import pytest

_MARKET_PULSE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "market", "market_pulse")
)
if _MARKET_PULSE_DIR not in sys.path:
    sys.path.insert(0, _MARKET_PULSE_DIR)

import collect_and_send as cs  # noqa: E402


def _patch_price_series(monkeypatch, series_map, has_market_data=True):
    """ticker→レコードリストのdictを渡し、_md_get_price_series()をmockする"""
    monkeypatch.setattr(cs, "HAS_MARKET_DATA", has_market_data)
    monkeypatch.setattr(cs, "_md_get_price_series", lambda ticker, days: series_map.get(ticker, []))


def _patch_ma_deviation(monkeypatch, dev_map):
    monkeypatch.setattr(cs, "_md_get_ma_deviation", lambda ticker, window: dev_map.get((ticker, window)))


class TestFetchRecentRecords:
    def test_returns_last_two_real_closes(self, monkeypatch):
        _patch_price_series(monkeypatch, {"XYZ": [
            {"date": "2026-08-06", "close": 100.0, "volume": 1000, "_gap": False},
            {"date": "2026-08-07", "close": 105.0, "volume": 1100, "_gap": False},
        ]})
        result = cs.fetch_recent_records("XYZ")
        assert len(result) == 2
        assert result[-1]["close"] == 105.0

    def test_ignores_gap_placeholders(self, monkeypatch):
        """単発の営業日欠損（_gap: True）を除外し、実データの末尾count件を使う"""
        _patch_price_series(monkeypatch, {"XYZ": [
            {"date": "2026-08-04", "close": 100.0, "volume": 1000, "_gap": False},
            {"date": "2026-08-05", "close": 999.0, "_gap": True},
            {"date": "2026-08-06", "close": 110.0, "volume": 1200, "_gap": False},
            {"date": "2026-08-07", "close": 121.0, "volume": 1300, "_gap": False},
        ]})
        result = cs.fetch_recent_records("XYZ")
        assert [r["close"] for r in result] == [110.0, 121.0]

    def test_insufficient_real_data_returns_none(self, monkeypatch):
        _patch_price_series(monkeypatch, {"XYZ": [
            {"date": "2026-08-07", "close": 100.0, "volume": 1000, "_gap": False},
        ]})
        assert cs.fetch_recent_records("XYZ") is None

    def test_empty_series_returns_none(self, monkeypatch):
        _patch_price_series(monkeypatch, {"XYZ": []})
        assert cs.fetch_recent_records("XYZ") is None

    def test_market_data_unavailable_returns_none(self, monkeypatch):
        _patch_price_series(monkeypatch, {}, has_market_data=False)
        assert cs.fetch_recent_records("XYZ") is None

    def test_unexpected_exception_returns_none(self, monkeypatch):
        def _raise(ticker, days):
            raise RuntimeError("simulated failure")
        monkeypatch.setattr(cs, "HAS_MARKET_DATA", True)
        monkeypatch.setattr(cs, "_md_get_price_series", _raise)
        assert cs.fetch_recent_records("XYZ") is None

    def test_custom_count_requests_more_records(self, monkeypatch):
        _patch_price_series(monkeypatch, {"XYZ": [
            {"date": f"2026-08-{d:02d}", "close": float(d), "volume": 100, "_gap": False}
            for d in range(1, 6)
        ]})
        result = cs.fetch_recent_records("XYZ", count=3, days=10)
        assert [r["close"] for r in result] == [3.0, 4.0, 5.0]


class TestFormatLine:
    def test_none_records_shows_restricted_message(self):
        assert "取得制限あり" in cs.format_line("テスト", None)

    def test_normal_case_shows_value_and_change(self):
        records = [
            {"date": "2026-08-06", "close": 100.0, "volume": 1000},
            {"date": "2026-08-07", "close": 105.0, "volume": 1100},
        ]
        line = cs.format_line("テスト", records)
        assert "105.00" in line
        assert "+5.00" in line
        assert "+5.00%" in line
        assert "08/07" in line

    def test_volume_ratio_included_when_both_positive(self):
        records = [
            {"date": "2026-08-06", "close": 100.0, "volume": 1000},
            {"date": "2026-08-07", "close": 105.0, "volume": 2000},
        ]
        line = cs.format_line("テスト", records)
        assert "前日比出来高比:2.00" in line

    def test_single_record_shows_no_diff(self):
        records = [{"date": "2026-08-07", "close": 100.0, "volume": 1000}]
        line = cs.format_line("テスト", records)
        assert "100.00" in line
        assert "+0.00 (+0.00%)" in line


class TestGetSp500MaDeviation:
    def test_normal_case_computed_correctly(self, monkeypatch):
        _patch_ma_deviation(monkeypatch, {("^GSPC", 50): 3.4, ("^GSPC", 200): 2.0})
        _patch_price_series(monkeypatch, {"^GSPC": [
            {"date": f"day{i}", "close": 100.0 + i * 0.1, "_gap": False} for i in range(220)
        ]})
        result = cs._get_sp500_ma_deviation()
        assert result["deviation_50"] == 3.4
        assert result["above_ma200"] is True

    def test_above_ma200_false_when_deviation_negative(self, monkeypatch):
        _patch_ma_deviation(monkeypatch, {("^GSPC", 50): -1.0, ("^GSPC", 200): -2.0})
        _patch_price_series(monkeypatch, {"^GSPC": [
            {"date": f"day{i}", "close": 100.0, "_gap": False} for i in range(220)
        ]})
        result = cs._get_sp500_ma_deviation()
        assert result["above_ma200"] is False

    def test_ma200_slope_true_when_recent_ma_higher(self, monkeypatch):
        """直近200日平均 > 10日前時点の200日平均 → slope=True"""
        _patch_ma_deviation(monkeypatch, {("^GSPC", 50): 1.0, ("^GSPC", 200): 1.0})
        # 単調増加系列: 直近window(-200:)の平均は必ず10日前window(-210:-10)の平均を上回る
        closes = [{"date": f"day{i}", "close": float(i), "_gap": False} for i in range(220)]
        _patch_price_series(monkeypatch, {"^GSPC": closes})
        result = cs._get_sp500_ma_deviation()
        assert result["ma200_slope"] is True

    def test_insufficient_history_returns_none_deviation(self, monkeypatch):
        _patch_ma_deviation(monkeypatch, {("^GSPC", 50): None, ("^GSPC", 200): None})
        _patch_price_series(monkeypatch, {"^GSPC": []})
        assert cs._get_sp500_ma_deviation() is None

    def test_market_data_unavailable_returns_none(self, monkeypatch):
        monkeypatch.setattr(cs, "HAS_MARKET_DATA", False)
        assert cs._get_sp500_ma_deviation() is None

    def test_short_series_yields_none_slope(self, monkeypatch):
        """window=220未満（210件未満）ではma200_slopeがNoneになる"""
        _patch_ma_deviation(monkeypatch, {("^GSPC", 50): 3.4, ("^GSPC", 200): 2.0})
        _patch_price_series(monkeypatch, {"^GSPC": [
            {"date": f"day{i}", "close": 100.0, "_gap": False} for i in range(100)
        ]})
        result = cs._get_sp500_ma_deviation()
        assert result["ma200_slope"] is None


class TestFetchQqqTechData:
    def test_normal_case_computed_correctly(self, monkeypatch):
        _patch_ma_deviation(monkeypatch, {("QQQ", 125): 8.11})
        qqq_series = [{"date": f"day{i}", "close": 100.0 + i, "_gap": False} for i in range(25)]
        spy_series = [{"date": f"day{i}", "close": 200.0 + i * 0.5, "_gap": False} for i in range(25)]
        _patch_price_series(monkeypatch, {"QQQ": qqq_series, "SPY": spy_series})
        qqq_vs_ma125, qqq_vs_spy_20d = cs.fetch_qqq_tech_data()
        assert qqq_vs_ma125 == 8.11
        assert qqq_vs_spy_20d is not None

    def test_missing_ma125_returns_none_tuple(self, monkeypatch):
        _patch_ma_deviation(monkeypatch, {("QQQ", 125): None})
        _patch_price_series(monkeypatch, {"QQQ": [], "SPY": []})
        assert cs.fetch_qqq_tech_data() == (None, None)

    def test_market_data_unavailable_returns_none_tuple(self, monkeypatch):
        monkeypatch.setattr(cs, "HAS_MARKET_DATA", False)
        assert cs.fetch_qqq_tech_data() == (None, None)

    def test_insufficient_20d_series_leaves_spy_diff_none(self, monkeypatch):
        _patch_ma_deviation(monkeypatch, {("QQQ", 125): 5.0})
        _patch_price_series(monkeypatch, {
            "QQQ": [{"date": "d1", "close": 100.0, "_gap": False}],
            "SPY": [{"date": "d1", "close": 200.0, "_gap": False}],
        })
        qqq_vs_ma125, qqq_vs_spy_20d = cs.fetch_qqq_tech_data()
        assert qqq_vs_ma125 == 5.0
        assert qqq_vs_spy_20d is None


class TestLoadDivHistory:
    """[[MARKETPULSE-MINOR-INCONSISTENCIES-1]]⑤対応: _load_div_history()の
    フォールバックがCNN由来（fear_greed.score）を使い、feargreedchart.com由来
    （tech_pulse.components.fg_score）を使わないことを検証する回帰テスト。
    """

    def _write_json(self, tmp_path, entries):
        import json
        path = tmp_path / "market_data.json"
        path.write_text(json.dumps(entries), encoding="utf-8")
        return str(path)

    def _recent_date(self, days_ago=1):
        from datetime import datetime, timedelta, timezone
        return (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()

    def test_uses_stored_divergence_value_when_present(self, tmp_path):
        entries = [{
            "date": self._recent_date(1),
            "tech_pulse": {"score": 70, "divergence": {"value": 12.3}},
        }]
        path = self._write_json(tmp_path, entries)
        assert cs._load_div_history(path, window=90) == [12.3]

    def test_fallback_uses_cnn_score_not_feargreedchart_score(self, tmp_path):
        """divergence.value欠損時、fear_greed.score（CNN）を使い、
        tech_pulse.components.fg_score（feargreedchart.com）は使わないこと。
        CNN=30・feargreedchart.com=57のように大きく異なる値を与え、
        混入していれば誤った期待値になるよう設計する。
        """
        entries = [{
            "date": self._recent_date(2),
            "tech_pulse": {
                "score": 72,
                "components": {"fg_score": 57},  # feargreedchart.com（使われてはいけない）
                # divergence未記録（旧エントリを再現）
            },
            "fear_greed": {"score": 30},  # CNN（使われるべき）
        }]
        path = self._write_json(tmp_path, entries)
        result = cs._load_div_history(path, window=90)
        assert result == [72 - 30]  # = 42.0
        assert result != [72 - 57]  # feargreedchart.com由来(15.0)ではない

    def test_entry_missing_both_scores_is_skipped(self, tmp_path):
        """tech_pulseブロック自体が丸ごとnull（旧スキーマ）のエントリは、
        フォールバック条件を満たさず完全にスキップされる
        （2026-08-26②で確認した実データの挙動と同じ）。"""
        entries = [
            {"date": self._recent_date(3), "tech_pulse": None, "fear_greed": None},
            {"date": self._recent_date(1), "tech_pulse": {"score": 60, "divergence": {"value": 5.0}}},
        ]
        path = self._write_json(tmp_path, entries)
        assert cs._load_div_history(path, window=90) == [5.0]

    def test_entry_outside_window_excluded(self, tmp_path):
        entries = [
            {"date": self._recent_date(200), "tech_pulse": {"score": 70, "divergence": {"value": 99.0}}},
            {"date": self._recent_date(1), "tech_pulse": {"score": 70, "divergence": {"value": 1.0}}},
        ]
        path = self._write_json(tmp_path, entries)
        assert cs._load_div_history(path, window=90) == [1.0]


class TestCalcHindenburgActive:
    """[[MARKETPULSE-MINOR-INCONSISTENCIES-1]]①対応: 固定値500ではなく
    breadthの実測total_stocksを使うことを検証する。
    """

    def test_none_breadth_returns_none(self):
        assert cs.calc_hindenburg_active(None) == None  # noqa: E711
        assert cs.calc_hindenburg_active({}) is None

    def test_uses_actual_total_stocks_not_fixed_500(self):
        """total_stocks=503のとき、閾値は503*0.022=11.066。
        nl=11（500基準の閾値11.0なら発火するが、503基準なら11.066>11で不発火）
        という実データ（2026-04-02等）で確認済みの境界ケースを再現する。"""
        breadth = {"new_highs_52w": 31, "new_lows_52w": 11, "total_stocks": 503}
        assert cs.calc_hindenburg_active(breadth) is False

    def test_fires_when_actual_threshold_exceeded(self):
        breadth = {"new_highs_52w": 12, "new_lows_52w": 12, "total_stocks": 503}
        assert cs.calc_hindenburg_active(breadth) is True

    def test_missing_total_stocks_falls_back_to_500(self):
        # total_stocks欠損時は旧来の500基準にフォールバック（閾値11.0）
        breadth = {"new_highs_52w": 11, "new_lows_52w": 11}
        assert cs.calc_hindenburg_active(breadth) is True


class TestBreadthSummaryFields:
    """[[MARKETPULSE-MINOR-INCONSISTENCIES-1]]④対応: breadth_summaryの
    ホワイトリストから漏れていた5フィールドが追加されたことを検証する。
    """

    def test_previously_missing_fields_now_passed_through(self, monkeypatch):
        fake_breadth = {
            "date": "2026-08-25", "advances": 206, "declines": 292, "unchanged": 3,
            "ad_ratio_1d": 0.71, "ad_ratio_5d": 1.05, "new_highs_52w": 39, "new_lows_52w": 9,
            "nh_nl_diff": 30, "total_stocks": 501, "pct_above_50ma": 58.3, "pct_above_200ma": 70.9,
            "rsp_return_1d": -0.072, "spy_return_1d": 0.32, "rsp_spy_divergence_1d": -0.392,
            "rsp_spy_divergence_20d_avg": -0.075, "ad_line": 1684, "mcclellan_oscillator": -6.9,
        }
        monkeypatch.setattr(cs, "_load_latest_breadth", lambda: fake_breadth)
        result = cs.compute_sentiment({"VIX指数": {"value": 16.0}})
        bs = result["breadth"]
        for key in ("unchanged", "ad_ratio_1d", "total_stocks", "rsp_return_1d", "spy_return_1d"):
            assert bs[key] == fake_breadth[key], f"{key} not passed through"

    def test_none_breadth_yields_none_summary(self, monkeypatch):
        monkeypatch.setattr(cs, "_load_latest_breadth", lambda: None)
        result = cs.compute_sentiment({"VIX指数": {"value": 16.0}})
        assert result["breadth"] is None


class TestSaveDataCsvFields:
    """[[MARKETPULSE-MINOR-INCONSISTENCIES-1]]③対応: CSV_COLUMNS未登録の
    ため無条件に欠落していたNASDAQ本体・volume_ratio系フィールドが
    正しくCSVへ書き出されることを検証する。
    """

    def _patch_paths(self, monkeypatch, tmp_path):
        monkeypatch.setattr(cs, "DATA_DIR", str(tmp_path))
        monkeypatch.setattr(cs, "JSON_PATH", str(tmp_path / "market_data.json"))
        monkeypatch.setattr(cs, "CSV_PATH", str(tmp_path / "market_data.csv"))

    def test_nasdaq_and_volume_ratio_fields_written(self, tmp_path, monkeypatch):
        import csv as csv_mod
        self._patch_paths(monkeypatch, tmp_path)
        structured_data = {
            "VIX指数": {"value": 16.7, "change": 0.4, "change_percent": 2.6, "volume_ratio": 1.1, "date": "2026-08-25"},
            "NASDAQ": {"value": 24000.0, "change": 50.0, "change_percent": 0.2, "volume_ratio": 0.95, "date": "2026-08-25"},
            "S&P500": {"value": 7650.0, "change": -10.0, "change_percent": -0.13, "volume_ratio": 1.02, "date": "2026-08-25"},
        }
        cs.save_data_to_json_and_csv("report", structured_data, {"score": 57.3, "label": "NEUTRAL"})

        with open(cs.CSV_PATH, encoding="utf-8") as f:
            rows = list(csv_mod.DictReader(f))
        row = rows[-1]
        assert row["NASDAQ_value"] == "24000.0"
        assert row["NASDAQ_change"] == "50.0"
        assert row["NASDAQ_change_percent"] == "0.2"
        assert row["NASDAQ_volume_ratio"] == "0.95"
        assert row["VIX指数_volume_ratio"] == "1.1"
        assert row["S&P500_volume_ratio"] == "1.02"
        # 既存フィールドが影響を受けていないことも確認
        assert row["VIX指数_value"] == "16.7"
        assert list(rows[0].keys()) == cs.CSV_COLUMNS

    def test_existing_csv_header_migrates_without_data_loss(self, tmp_path, monkeypatch):
        """旧ヘッダー（NASDAQ列なし）の既存CSVに対し保存すると、既存行が
        保持されたままヘッダーが新CSV_COLUMNSへ自動更新されることを確認する。"""
        import csv as csv_mod
        self._patch_paths(monkeypatch, tmp_path)
        old_columns = ["date", "judgment", "VIX指数_value", "VIX指数_change",
                       "VIX指数_change_percent", "sentiment_score", "sentiment_label", "summary"]
        with open(cs.CSV_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv_mod.DictWriter(f, fieldnames=old_columns)
            w.writeheader()
            w.writerow({"date": "2026-08-20T00:00:00+09:00", "judgment": "晴れ",
                        "VIX指数_value": "15.0", "VIX指数_change": "0.1",
                        "VIX指数_change_percent": "0.7", "sentiment_score": "60",
                        "sentiment_label": "NEUTRAL", "summary": "old row"})

        structured_data = {"NASDAQ": {"value": 24000.0, "change": 50.0, "change_percent": 0.2, "volume_ratio": 0.95, "date": "2026-08-25"}}
        cs.save_data_to_json_and_csv("new report", structured_data, {"score": 57.3, "label": "NEUTRAL"})

        with open(cs.CSV_PATH, encoding="utf-8") as f:
            rows = list(csv_mod.DictReader(f))
        assert len(rows) == 2
        assert list(rows[0].keys()) == cs.CSV_COLUMNS
        assert rows[0]["VIX指数_value"] == "15.0"  # 旧データが保持されている
        assert rows[0]["summary"] == "old row"
        assert rows[0]["NASDAQ_value"] == ""       # 旧行に新列は空欄で追加
        assert rows[1]["NASDAQ_value"] == "24000.0"


class TestFetchCnnFearGreed:
    """[[FEARGREED-DUPKEY-BUG-1]]対応: previous_closeが正しく生API応答の
    previous_close（fear_greed.fetch()経由）から取得され、
    previous_1_week（旧実装が誤って両方に使い回していた値）とは
    区別されることを検証する。
    """

    def _mock_fetch(self, monkeypatch, fear_and_greed_dict):
        import fear_greed
        monkeypatch.setattr(fear_greed, "fetch", lambda: {"fear_and_greed": fear_and_greed_dict})

    def test_previous_close_distinct_from_one_week_ago(self, monkeypatch):
        """previous_closeとprevious_1_weekに意図的に異なる値を与え、
        previous_closeがprevious_1_week（旧実装のバグ値）ではなく
        真の値を返すことを確認する。"""
        self._mock_fetch(monkeypatch, {
            "score": 58.6, "rating": "greed",
            "previous_close": 58.8, "previous_1_week": 57.2, "previous_1_month": 41.3428571428571,
        })
        result = cs.fetch_cnn_fear_greed()
        assert result["previous_close"] == 58.8
        assert result["previous_close"] != 57.2  # previous_1_week由来のバグ値ではない
        assert result["one_week_ago"] == 57.2
        assert result["one_month_ago"] == 41.34
        assert result["score"] == 58.6
        assert result["rating"] == "greed"

    def test_missing_previous_close_returns_none(self, monkeypatch):
        self._mock_fetch(monkeypatch, {
            "score": 50.0, "rating": "neutral",
            "previous_1_week": 48.0, "previous_1_month": 45.0,
            # previous_close欠損
        })
        result = cs.fetch_cnn_fear_greed()
        assert result["previous_close"] is None
        assert result["one_week_ago"] == 48.0

    def test_fetch_exception_returns_none(self, monkeypatch):
        import fear_greed
        def _raise():
            raise ConnectionError("network down")
        monkeypatch.setattr(fear_greed, "fetch", _raise)
        assert cs.fetch_cnn_fear_greed() is None


class TestCheckDataFreshness:
    """check_data_freshness(): daily/の最新日付が期待する終値日より古い場合にstale=True
    （MARKETPULSE-MDD-CHECKOUT-RACE-1、2026-09-26）"""

    def _patch_latest(self, monkeypatch, dates):
        import common.market_data.reader as rd
        monkeypatch.setattr(cs, "HAS_MARKET_DATA", True)
        monkeypatch.setattr(rd, "get_latest_price", lambda s: {"date": dates[s], "close": 1.0} if dates.get(s) else None)

    def test_fresh_after_close(self, monkeypatch):
        from datetime import datetime, timezone
        self._patch_latest(monkeypatch, {"^GSPC": "2026-09-25", "SPY": "2026-09-25"})
        r = cs.check_data_freshness(datetime(2026, 9, 26, 0, 1, tzinfo=timezone.utc))
        assert r["expected_close_date"] == "2026-09-25" and r["stale"] is False

    def test_stale_when_checkout_before_daily_push(self, monkeypatch):
        """2026-09-18（JST）型: 09-17の終値確定後なのにdaily/が09-16のまま"""
        from datetime import datetime, timezone
        self._patch_latest(monkeypatch, {"^GSPC": "2026-09-16", "SPY": "2026-09-16"})
        r = cs.check_data_freshness(datetime(2026, 9, 17, 23, 37, tzinfo=timezone.utc))
        assert r["expected_close_date"] == "2026-09-17" and r["stale"] is True

    def test_before_close_expects_previous_day(self, monkeypatch):
        from datetime import datetime, timezone
        self._patch_latest(monkeypatch, {"^GSPC": "2026-09-24", "SPY": "2026-09-24"})
        r = cs.check_data_freshness(datetime(2026, 9, 25, 19, 0, tzinfo=timezone.utc))
        assert r["expected_close_date"] == "2026-09-24" and r["stale"] is False

    def test_weekend_expects_friday(self, monkeypatch):
        from datetime import datetime, timezone
        self._patch_latest(monkeypatch, {"^GSPC": "2026-09-25", "SPY": "2026-09-25"})
        r = cs.check_data_freshness(datetime(2026, 9, 27, 12, 0, tzinfo=timezone.utc))
        assert r["expected_close_date"] == "2026-09-25" and r["stale"] is False


class TestAlignedPairCloses:
    """MARKETPULSE-HYG-LQD-DATE-MIX-1（2026-09-26）: HYG対LQD比は両方の終値がそろう
    最新の共通日と、その直前の営業日で計算する"""

    @staticmethod
    def _rows(pairs):
        return [{"date": d, "close": c, "_gap": False} if c is not None else {"date": d, "_gap": True}
                for d, c in pairs]

    def test_missing_lqd_latest_uses_previous_common_day(self, monkeypatch):
        """2026-09-26型: HYGは09-25あり、LQDは09-25欠損 → 09-23→09-24で計算"""
        _patch_price_series(monkeypatch, {
            "HYG": self._rows([("2026-09-23", 78.10), ("2026-09-24", 77.89), ("2026-09-25", 77.86)]),
            "LQD": self._rows([("2026-09-23", 103.89), ("2026-09-24", 103.15), ("2026-09-25", None)]),
        })
        (d0, h0, l0), (d1, h1, l1) = cs._aligned_pair_closes("HYG", "LQD")
        assert (d0, d1) == ("2026-09-23", "2026-09-24")
        assert (h1, l1) == (77.89, 103.15)

    def test_hole_on_previous_day_returns_none(self, monkeypatch):
        _patch_price_series(monkeypatch, {
            "HYG": self._rows([("2026-09-23", 78.10), ("2026-09-24", 77.89), ("2026-09-25", 77.86)]),
            "LQD": self._rows([("2026-09-23", 103.89), ("2026-09-24", None), ("2026-09-25", 103.21)]),
        })
        assert cs._aligned_pair_closes("HYG", "LQD") is None
