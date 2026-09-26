"""
tests/test_market_data_fetcher.py

BACKLOG [[MARKETDATA-LAYER-CONSTRUCTION-1]] common/market_data/fetcher.py の
単体テスト。保存前検証ロジック（validate_price_record・validate_attributes_record）
とアトミック書き込み・重複排除ロジックを検証する。ネットワークアクセス
（yfinance呼び出し）は行わない。

実行方法:
    python -m pytest tests/test_market_data_fetcher.py -v
"""

import json
import os

import pandas as pd

from common.market_data import fetcher


class TestValidatePriceRecord:
    def test_normal_record_has_no_warnings(self):
        record = {"open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0, "volume": 1000}
        assert fetcher.validate_price_record(record) == []

    def test_close_not_positive_is_flagged(self):
        record = {"open": 10.0, "high": 12.0, "low": 9.0, "close": -1.0, "volume": 1000}
        warnings = fetcher.validate_price_record(record)
        assert any("close" in w for w in warnings)

    def test_volume_zero_is_flagged(self):
        record = {"open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0, "volume": 0}
        warnings = fetcher.validate_price_record(record)
        assert any("volume" in w for w in warnings)

    def test_volume_missing_key_is_not_checked(self):
        """フィールド自体が存在しない場合は検証をスキップする
        （fetch_daily_prices()は.history()由来のためvolume自体は常に含まれるが、
        将来別経路のレコードで一部フィールドが欠けるケースに備えた仕様）"""
        record = {"open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0}
        assert fetcher.validate_price_record(record) == []

    def test_high_less_than_close_is_flagged(self):
        record = {"open": 10.0, "high": 10.5, "low": 9.0, "close": 11.0, "volume": 1000}
        warnings = fetcher.validate_price_record(record)
        assert any("high >= close >= low" in w for w in warnings)

    def test_close_less_than_low_is_flagged(self):
        record = {"open": 10.0, "high": 12.0, "low": 9.0, "close": 8.0, "volume": 1000}
        warnings = fetcher.validate_price_record(record)
        assert any("high >= close >= low" in w for w in warnings)

    def test_fifty_two_week_inverted_is_flagged(self):
        record = {
            "close": 11.0, "volume": 1000,
            "fifty_two_week_high": 5.0, "fifty_two_week_low": 20.0,
        }
        warnings = fetcher.validate_price_record(record)
        assert any("fifty_two_week" in w for w in warnings)

    def test_fifty_two_week_absent_is_not_checked(self):
        """yfinance側の52週高安値が存在しない場合（.history()のみの通常経路）は
        検証自体をスキップする"""
        record = {"open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0, "volume": 1000}
        assert fetcher.validate_price_record(record) == []

    def test_multiple_violations_all_reported(self):
        record = {"open": -1.0, "high": 1.0, "low": 9.0, "close": -1.0, "volume": 0}
        warnings = fetcher.validate_price_record(record)
        assert len(warnings) >= 3


class TestValidateAttributesRecord:
    def test_matching_market_cap_has_no_warnings(self):
        record = {"current_price": 100.0, "shares_outstanding": 1000, "market_cap": 100000}
        assert fetcher.validate_attributes_record(record) == []

    def test_missing_fields_skips_check(self):
        """指数等でshares_outstanding/market_capが存在しない場合は検証をスキップする"""
        record = {"current_price": 7748.78}
        assert fetcher.validate_attributes_record(record) == []

    def test_large_cap_relative_tolerance_exceeded_is_flagged(self):
        record = {
            "current_price": 300.0, "shares_outstanding": 15_000_000_000,
            "market_cap": 5_000_000_000_000,
        }
        warnings = fetcher.validate_attributes_record(record)
        assert len(warnings) == 1
        assert "market_cap mismatch" in warnings[0]

    def test_large_cap_within_relative_tolerance_is_not_flagged(self):
        # computed = 300 * 15e9 = 4.5e12, reported差1%以内
        record = {
            "current_price": 300.0, "shares_outstanding": 15_000_000_000,
            "market_cap": 4_540_000_000_000,
        }
        assert fetcher.validate_attributes_record(record) == []

    def test_small_cap_absolute_floor_protects_against_false_positive(self):
        """小型株では相対2%が小さすぎるため、絶対$1,000,000フロアが優先される"""
        record = {"current_price": 1.0, "shares_outstanding": 1_000_000, "market_cap": 1_950_000}
        assert fetcher.validate_attributes_record(record) == []

    def test_small_cap_exceeding_absolute_floor_is_flagged(self):
        record = {"current_price": 1.0, "shares_outstanding": 1_000_000, "market_cap": 2_500_000}
        warnings = fetcher.validate_attributes_record(record)
        assert len(warnings) == 1


class TestAtomicWriteJson:
    def test_write_and_read_roundtrip(self, tmp_path):
        path = os.path.join(str(tmp_path), "sub", "out.json")
        fetcher._atomic_write_json(path, {"a": 1})
        assert json.load(open(path, encoding="utf-8")) == {"a": 1}

    def test_no_leftover_tempfile_after_success(self, tmp_path):
        path = os.path.join(str(tmp_path), "out.json")
        fetcher._atomic_write_json(path, {"a": 1})
        leftovers = [f for f in os.listdir(str(tmp_path)) if f.startswith(".tmp_")]
        assert leftovers == []

    def test_existing_file_untouched_when_write_fails(self, tmp_path):
        """書き込み中に例外が発生しても、既存ファイルは元の内容のまま残る
        （tempfile→os.replace()方式のアトミック性の確認）"""
        path = os.path.join(str(tmp_path), "out.json")
        fetcher._atomic_write_json(path, {"version": 1})

        class Unserializable:
            def __iter__(self):
                raise RuntimeError("simulated crash during write")

        try:
            fetcher._atomic_write_json(path, {"version": 2, "data": Unserializable()})
        except TypeError:
            pass

        assert json.load(open(path, encoding="utf-8")) == {"version": 1}
        leftovers = [f for f in os.listdir(str(tmp_path)) if f.startswith(".tmp_")]
        assert leftovers == []


class TestViolationsLogSections:
    def test_empty_warnings_still_written(self, tmp_path):
        base = str(tmp_path)
        fetcher._write_violations_section(
            "AAPL", "daily_price_validation",
            {"checked_at": "2026-08-10T00:00:00+00:00", "date": "2026-08-10", "warnings": []},
            base_dir=base,
        )
        path = fetcher._violations_log_path("AAPL", base)
        saved = json.load(open(path, encoding="utf-8"))
        assert saved["daily_price_validation"]["warnings"] == []

    def test_daily_and_attributes_sections_do_not_clobber_each_other(self, tmp_path):
        base = str(tmp_path)
        fetcher._write_violations_section(
            "AAPL", "daily_price_validation",
            {"checked_at": "t1", "date": "2026-08-10", "warnings": ["daily warn"]},
            base_dir=base,
        )
        fetcher._write_violations_section(
            "AAPL", "attributes_validation",
            {"checked_at": "t2", "warnings": ["attr warn"]},
            base_dir=base,
        )
        path = fetcher._violations_log_path("AAPL", base)
        saved = json.load(open(path, encoding="utf-8"))
        assert saved["daily_price_validation"]["warnings"] == ["daily warn"]
        assert saved["attributes_validation"]["warnings"] == ["attr warn"]


class TestAppendDailyRecordDedup:
    def test_same_date_refetch_replaces_not_duplicates(self, tmp_path):
        base = str(tmp_path)
        record_v1 = {"date": "2026-08-10", "close": 100.0, "volume": 1000, "_validation_warnings": []}
        record_v2 = {"date": "2026-08-10", "close": 101.0, "volume": 1100, "_validation_warnings": []}
        fetcher._append_daily_record("AAPL", record_v1, base_dir=base)
        fetcher._append_daily_record("AAPL", record_v2, base_dir=base)
        path = os.path.join(base, "daily", "AAPL.json")
        saved = json.load(open(path, encoding="utf-8"))
        assert len(saved["records"]) == 1
        assert saved["records"][0]["close"] == 101.0

    def test_different_dates_both_kept_sorted(self, tmp_path):
        base = str(tmp_path)
        fetcher._append_daily_record(
            "AAPL", {"date": "2026-08-11", "close": 102.0, "volume": 900, "_validation_warnings": []},
            base_dir=base,
        )
        fetcher._append_daily_record(
            "AAPL", {"date": "2026-08-10", "close": 100.0, "volume": 1000, "_validation_warnings": []},
            base_dir=base,
        )
        path = os.path.join(base, "daily", "AAPL.json")
        saved = json.load(open(path, encoding="utf-8"))
        dates = [r["date"] for r in saved["records"]]
        assert dates == ["2026-08-10", "2026-08-11"]


class TestNyseCalendar:
    def test_known_fixed_holiday_is_not_trading_day(self):
        # 感謝祭2026-11-26
        assert fetcher.is_trading_day("2026-11-26") is False

    def test_regular_weekday_is_trading_day(self):
        assert fetcher.is_trading_day("2026-08-10") is True

    def test_observed_july_fourth_holiday_is_not_trading_day(self):
        # 2026-07-04は土曜のため2026-07-03（金）が振替休場になる
        assert fetcher.is_trading_day("2026-07-03") is False

    def test_regular_weekend_is_not_trading_day(self):
        assert fetcher.is_trading_day("2026-08-08") is False


class TestFetchWeeklyAttributesSchema:
    """[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-2（attributes/スキーマ拡張）の
    フィールドマッピング回帰テスト。.infoをmonkeypatchし、ネットワークアクセス
    なしでどのyfinanceキーがどの出力キーにマップされるかを検証する。"""

    _FAKE_INFO = {
        "currentPrice": 100.0, "marketCap": 1_000_000_000,
        "enterpriseValue": 1_050_000_000,
        "trailingPE": 20.0, "forwardPE": 18.0,
        "trailingPegRatio": 1.5, "pegRatio": 1.6,
        "priceToSalesTrailing12Months": 5.0, "enterpriseToEbitda": 12.0,
        "beta": 1.1, "sector": "Technology", "industry": "Software",
        "dividendYield": 0.34,                    # 百分率表記（実測で判明した罠フィールド）
        "trailingAnnualDividendYield": 0.0034,     # 小数表記（正しい採用元）
        "payoutRatio": 0.12,
        "sharesOutstanding": 1_000_000_000, "impliedSharesOutstanding": 1_010_000_000,
        "forwardEps": 5.0,
        "targetMeanPrice": 110.0, "targetMedianPrice": 112.0,
        "targetLowPrice": 90.0, "targetHighPrice": 130.0,
        "numberOfAnalystOpinions": 20, "recommendationKey": "buy",
        "totalDebt": 500_000_000,
    }

    def _patch_info(self, monkeypatch, info, calendar=None, calendar_raises=False,
                     earnings_dates=None, earnings_dates_raises=False):
        class _FakeTicker:
            def __init__(self, symbol):
                pass
            @property
            def info(self):
                return info
            @property
            def calendar(self):
                if calendar_raises:
                    raise RuntimeError("simulated calendar failure")
                return calendar
            @property
            def earnings_dates(self):
                if earnings_dates_raises:
                    raise RuntimeError("simulated earnings_dates failure")
                return earnings_dates
        monkeypatch.setattr(fetcher, "_USE_SAFE_YF", False)
        monkeypatch.setattr(fetcher.yf, "Ticker", _FakeTicker)

    def test_new_fields_are_extracted_with_correct_mapping(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        self._patch_info(monkeypatch, dict(self._FAKE_INFO))
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["enterprise_value"] == 1_050_000_000
        assert saved["forward_pe"] == 18.0
        assert saved["payout_ratio"] == 0.12
        assert saved["implied_shares_outstanding"] == 1_010_000_000
        assert saved["target_median_price"] == 112.0
        assert saved["target_low_price"] == 90.0
        assert saved["target_high_price"] == 130.0
        assert saved["analyst_count"] == 20
        assert saved["analyst_recommendation_key"] == "buy"
        # 既存フィールドも維持されていること
        assert saved["target_mean_price"] == 110.0
        assert saved["trailing_pe"] == 20.0

    def test_dividend_yield_uses_trailing_annual_not_percent_scaled_field(self, tmp_path, monkeypatch):
        """dividend_yieldはdividendYield（百分率表記、実測でAAPL=0.34≒0.34%と
        判明した罠フィールド）ではなくtrailingAnnualDividendYield（小数表記）
        由来であることを回帰テストする"""
        base = str(tmp_path)
        self._patch_info(monkeypatch, dict(self._FAKE_INFO))
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["dividend_yield"] == 0.0034  # trailingAnnualDividendYield由来
        assert saved["dividend_yield"] != 0.34    # dividendYield（誤ったスケール）ではない

    def test_previous_close_is_not_captured(self, tmp_path, monkeypatch):
        """previousCloseはdaily/層の責務のためattributes/には含めない設計を
        回帰テストする（層またぎ再計算の禁止、BACKLOG確定事項4）"""
        base = str(tmp_path)
        info = dict(self._FAKE_INFO)
        info["previousClose"] = 999.0
        self._patch_info(monkeypatch, info)
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert "previous_close" not in saved
        assert 999.0 not in saved.values()

    def test_missing_new_fields_default_to_none_not_error(self, tmp_path, monkeypatch):
        """新フィールドが.infoに存在しない銘柄（指数等）でも例外にならずNoneになる"""
        base = str(tmp_path)
        self._patch_info(monkeypatch, {"currentPrice": 100.0})
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        for key in ("enterprise_value", "forward_pe", "payout_ratio", "implied_shares_outstanding",
                    "target_median_price", "target_low_price", "target_high_price",
                    "analyst_count", "analyst_recommendation_key"):
            assert saved[key] is None


class TestFetchWeeklyAttributesHypecorePrereqFields:
    """[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8前提作業2
    （hypecore.py切替の前提、attributes/スキーマ拡張7フィールド）の
    フィールドマッピング回帰テスト。"""

    _FAKE_INFO = dict(TestFetchWeeklyAttributesSchema._FAKE_INFO)
    _FAKE_INFO.update({
        "revenueGrowth": 0.164, "earningsGrowth": 0.287, "grossMargins": 0.48653,
        "recommendationMean": 2.08696, "shortPercentOfFloat": 0.01, "shortRatio": 2.28,
        "averageVolume": 56_619_253, "averageVolume10days": 60_000_000,
        "volume": 40_000_000,  # 現在出来高（attributes/には保存されないことを検証する）
    })

    def _patch_info(self, monkeypatch, info):
        schema = TestFetchWeeklyAttributesSchema()
        schema._patch_info(monkeypatch, info)

    def test_seven_fields_extracted_with_correct_mapping(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        self._patch_info(monkeypatch, dict(self._FAKE_INFO))
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["revenue_growth"] == 0.164
        assert saved["earnings_growth"] == 0.287
        assert saved["gross_margins"] == 0.48653
        assert saved["recommendation_mean"] == 2.08696
        assert saved["short_pct_float"] == 0.01
        assert saved["short_ratio"] == 2.28
        assert saved["average_volume"] == 56_619_253

    def test_recommendation_mean_and_key_are_both_kept_distinct(self, tmp_path, monkeypatch):
        """recommendation_mean（数値）とanalyst_recommendation_key（文字列）が
        混同されず両方独立して保存されることを検証する"""
        base = str(tmp_path)
        self._patch_info(monkeypatch, dict(self._FAKE_INFO))
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["recommendation_mean"] == 2.08696
        assert isinstance(saved["recommendation_mean"], float)
        assert saved["analyst_recommendation_key"] == "buy"
        assert isinstance(saved["analyst_recommendation_key"], str)

    def test_average_volume_falls_back_to_10days_when_primary_absent(self, tmp_path, monkeypatch):
        """averageVolume不在時はaverageVolume10daysへフォールバックする
        （hypecore.py::fetch_info_snapshot()と同じフォールバック順序）"""
        base = str(tmp_path)
        info = dict(self._FAKE_INFO)
        del info["averageVolume"]
        self._patch_info(monkeypatch, info)
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["average_volume"] == 60_000_000

    def test_current_volume_is_not_captured(self, tmp_path, monkeypatch):
        """現在出来高（info["volume"]）はdaily/層の責務のためattributes/には
        含めない設計を回帰テストする（previousClose非保存と同じ理由、
        BACKLOG確定事項4「層またぎ再計算の禁止」）"""
        base = str(tmp_path)
        self._patch_info(monkeypatch, dict(self._FAKE_INFO))
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert "volume" not in saved
        assert "current_volume" not in saved

    def test_missing_seven_fields_default_to_none_not_error(self, tmp_path, monkeypatch):
        """7フィールドが.infoに存在しない銘柄（指数等）でも例外にならずNoneになる"""
        base = str(tmp_path)
        self._patch_info(monkeypatch, {"currentPrice": 100.0})
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        for key in ("revenue_growth", "earnings_growth", "gross_margins", "recommendation_mean",
                    "short_pct_float", "short_ratio", "average_volume"):
            assert saved[key] is None


class TestFetchWeeklyAttributesCountryField:
    """[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序5（診断ツール2ファイル
    切替、audit.py::audit_ticker()のカナダ企業判定の前提）で追加した
    countryフィールドの回帰テスト。"""

    def _patch_info(self, monkeypatch, info):
        schema = TestFetchWeeklyAttributesSchema()
        schema._patch_info(monkeypatch, info)

    def test_country_is_extracted(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        info = dict(TestFetchWeeklyAttributesSchema._FAKE_INFO)
        info["country"] = "Canada"
        self._patch_info(monkeypatch, info)
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["country"] == "Canada"

    def test_missing_country_defaults_to_none_not_error(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        self._patch_info(monkeypatch, {"currentPrice": 100.0})
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["country"] is None


class TestFetchWeeklyAttributesNumericTypeGuard:
    """[[MARKETDATA-TRAILING-PE-STRING-INFINITY-1]]: yfinance .infoが数値
    フィールドに非数値を返すケース（ZETAのtrailingPE="Infinity"を実測確認）で
    Noneへ正規化されることの回帰テスト。"""

    def _patch_info(self, monkeypatch, info):
        schema = TestFetchWeeklyAttributesSchema()
        schema._patch_info(monkeypatch, info)

    def test_string_infinity_is_normalized_to_none(self, tmp_path, monkeypatch):
        """ZETA実測値: trailingPEが文字列"Infinity"として返るケース"""
        base = str(tmp_path)
        info = dict(TestFetchWeeklyAttributesSchema._FAKE_INFO)
        info["trailingPE"] = "Infinity"
        self._patch_info(monkeypatch, info)
        fetcher.fetch_weekly_attributes(["ZETA"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "ZETA.json"), encoding="utf-8"))
        assert saved["trailing_pe"] is None

    def test_bool_is_not_treated_as_numeric(self, tmp_path, monkeypatch):
        """isinstance(True, int) == True の罠: boolは数値として扱わずNoneにする"""
        base = str(tmp_path)
        info = dict(TestFetchWeeklyAttributesSchema._FAKE_INFO)
        info["beta"] = True
        self._patch_info(monkeypatch, info)
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["beta"] is None

    def test_normal_numeric_values_pass_through_unchanged(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        self._patch_info(monkeypatch, dict(TestFetchWeeklyAttributesSchema._FAKE_INFO))
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["trailing_pe"] == 20.0
        assert saved["beta"] == 1.1
        assert saved["market_cap"] == 1_000_000_000


class TestFetchWeeklyAttributesCalendar:
    """[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-4（pipeline.py .calendar
    切替）で追加したcalendarフィールドの回帰テスト。.calendarをmonkeypatchし、
    ネットワークアクセスなしで{"earnings_date": [...]}形式への変換を検証する。"""

    _FAKE_INFO = TestFetchWeeklyAttributesSchema._FAKE_INFO

    def _patch(self, monkeypatch, calendar=None, calendar_raises=False):
        schema = TestFetchWeeklyAttributesSchema()
        schema._patch_info(monkeypatch, dict(self._FAKE_INFO), calendar=calendar, calendar_raises=calendar_raises)

    def test_earnings_date_is_converted_to_iso_strings(self, tmp_path, monkeypatch):
        import datetime as _dt
        base = str(tmp_path)
        self._patch(monkeypatch, calendar={
            "Earnings Date": [_dt.date(2026, 10, 30)],
            "Dividend Date": _dt.date(2026, 8, 13),  # 使用しないフィールド、保存されないこと
        })
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["calendar"] == {"earnings_date": ["2026-10-30"]}

    def test_multiple_earnings_dates_all_preserved_in_order(self, tmp_path, monkeypatch):
        import datetime as _dt
        base = str(tmp_path)
        self._patch(monkeypatch, calendar={"Earnings Date": [_dt.date(2026, 10, 30), _dt.date(2027, 1, 29)]})
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["calendar"]["earnings_date"] == ["2026-10-30", "2027-01-29"]

    def test_missing_earnings_date_key_yields_empty_calendar(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        self._patch(monkeypatch, calendar={"Dividend Date": "2026-08-13"})
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["calendar"] == {}

    def test_calendar_none_yields_empty_calendar(self, tmp_path, monkeypatch):
        """指数等、.calendarがNoneを返す銘柄でも例外にならず空dictになる"""
        base = str(tmp_path)
        self._patch(monkeypatch, calendar=None)
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["calendar"] == {}

    def test_calendar_fetch_failure_does_not_block_other_fields(self, tmp_path, monkeypatch):
        """.calendar呼び出しが例外を送出しても、.info由来の他フィールドの
        保存は妨げられない（calendar取得失敗の独立性）"""
        base = str(tmp_path)
        self._patch(monkeypatch, calendar_raises=True)
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["calendar"] == {}
        assert saved["forward_pe"] == 18.0  # .info由来フィールドは正常に保存される


class TestFetchWeeklyAttributesNextQuarterEps:
    """[[EPS-1]]で追加したnext_quarter_eps_estimate/next_quarter_eps_dateの
    回帰テスト。Ticker.earnings_datesをmonkeypatchし、ネットワークアクセスなしで
    「Reported EPSがNaN（未発表）の直近行のEPS Estimate」抽出ロジックを検証する。"""

    _FAKE_INFO = TestFetchWeeklyAttributesSchema._FAKE_INFO

    def _patch(self, monkeypatch, earnings_dates=None, earnings_dates_raises=False):
        schema = TestFetchWeeklyAttributesSchema()
        schema._patch_info(monkeypatch, dict(self._FAKE_INFO),
                            earnings_dates=earnings_dates, earnings_dates_raises=earnings_dates_raises)

    @staticmethod
    def _make_earnings_dates(rows):
        """rows: [(date_str, eps_estimate, reported_eps), ...]（ADBE実データ相当の
        新しい順）からyfinance実物と同型のDataFrameを構築する"""
        idx = pd.to_datetime([r[0] for r in rows])
        return pd.DataFrame(
            {"EPS Estimate": [r[1] for r in rows], "Reported EPS": [r[2] for r in rows],
             "Surprise(%)": [None] * len(rows)},
            index=idx,
        )

    def test_next_quarter_eps_extracted_from_unreported_future_row(self, tmp_path, monkeypatch):
        """ADBE実データ相当: 直近行がReported EPS=NaN（未発表）かつ
        EPS Estimateが存在する場合、その値・日付が抽出されること"""
        base = str(tmp_path)
        df = self._make_earnings_dates([
            ("2026-12-09", 6.33, None),
            ("2026-09-10", 6.09, 6.13),
            ("2026-06-11", 5.81, 5.96),
        ])
        self._patch(monkeypatch, earnings_dates=df)
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["next_quarter_eps_estimate"] == 6.33
        assert saved["next_quarter_eps_date"] == "2026-12-09"

    def test_future_row_with_nan_estimate_yields_none(self, tmp_path, monkeypatch):
        """CIX/FROG実データ相当: 未発表行は存在するがEPS Estimate自体が
        アナリスト網羅性不足でNaNの場合、Noneのまま（例外にならない）"""
        base = str(tmp_path)
        df = self._make_earnings_dates([
            ("2026-11-15", None, None),
            ("2026-08-14", 0.05, 0.06),
        ])
        self._patch(monkeypatch, earnings_dates=df)
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["next_quarter_eps_estimate"] is None
        assert saved["next_quarter_eps_date"] is None

    def test_no_unreported_future_row_yields_none(self, tmp_path, monkeypatch):
        """全行がReported EPS確定済み（次回決算日が未定でyfinance側に
        まだ行が生成されていない）場合もNoneのまま"""
        base = str(tmp_path)
        df = self._make_earnings_dates([("2026-06-11", 5.81, 5.96)])
        self._patch(monkeypatch, earnings_dates=df)
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["next_quarter_eps_estimate"] is None

    def test_earnings_dates_none_yields_none(self, tmp_path, monkeypatch):
        """指数等、.earnings_datesがNoneを返す銘柄でも例外にならずNoneになる"""
        base = str(tmp_path)
        self._patch(monkeypatch, earnings_dates=None)
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["next_quarter_eps_estimate"] is None
        assert saved["next_quarter_eps_date"] is None

    def test_earnings_dates_fetch_failure_does_not_block_other_fields(self, tmp_path, monkeypatch):
        """.earnings_dates呼び出しが例外を送出しても、.info由来の他フィールドの
        保存は妨げられない（calendar取得失敗と同型の独立性）"""
        base = str(tmp_path)
        self._patch(monkeypatch, earnings_dates_raises=True)
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["next_quarter_eps_estimate"] is None
        assert saved["forward_pe"] == 18.0  # .info由来フィールドは正常に保存される

    def test_nearest_unreported_row_picked_when_multiple_future_rows_exist(self, tmp_path, monkeypatch):
        """将来行が複数存在する場合、最も近い日付の行が採用されること
        （sort_index()による最小日付選択の回帰テスト）"""
        base = str(tmp_path)
        df = self._make_earnings_dates([
            ("2027-03-10", 7.00, None),
            ("2026-12-09", 6.33, None),
            ("2026-09-10", 6.09, 6.13),
        ])
        self._patch(monkeypatch, earnings_dates=df)
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["next_quarter_eps_estimate"] == 6.33
        assert saved["next_quarter_eps_date"] == "2026-12-09"

    def test_past_row_with_missing_reported_eps_is_not_mistaken_for_future(self, tmp_path, monkeypatch):
        """ZETA実データで発見: 過去の決算行がYahoo側のデータ欠損で
        Reported EPS=NaNのまま残っているケース（Surprise(%)は算出済み
        だがReported EPS自体が欠落）。Reported EPS=NaNだけを条件にすると
        この4年以上前の過去欠損行を「次回決算」と誤認する
        （実際に2022-05-10のZETA実データで発生した回帰）。日付が現在時刻
        より未来の行のみを対象とすることでこの過去欠損行を除外する。"""
        base = str(tmp_path)
        df = self._make_earnings_dates([
            ("2026-11-03", 0.28, None),      # 真の次回決算（未発表）
            ("2026-08-04", 0.19, 0.27),
            ("2022-05-10", 0.04, None),      # 過去のデータ欠損行（誤認の原因）
            ("2022-02-23", 0.05, 0.11),
        ])
        self._patch(monkeypatch, earnings_dates=df)
        fetcher.fetch_weekly_attributes(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "attributes", "XYZ.json"), encoding="utf-8"))
        assert saved["next_quarter_eps_estimate"] == 0.28
        assert saved["next_quarter_eps_date"] == "2026-11-03"


class TestMergeDailyRecords:
    def test_disjoint_dates_are_all_kept_sorted(self):
        existing = [{"date": "2026-08-10", "close": 100.0}]
        new = [{"date": "2026-08-11", "close": 101.0}, {"date": "2026-08-09", "close": 99.0}]
        merged = fetcher._merge_daily_records(existing, new)
        assert [r["date"] for r in merged] == ["2026-08-09", "2026-08-10", "2026-08-11"]

    def test_overlapping_date_is_overwritten_by_new_value(self):
        existing = [{"date": "2026-08-10", "close": 100.0, "_validation_warnings": ["stale mock"]}]
        new = [{"date": "2026-08-10", "close": 305.49, "_validation_warnings": []}]
        merged = fetcher._merge_daily_records(existing, new)
        assert len(merged) == 1
        assert merged[0]["close"] == 305.49
        assert merged[0]["_validation_warnings"] == []

    def test_empty_existing_returns_new_only(self):
        new = [{"date": "2026-08-10", "close": 100.0}]
        assert fetcher._merge_daily_records([], new) == new

    def test_empty_new_returns_existing_only(self):
        existing = [{"date": "2026-08-10", "close": 100.0}]
        assert fetcher._merge_daily_records(existing, []) == existing

    def test_records_without_date_are_ignored(self):
        existing = [{"date": "2026-08-10", "close": 100.0}]
        new = [{"close": 999.0}]  # date欠落レコードはマージ対象外
        merged = fetcher._merge_daily_records(existing, new)
        assert len(merged) == 1
        assert merged[0]["close"] == 100.0


class TestDownloadHistoricalBarsStartParam:
    """[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8前提作業1
    （2026-08-11）: _download_historical_bars()のstart引数がyf.download()
    のstart=/period=の呼び分けに正しく反映されることを検証する。
    period="5y"は本日基準の相対期間のためhypecore.py切替が要求する
    固定開始日を満たせない（事前調査で実測）ことがこの機能追加の動機。"""

    def _patch_yf_download(self, monkeypatch, captured_kwargs):
        def _fake_download(symbols, **kwargs):
            captured_kwargs.update(kwargs)
            captured_kwargs["symbols"] = symbols
            import pandas as pd
            return pd.DataFrame()  # emptyなので以降の処理はスキップされる
        monkeypatch.setattr(fetcher.yf, "download", _fake_download)

    def test_start_specified_calls_yf_download_with_start_not_period(self, monkeypatch):
        captured = {}
        self._patch_yf_download(monkeypatch, captured)
        fetcher._download_historical_bars(["AAPL"], period="1y", start="2021-01-01")
        assert captured.get("start") == "2021-01-01"
        assert "period" not in captured

    def test_start_omitted_calls_yf_download_with_period(self, monkeypatch):
        captured = {}
        self._patch_yf_download(monkeypatch, captured)
        fetcher._download_historical_bars(["AAPL"], period="1y")
        assert captured.get("period") == "1y"
        assert "start" not in captured

    def test_backfill_daily_prices_passes_start_through(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        captured = {}

        def _fake_download_bars(symbols, period="1y", start=None):
            captured["period"] = period
            captured["start"] = start
            return {}

        monkeypatch.setattr(fetcher, "_download_historical_bars", _fake_download_bars)
        fetcher.backfill_daily_prices(["AAPL"], start="2021-01-01", base_dir=base)
        assert captured["start"] == "2021-01-01"


class TestBackfillDailyPrices:
    """backfill_daily_prices()はネットワークアクセス（_download_historical_bars）
    をmonkeypatchし、マージ・検証・アトミック書き込み・violations_logの
    ロジックのみを検証する。"""

    def _patch_history(self, monkeypatch, bars_by_symbol):
        monkeypatch.setattr(
            fetcher, "_download_historical_bars",
            lambda symbols, period="1y", start=None: bars_by_symbol,
        )

    def _make_bar(self, date, close=100.0, warnings=None):
        return {
            "date": date, "open": close - 0.5, "high": close + 1.0, "low": close - 1.0,
            "close": close, "volume": 1000,
        }

    def test_merges_with_existing_single_day_record(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        # 日次cronが既に取得済みの1日分（モック値）
        fetcher._append_daily_record(
            "AAPL", {"date": "2026-08-10", "close": 303.0, "volume": 5000000, "_validation_warnings": []},
            base_dir=base,
        )
        backfill_bars = {"AAPL": [self._make_bar("2026-08-08", 98.0), self._make_bar("2026-08-10", 305.49)]}
        self._patch_history(monkeypatch, backfill_bars)

        fetcher.backfill_daily_prices(["AAPL"], period="1y", base_dir=base)

        path = os.path.join(base, "daily", "AAPL.json")
        saved = json.load(open(path, encoding="utf-8"))
        dates = [r["date"] for r in saved["records"]]
        assert dates == ["2026-08-08", "2026-08-10"]
        # 既存の2026-08-10（モック値close=303.0）はバックフィル取得値で上書きされる
        aug10 = [r for r in saved["records"] if r["date"] == "2026-08-10"][0]
        assert aug10["close"] == 305.49

    def test_missing_symbol_in_history_is_skipped_gracefully(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        self._patch_history(monkeypatch, {})  # AAPLの取得結果が丸ごと欠落
        fetcher.backfill_daily_prices(["AAPL"], period="1y", base_dir=base)
        path = os.path.join(base, "daily", "AAPL.json")
        assert not os.path.exists(path)

    def test_validation_warnings_are_embedded_per_record(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        # 出来高0で検証失敗させる（終値<=0の足は2026-09-26以降保存しないため、
        # 終値は有効なまま別の項目で警告を出す。MARKETDATA-DAILY-CLOSE-NONE-PERMANENT-1）
        bad_bar = dict(self._make_bar("2026-08-10"), volume=0)
        self._patch_history(monkeypatch, {"AAPL": [bad_bar]})

        fetcher.backfill_daily_prices(["AAPL"], period="1y", base_dir=base)

        path = os.path.join(base, "daily", "AAPL.json")
        saved = json.load(open(path, encoding="utf-8"))
        assert saved["records"][0]["_validation_warnings"] != []

        violations_path = fetcher._violations_log_path("AAPL", base)
        violations = json.load(open(violations_path, encoding="utf-8"))
        assert violations["backfill_price_validation"]["dates_with_warnings"] == ["2026-08-10"]

    def test_backfill_section_does_not_clobber_daily_section(self, tmp_path, monkeypatch):
        """backfillのviolations_log書き込みが、既存のdaily_price_validation
        セクション（直近の日次チェック）を上書きしないことを確認する"""
        base = str(tmp_path)
        fetcher._write_violations_section(
            "AAPL", "daily_price_validation",
            {"checked_at": "t1", "date": "2026-08-10", "warnings": []},
            base_dir=base,
        )
        self._patch_history(monkeypatch, {"AAPL": [self._make_bar("2026-08-09")]})
        fetcher.backfill_daily_prices(["AAPL"], period="1y", base_dir=base)

        violations_path = fetcher._violations_log_path("AAPL", base)
        violations = json.load(open(violations_path, encoding="utf-8"))
        assert violations["daily_price_validation"]["date"] == "2026-08-10"
        assert "backfill_price_validation" in violations

    def test_no_symbols_is_a_noop(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        called = {"count": 0}

        def _fail_if_called(symbols, period="1y"):
            called["count"] += 1
            return {}

        monkeypatch.setattr(fetcher, "_download_historical_bars", _fail_if_called)
        fetcher.backfill_daily_prices([], period="1y", base_dir=base)
        assert called["count"] == 0


class TestFetchAnalystEventsSchema:
    """[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8前提作業3
    （analyst_history/スキーマ拡張、earnings_history・
    recommendations_history追加）の回帰テスト。upgrades_downgrades・
    earnings_history・recommendationsをmonkeypatchし、ネットワーク
    アクセスなしで抽出・重複排除ロジックを検証する。"""

    _UPGRADES_DOWNGRADES_DF = pd.DataFrame(
        {
            "Firm": ["UBS"], "ToGrade": ["Buy"], "FromGrade": ["Hold"], "Action": ["up"],
            "priceTargetAction": ["Raises"], "currentPriceTarget": [220.0], "priorPriceTarget": [200.0],
        },
        index=pd.to_datetime(["2026-08-01"]),
    )

    _EARNINGS_HISTORY_DF = pd.DataFrame(
        {
            "epsActual": [2.02], "epsEstimate": [1.89243],
            "epsDifference": [0.13], "surprisePercent": [0.0674],
        },
        index=pd.to_datetime(["2026-06-30"]),
    )

    _RECOMMENDATIONS_DF = pd.DataFrame({
        "period": ["0m", "-1m", "-2m", "-3m"],
        "strongBuy": [6, 6, 6, 7],
        "buy": [21, 22, 22, 23],
        "hold": [15, 14, 16, 15],
        "sell": [2, 2, 1, 1],
        "strongSell": [2, 2, 2, 2],
    })

    def _patch_ticker(self, monkeypatch, upgrades_downgrades=None, earnings_history=None,
                       recommendations=None, earnings_history_raises=False):
        class _FakeTicker:
            def __init__(self, symbol):
                pass
            @property
            def upgrades_downgrades(self):
                return upgrades_downgrades
            @property
            def earnings_history(self):
                if earnings_history_raises:
                    raise RuntimeError("simulated earnings_history failure")
                return earnings_history
            @property
            def recommendations(self):
                return recommendations
        monkeypatch.setattr(fetcher.yf, "Ticker", _FakeTicker)

    def test_earnings_history_extracted_with_correct_mapping(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        self._patch_ticker(monkeypatch, earnings_history=self._EARNINGS_HISTORY_DF.copy())
        fetcher.fetch_analyst_events(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "analyst_history", "XYZ.json"), encoding="utf-8"))
        assert len(saved["earnings_history"]) == 1
        entry = saved["earnings_history"][0]
        assert entry["quarter"] == "2026-06-30"
        assert entry["eps_actual"] == 2.02
        assert entry["eps_estimate"] == 1.89243
        assert entry["eps_difference"] == 0.13
        assert entry["surprise_percent"] == 0.0674

    def test_surprise_percent_stored_as_raw_decimal_not_percent_scaled(self, tmp_path, monkeypatch):
        """surprise_percentはYahoo生値（小数）のまま保存し、hypecore.py側の
        *100変換のような百分率化は行わない（生値保存・変換は消費側の方針）"""
        base = str(tmp_path)
        self._patch_ticker(monkeypatch, earnings_history=self._EARNINGS_HISTORY_DF.copy())
        fetcher.fetch_analyst_events(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "analyst_history", "XYZ.json"), encoding="utf-8"))
        assert saved["earnings_history"][0]["surprise_percent"] == 0.0674
        assert saved["earnings_history"][0]["surprise_percent"] != 6.74

    def test_recommendations_history_uses_0m_row_and_computes_buy_hold_ratio(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        self._patch_ticker(monkeypatch, recommendations=self._RECOMMENDATIONS_DF.copy())
        fetcher.fetch_analyst_events(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "analyst_history", "XYZ.json"), encoding="utf-8"))
        assert len(saved["recommendations_history"]) == 1
        entry = saved["recommendations_history"][0]
        assert entry["strong_buy"] == 6
        assert entry["buy"] == 21
        assert entry["hold"] == 15
        assert entry["sell"] == 2
        assert entry["strong_sell"] == 2
        # buy_hold_ratio = (6+21)/(6+21+15+2+2) = 27/46
        assert entry["buy_hold_ratio"] == round(27 / 46, 4)

    def test_recommendations_falls_back_to_first_row_when_0m_absent(self, tmp_path, monkeypatch):
        """"0m"行が存在しない場合（KULR等、履歴の浅い銘柄）は先頭行を使う
        （hypecore.py::fetch_analyst_history()と同じフォールバック）"""
        base = str(tmp_path)
        single_row_df = pd.DataFrame({
            "period": ["0m"], "strongBuy": [0], "buy": [1], "hold": [0], "sell": [0], "strongSell": [0],
        })
        self._patch_ticker(monkeypatch, recommendations=single_row_df)
        fetcher.fetch_analyst_events(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "analyst_history", "XYZ.json"), encoding="utf-8"))
        entry = saved["recommendations_history"][0]
        assert entry["buy"] == 1
        assert entry["buy_hold_ratio"] == 1.0

    def test_quarter_dedup_overwrites_same_quarter_on_refetch(self, tmp_path, monkeypatch):
        """同一四半期を再取得した場合は上書きされ、重複エントリにならない"""
        base = str(tmp_path)
        self._patch_ticker(monkeypatch, earnings_history=self._EARNINGS_HISTORY_DF.copy())
        fetcher.fetch_analyst_events(["XYZ"], base_dir=base)
        fetcher.fetch_analyst_events(["XYZ"], base_dir=base)  # 再実行（同一四半期）

        saved = json.load(open(os.path.join(base, "analyst_history", "XYZ.json"), encoding="utf-8"))
        assert len(saved["earnings_history"]) == 1

    def test_new_quarter_accumulates_alongside_existing(self, tmp_path, monkeypatch):
        """新しい四半期のデータは既存の四半期データを消さずに蓄積される
        （upgrades_downgradesと同型のread-modify-writeマージ）"""
        base = str(tmp_path)
        path = os.path.join(base, "analyst_history", "XYZ.json")
        fetcher._atomic_write_json(path, {
            "symbol": "XYZ", "events": [], "recommendations_history": [],
            "earnings_history": [
                {"quarter": "2026-03-31", "eps_actual": 2.01, "eps_estimate": 1.94275,
                 "eps_difference": 0.07, "surprise_percent": 0.0346},
            ],
        })
        self._patch_ticker(monkeypatch, earnings_history=self._EARNINGS_HISTORY_DF.copy())  # 2026-06-30
        fetcher.fetch_analyst_events(["XYZ"], base_dir=base)

        saved = json.load(open(path, encoding="utf-8"))
        quarters = sorted(e["quarter"] for e in saved["earnings_history"])
        assert quarters == ["2026-03-31", "2026-06-30"]

    def test_recommendations_same_day_dedup_overwrites(self, tmp_path, monkeypatch):
        """同一日に複数回実行しても重複エントリにならない（手動再実行時の安全策）"""
        base = str(tmp_path)
        self._patch_ticker(monkeypatch, recommendations=self._RECOMMENDATIONS_DF.copy())
        fetcher.fetch_analyst_events(["XYZ"], base_dir=base)
        fetcher.fetch_analyst_events(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "analyst_history", "XYZ.json"), encoding="utf-8"))
        assert len(saved["recommendations_history"]) == 1

    def test_recommendations_accumulates_across_different_dates(self, tmp_path, monkeypatch):
        """異なる日付のスナップショットは蓄積される（週次実行を想定した
        シミュレーション: 過去の週のスナップショットが残ったまま、今週分が
        新規追加される）"""
        base = str(tmp_path)
        path = os.path.join(base, "analyst_history", "XYZ.json")
        fetcher._atomic_write_json(path, {
            "symbol": "XYZ", "events": [], "earnings_history": [],
            "recommendations_history": [
                {"date": "2026-08-04", "strong_buy": 5, "buy": 20, "hold": 16,
                 "sell": 2, "strong_sell": 2, "buy_hold_ratio": 0.5556},
            ],
        })
        self._patch_ticker(monkeypatch, recommendations=self._RECOMMENDATIONS_DF.copy())
        fetcher.fetch_analyst_events(["XYZ"], base_dir=base)

        saved = json.load(open(path, encoding="utf-8"))
        dates = sorted(r["date"] for r in saved["recommendations_history"])
        assert len(dates) == 2
        assert "2026-08-04" in dates

    def test_earnings_history_failure_does_not_block_other_sources(self, tmp_path, monkeypatch):
        """earnings_history取得が例外を送出しても、events・
        recommendations_historyの保存は妨げられない（3系統の独立性）"""
        base = str(tmp_path)
        self._patch_ticker(
            monkeypatch,
            upgrades_downgrades=self._UPGRADES_DOWNGRADES_DF.copy(),
            recommendations=self._RECOMMENDATIONS_DF.copy(),
            earnings_history_raises=True,
        )
        fetcher.fetch_analyst_events(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "analyst_history", "XYZ.json"), encoding="utf-8"))
        assert len(saved["events"]) == 1
        assert len(saved["recommendations_history"]) == 1
        assert saved["earnings_history"] == []

    def test_all_sources_empty_writes_empty_arrays_gracefully(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        self._patch_ticker(monkeypatch)
        fetcher.fetch_analyst_events(["XYZ"], base_dir=base)

        saved = json.load(open(os.path.join(base, "analyst_history", "XYZ.json"), encoding="utf-8"))
        assert saved["events"] == []
        assert saved["earnings_history"] == []
        assert saved["recommendations_history"] == []
