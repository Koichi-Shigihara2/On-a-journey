"""[[MARKET-CALENDAR-UNIVERSE-UNIFY-1]]（2026-10-23）: common/market_data/calendar_universe.py。
NYSEの取引日と連邦の祝日は別の概念として別の関数で持つ。nyse_holidays.json・sp500_tickers.json の書き手もここ。"""
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.market_data import calendar_universe as cu  # noqa: E402


def test_nyse_and_federal_holidays_are_different_concepts():
    # コロンブス・デー（連邦の祝日、NYSEは開く）・聖金曜日とジューンティーンス（NYSEは休場、us_federal_holidays には無い）
    assert date(2026, 10, 12) in cu.us_federal_holidays(2026) and cu.is_nyse_trading_day("2026-10-12")
    assert date(2026, 4, 3) not in cu.us_federal_holidays(2026) and not cu.is_nyse_trading_day("2026-04-03")
    assert date(2026, 6, 19) not in cu.us_federal_holidays(2026) and "2026-06-19" in cu.nyse_holidays("2026-01-01", "2026-12-31")
    # 連邦の営業日の加算はコロンブス・デーを飛ばす（S&P500 の t+N は今はこの数え方。FINDINGS 2026-10-23）
    assert cu.us_federal_business_days_add(date(2026, 10, 9), 1) == date(2026, 10, 13)
    assert cu.previous_us_federal_business_day(date(2026, 10, 12)) == date(2026, 10, 9)


def test_nyse_trading_days_ending_and_latest_closed_day():
    assert cu.nyse_trading_days_ending("2026-10-12", 3) == ["2026-10-08", "2026-10-09", "2026-10-12"]
    assert cu.nyse_trading_days_ending(date(2026, 10, 11), 1) == ["2026-10-09"]   # 日曜はその前の取引日まで
    assert cu.nyse_trading_days_ending("2026-10-12", 0) == []
    # 引けの前は前の取引日、引けの後はその日
    assert cu.nyse_latest_closed_day(datetime(2026, 10, 12, 19, 0, tzinfo=timezone.utc)) == "2026-10-09"
    assert cu.nyse_latest_closed_day(datetime(2026, 10, 12, 20, 5, tzinfo=timezone.utc)) == "2026-10-12"


def test_write_nyse_holidays(tmp_path):
    path = str(tmp_path / "nyse_holidays.json")
    hol = cu.write_nyse_holidays(path, now_utc=datetime(2026, 6, 1, tzinfo=timezone.utc))
    d = json.load(open(path, encoding="utf-8"))
    assert d["from"] == "2025-01-01" and d["to"] == "2027-12-31" and d["holidays"] == hol
    assert "2026-11-26" in hol and "2026-10-12" not in hol and d["source"] == "pandas_market_calendars XNYS"


def test_write_sp500_copy_copies_the_cache(tmp_path):
    fetched = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    (tmp_path / cu.SP500_CACHE_FILENAME).write_text(json.dumps({"fetched_at": fetched, "count": 2, "tickers": ["AAA", "BBB"]}),
                                                    encoding="utf-8")
    out = str(tmp_path / "copy" / "sp500_tickers.json")
    assert cu.write_sp500_copy(out, base_dir=str(tmp_path)) == ["AAA", "BBB"]
    assert json.load(open(out, encoding="utf-8")) == {"fetched_at": fetched, "count": 2, "tickers": ["AAA", "BBB"]}
