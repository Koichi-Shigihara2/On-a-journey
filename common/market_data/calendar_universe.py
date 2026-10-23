"""
common/market_data/calendar_universe.py

市場の暦と銘柄の集合の置き場（[[MARKET-CALENDAR-UNIVERSE-UNIFY-1]]、2026-10-23）。
取引日・休場日・日足の確定時刻（暫定の足）・連邦の祝日・S&P500の構成銘柄を、意味ごとの関数でここだけに置く。
読み手（市場データ層・TANUKI・Market Pulse・MACRO PULSE・rcc）はここから呼び、pandas_market_calendars を直接呼ばない。

意味の違うものは別の関数にする（同じにしない）:
  - NYSEの取引日・休場日・取引時間: pandas_market_calendars の XNYS（2000〜2027年で NYSE の暦と取引日・時刻とも同じ）
  - 銘柄ごとの取引日: ^N225 は JPX、先物は CME_TradeDate（清算のある取引日）、JPY=X は土日を除く日
  - 連邦の祝日（経済指標の公表の予定用。MACRO PULSE）: コロンブス・デー・退役軍人の日を含み、聖金曜日・ジューンティーンスを含まない。
    NYSEの休場日ではない（規則は 2026-10-23 までの 05_main.py の us_holidays のまま）
  - 日足の確定時刻と暫定の足の判定（市場データ層の fetcher.py の決まりのまま）
  - S&P500の構成銘柄（common/market_data/_sp500_constituents_cache.json を正。Wikipedia→GitHub CSV、7日の控え）
"""

import calendar as _stdlib_calendar
import functools
import io
import json
import os
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal
import requests

MARKET_DATA_DIR = os.path.dirname(os.path.abspath(__file__))

# ── NYSE ──────────────────────────────────────────────────────────
NYSE_CALENDAR_NAME = "XNYS"
_CALENDARS: Dict[str, Any] = {}
_TRADING_DAYS_CACHE: Dict[str, List[str]] = {}
_NYSE_CLOSE_CACHE: Dict[str, Optional[datetime]] = {}


def exchange_calendar(name: str = NYSE_CALENDAR_NAME):
    """取引所の暦（pandas_market_calendars、プロセス内でキャッシュ）。"NYSE" は XNYS と同じに扱う。"""
    name = NYSE_CALENDAR_NAME if name == "NYSE" else name
    if name not in _CALENDARS:
        _CALENDARS[name] = mcal.get_calendar(name)
    return _CALENDARS[name]


def nyse_calendar():
    return exchange_calendar(NYSE_CALENDAR_NAME)


def exchange_schedule(name: str, start: Any, end: Any) -> pd.DataFrame:
    """取引所の取引時間の表（行=取引日、market_open・market_close は UTC）。"""
    return exchange_calendar(name).schedule(start_date=_iso(start), end_date=_iso(end))


def exchange_trading_days(name: str) -> List[str]:
    """取引所の暦の2000-01-01〜本日+10日の取引日（'YYYY-MM-DD'、昇順。プロセス内でキャッシュ）。"""
    key = NYSE_CALENDAR_NAME if name == "NYSE" else name
    if key not in _TRADING_DAYS_CACHE:
        end = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
        _TRADING_DAYS_CACHE[key] = [d.strftime("%Y-%m-%d")
                                    for d in exchange_calendar(key).valid_days(start_date="2000-01-01", end_date=end)]
    return _TRADING_DAYS_CACHE[key]


def nyse_trading_days(start: Any, end: Any) -> List[str]:
    """start〜end（両端を含む）のNYSEの取引日（'YYYY-MM-DD'、昇順）。"""
    return [d.strftime("%Y-%m-%d") for d in nyse_calendar().valid_days(start_date=_iso(start), end_date=_iso(end))]


def is_nyse_trading_day(day: Any) -> bool:
    """その日がNYSEの取引日か。day は date/datetime または 'YYYY-MM-DD'。"""
    return len(nyse_trading_days(day, day)) > 0


def nyse_trading_days_ending(anchor: Any, n: int) -> List[str]:
    """anchor（その日を含む）までの直近n取引日（'YYYY-MM-DD'、古い順）。n<=0 なら空。"""
    if n <= 0:
        return []
    return list(_trading_days_ending(_as_date(anchor), n))


@functools.lru_cache(maxsize=256)
def _trading_days_ending(anchor: date, n: int) -> tuple:
    return tuple(nyse_trading_days(anchor - timedelta(days=n * 3 + 15), anchor)[-n:])


def nyse_close_utc(day: str) -> Optional[datetime]:
    """その日のNYSEの引けの時刻（UTC、短縮取引日は早い）。取引日でなければ None。"""
    if day not in _NYSE_CLOSE_CACHE:
        s = exchange_schedule(NYSE_CALENDAR_NAME, day, day)
        _NYSE_CLOSE_CACHE[day] = s["market_close"].iloc[0].to_pydatetime() if len(s) else None
    return _NYSE_CLOSE_CACHE[day]


def nyse_latest_closed_day(now_utc: datetime, lookback_days: int = 10) -> Optional[str]:
    """now_utc の時点で引けを過ぎた最新のNYSEの取引日（lookback_days 日まで遡る）。無ければ None。"""
    s = exchange_schedule(NYSE_CALENDAR_NAME, (now_utc - timedelta(days=lookback_days)).date(), now_utc.date())
    closed = s[s["market_close"] <= now_utc]
    return closed.index[-1].strftime("%Y-%m-%d") if len(closed) else None


def nyse_holidays(start: Any, end: Any) -> List[str]:
    """start〜end の土日以外のNYSEの休場日（'YYYY-MM-DD'、昇順）。"""
    valid = set(nyse_trading_days(start, end))
    return [d.strftime("%Y-%m-%d") for d in pd.bdate_range(_iso(start), _iso(end)) if d.strftime("%Y-%m-%d") not in valid]


def write_nyse_holidays(path: str, now_utc: Optional[datetime] = None) -> Optional[List[str]]:
    """前年〜翌年のNYSEの休場日（土日以外）を path に書く（Market Pulse の docs/.../data/nyse_holidays.json。画面が休場の行に使う）。
    失敗しても例外を外に出さない（毎晩の実行を止めない）。"""
    now_utc = now_utc or datetime.now(timezone.utc)
    try:
        y = now_utc.year
        start, end = f"{y - 1}-01-01", f"{y + 1}-12-31"
        hol = nyse_holidays(start, end)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"source": f"pandas_market_calendars {NYSE_CALENDAR_NAME}", "from": start, "to": end, "holidays": hol},
                      f, ensure_ascii=False, indent=1)
        return hol
    except Exception as e:
        print(f"[WARN] NYSE休場日の書き出しに失敗: {e}")
        return None


# ── 銘柄ごとの取引日 ───────────────────────────────────────────────
FUTURES_SYMBOLS = {"CL=F", "GC=F", "ES=F", "NQ=F", "NIY=F"}
# 取引日の情報源（推測で作らない）: 先物はCME_TradeDate（清算のある取引日。CMEGlobex_*はLabor Dayの短縮取引を
# 取引日に含むが、清算は無くYahooの日足も無い）、^N225はJPX。JPY=Xは土日を除く
_SYMBOL_CALENDARS = {"^N225": "JPX", **{s: "CME_TradeDate" for s in FUTURES_SYMBOLS}}
_WEEKDAY_ONLY_SYMBOLS = {"JPY=X"}
_TRADING_DAY_SETS: Dict[str, set] = {}


def is_symbol_trading_day(symbol: str, day: str) -> bool:
    """その日がその銘柄の取引日か。対象はJPY=X・先物・^N225で、それ以外の銘柄は判定しない（True）。"""
    if symbol in _WEEKDAY_ONLY_SYMBOLS:
        return datetime.strptime(day, "%Y-%m-%d").weekday() < 5
    name = _SYMBOL_CALENDARS.get(symbol)
    if name is None:
        return True
    if name not in _TRADING_DAY_SETS:
        _TRADING_DAY_SETS[name] = set(exchange_trading_days(name))
    return day in _TRADING_DAY_SETS[name]


# ── 日足の確定時刻と暫定の足 ─────────────────────────────────────────
# [[MARKETDATA-DAILY-PROVISIONAL-ROWS-1]]: Yahooの日足の確定時刻（history_metadataのexchangeTimezoneName・currentTradingPeriodで確認）:
#   - 米国株・ETF・米国指数: NYSEの引け（短縮取引日を含む）。^VIXは15:15 CT（引け＋15分）、^TNXは14:00 CT（引け−1時間）
#   - ^N225: 15:30 JST。ただしYahooは引け後も翌朝まで終値の無い足（出来高0）を返し、取引時間中の足も出来高0のため、
#     出来高が0の足は確定とみなさない
#   - JPY=X: 翌日0:00 Europe/London（Yahooの為替の日足はロンドンの暦日）
#   - 先物: 翌日0:00 America/New_York（Yahooの先物の日足はニューヨークの暦日。確定した終値は清算値）
_TZ_NY, _TZ_LDN, _TZ_TYO = ZoneInfo("America/New_York"), ZoneInfo("Europe/London"), ZoneInfo("Asia/Tokyo")
# 先物の清算値がYahooの日足に入るまでの遅れ。実測（2026-09-29〜30）: 09-28の足は09-29 01:06 UTCではまだ取引中の値、
# 09-29 22:26 UTCには清算値。09-29の足は09-30 00:15 UTCでもまだ取引中の値。確定の判定は安全側に、NYの翌日0時から24時間後とする
FUTURES_FINAL_DELAY = timedelta(hours=24)


def bar_final_at(symbol: str, day: str) -> Optional[datetime]:
    """Yahooの日足（symbol・day）が確定する時刻（UTC）。取引日でない場合はNone。"""
    d = datetime.strptime(day, "%Y-%m-%d").date()
    if symbol == "JPY=X":
        return datetime.combine(d + timedelta(days=1), dtime(0, 0), _TZ_LDN).astimezone(timezone.utc)
    if symbol in FUTURES_SYMBOLS:
        return datetime.combine(d + timedelta(days=1), dtime(0, 0), _TZ_NY).astimezone(timezone.utc) + FUTURES_FINAL_DELAY
    if symbol == "DX-Y.NYB":
        # ドル指数（ICEの先物に基づく指数）: Yahooの日足はニューヨークの暦日（実装B、指示書㉖）
        return datetime.combine(d + timedelta(days=1), dtime(0, 0), _TZ_NY).astimezone(timezone.utc)
    if symbol == "^N225":
        return datetime.combine(d, dtime(15, 30), _TZ_TYO).astimezone(timezone.utc)
    close = nyse_close_utc(day)
    if close is None:
        return None
    if symbol == "^VIX":
        return close + timedelta(minutes=15)
    if symbol == "^TNX":
        return close - timedelta(hours=1)
    return close


def is_flat_futures_bar(symbol: str, bar: Dict[str, Any]) -> bool:
    """先物のO=H=L=Cかつ出来高0の足（2026-10-11、Koichi判断）。暫定の行を確定にする根拠として扱わない。
    Yahooの直近2年の日足ではNIY=Fだけに出る（501本中279本）。取引中の値（出来高つき）で保存した行が、後でこの足に変わる
    （NIY=Fの09-30: 66995・出来高5,911 → 66810・出来高0）。出来高が常に0の銘柄（JPY=X・DX-Y.NYB・^TNX等）は対象外。"""
    if symbol not in FUTURES_SYMBOLS or bar.get("volume"):
        return False
    o, h, l, c = (bar.get(k) for k in ("open", "high", "low", "close"))
    return c is not None and o == h == l == c


def is_provisional_bar(symbol: str, bar: Dict[str, Any], now: Optional[datetime] = None) -> bool:
    """確定前の日足か（確定時刻より前に取得した足、^N225の出来高0の足、先物の平らな足）。"""
    now = now or datetime.now(timezone.utc)
    if symbol == "^N225" and not bar.get("volume"):
        return True
    if is_flat_futures_bar(symbol, bar):
        return True
    final = bar_final_at(symbol, bar["date"])
    return final is not None and now < final


def expected_provisional_kind(symbol: str, day: str) -> Optional[str]:
    """夜の取得の時点で、構造上まだ確定していない日足か（設計上の暫定）。その種類を返す。

    夜の取得はNYSEの引け＋CLOSE_WAIT（common/market_data/daily_guard.py）以降に行う。日足の確定時刻（bar_final_at）が
    それより後の銘柄は、夜の取得で保存した当日の行が必ず暫定になる（先物の清算値・為替やドル指数の日の区切り）。
      - "清算前": 先物（FUTURES_SYMBOLS。確定値は清算値）
      - "日中": それ以外（JPY=X〈ロンドンの暦日〉・DX-Y.NYB〈ニューヨークの暦日〉など、日の区切りがNYSEの引けより後）
      - None: 夜の取得の時点で確定しているはずの足（米国株・ETF・指数・^N225等）。これが暫定なら想定外
    NYSEの休場日の足（為替等）は、その日の16:00（ニューヨーク時間）を引けとみなす。
    """
    from common.market_data.daily_guard import CLOSE_WAIT
    final = bar_final_at(symbol, day)
    if final is None:
        return None
    close = nyse_close_utc(day)
    if close is None:
        d = datetime.strptime(day, "%Y-%m-%d").date()
        close = datetime.combine(d, dtime(16, 0), _TZ_NY).astimezone(timezone.utc)
    if final <= close + CLOSE_WAIT:
        return None
    return "清算前" if symbol in FUTURES_SYMBOLS else "日中"


def record_provisional_kind(symbol: str, record: Dict[str, Any]):
    """daily/の行が暫定（`_provisional`）なら、設計上の暫定の種類（"清算前"・"日中"）か、想定外の暫定（None）を返す。
    暫定でなければ False。判定に失敗しても例外を外に出さない（None）。"""
    if not record.get("_provisional"):
        return False
    try:
        return expected_provisional_kind(symbol, record["date"])
    except Exception as e:
        print(f"[WARN] 暫定の種類の判定失敗（{symbol}）: {e}")
        return None


def provisional_kind_at(symbol: str, bar_time: datetime) -> Optional[str]:
    """取引中の足（bar_time）が属する日足がまだ確定していなければ、その種類。判定に失敗しても例外を外に出さない（None）。"""
    try:
        return expected_provisional_kind(symbol, bar_time.astimezone(timezone.utc).strftime("%Y-%m-%d"))
    except Exception as e:
        print(f"[WARN] 暫定の種類の判定失敗（{symbol}）: {e}")
        return None


# ── 連邦の祝日（経済指標の公表の予定用。NYSEの休場日ではない） ────────────────
def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(weekday - first.weekday()) % 7 + (n - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    last = date(year, month, _stdlib_calendar.monthrange(year, month)[1])
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _observed(d: date) -> date:
    return d - timedelta(days=1) if d.weekday() == 5 else d + timedelta(days=1) if d.weekday() == 6 else d


def us_federal_holidays(year: int) -> set:
    """その年の連邦の祝日（振替を含む）。元日・キング牧師の日・大統領の日・戦没将兵追悼記念日・独立記念日・レイバー・デー・
    コロンブス・デー・退役軍人の日・感謝祭・クリスマス。ジューンティーンスは含まない。元日が土曜の年はその年の12-31を入れる
    （2026-10-23 までの MACRO PULSE の us_holidays の規則のまま。経済指標の公表の予定に使う）。"""
    ny = date(year, 1, 1)
    return {
        date(year, 12, 31) if ny.weekday() == 5 else date(year, 1, 2) if ny.weekday() == 6 else ny,
        _nth_weekday(year, 1, 0, 3), _nth_weekday(year, 2, 0, 3), _last_weekday(year, 5, 0),
        _observed(date(year, 7, 4)), _nth_weekday(year, 9, 0, 1), _nth_weekday(year, 10, 0, 2),
        _observed(date(year, 11, 11)), _nth_weekday(year, 11, 3, 4), _observed(date(year, 12, 25)),
    }


def us_federal_business_days_add(start: date, n: int) -> date:
    """start の n 営業日後（土日・連邦の祝日を除く）。"""
    holidays = us_federal_holidays(start.year) | us_federal_holidays(start.year + 1)
    count = 0
    d = start + timedelta(days=1)
    while True:
        if d.weekday() < 5 and d not in holidays:
            count += 1
            if count == n:
                return d
        d += timedelta(days=1)


def previous_us_federal_business_day(d: date) -> date:
    """d が土日・連邦の祝日なら、それより前の最も近い営業日（d が営業日ならそのまま）。"""
    while d.weekday() >= 5 or d in us_federal_holidays(d.year):
        d = d - timedelta(days=1)
    return d


# ── S&P500の構成銘柄 ────────────────────────────────────────────────
SP500_CACHE_FILENAME = "_sp500_constituents_cache.json"


def sp500_constituents(base_dir: Optional[str] = None, max_age_days: int = 7) -> List[str]:
    """S&P500構成銘柄リスト（Wikipedia→GitHub CSVフォールバック、{max_age_days}日の控え
    common/market_data/_sp500_constituents_cache.json を正とする）。両方失敗なら期限切れの控え、それも無ければ空のリスト。"""
    from common.market_data.fetcher import _atomic_write_json, _now_iso
    cache_path = os.path.join(base_dir if base_dir is not None else MARKET_DATA_DIR, SP500_CACHE_FILENAME)

    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                cache = json.load(f)
            fetched_at = datetime.fromisoformat(cache["fetched_at"])
            if fetched_at.tzinfo is None:
                fetched_at = fetched_at.replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc) - fetched_at).days < max_age_days and cache.get("tickers"):
                return cache["tickers"]
        except Exception:
            pass

    tickers: Optional[List[str]] = None
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; MarketDataFetcher/1.0)"}
        resp = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=headers, timeout=30)
        resp.raise_for_status()
        df = pd.read_html(io.StringIO(resp.text))[0]
        tickers = [t.strip() for t in df["Symbol"].str.replace(".", "-", regex=False).tolist() if t.strip()]
    except Exception as e:
        print(f"   [WARN] S&P500構成銘柄のWikipedia取得失敗: {e}")

    if not tickers:
        try:
            resp = requests.get("https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv",
                                timeout=30)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            tickers = [t.strip() for t in df["Symbol"].str.replace(".", "-", regex=False).tolist() if t.strip()]
        except Exception as e:
            print(f"   [WARN] S&P500構成銘柄のGitHub CSV取得失敗: {e}")

    if tickers and len(tickers) >= 400:
        _atomic_write_json(cache_path, {"fetched_at": _now_iso(), "count": len(tickers), "tickers": tickers})
        return tickers

    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return json.load(f).get("tickers", [])
        except Exception:
            pass
    return []


def write_sp500_copy(path: str, base_dir: Optional[str] = None) -> List[str]:
    """S&P500構成銘柄の控え（正）の写しを path に書く（Market Pulse の docs/.../data/sp500_tickers.json。
    browser_checks/market_pulse_elements.py が読む）。書いた銘柄のリストを返す（空なら書かない）。"""
    tickers = sp500_constituents(base_dir=base_dir)
    if not tickers:
        return []
    cache_path = os.path.join(base_dir if base_dir is not None else MARKET_DATA_DIR, SP500_CACHE_FILENAME)
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            fetched_at = json.load(f).get("fetched_at")
    except Exception:
        fetched_at = None
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"fetched_at": fetched_at, "count": len(tickers), "tickers": tickers}, f, ensure_ascii=False, indent=2)
    return tickers


# ── 内部 ─────────────────────────────────────────────────────────
def _iso(day: Any) -> str:
    return day.isoformat()[:10] if hasattr(day, "isoformat") else str(day)[:10]


def _as_date(day: Any) -> date:
    if isinstance(day, datetime):
        return day.date()
    if isinstance(day, date):
        return day
    return datetime.strptime(str(day)[:10], "%Y-%m-%d").date()
