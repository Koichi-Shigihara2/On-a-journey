"""Market Pulse 実装C（指示書㉗、2026-09-30）: 段階8の予定（今後7日）。

設計書 MARKET_PULSE_REDESIGN.md 4章の取得元のとおり。
- 経済指標: FRED API `releases/dates`（重要指標だけ。公表時刻はFREDに無いため日付のみ表示し、推測で時刻を付けない）
- FOMC・FRB要人: federalreserve.govの`/json/calendar.json`（FOMC・Speeches・Testimony・Beige。時刻は米国東部時間）
- 決算: 保有銘柄・TANUKI TAILの監視銘柄・M7に限る（TANUKI VALUATIONのlatest.jsonのnext_earnings_date、
  無い銘柄はcommon/market_data/attributesのcalendar）
- 米国の祝日・短縮取引: pandas_market_calendars（NYSE）。日本の祝日: 同（JPX）
- 満期: 米国の月次オプション（第3金曜、休場なら前営業日。3・6・9・12月は先物・オプションの同時満期）、
  日本のSQ（第2金曜、休場なら前営業日。3・6・9・12月はメジャーSQ）
取得に失敗した取得元は推測で埋めず、failedに記録する。
"""
from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
JST = ZoneInfo("Asia/Tokyo")
WINDOW_DAYS = 7
# FRED release_id → 表示名（重要指標だけ）
ECON_RELEASES = {50: "雇用統計", 10: "消費者物価指数（CPI）", 46: "生産者物価指数（PPI）", 53: "GDP",
                 54: "個人所得・個人消費支出（PCE）", 9: "小売売上高", 192: "JOLTS（求人・離職）", 194: "ADP雇用統計",
                 180: "新規失業保険申請件数", 91: "ミシガン大学消費者信頼感", 95: "製造業受注（耐久財を含む）",
                 13: "鉱工業生産・設備稼働率"}
FED_TYPES = {"FOMC": "FOMC", "Speeches": "FRB要人の講演", "Testimony": "FRB要人の議会証言", "Beige": "ベージュブック"}


def _et_to_jst(d: date, hhmm: Optional[time]) -> Optional[str]:
    if hhmm is None:
        return None
    return datetime.combine(d, hhmm, ET).astimezone(JST).strftime("%Y-%m-%d %H:%M")


def parse_fed_time(s: str) -> Optional[time]:
    """"3:30 p.m." → 15:30、"10:00 a.m." → 10:00。読めなければNone。"""
    m = re.match(r"\s*(\d{1,2}):(\d{2})\s*([ap])\.?\s*m\.?", s or "", re.I)
    if not m:
        return None
    h, mi, ap = int(m.group(1)), int(m.group(2)), m.group(3).lower()
    if ap == "p" and h != 12:
        h += 12
    if ap == "a" and h == 12:
        h = 0
    return time(h, mi)


def _http_json(url: str, params: Optional[dict] = None) -> Any:
    import requests
    r = requests.get(url, params=params, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return json.loads(r.content.decode("utf-8-sig"))


def econ_events(start: date, end: date, get_json: Callable = _http_json, api_key: Optional[str] = None) -> List[dict]:
    key = api_key if api_key is not None else os.getenv("FRED_API_KEY")
    if not key:
        raise RuntimeError("FRED_API_KEYが無い")
    j = get_json("https://api.stlouisfed.org/fred/releases/dates",
                 {"api_key": key, "file_type": "json", "realtime_start": start.isoformat(), "realtime_end": end.isoformat(),
                  "include_release_dates_with_no_data": "true", "limit": 1000, "sort_order": "asc"})
    out = []
    for x in j.get("release_dates", []):
        rid = x.get("release_id")
        if rid in ECON_RELEASES and start.isoformat() <= x["date"] <= end.isoformat():
            out.append({"date_et": x["date"], "time_et": None, "time_jst": None, "kind": "経済指標",
                        "title": ECON_RELEASES[rid], "detail": x.get("release_name"), "source": "FRED"})
    return out


def fed_events(start: date, end: date, get_json: Callable = _http_json) -> List[dict]:
    j = get_json("https://www.federalreserve.gov/json/calendar.json")
    out = []
    for e in j.get("events", []):
        typ = e.get("type")
        if typ not in FED_TYPES:
            continue
        try:
            y, mo = map(int, e.get("month", "").split("-"))
            days = [int(x) for x in re.findall(r"\d+", e.get("days") or "")]
        except ValueError:
            continue
        if not days:
            continue
        d = date(y, mo, days[-1])   # 複数日の会合は最終日（声明・会見の日）
        if not (start <= d <= end):
            continue
        tm = parse_fed_time(e.get("time") or "")
        out.append({"date_et": d.isoformat(), "time_et": tm.strftime("%H:%M") if tm else None, "time_jst": _et_to_jst(d, tm),
                    "kind": FED_TYPES[typ], "title": re.sub(r"\s+", " ", e.get("title") or "").strip(),
                    "detail": re.sub(r"&[#\w]+;", "", e.get("location") or "").strip() or None, "source": "Federal Reserve"})
    return out


def earnings_events(start: date, end: date, tickers: List[str], repo_root: str,
                    get_calendar: Optional[Callable[[str], dict]] = None) -> List[dict]:
    out = []
    for t in tickers:
        d = None
        p = os.path.join(repo_root, "docs", "value-monitor", "tanuki_valuation", "data", t, "latest.json")
        src = "TANUKI VALUATION"
        try:
            d = json.load(open(p, encoding="utf-8")).get("next_earnings_date")
        except Exception:
            d = None
        if not d and get_calendar is not None:
            try:
                ed = (get_calendar(t) or {}).get("earnings_date") or []
                d = ed[0] if ed else None
                src = "market_data attributes"
            except Exception:
                d = None
        if d and start.isoformat() <= d[:10] <= end.isoformat():
            out.append({"date_et": d[:10], "time_et": None, "time_jst": None, "kind": "決算", "title": f"{t} 決算発表",
                        "detail": None, "source": src})
    return out


def _schedule(cal_name: str, start: date, end: date):
    import pandas_market_calendars as mcal
    return mcal.get_calendar(cal_name).schedule(start_date=start.isoformat(), end_date=end.isoformat())


def holiday_events(start: date, end: date) -> List[dict]:
    """米国（NYSE）・日本（JPX）の休場日と、米国の短縮取引日。"""
    import pandas as pd
    out = []
    for cal_name, label in (("NYSE", "米国"), ("JPX", "日本")):
        sched = _schedule(cal_name, start, end)
        open_days = {d.date() for d in sched.index}
        for d in pd.bdate_range(start, end):
            if d.date() not in open_days:
                out.append({"date_et" if label == "米国" else "date_local": d.date().isoformat(), "time_et": None, "time_jst": None,
                            "kind": "休場", "title": f"{label}市場 休場", "detail": None, "source": f"{cal_name}カレンダー",
                            "region": label})
        if cal_name == "NYSE":
            for d, row in sched.iterrows():
                close_et = row["market_close"].tz_convert(ET)
                if close_et.hour < 16:
                    out.append({"date_et": d.date().isoformat(), "time_et": close_et.strftime("%H:%M"),
                                "time_jst": close_et.tz_convert(JST).strftime("%Y-%m-%d %H:%M"), "kind": "短縮取引",
                                "title": f"米国市場 短縮取引（{close_et:%H:%M} ET 引け）", "detail": None,
                                "source": "NYSEカレンダー", "region": "米国"})
    return out


def _nth_weekday(y: int, m: int, weekday: int, n: int) -> date:
    d = date(y, m, 1)
    d += timedelta(days=(weekday - d.weekday()) % 7)
    return d + timedelta(weeks=n - 1)


def _prev_open(d: date, cal_name: str) -> date:
    sched = _schedule(cal_name, d - timedelta(days=10), d)
    days = [x.date() for x in sched.index if x.date() <= d]
    return days[-1] if days else d


def expiry_events(start: date, end: date) -> List[dict]:
    out = []
    months = {(start.year, start.month), (end.year, end.month)}
    for y, m in sorted(months):
        us = _prev_open(_nth_weekday(y, m, 4, 3), "NYSE")      # 第3金曜
        if start <= us <= end:
            q = m in (3, 6, 9, 12)
            out.append({"date_et": us.isoformat(), "time_et": None, "time_jst": None, "kind": "満期",
                        "title": "米国 月次オプション満期" + ("（先物・オプションの同時満期）" if q else ""), "detail": None,
                        "source": "計算（第3金曜）", "region": "米国"})
        jp = _prev_open(_nth_weekday(y, m, 4, 2), "JPX")       # 第2金曜
        if start <= jp <= end:
            q = m in (3, 6, 9, 12)
            out.append({"date_local": jp.isoformat(), "time_et": None, "time_jst": None, "kind": "満期",
                        "title": "日本 " + ("メジャーSQ" if q else "SQ（日経225オプション）"), "detail": None,
                        "source": "計算（第2金曜）", "region": "日本"})
    return out


def build_calendar(repo_root: str, earnings_tickers: List[str], now: Optional[datetime] = None,
                   get_json: Callable = _http_json, get_calendar: Optional[Callable[[str], dict]] = None) -> Dict[str, Any]:
    """今後7日（米国東部時間の今日から）の予定。取得元ごとに失敗をfailedに記録する。"""
    now = now or datetime.now(timezone.utc)
    start = now.astimezone(ET).date()
    end = start + timedelta(days=WINDOW_DAYS - 1)
    events: List[dict] = []
    failed: List[str] = []
    for name, fn in (("経済指標（FRED）", lambda: econ_events(start, end, get_json)),
                     ("FOMC・FRB要人（Federal Reserve）", lambda: fed_events(start, end, get_json)),
                     ("決算", lambda: earnings_events(start, end, earnings_tickers, repo_root, get_calendar)),
                     ("祝日・短縮取引", lambda: holiday_events(start, end)),
                     ("満期", lambda: expiry_events(start, end))):
        try:
            events.extend(fn())
        except Exception as ex:
            print(f"[WARN] 予定の取得に失敗: {name} ({type(ex).__name__}: {ex})")
            failed.append(name)
    key = lambda e: (e.get("date_et") or e.get("date_local") or "", e.get("time_et") or "99:99", e["kind"], e["title"])
    events.sort(key=key)
    status = "ok" if not failed else ("failed" if len(failed) == 5 else "partial")
    return {"fetched_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "start_et": start.isoformat(), "end_et": end.isoformat(),
            "events": events, "failed": failed, "status": status, "earnings_tickers": earnings_tickers}
