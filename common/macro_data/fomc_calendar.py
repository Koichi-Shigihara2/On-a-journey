"""FOMCの予定のページ（FRBの fomccalendars.htm）の読み取りと、取れた回の結果のキャッシュ（[[EXTERNAL-SOURCE-CHANGE-WATCH-1]] A2、2026-11-04）。

MACRO PULSE（src/market/macro_pulse/05_main.py）は最新のFOMC声明をこのページのリンクから探す。以前はページが取れない日に
決め打ちの日付の一覧（20260507・20260318…）を使っていて、実際の声明の日と合わず、2026-05-07より後の会合が無かった
（FINDINGS 2026-10-26）。今は、ページが取れた回の結果（声明のリンク・会合の予定日）を CACHE_PATH に保存し、取れない日は
それを使う（予備）。予備を使った回は出力（fed_context.csv の fomc_date_source）に印を付ける。

キャッシュが古いとき（最後に取れてから STALE_DAYS 日を超える、または保存した会合の予定が全部過去で次の会合を知らない）は、
System Health の [P] ExternalChange が⚠️にする（重大度は common/fetch_failure.py の表の「予備のキャッシュが古い」）。
"""
from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

CACHE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fomc_calendar_cache.json")
STALE_DAYS = 7   # MACRO PULSE は毎日の実行でページを取る（refresh_fed_context_on_new_statement）。7日取れなければ知らせる

# 声明のリンク（日付はリンクのファイル名のYYYYMMDD）
STATEMENT_LINK_PATTERNS = (
    r'href="(/newsevents/pressreleases/monetary(\d{8})a\d?\.htm)"',
    r'href="(/monetarypolicy/(\d{8})a\d?\.htm)"',
)
_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}   # 先頭の3文字（「Jan/Feb」等の略記も）


def parse_statements(html: str) -> List[Tuple[str, str]]:
    """ページの声明のリンク [(YYYYMMDD, パス)]（日付の新しい順。同じ日はページに先に出るもの〈声明〉を先に）"""
    out: List[Tuple[str, str]] = []
    for pat in STATEMENT_LINK_PATTERNS:
        out += re.findall(pat, html)
    seen, uniq = set(), []
    for path, dt in out:
        if path not in seen:
            seen.add(path)
            uniq.append((dt, path))
    uniq.sort(key=lambda x: x[0], reverse=True)   # 安定な並べ替え（同じ日はページの順のまま）
    return uniq


def parse_meetings(html: str) -> List[str]:
    """ページの定例の会合の最終日 ['YYYY-MM-DD']（古い順）。年の見出し「YYYY FOMC Meetings」の下の月・日（「27-28」「8-9*」
    「April/May 30-1」）。日が1つだけの行（「22 (notation vote)」等の臨時）は除く"""
    parts = re.split(r"(\d{4}) FOMC Meetings", html)
    days: List[str] = []
    for i in range(1, len(parts) - 1, 2):
        year, body = int(parts[i]), parts[i + 1]
        months = re.findall(r'fomc-meeting__month[^>]*>\s*(?:<strong>)?\s*([A-Za-z/]+)', body)
        dates = re.findall(r'fomc-meeting__date[^>]*>\s*([^<]*)', body)
        for mon, d in zip(months, dates):
            m = re.match(r"\s*(\d{1,2})\s*-\s*(\d{1,2})", d)
            if not m:
                continue
            mname = mon.split("/")[-1].strip().lower()[:3]
            if mname not in _MONTHS:
                continue
            try:
                days.append(date(year, _MONTHS[mname], int(m.group(2))).isoformat())
            except ValueError:
                continue
    return sorted(set(days))


def save_cache(html: str, now: Optional[datetime] = None, path: str = CACHE_PATH) -> Dict[str, Any]:
    """取れたページの結果を保存する（声明のリンクが1つも無いページは保存しない＝作りが変わったら前の結果を残す）"""
    stmts = parse_statements(html)
    meetings = parse_meetings(html)
    if not stmts:
        return {}
    now = now or datetime.now(timezone.utc)
    rec = {"last_success_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
           "statements": [{"date": f"{d[:4]}-{d[4:6]}-{d[6:]}", "path": p} for d, p in stmts],
           "meetings": meetings}
    old = load_cache(path)
    if old and {k: v for k, v in old.items() if k != "last_success_utc"} == {k: v for k, v in rec.items() if k != "last_success_utc"} \
            and old.get("last_success_utc", "")[:10] == rec["last_success_utc"][:10]:
        return old   # 同じ日に同じ中身なら書かない（毎回のcommitを増やさない）
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rec, f, ensure_ascii=False, indent=1)
        f.write("\n")
    return rec


def load_cache(path: str = CACHE_PATH) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f) or {}
    except (OSError, ValueError):
        return {}


def latest_statement_from_cache(target_date: Optional[date] = None, path: str = CACHE_PATH) -> Tuple[Optional[str], Optional[str]]:
    """予備: キャッシュの声明のうち target_date 以前の最新 (日付'YYYY-MM-DD', パス)。無ければ (None, None)"""
    limit = (target_date or date.max).isoformat()
    for s in load_cache(path).get("statements") or []:
        if s.get("date") and s["date"] <= limit:
            return s["date"], s.get("path")
    return None, None


def staleness(today: Optional[date] = None, path: str = CACHE_PATH) -> List[str]:
    """キャッシュが古い理由（無ければ空）"""
    today = today or date.today()
    c = load_cache(path)
    if not c:
        return ["FOMCの予定のキャッシュが無い"]
    out = []
    try:
        last = datetime.fromisoformat(c["last_success_utc"].replace("Z", "+00:00")).date()
        age = (today - last).days
        if age > STALE_DAYS:
            out.append(f"FOMCの予定のページが最後に取れてから{age}日（上限{STALE_DAYS}日、{last.isoformat()}）")
    except (KeyError, ValueError):
        out.append("FOMCの予定のキャッシュに最後に取れた時刻が無い")
    meetings = c.get("meetings") or []
    if not meetings or max(meetings) < today.isoformat():
        out.append(f"保存した会合の予定がすべて過去（最後 {max(meetings) if meetings else 'なし'}）")
    return out


def summary(today: Optional[date] = None, path: str = CACHE_PATH) -> str:
    """System Health の表示の1行（古くないとき）"""
    today = today or date.today()
    c = load_cache(path)
    nxt = next((m for m in c.get("meetings") or [] if m >= today.isoformat()), None)
    return f"FOMCの予定 最後に取れた {str(c.get('last_success_utc', ''))[:10]}・次の会合 {nxt or '不明'}"
