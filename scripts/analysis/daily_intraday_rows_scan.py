"""daily/の各行が「その市場の日足が確定する前」に保存されていないかを洗い出す（指示書㉔の追加確認）。

daily/の行は保存時刻を持たないため、gitの履歴から各行を最後に変更したcommitの時刻を保存時刻とみなす
（commitは取得の後なので、commit時刻が日足の確定時刻より前なら、取得も確定前）。
日足の確定時刻（Yahooの日足の区切り。yfinanceのhistory_metadataで確認した定義）:
  - 米国株・ETF・米国指数（^GSPC・^IXIC・^DJI・^NYA・^RUT・^VIX9D）: NYSEの引け（短縮取引日を含む、pandas_market_calendars）
  - ^VIX: 15:15 CT（NYSEの引け＋15分）、^TNX: 14:00 CT（NYSEの引け−1時間）
  - ^N225: 15:30 JST（OSA）
  - JPY=X: 翌日0:00 Europe/London（Yahooの為替の日足はロンドンの暦日）
  - CL=F・GC=F: 翌日0:00 America/New_York（Yahooの先物の日足はニューヨークの暦日）
対象は日次取得が始まった2026-08-06以降の日付の行（それ以前はバックフィルで、確定後に保存されている）。
読むだけで、daily/は変更しない。

使い方: python scripts/analysis/daily_intraday_rows_scan.py --out <JSON>
"""
import argparse
import json
import os
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SINCE = "2026-08-06"
REC = re.compile(r'"date": "(\d{4}-\d\d-\d\d)",\s*"open": ([^,]+),\s*"high": ([^,]+),\s*"low": ([^,]+),\s*"close": ([^,]+),\s*"volume": ([^,\n]+)')
NY, LDN, TYO = ZoneInfo("America/New_York"), ZoneInfo("Europe/London"), ZoneInfo("Asia/Tokyo")


def git(*a):
    return subprocess.run(["git", "-C", REPO, *a], capture_output=True, text=True, encoding="utf-8").stdout


def nyse_closes():
    import pandas_market_calendars as mcal
    s = mcal.get_calendar("NYSE").schedule(start_date=SINCE, end_date=(datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d"))
    return {d.strftime("%Y-%m-%d"): c.to_pydatetime() for d, c in zip(s.index, s["market_close"])}


def bar_end(sym, day, closes):
    d = datetime.strptime(day, "%Y-%m-%d")
    if sym == "JPY=X":
        return (datetime.combine(d.date() + timedelta(days=1), time(0, 0), LDN)).astimezone(timezone.utc)
    if sym in ("CL=F", "GC=F"):
        return (datetime.combine(d.date() + timedelta(days=1), time(0, 0), NY)).astimezone(timezone.utc)
    if sym == "^N225":
        return datetime.combine(d.date(), time(15, 30), TYO).astimezone(timezone.utc)
    c = closes.get(day)
    if c is None:
        return None
    if sym == "^VIX":
        return c + timedelta(minutes=15)
    if sym == "^TNX":
        return c - timedelta(hours=1)
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    commits = [l.split(" ", 2) for l in git("log", "--reverse", "--format=%H %cI %an", "--", "common/market_data/daily/").splitlines()]
    cat = subprocess.Popen(["git", "-C", REPO, "cat-file", "--batch"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    state, saved = {}, {}
    for h, ts, author in commits:
        t = datetime.fromisoformat(ts).astimezone(timezone.utc)
        files = [f for f in git("diff-tree", "-r", "--root", "--no-commit-id", "--name-only", h, "--",
                                "common/market_data/daily/").splitlines() if f.endswith(".json")]
        for f in files:
            cat.stdin.write(f"{h}:{f}\n".encode()); cat.stdin.flush()
            hdr = cat.stdout.readline().split()
            if len(hdr) < 3 or hdr[1] != b"blob":
                continue
            data = cat.stdout.read(int(hdr[2])); cat.stdout.read(1)
            tail = data[-30000:].decode("utf-8", "ignore")
            sym = os.path.basename(f)[:-5]
            for m in REC.finditer(tail):
                day = m.group(1)
                if day < SINCE:
                    continue
                val = (m.group(5).strip(), m.group(6).strip())
                if state.get((sym, day)) != val:
                    state[(sym, day)] = val
                    saved[(sym, day)] = (t, h[:10], author)
    cat.stdin.close()
    closes = nyse_closes()
    # 現在のHEADに残っている行だけを判定
    head = {}
    for f in os.listdir(os.path.join(REPO, "common", "market_data", "daily")):
        if f.endswith(".json"):
            sym = f[:-5]
            for r in json.load(open(os.path.join(REPO, "common", "market_data", "daily", f), encoding="utf-8"))["records"]:
                if r.get("date", "") >= SINCE:
                    head[(sym, r["date"])] = r
    before, near, unknown = [], [], []
    for key, r in head.items():
        sym, day = key
        if key not in saved:
            unknown.append(key); continue
        t, h, author = saved[key]
        end = bar_end(sym, day, closes)
        if end is None:
            continue
        row = {"symbol": sym, "date": day, "saved_at": t.strftime("%Y-%m-%dT%H:%MZ"), "bar_end": end.strftime("%Y-%m-%dT%H:%MZ"),
               "commit": h, "author": author, "close": r.get("close"), "volume": r.get("volume")}
        if t < end:
            before.append(row)
        elif t < end + timedelta(minutes=10):
            near.append(row)
    kind = lambda s: ("為替" if s == "JPY=X" else "先物" if s in ("CL=F", "GC=F") else "日本指数" if s == "^N225"
                      else "米国指数" if s.startswith("^") else "個別株・ETF")
    out = {"rows_checked": len(head), "saved_before_bar_end": len(before), "within_10min_after_end": len(near),
           "by_kind": dict(Counter(kind(r["symbol"]) for r in before)),
           "by_symbol": dict(Counter(r["symbol"] for r in before).most_common()),
           "by_commit": dict(Counter(f"{r['commit']} {r['saved_at'][:13]} {r['author']}" for r in before).most_common()),
           "rows": sorted(before, key=lambda r: (r["symbol"], r["date"])), "near": near, "unknown_save_time": len(unknown)}
    json.dump(out, open(args.out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps({k: v for k, v in out.items() if k not in ("rows", "near")}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
