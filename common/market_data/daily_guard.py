"""Market Data Daily Updateの起動ガード（指示書㉕ STEP A、2026-09-30）。

Market_Data_Daily_Update.ymlは20:17〜23:17 UTCの間に30分おきに起動する。各起動で、次の2つを判定し、
取得が必要なときだけ run=true を出力する（GitHub Actionsの$GITHUB_OUTPUTに追記する形式）。
  (1) NYSEのその日の引けから20分経っていなければ、何もせず終了する（夏時間・冬時間・短縮取引日は
      pandas_market_calendarsのNYSEカレンダーで自動判定。休場日も何もしない）
  (2) その日の終値が既にdaily/にそろっていれば、何もせず終了する
      （直近10日に行がある銘柄のうち、その日の終値つきの行を持つ銘柄が95%以上）

使い方: python common/market_data/daily_guard.py [--now 2026-09-29T20:30:00Z]
出力（標準出力）: run=true|false / reason=... / expected_date=YYYY-MM-DD
"""
import argparse
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "daily")
CLOSE_WAIT = timedelta(minutes=20)
COMPLETE_RATIO = 0.95


def _valid_close(r):
    c = r.get("close")
    return isinstance(c, (int, float)) and not math.isnan(c) and c > 0


def completeness(day: str, base: str = BASE) -> tuple:
    """(その日の終値を持つ銘柄数, 直近10日に行がある銘柄数)。^N225・為替・先物は区切りが違うため数えない。"""
    since = (datetime.strptime(day, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
    have = active = 0
    for f in os.listdir(base):
        sym = f[:-5]
        if not f.endswith(".json") or sym in ("^N225", "JPY=X", "CL=F", "GC=F"):
            continue
        try:
            recs = json.load(open(os.path.join(base, f), encoding="utf-8")).get("records", [])
        except Exception:
            continue
        recent = [r for r in recs[-15:] if r.get("date", "") >= since]
        if not recent:
            continue
        active += 1
        if any(r.get("date") == day and _valid_close(r) for r in recent):
            have += 1
    return have, active


def decide(now: datetime, base: str = BASE) -> dict:
    ny_day = now.astimezone(ZoneInfo("America/New_York")).date()
    sched = mcal.get_calendar("NYSE").schedule(start_date=ny_day.isoformat(), end_date=ny_day.isoformat())
    day = ny_day.isoformat()
    if sched.empty:
        return {"run": "false", "reason": f"NYSE休場日（{day}）", "expected_date": day}
    close = sched["market_close"].iloc[0].to_pydatetime()
    if now < close + CLOSE_WAIT:
        return {"run": "false", "reason": f"引け（{close:%H:%M} UTC）から20分経っていない", "expected_date": day}
    have, active = completeness(day, base)
    if active and have / active >= COMPLETE_RATIO:
        return {"run": "false", "reason": f"{day}の終値はそろっている（{have}/{active}銘柄）", "expected_date": day}
    return {"run": "true", "reason": f"{day}の終値が未取得（{have}/{active}銘柄）", "expected_date": day}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--now", default=None)
    args = ap.parse_args()
    now = datetime.fromisoformat(args.now.replace("Z", "+00:00")) if args.now else datetime.now(timezone.utc)
    out = decide(now)
    for k, v in out.items():
        print(f"{k}={v}")
    print(f"[guard] {now:%Y-%m-%d %H:%M} UTC: {out['reason']} → run={out['run']}", file=sys.stderr)


if __name__ == "__main__":
    main()
