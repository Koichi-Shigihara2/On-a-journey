"""MACRO PULSE: 05_events.csv の既存の行に sp500_t0_asof（sp500_t0が何日の終値か）を埋める
（一度だけの修復、2026-10-04 指示書M-3 STEP 5、[[MACRO-PULSE-TICKER-SP500-NO-ASOF-1]]）

sp500_t0 の値（小数2桁）と一致する S&P500 の終値（common/macro_data/series/SP500.json、FRED。2016-08以降）の観測日のうち、
行の日付の0〜7日前のものが**1つだけ**ある行に、その観測日を入れる。行の日付は、取り込み分（updated_at=2026-03-28/29）は
release_date、実行分は updated_at（UTC）の米国東部時間の暦日。
一致が無い行（FREDの記録より前・別の取得元の値など）、一致が行の日付の8日以上前（何年も前の同じ値の終値など）か後にしか無い行、
候補が2つ以上ある行は空のまま（推測で埋めない）。
既に sp500_t0_asof がある行は変えない。

05_events.csv は merge=ours のため、kaihatsu へ統合した後に kaihatsu 上で実行して commit する。
使い方:
  python scripts/analysis/macro_events_sp500_asof_repair.py           # 確認だけ
  python scripts/analysis/macro_events_sp500_asof_repair.py --apply   # 書き換える
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EVENTS = os.path.join(REPO_ROOT, "docs", "market-monitor", "macro-pulse", "data", "05_events.csv")
SP500 = os.path.join(REPO_ROOT, "common", "macro_data", "series", "SP500.json")
IMPORT_DAYS = ("2026-03-28", "2026-03-29")
MAX_GAP_DAYS = 7
NY = ZoneInfo("America/New_York")


def row_date(r) -> str:
    """取り込み分はrelease_date、実行分はupdated_at（UTC）の米国東部時間の暦日。"""
    if r["updated_at"][:10] in IMPORT_DAYS:
        return r["release_date"]
    try:
        u = datetime.strptime(r["updated_at"].strip()[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return r["release_date"]
    return u.astimezone(NY).date().isoformat()


def match_asof(value: float, rdate: str, by_value: dict):
    """(観測日 or None, 理由, 行の日付−観測日の日数〈一番近い一致〉 or None)。"""
    obs = by_value.get(round(value, 2), [])
    if not obs:
        return None, "一致なし（空のまま）", None
    gaps = sorted(((date.fromisoformat(rdate) - date.fromisoformat(d)).days, d) for d in obs)
    near = min(gaps, key=lambda g: (abs(g[0]), g[0]))[0]
    inwin = [d for g, d in gaps if 0 <= g <= MAX_GAP_DAYS]
    if len(inwin) == 1:
        return inwin[0], "観測日を設定", near
    if len(inwin) > 1:
        return None, "差0〜7日に候補が複数（空のまま）", near
    return None, "一致が差0〜7日の外だけ（空のまま）", near


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    with open(SP500, encoding="utf-8") as f:
        recs = json.load(f)
    recs = recs if isinstance(recs, list) else recs["records"]
    by_value: dict = {}
    for r in recs:
        if r.get("value") is not None:
            by_value.setdefault(round(float(r["value"]), 2), []).append(r["as_of"])
    ev = pd.read_csv(EVENTS, dtype=str).fillna("")
    if "sp500_t0_asof" not in ev.columns:
        ev["sp500_t0_asof"] = ""
    st = Counter()
    gap_set = Counter()
    outside = []
    for idx, r in ev.iterrows():
        if not r["sp500_t0"]:
            continue
        kind = "取り込み分" if r["updated_at"][:10] in IMPORT_DAYS else "実行分"
        if r["sp500_t0_asof"]:
            st[(kind, "既存のまま")] += 1
            continue
        rd = row_date(r)
        asof, why, near = match_asof(float(r["sp500_t0"]), rd, by_value)
        st[(kind, why)] += 1
        if asof:
            ev.at[idx, "sp500_t0_asof"] = asof
            gap_set[(date.fromisoformat(rd) - date.fromisoformat(asof)).days] += 1
        elif why.startswith("一致が"):
            outside.append((kind, r["indicator"], r["release_date"], r["updated_at"], r["sp500_t0"], rd, near))
    for k in sorted(st):
        print(f"  {k[0]}: {k[1]} {st[k]}")
    print("設定した行の「行の日付−観測日」の分布:", dict(sorted(gap_set.items())))
    og = Counter(("8日以上前" if o[6] >= 8 else "行の日付より後") for o in outside)
    print(f"差0〜7日の外だけで除いた行: {len(outside)}（{dict(og)}）。例:")
    for o in sorted(outside, key=lambda o: -abs(o[6]))[:8]:
        print(f"    {o[0]} {o[1]} release_date={o[2]} updated_at={o[3]} sp500_t0={o[4]} 行の日付={o[5]} 一番近い一致との差={o[6]}日")
    if not args.apply:
        print("（確認のみ。--apply で書き換える）")
        return 0
    ev.to_csv(EVENTS, index=False, encoding="utf-8")
    print("書き換えた")
    return 0


if __name__ == "__main__":
    sys.exit(main())
