"""MACRO PULSE: 05_events.csv の既存の行に sp500_t0_asof（sp500_t0が何日の終値か）を埋める
（一度だけの修復、2026-10-04 指示書M-3 STEP 5、[[MACRO-PULSE-TICKER-SP500-NO-ASOF-1]]）

sp500_t0 の値（小数2桁）と一致する S&P500 の終値（common/macro_data/series/SP500.json、FRED。2016-08以降）の観測日のうち、
行の release_date と updated_at の日付の遅いほう以前のものが**1つだけ**ある行に、その観測日を入れる。
一致が無い行（FREDの記録より前・別の取得元の値など）と、候補が2つ以上ある行は空のまま（推測で埋めない）。
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

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EVENTS = os.path.join(REPO_ROOT, "docs", "market-monitor", "macro-pulse", "data", "05_events.csv")
SP500 = os.path.join(REPO_ROOT, "common", "macro_data", "series", "SP500.json")
IMPORT_DAYS = ("2026-03-28", "2026-03-29")


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
    for idx, r in ev.iterrows():
        if not r["sp500_t0"]:
            continue
        kind = "取り込み分" if r["updated_at"][:10] in IMPORT_DAYS else "実行分"
        if r["sp500_t0_asof"]:
            st[(kind, "既存のまま")] += 1
            continue
        bound = max(r["release_date"], r["updated_at"][:10])
        cands = [d for d in by_value.get(round(float(r["sp500_t0"]), 2), []) if d <= bound]
        if len(cands) == 1:
            ev.at[idx, "sp500_t0_asof"] = cands[0]
            st[(kind, "観測日を設定")] += 1
        else:
            st[(kind, "一致なし（空のまま）" if not cands else "候補が複数（空のまま）")] += 1
    for k in sorted(st):
        print(f"  {k[0]}: {k[1]} {st[k]}")
    if not args.apply:
        print("（確認のみ。--apply で書き換える）")
        return 0
    ev.to_csv(EVENTS, index=False, encoding="utf-8")
    print("書き換えた")
    return 0


if __name__ == "__main__":
    sys.exit(main())
