"""MACRO PULSE: 05_events.csv の CFNAI の行の値を、単月の CFNAI から 3ヶ月移動平均（CFNAIMA3）に置き換える
（一度だけの修復、2026-10-03 指示書M-2 STEP 6-1、[[MACRO-PULSE-CFNAI-MA3-SERIES-1]]）

INDICATOR_CONFIG の fred_id を CFNAIMA3 に変えても、refresh_monthly_indicators() は既に値がある観測月の行を
上書きしないため、既存の行（単月の値、2026-08月分まで）は残り、次の月の発表まで画面・スコアは単月の値のままになる。
この修復は、indicator が "Chicago Fed National Activity" の行の actual を、同じ観測月の CFNAIMA3
（common/macro_data/series/CFNAIMA3.json、現在の版）の値に置き換える。release_date・updated_at・その他の列は変えない。
CFNAIMA3 に同じ観測月の値が無い行は触らない（一覧に出す）。05_weekly_analysis.csv は変更しない。

05_events.csv は .gitattributes の merge=ours のため、kaihatsu へ統合した後に kaihatsu 上で実行して commit する。

使い方:
  python scripts/analysis/macro_cfnai_ma3_rows_repair.py          # 確認だけ
  python scripts/analysis/macro_cfnai_ma3_rows_repair.py --apply  # 書き換える
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EVENTS = os.path.join(REPO_ROOT, "docs", "market-monitor", "macro-pulse", "data", "05_events.csv")
MA3 = os.path.join(REPO_ROOT, "common", "macro_data", "series", "CFNAIMA3.json")
IND = "Chicago Fed National Activity"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    with open(MA3, encoding="utf-8") as f:
        d = json.load(f)
    d = d if isinstance(d, list) else d.get("records", [])
    ma3 = {r["as_of"]: r["value"] for r in d if r.get("value") is not None}
    ev = pd.read_csv(EVENTS, dtype=str).fillna("")
    rows = ev[ev["indicator"] == IND]
    changed, missing, same = [], [], 0
    for idx, r in rows.iterrows():
        v = ma3.get(r["release_date"])
        if v is None:
            missing.append(r["release_date"])
            continue
        if r["actual"] != "" and abs(float(r["actual"]) - float(v)) < 1e-9:
            same += 1
            continue
        changed.append((idx, r["release_date"], r["actual"], v))
    print(f"CFNAIの行 {len(rows)}件: 置き換え {len(changed)}件・同じ値 {same}件・CFNAIMA3に無い {len(missing)}件 {missing[:10]}")
    for _, d0, old, new in changed[-6:]:
        print(f"  {d0}: {old} → {new}")
    if not args.apply:
        print("（確認のみ。--apply で書き換える）")
        return 0
    for idx, _, _, v in changed:
        ev.at[idx, "actual"] = str(v)
    ev.to_csv(EVENTS, index=False, encoding="utf-8")
    print("書き換えた")
    return 0


if __name__ == "__main__":
    sys.exit(main())
