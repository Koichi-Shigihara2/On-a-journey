"""MACRO PULSE: 05_liquidity.csv の既存の行に m2_yoy_pct・m2_yoy_pctile を埋める（一度だけの修復、2026-10-04 指示書M-5 STEP 4）

各行の m2（その日に書いた M2SL の値）と一致する観測月を common/macro_data/series/M2SL.json（最新の版）で探し、その観測月までの
データで 05_main.py::m2_yoy_position() を計算する。一致する観測月が無い行（後から改定された値など）と、候補が2つ以上の行は空のまま。
既に値がある行は変えない。

05_liquidity.csv は merge=ours のため、kaihatsu へ統合した後に kaihatsu 上で実行して commit する。
使い方:
  python scripts/analysis/macro_liquidity_m2_yoy_repair.py           # 確認だけ
  python scripts/analysis/macro_liquidity_m2_yoy_repair.py --apply   # 書き換える
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import sys
from collections import Counter

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LIQ = os.path.join(REPO, "docs", "market-monitor", "macro-pulse", "data", "05_liquidity.csv")
spec = importlib.util.spec_from_file_location("m05", os.path.join(REPO, "src", "market", "macro_pulse", "05_main.py"))
m05 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m05)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    with open(os.path.join(REPO, "common", "macro_data", "series", "M2SL.json"), encoding="utf-8") as f:
        d = json.load(f)
    d = d if isinstance(d, list) else d["records"]
    m2 = sorted((r["as_of"], float(r["value"])) for r in d if r.get("value") is not None)
    with open(LIQ, encoding="utf-8", newline="") as f:
        rd = csv.DictReader(f)
        cols = list(rd.fieldnames)
        rows = list(rd)
    for c in ("m2_yoy_pct", "m2_yoy_pctile"):
        if c not in cols:
            cols.append(c)
    st = Counter()
    cache = {}
    for r in rows:
        r.setdefault("m2_yoy_pct", "")
        r.setdefault("m2_yoy_pctile", "")
        if r["m2_yoy_pct"] or not r.get("m2"):
            st["既存の値あり・m2なし（変えない）"] += 1
            continue
        v = float(r["m2"])
        cands = [i for i, (dd, x) in enumerate(m2) if abs(x - v) < 0.05 and dd <= r["date"]]
        if len(cands) != 1:
            st["一致する観測月なし（空のまま）" if not cands else "候補が複数（空のまま）"] += 1
            continue
        i = cands[0]
        if i not in cache:
            cache[i] = m05.m2_yoy_position(m2[: i + 1])
        yoy, pct = cache[i]
        if yoy is None:
            st["前年比が計算できない（空のまま）"] += 1
            continue
        r["m2_yoy_pct"] = m05._fmt_pct(yoy)
        r["m2_yoy_pctile"] = str(pct)
        st[f"設定（観測月 {m2[i][0][:7]}）"] += 1
    for k in sorted(st):
        print(f"  {k}: {st[k]}")
    last = rows[-1]
    print(f"最新行 {last['date']}: m2={last.get('m2')} m2_yoy_pct={last['m2_yoy_pct']} m2_yoy_pctile={last['m2_yoy_pctile']}")
    if not args.apply:
        print("（確認のみ。--apply で書き換える）")
        return 0
    with open(LIQ, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    print("書き換えた")
    return 0


if __name__ == "__main__":
    sys.exit(main())
