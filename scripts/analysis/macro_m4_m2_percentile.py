"""MACRO PULSE 調査（読み取り専用、2026-10-04 指示書M-4 STEP 4）: M2 の今の位置（名目の水準・前年比・実質）

- M2: common/macro_data/series/M2SL.json（月次、十億ドル）。CPI: FRED CPIAUCSL（月次）。実質＝M2 / (CPI / 最新のCPI)（最新月のドル）
- パーセンタイル＝今の値以下の観測の割合（%、index.html の pctRank() と同じ「≤」の数え方）
- 今の画面: 05_liquidity.csv の全行（2023-01〜の日次の行。月次の値が日ごとにくり返し入る）の中での名目の水準の pctRank()。
  各日の表示（その日までの行での pctRank）の推移も出す（levelBar は ≥70 が緑、≤30 が赤）

使い方（リポジトリの根で、FRED_API_KEY を設定して）:
  python scripts/analysis/macro_m4_m2_percentile.py [--cache DIR]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from bisect import bisect_right

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from macro_m4_score_vs_recession import fred  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LIQ = os.path.join(REPO, "docs", "market-monitor", "macro-pulse", "data", "05_liquidity.csv")


def rank(hist: list, v: float) -> float:
    s = sorted(hist)
    return 100 * bisect_right(s, v) / len(s)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=os.path.join(tempfile.gettempdir(), "macro_m4_cache"))
    args = ap.parse_args()
    with open(os.path.join(REPO, "common", "macro_data", "series", "M2SL.json"), encoding="utf-8") as f:
        d = json.load(f)
    d = d if isinstance(d, list) else d["records"]
    m2 = sorted((r["as_of"], float(r["value"])) for r in d if r.get("value") is not None)
    cpi = dict(fred("CPIAUCSL", os.environ.get("FRED_API_KEY", ""), args.cache))
    last_d, last_v = m2[-1]
    md = dict(m2)
    yoy = [(dd, (v / md[f"{int(dd[:4]) - 1}{dd[4:]}"] - 1) * 100) for dd, v in m2 if f"{int(dd[:4]) - 1}{dd[4:]}" in md]
    cpi_last = cpi[max(k for k in cpi if k <= last_d)]
    real = [(dd, v / (cpi[dd] / cpi_last)) for dd, v in m2 if dd in cpi]

    print(f"## M2 の今の位置（最新の観測 {last_d}、M2SL {m2[0][0][:7]}〜、CPIAUCSL の最新 {max(cpi)}）")
    print("| 尺度 | 今の値 | 全期間のパーセンタイル | 過去5年（直近60か月）の範囲 | 過去5年の中のパーセンタイル | 過去10年の中のパーセンタイル |")
    print("|---|---|---|---|---|---|")
    for lab, ser, unit in (("名目の水準（十億ドル）", m2, ""), ("前年比（%）", yoy, "%"), ("実質（最新月のドル、十億ドル）", real, "")):
        vals = [v for _, v in ser]
        cur = ser[-1][1]
        v5 = vals[-60:]
        v10 = vals[-120:]
        print(f"| {lab} | {cur:,.1f}{unit}（{ser[-1][0]}） | {rank(vals, cur):.0f}（{ser[0][0][:4]}〜、{len(vals)}か月） | "
              f"{min(v5):,.1f}〜{max(v5):,.1f}{unit} | {rank(v5, cur):.0f} | {rank(v10, cur):.0f} |")
    rmax = max(real, key=lambda x: x[1])
    print(f"\n実質の最大: {rmax[0]} {rmax[1]:,.1f}（今の値は最大の {100 * real[-1][1] / rmax[1]:.1f}%）。"
          f"名目の最大: {max(m2, key=lambda x: x[1])[0]}")

    li = pd.read_csv(LIQ, dtype=str).fillna("").sort_values("date")
    li = li[li.m2 != ""]
    vals = [float(x) for x in li.m2]
    print(f"\n## 今の画面（05_liquidity.csv の {li.date.iloc[0]}〜{li.date.iloc[-1]} の {len(vals)}行の中での pctRank）")
    print(f"今の表示: {round(rank(vals, vals[-1]))}（M2 {vals[-1]:,.1f}）")
    disp = [(li.date.iloc[i], round(rank(vals[: i + 1], vals[i]))) for i in range(len(vals))]
    for y in sorted({dd[:4] for dd, _ in disp}):
        ys = [r for dd, r in disp if dd[:4] == y]
        print(f"- {y}年の各日の表示: 最小 {min(ys)}・最大 {max(ys)}・70以上の日 {sum(1 for r in ys if r >= 70)}／{len(ys)}日・"
              f"30以下の日 {sum(1 for r in ys if r <= 30)}日")
    return 0


if __name__ == "__main__":
    sys.exit(main())
