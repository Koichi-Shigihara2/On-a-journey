"""MACRO PULSE 調査（読み取り専用、2026-10-04 指示書M-4 STEP 2）: 8指標の閾値の、過去の分布の中での位置

- 閾値は 2026-10-04 時点の kaihatsu のコードから写した（スコアの段: 05_main.py `_compute_current_score()`・index.html `computeCurrentScore()`、
  過去の補間: index.html `computeScoreAsOf()` の `calcSignal()`、ヘルスバー: index.html `renderL2()` の `L2_CFG`）
- 分布: FRED の最新の値の全履歴（HY は FRED が直近3年しか返さないため common/macro_data/series/BAMLH0A0HYM2.json〈1996-12〜〉）。
  パーセンタイル＝その閾値より小さい観測の割合（%）
- Michigan（UMCSENT）は1978年以降（月次）で、スコアの段ごとにいた月の割合と、補間の点数が30未満（拡張の範囲）だった月の割合

使い方（リポジトリの根で、FRED_API_KEY を設定して）:
  python scripts/analysis/macro_m4_thresholds.py [--cache DIR]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from macro_m4_score_vs_recession import fred  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# (表示名, FRED系列, 向き〈高いほど後退側なら"up"〉, スコアの段, 補間の折れ点, ヘルスバー(bull, mid, bear), カードのthresh表示)
SPEC = [
    ("YC 10Y-2Y", "T10Y2Y", "down", [-0.5, 0, 0.5], [-0.5, 0, 0.5, 1.5], (0.5, 0, -0.2), "BULL≥+0.5% / BEAR<-0.5%"),
    ("HY Spread", "BAMLH0A0HYM2", "up", [3.5, 4.5, 6.0], [2.5, 3.5, 4.5, 6.0], (4.0, 5.0, 6.5), "BULL≤3.5% / BEAR>6.0%"),
    ("Philly Fed Mfg", "GACDFSA066MSFRBPHI", "down", [-10, 0, 5], [-15, 0, 10], (5, 0, -5), "BULL≥+5 / BEAR<-10"),
    ("CFNAI MA3", "CFNAIMA3", "down", [-0.7, -0.35], [-0.7, -0.35, 0], (0, -0.2, -0.7), "BULL≥-0.35 / BEAR<-0.7"),
    ("Initial Claims 4WMA", "IC4WSA", "up", [215000, 250000, 300000], [180000, 215000, 250000, 300000],
     (215000, 230000, 245000), "BULL≤215K / BEAR>300K"),
    ("Building Permits", "PERMIT", "down", [1100, 1300, 1500], [800, 1100, 1400, 1800], (1500, 1300, 1100),
     "BULL≥1500K / BEAR≤1100K"),
    ("Michigan Sent.", "UMCSENT", "down", [60, 75, 90], [55, 75, 95], (90, 80, 65), "NEUTRAL≥90 / BEAR<60"),
    ("Sahm Rule", "SAHMCURRENT", "up", [0.3, 0.5], [0, 0.3, 0.5], (0.3, 0.3, 0.5), "BULL<0.3 / BEAR≥0.5"),
]


def load(sid: str, key: str, cache: str) -> list:
    if sid == "BAMLH0A0HYM2":
        with open(os.path.join(REPO, "common", "macro_data", "series", f"{sid}.json"), encoding="utf-8") as f:
            d = json.load(f)
        d = d if isinstance(d, list) else d["records"]
        return sorted((r["as_of"], float(r["value"])) for r in d if r.get("value") is not None)
    return fred(sid, key, cache)


def pct(vals: list, th: float) -> str:
    return f"{100 * sum(1 for v in vals if v < th) / len(vals):.1f}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=os.path.join(tempfile.gettempdir(), "macro_m4_cache"))
    args = ap.parse_args()
    key = os.environ.get("FRED_API_KEY", "")
    print("## 閾値と、過去の分布の中での位置（パーセンタイル＝閾値より小さい観測の割合）")
    print("| 指標（系列） | 期間・件数 | 中央値 | 最新 | スコアの段の閾値（%ile） | 補間の折れ点（%ile） | ヘルスバー bull/mid/bear（%ile） | カードの表示 |")
    print("|---|---|---|---|---|---|---|---|")
    data = {}
    for name, sid, _dir, steps, knots, hb, thresh in SPEC:
        obs = load(sid, key, args.cache)
        data[sid] = obs
        vals = sorted(v for _, v in obs)
        med = vals[len(vals) // 2]
        f = lambda ts: "・".join(f"{t:g}（{pct(vals, t)}）" for t in ts)  # noqa: E731
        print(f"| {name}（{sid}） | {obs[0][0][:7]}〜{obs[-1][0][:7]}・{len(obs)} | {med:g} | {obs[-1][1]:g}（{obs[-1][0]}） | "
              f"{f(steps)} | {f(knots)} | {f(hb)} | {thresh} |")

    # Michigan: 1978年以降（月次）
    um = [(d, v) for d, v in data["UMCSENT"] if d >= "1978-01-01"]
    n = len(um)
    tiers = [("<60（82点・後退シグナル）", lambda v: v < 60), ("60〜75未満（72点・後退シグナル）", lambda v: 60 <= v < 75),
             ("75〜90未満（60点・注意）", lambda v: 75 <= v < 90), ("90以上（30点・中立）", lambda v: v >= 90)]
    print(f"\n## Michigan（UMCSENT、{um[0][0][:7]}〜{um[-1][0][:7]}・{n}か月）のスコアの段ごとの月の割合")
    print("| 段（今のゲージ・週次スナップショットの段階関数） | 月数 | 割合 |")
    print("|---|---|---|")
    for lab, fn in tiers:
        k = sum(1 for _, v in um if fn(v))
        print(f"| {lab} | {k} | {100 * k / n:.1f}% |")

    def lerp_mi(v):
        if v <= 55:
            return 82
        if v <= 75:
            return round(82 + (v - 55) / 20 * (55 - 82))
        if v <= 95:
            return round(55 + (v - 75) / 20 * (25 - 55))
        return 20
    k30 = sum(1 for _, v in um if lerp_mi(v) < 30)
    print(f"\n補間（スコア推移の過去の点、`calcSignal('cbcc')`）で30点未満（拡張の範囲）だった月: {k30}（{100 * k30 / n:.1f}%）。"
          f"段階関数では最も良い段が30点（中立）で、30点未満の段は無い")
    print("年代ごとの90以上の月の割合: " + "・".join(
        f"{dec}年代 {100 * sum(1 for d, v in um if d[:3] == dec[:3] and v >= 90) / max(1, sum(1 for d, _ in um if d[:3] == dec[:3])):.0f}%"
        for dec in ("1970", "1980", "1990", "2000", "2010", "2020")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
