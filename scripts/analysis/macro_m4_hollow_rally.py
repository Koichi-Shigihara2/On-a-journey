"""MACRO PULSE 調査（読み取り専用、2026-10-04 指示書M-4 STEP 3）: Hollow Rally・ステルスの発火と、その後の S&P500

- 判定は M-2 STEP 4 の after と同じ: 05_liquidity.csv の各日 D について、その日に公表済みだった値（H.4.1の週次系列は as_of+1日 <= D、
  RRP・SP500 は as_of < D）で 05_main.py::weekly_liquidity_state() を計算する
- 週: H.4.1 の基準日（水曜、weekly_liquidity_state の h41_date）ごと。その週に1日でも条件を満たせば「発火した週」
- 事後のリターン: 発火した週はその週で最初に満たした日、発火しなかった週は週の最初の日を基準日 D とし、D に分かっていた最後の
  S&P500 の終値（as_of < D）から 5・20営業日後の終値までの変化率（%）。20営業日後がまだ無い週は除く
- 「EASING認識の見直しを推奨」（absorb_exceeds_supply）: 直近2週の ΔRRP・ΔTGA・ΔWALCL に分けて、どの条件で出ているかを日数で数える
- 期間は 2023-01〜（05_liquidity.csv の範囲）。週の数が少なく、結論の強さには限界がある

使い方: python scripts/analysis/macro_m4_hollow_rally.py
"""
from __future__ import annotations

import importlib.util
import json
import os
import statistics
import sys
from collections import Counter
from datetime import date, timedelta

import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LIQ = os.path.join(REPO, "docs", "market-monitor", "macro-pulse", "data", "05_liquidity.csv")
spec = importlib.util.spec_from_file_location("m05", os.path.join(REPO, "src", "market", "macro_pulse", "05_main.py"))
m05 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m05)
WEEKLY = ("WALCL", "WTREGEN", "WDTGAL", "WRBWFRBL")


def load(sid):
    with open(os.path.join(REPO, "common", "macro_data", "series", f"{sid}.json"), encoding="utf-8") as f:
        d = json.load(f)
    d = d if isinstance(d, list) else d.get("records", [])
    return sorted((x["as_of"], float(x["value"])) for x in d if x.get("value") is not None)


def stats(xs):
    if not xs:
        return "—"
    return (f"{len(xs)}件・平均{statistics.mean(xs):+.2f}%・中央値{statistics.median(xs):+.2f}%・"
            f"下落{100 * sum(1 for x in xs if x < 0) / len(xs):.0f}%")


def main() -> int:
    full = {sid: load(sid) for sid in m05._WEEKLY_LIQ_SERIES}
    sp = full["SP500"]
    sp_dates = [d for d, _ in sp]
    days = sorted(pd.read_csv(LIQ, dtype=str).date)
    rows = []
    for D in days:
        Dd = date.fromisoformat(D)
        s = {}
        for sid, pts in full.items():
            s[sid] = ([(a, v) for a, v in pts if date.fromisoformat(a) + timedelta(days=1) <= Dd] if sid in WEEKLY
                      else [(a, v) for a, v in pts if a < D])
        st = m05.weekly_liquidity_state(s, Dd)
        # EASINGの分解: 直近2週（weekly_liquidity_state と同じ作り方）
        walcl = s["WALCL"]
        tga = dict(s["WTREGEN"]) or {}
        tga_alt = dict(s["WDTGAL"])
        rrp = s["RRPONTSYD"]
        wk = []
        for w, fed in walcl[-9:]:
            t = tga.get(w, tga_alt.get(w))
            rb = None
            for dd, v in rrp:
                if dd <= w:
                    rb = v
                else:
                    break
            if t is not None and rb is not None:
                wk.append((w, fed, t, rb * 1000))
        comp = None
        if len(wk) >= 2:
            (_, f0, t0, r0), (_, f1, t1, r1) = wk[-2], wk[-1]
            comp = dict(d_fed=f1 - f0, d_tga=t1 - t0, d_rrp=r1 - r0)
            vol = max(0, comp["d_rrp"] + comp["d_tga"])
            assert (vol > 0 and vol > max(0, comp["d_fed"])) == st["absorb_exceeds_supply"], D
        # 基準日Dに分かっていた最後の終値の位置
        k = None
        for i in range(len(sp_dates) - 1, -1, -1):
            if sp_dates[i] < D:
                k = i
                break
        fwd = {}
        for n in (5, 20):
            fwd[n] = (sp[k + n][1] / sp[k][1] - 1) * 100 if k is not None and k + n < len(sp) else None
        rows.append(dict(D=D, w=st["h41_date"], sp5=st["sp500_5d_pct"], nl=st["net_liq_wow_pct"],
                         easing=st["absorb_exceeds_supply"], comp=comp, f5=fwd[5], f20=fwd[20]))

    weeks = {}
    for r in rows:
        if r["w"]:
            weeks.setdefault(r["w"], []).append(r)
    print(f"対象: {days[0]}〜{days[-1]} の {len(rows)}日、H.4.1の週 {len(weeks)}週（S&P500の終値は {sp[0][0]}〜{sp[-1][0]}）")

    def week_table(sp_th, nl_th):
        fired, quiet = [], []
        for w, rs in weeks.items():
            hit = next((r for r in rs if r["sp5"] is not None and r["nl"] is not None and r["sp5"] > sp_th and r["nl"] < nl_th), None)
            (fired if hit else quiet).append(hit or rs[0])
        return fired, quiet

    print("\n## 1. Hollow Rally（今の閾値: S&P500 5営業日 > +1% かつ NET流動性の前週比 < −0.5%）の週と、その後の S&P500")
    fired, quiet = week_table(1.0, -0.5)
    print("| | 週数 | 5営業日後 | 20営業日後 |")
    print("|---|---|---|---|")
    for lab, g in (("発火した週", fired), ("発火しなかった週", quiet)):
        print(f"| {lab} | {len(g)} | {stats([r['f5'] for r in g if r['f5'] is not None])} | "
              f"{stats([r['f20'] for r in g if r['f20'] is not None])} |")

    # M-2 STEP 4 の数え方（日付 D の暦の週〈水曜〜翌火曜〉で区切る）との違い
    cal = {}
    for r in rows:
        Dd = date.fromisoformat(r["D"])
        cal.setdefault(Dd - timedelta(days=(Dd.weekday() - 2) % 7), []).append(r)
    cal_fired = sum(1 for rs in cal.values()
                    if any(r["sp5"] is not None and r["nl"] is not None and r["sp5"] > 1.0 and r["nl"] < -0.5 for r in rs))
    print(f"\n（週の区切り: 上の表は判定に使ったH.4.1の基準日〈h41_date、公表の翌日に切り替わる〉ごと。M-2 STEP 4 は日付の暦の週"
          f"〈水曜〜翌火曜〉ごとで、その数え方では {len(cal)}週中 {cal_fired}週。発火した日が2つの暦の週にまたがる分だけ多い）")

    print("\n## 2. 閾値の9通り（発火した週の数と、その後の S&P500）")
    print("| S&P500 5営業日 | NET流動性 前週比 | 発火した週 | 5営業日後 | 20営業日後 |")
    print("|---|---|---|---|---|")
    for spt in (1.0, 2.0, 3.0):
        for nlt in (-0.5, -1.0, -2.0):
            f, _ = week_table(spt, nlt)
            print(f"| > +{spt:g}% | < {nlt:g}% | {len(f)} | {stats([r['f5'] for r in f if r['f5'] is not None])} | "
                  f"{stats([r['f20'] for r in f if r['f20'] is not None])} |")
    print(f"| 参考: 全ての週 | | {len(weeks)} | {stats([rs[0]['f5'] for rs in weeks.values() if rs[0]['f5'] is not None])} | "
          f"{stats([rs[0]['f20'] for rs in weeks.values() if rs[0]['f20'] is not None])} |")

    print("\n## 3. 「EASING認識の見直しを推奨」（ΔRRP＋ΔTGA > 0 かつ ΔRRP＋ΔTGA > max(0, ΔWALCL)、直近2週の比較）の分解")
    ev = [r for r in rows if r["easing"]]
    n_all = len(rows)
    c = Counter()
    for r in ev:
        x = r["comp"]
        c["WALCLが減った・横ばいの週（供給額0として判定）" if x["d_fed"] <= 0 else "WALCLが増えた週"] += 1
        c[("RRPとTGAが両方増" if x["d_rrp"] > 0 and x["d_tga"] > 0 else
           "TGAだけ増（RRPは減・横ばい）" if x["d_tga"] > 0 else "RRPだけ増（TGAは減・横ばい）")] += 1
    print(f"発火した日: {len(ev)}／{n_all}日（週: {sum(1 for rs in weeks.values() if any(r['easing'] for r in rs))}／{len(weeks)}週）")
    print("| 内訳 | 日数 | 発火した日に占める割合 |")
    print("|---|---|---|")
    for k_, v in c.most_common():
        print(f"| {k_} | {v} | {100 * v / len(ev):.0f}% |")
    qt = [r for r in rows if r["comp"] and r["comp"]["d_fed"] <= 0]
    print(f"\n参考: WALCLが減った・横ばいの週を使う日は全体で {len(qt)}／{n_all}日。そのうち発火 "
          f"{sum(1 for r in qt if r['easing'])}日（{100 * sum(1 for r in qt if r['easing']) / max(1, len(qt)):.0f}%）。"
          f"WALCLが増えた週を使う日 {n_all - len(qt)}日のうち発火 {sum(1 for r in ev if r['comp']['d_fed'] > 0)}日")
    print("年ごとの発火した日: " + "・".join(f"{y} {sum(1 for r in ev if r['D'][:4] == y)}／{sum(1 for r in rows if r['D'][:4] == y)}"
                                       for y in sorted({r['D'][:4] for r in rows})))
    return 0


if __name__ == "__main__":
    sys.exit(main())
