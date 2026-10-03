"""MACRO PULSE 流動性モニター: Hollow Rally・ステルスの発火回数の before/after（2026-10-03 指示書M-2 STEP 4、読み取り専用）

[[MACRO-PULSE-LIQUIDITY-DAILY-ROWS-AS-WEEKS-1]]
- before: 05_liquidity.csv の行で判定（Hollow Rally: S&Pの6行前比 > +1% かつ NET流動性の直前の行比 < -0.5%。
          ステルス: CSVに保存された stealth_signal・stealth_alert 列＝日次の行で数えた値）
- after:  05_main.py::weekly_liquidity_state()（H.4.1の基準日〈水曜〉の値の前週比、S&Pの5営業日リターン）を、
          各行の日付 D の時点で公表済みだったデータだけで計算する
          （H.4.1の週次系列は水曜の翌日〈木曜〉公表 → as_of+1日 <= D、RRP・SP500は翌日公表 → as_of < D）

使い方: python scripts/analysis/macro_liquidity_weekly_before_after.py [--md 出力.md]
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
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


def f(x):
    return float(x) if x not in ("", None) else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--md", default=None)
    args = ap.parse_args()
    full = {sid: load(sid) for sid in m05._WEEKLY_LIQ_SERIES}
    li = pd.read_csv(LIQ, dtype=str).fillna("").sort_values("date").reset_index(drop=True)
    recs = li.to_dict("records")
    out = []
    for i, r in enumerate(recs):
        D = r["date"]
        Dd = date.fromisoformat(D)
        # before: Hollow（現行JSの判定）
        sp = [x for x in recs[: i + 1] if x["sp500"] != ""]
        nl = [x for x in recs[: i + 1] if x["net_liquidity"] != ""]
        b_sp = b_nl = None
        if len(sp) >= 6 and len(nl) >= 2:
            b_sp = (f(sp[-1]["sp500"]) - f(sp[-6]["sp500"])) / abs(f(sp[-6]["sp500"])) * 100
            b_nl = (f(nl[-1]["net_liquidity"]) - f(nl[-2]["net_liquidity"])) / abs(f(nl[-2]["net_liquidity"]) or 1) * 100
        b_hollow = b_sp is not None and b_sp > 1.0 and b_nl < -0.5
        # after: その日に公表済みだった値で weekly_liquidity_state()
        s = {}
        for sid, pts in full.items():
            if sid in WEEKLY:
                s[sid] = [(a, v) for a, v in pts if (date.fromisoformat(a) + timedelta(days=1)) <= Dd]
            else:
                s[sid] = [(a, v) for a, v in pts if a < D]
        st = m05.weekly_liquidity_state(s, Dd)
        a_hollow = (st["sp500_5d_pct"] is not None and st["net_liq_wow_pct"] is not None
                    and st["sp500_5d_pct"] > 1.0 and st["net_liq_wow_pct"] < -0.5)
        a_alerts = []
        if st["absorb_weeks"] >= 4:
            a_alerts.append("政策EASINGの効果が限定的")
        if st["decline_weeks"] >= 3:
            a_alerts.append("実質的にTIGHTENINGに近い状態")
        if st["absorb_exceeds_supply"]:
            a_alerts.append("EASING認識の見直しを推奨")
        b_alerts = [a.split("（")[0] for a in r["stealth_alert"].split("|") if a.strip()]
        out.append(dict(date=D, has_cols=r["net_liq_decline_weeks"] != "",
                        b_hollow=b_hollow, b_sp6row=b_sp, b_nl_prevrow=b_nl,
                        a_hollow=a_hollow, a_sp5d=st["sp500_5d_pct"], a_nl_wow=st["net_liq_wow_pct"], h41=st["h41_date"],
                        b_signal=r["stealth_signal"], a_signal=st["signal"], b_decline=r["net_liq_decline_weeks"],
                        a_decline=st["decline_weeks"], a_absorb=st["absorb_weeks"],
                        b_alerts=b_alerts, a_alerts=a_alerts))
    df = pd.DataFrame(out)
    lines = []
    p = lines.append
    p(f"対象: 05_liquidity.csv {len(df)}行（{df.date.min()}〜{df.date.max()}）")
    p(f"Hollow Rally: before {int(df.b_hollow.sum())}日 → after {int(df.a_hollow.sum())}日")
    cols = df[df.has_cols]
    for name in ["政策EASINGの効果が限定的", "実質的にTIGHTENINGに近い状態", "EASING認識の見直しを推奨"]:
        b = sum(name in x for x in cols.b_alerts)
        a = sum(name in x for x in cols.a_alerts)
        aa = sum(name in x for x in df.a_alerts)
        p(f"ステルス「{name}」: before {b}日 → after {a}日（列がある{len(cols)}行で比較。afterの全履歴では{aa}日）")
    p(f"ステルス判定（列がある{len(cols)}行）: before {cols.b_signal.value_counts().to_dict()} → after {cols.a_signal.value_counts().to_dict()}")

    def fmt(v, n=2):
        return "—" if v is None or (isinstance(v, float) and pd.isna(v)) else f"{v:+.{n}f}"

    unexplained = []
    for label, cond in [("増えた日（beforeは不発火、afterは発火）", (~df.b_hollow) & df.a_hollow),
                        ("消えた日（beforeは発火、afterは不発火）", df.b_hollow & ~df.a_hollow)]:
        sub = df[cond]
        p(f"\n#### Hollow Rally {label}: {len(sub)}日")
        p("| 日付 | 日次: S&P 6行前比 | 日次: NET流動性 直前の行比 | 週: S&P 5営業日 | 週: NET流動性 前週比（H.4.1） | 説明 |")
        p("|---|---|---|---|---|---|")
        for _, x in sub.iterrows():
            why = []
            b_sp_ok = x.b_sp6row is not None and not pd.isna(x.b_sp6row) and x.b_sp6row > 1.0
            b_nl_ok = x.b_nl_prevrow is not None and not pd.isna(x.b_nl_prevrow) and x.b_nl_prevrow < -0.5
            a_sp_ok = x.a_sp5d is not None and not pd.isna(x.a_sp5d) and x.a_sp5d > 1.0
            a_nl_ok = x.a_nl_wow is not None and not pd.isna(x.a_nl_wow) and x.a_nl_wow < -0.5
            if b_sp_ok != a_sp_ok:
                why.append("S&Pの期間（6行→5営業日）")
            if b_nl_ok != a_nl_ok:
                why.append("NET流動性の間隔（前日→前週）")
            if not why:
                unexplained.append(x.date)
                why.append("**説明できない**")
            p(f"| {x.date} | {fmt(x.b_sp6row)}% | {fmt(x.b_nl_prevrow, 3)}% | {fmt(x.a_sp5d)}% | {fmt(x.a_nl_wow, 3)}%（{x.h41}） | {'・'.join(why)} |")
    p(f"\n日次→週次の変更で説明できない日: {unexplained if unexplained else 'なし'}")
    # H.4.1の週（水曜〜翌火曜）ごとに「その週に1日でも発火したか」で数える（2026-10-03 M-2レビュー）
    d2 = df.copy()
    dd = pd.to_datetime(d2.date)
    d2["week"] = (dd - pd.to_timedelta((dd.dt.weekday - 2) % 7, unit="D")).dt.strftime("%Y-%m-%d")
    wk = d2.groupby("week").agg(b=("b_hollow", "any"), a=("a_hollow", "any"))
    p(f"\n#### Hollow Rally をH.4.1の週（水曜〜翌火曜）単位で数えた件数（対象 {len(wk)}週）")
    p(f"- before（日次の判定）で発火した週: {int(wk.b.sum())}週")
    p(f"- after（週単位の判定）で発火した週: {int(wk.a.sum())}週")
    p(f"- 両方で発火 {int((wk.a & wk.b).sum())}週・afterだけ {int((wk.a & ~wk.b).sum())}週・beforeだけ {int((~wk.a & wk.b).sum())}週")
    text = "\n".join(lines)
    print(text)
    if args.md:
        with open(args.md, "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
    return 1 if unexplained else 0


if __name__ == "__main__":
    sys.exit(main())
