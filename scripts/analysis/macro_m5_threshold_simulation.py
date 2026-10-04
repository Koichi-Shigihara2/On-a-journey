"""MACRO PULSE 試算（実装しない、2026-10-04 指示書M-5 STEP 7）: HY・Claims・Permits・Michigan の段を「過去10年の分布の中の位置」で決める方式

- スコア: 05_main.py の段階関数（今のゲージ・週次スナップショットと同じ。_compute_current_score() と同じ先読み除外・改定値・重み50%の規則）を
  1997-01-01 以降の毎日について計算する（高速に作り直したもの。抜き出した日付で _compute_current_score() と一致することを確かめる）。
  スコア推移の画面（補間）ではない
- 今（before）: 05_main.py の SIGNAL_STEPS。試算（after）: CFNAI・Sahm・Philly・YC はそのまま、HY・Claims・Permits・Michigan は段の数と点数を
  変えず、段の境目を「その日に分かっていた過去10年の観測の分布の、パーセンタイル q の値」にする。q は、今の閾値より（悪い側に）ある観測の
  割合を1990年以降の全観測で求めたもの（各段の割合が今の閾値の1990年以降の割合と同じになる）
- 分布: FRED の最新の版の全履歴（HY は common/macro_data/series/BAMLH0A0HYM2.json〈1996-12〜〉。1990年以降の割合も1996-12以降、
  過去10年の窓も2006-12までは短い）。窓は「その日の10年前より後」から「その日に計算に使える最新の観測日」まで
- 後退期: FRED USREC

使い方（リポジトリの根で、FRED_API_KEY を設定して）:
  python scripts/analysis/macro_m5_threshold_simulation.py [--cache DIR]
"""
from __future__ import annotations

import argparse
import bisect
import importlib.util
import json
import os
import random
import sys
import tempfile
from collections import Counter
from datetime import date, datetime, timedelta

import pandas as pd
import requests

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
spec = importlib.util.spec_from_file_location("m05", os.path.join(REPO, "src", "market", "macro_pulse", "05_main.py"))
m05 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m05)

KEYS = [("Yield Curve 10Y-2Y", "yc"), ("HY Spread", "hy"), ("Philadelphia Fed Manufacturing", "philly"),
        ("Chicago Fed National Activity", "cfnai"), ("Initial Claims 4W MA", "claims"), ("Building Permits", "cbcc2"),
        ("Michigan Consumer Sentiment", "cbcc"), ("Sahm Rule Recession Indicator", "sahm")]
WEIGHTS = {"yc": 20, "hy": 15, "philly": 18, "cfnai": 12, "claims": 10, "cbcc2": 10, "cbcc": 8, "sahm": 7}
SIM = {"hy": "BAMLH0A0HYM2", "claims": "IC4WSA", "cbcc2": "PERMIT", "cbcc": "UMCSENT"}
START = date(1997, 1, 1)


def fred(sid: str, key: str, cache: str) -> list:
    os.makedirs(cache, exist_ok=True)
    p = os.path.join(cache, f"fred_{sid}.json")
    if os.path.exists(p):
        return [tuple(x) for x in json.load(open(p, encoding="utf-8"))]
    if sid == "BAMLH0A0HYM2":
        d = json.load(open(os.path.join(REPO, "common", "macro_data", "series", f"{sid}.json"), encoding="utf-8"))
        d = d if isinstance(d, list) else d["records"]
        obs = sorted((r["as_of"], float(r["value"])) for r in d if r.get("value") is not None)
    else:
        r = requests.get("https://api.stlouisfed.org/fred/series/observations",
                         params={"series_id": sid, "api_key": key, "file_type": "json"}, timeout=60)
        r.raise_for_status()
        obs = [(o["date"], float(o["value"])) for o in r.json()["observations"] if o["value"] != "."]
    json.dump(obs, open(p, "w", encoding="utf-8"))
    return obs


class Calc:
    """_compute_current_score() の段階関数のスコアを毎日速く出す。"""

    def __init__(self, ev: pd.DataFrame):
        self.rows = {}
        for ind, _ in KEYS:
            r = ev[(ev.indicator == ind) & (ev.actual.astype(str).str.strip() != "")]
            out = []
            for _, x in r.iterrows():
                k = m05._parse_known_at_utc(x.get("known_at", "")) or m05._parse_updated_at_utc(x.get("updated_at", ""))
                rv = str(x.get("revised_actual", "")).strip()
                out.append((x.release_date, k, float(x.actual), float(rv) if rv else None,
                            m05._parse_known_at_utc(x.get("revised_at", ""))))
            out.sort(key=lambda t: t[0])
            self.rows[ind] = out
        self._keys = {i: [a[0] for a in self.rows[i]] for i in self.rows}

    def known(self, ind: str, d: date, n: int = 3) -> list:
        """d の時点で使える行（観測日の新しい順に最大 n 件）: [(観測日, 値)]。"""
        cut = m05._known_cutoff_utc(d)
        ds = d.isoformat()
        arr = self.rows[ind]
        i = bisect.bisect_right(self._keys[ind], ds)
        out = []
        while i > 0 and len(out) < n:
            i -= 1
            od, k, a, rv, rt = arr[i]
            if k is not None and k > cut:
                continue
            out.append((od, rv if (rv is not None and rt is not None and rt <= cut) else a))
        return out

    def score(self, d: date, bounds: dict | None = None) -> tuple:
        """(スコア or None, {key: 点数}, {key: 最新の観測日})。bounds={key: [境目...]} を渡すと、その key の段の境目を置き換える。"""
        tot_w, acc, pts, obs = 0, 0.0, {}, {}
        for ind, key in KEYS:
            k3 = self.known(ind, d)
            if not k3:
                continue
            val = k3[0][1]
            obs[key] = k3[0][0]
            vals = [v for _, v in reversed(k3)]
            tr = 0
            if len(vals) >= 2:
                ch = [vals[j] - vals[j - 1] for j in range(1, len(vals))]
                avg = sum(ch) / len(ch)
                tr = 1 if avg > 0 else -1 if avg < 0 else 0
            if bounds and key in bounds:
                st = json.loads(json.dumps(m05.SIGNAL_STEPS[key]))
                for t, x in zip(st["tiers"], bounds[key]):
                    t["x"] = x
                s = _step(st, val, tr)
            else:
                s = m05._step_score(key, val, tr)
            pts[key] = s
            tot_w += WEIGHTS[key]
            acc += s * WEIGHTS[key]
        if tot_w < m05.SCORE_MIN_WEIGHT:
            return None, pts, obs
        return round(acc / tot_w), pts, obs


def _step(d: dict, val: float, tr: int) -> int:
    for t in d["tiers"]:
        hit = val < t["x"] if t["cmp"] == "<" else val > t["x"] if t["cmp"] == ">" else val >= t["x"]
        if hit:
            if t.get("trend") and tr == t["trend"]["dir"]:
                return t["score"] + t["trend"]["add"]
            return t["score"]
    return d["else"]["score"]


def worse_share(cmp: str, x: float, vals: list) -> float:
    """今の閾値より悪い側にある観測の割合。"""
    if cmp == "<":
        return sum(1 for v in vals if v < x) / len(vals)
    if cmp == ">":
        return sum(1 for v in vals if v > x) / len(vals)
    return sum(1 for v in vals if v >= x) / len(vals)


def quantile_bound(cmp: str, q: float, window: list) -> float:
    """window の中で、悪い側の割合が q になる境目。"""
    s = sorted(window)
    n = len(s)
    if cmp == "<":
        k = min(n - 1, max(0, int(round(q * n))))
        return s[k]
    k = min(n - 1, max(0, int(round((1 - q) * n)) - 1))
    return s[k]


def episodes(days: list) -> list:
    out = []
    for d in days:
        if out and (date.fromisoformat(d) - date.fromisoformat(out[-1][1])).days == 1:
            out[-1] = (out[-1][0], d, out[-1][2] + 1)
        else:
            out.append((d, d, 1))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=os.path.join(tempfile.gettempdir(), "macro_m5_cache"))
    ap.add_argument("--end", default=None)
    args = ap.parse_args()
    key = os.environ.get("FRED_API_KEY", "")
    ev = m05.load_events()
    calc = Calc(ev)
    end = date.fromisoformat(args.end) if args.end else m05.default_target_date(datetime.utcnow())

    # 1) 再計算が _compute_current_score() と一致するか（抜き出した日付）
    rng = random.Random(0)
    sample = [START + timedelta(days=rng.randrange((end - START).days)) for _ in range(25)] + [end]
    mism = [(d, calc.score(d)[0], m05._compute_current_score(ev, d)["score"]) for d in sample]
    mism = [x for x in mism if x[1] != x[2]]
    print(f"再計算の確かめ: {len(sample)}日で _compute_current_score() と不一致 {len(mism)}" + (f"（{mism[:3]}）" if mism else ""))

    # 2) 段の境目のパーセンタイル（1990年以降の割合）
    series = {k: fred(s, key, args.cache) for k, s in SIM.items()}
    qs = {}
    print("\n## 段の境目（今の閾値と、1990年以降の観測の中の位置）")
    print("| 指標 | 段（悪い順） | 今の閾値 | 悪い側の割合（1990年以降、q） | 今日の過去10年の分布での境目 |")
    print("|---|---|---|---|---|")
    for k, obs in series.items():
        v90 = [v for dd, v in obs if dd >= "1990-01-01"]
        tiers = m05.SIGNAL_STEPS[k]["tiers"]
        qs[k] = [worse_share(t["cmp"], t["x"], v90) for t in tiers]
    usrec = fred("USREC", key, args.cache)
    rec_month = {dd[:7]: v for dd, v in usrec}

    def bounds_at(d: date, obs_last: dict) -> dict:
        b = {}
        for k, obs in series.items():
            lo = (d - timedelta(days=3652)).isoformat()
            hi = obs_last.get(k) or d.isoformat()
            dates = [x[0] for x in obs]
            i0, i1 = bisect.bisect_right(dates, lo), bisect.bisect_right(dates, hi)
            win = [x[1] for x in obs[i0:i1]]
            if len(win) < 12:
                continue
            b[k] = [quantile_bound(t["cmp"], q, win) for t, q in zip(m05.SIGNAL_STEPS[k]["tiers"], qs[k])]
        return b

    _, _, obs_end = calc.score(end)
    b_end = bounds_at(end, obs_end)
    for k in series:
        for j, t in enumerate(m05.SIGNAL_STEPS[k]["tiers"]):
            now_b = f"{b_end[k][j]:.4g}" if k in b_end else "—"
            print(f"| {k} | {t['signal']}（{t['score']}点, {t['cmp']}） | {t['x']:g} | {100 * qs[k][j]:.1f}% | {now_b} |")

    # 3) 毎日のスコア（before / after）
    before, after, mi_tier = {}, {}, Counter()
    d = START
    while d <= end:
        s0, p0, ob = calc.score(d)
        bnd = bounds_at(d, ob)
        s1, p1, _ = calc.score(d, bnd)
        ds = d.isoformat()
        before[ds], after[ds] = s0, s1
        if d.day == 1 and "cbcc" in p0:
            mi_tier[("before", p0["cbcc"])] += 1
            mi_tier[("after", p1["cbcc"])] += 1
        d += timedelta(days=1)

    print(f"\n## 今日（計算日 {end}）のスコア: before {before[end.isoformat()]} → after {after[end.isoformat()]}")
    s0, p0, ob = calc.score(end)
    s1, p1, _ = calc.score(end, b_end)
    print("指標ごとの点数（before → after）: " + "・".join(f"{k} {p0.get(k)}→{p1.get(k)}" for _, k in KEYS))

    both = [x for x in before if before[x] is not None and after[x] is not None]
    diff = [(x, after[x] - before[x]) for x in both if after[x] != before[x]]
    print(f"\n## 1997年以降のスコア（段階関数、毎日）の変化: {len(both)}日中 {len(diff)}日が変わる")
    if diff:
        mx = max(diff, key=lambda t: abs(t[1]))
        print(f"最大の変化: {mx[1]:+d}（{mx[0]}、{before[mx[0]]}→{after[mx[0]]}）。大きさ: "
              + "・".join(f"{k}点 {v}日" for k, v in sorted(Counter(abs(x[1]) for x in diff).items())))
        print("年ごとの平均の変化: " + "・".join(
            f"{y} {sum(v for x, v in diff if x[:4] == y) / max(1, sum(1 for x in both if x[:4] == y)):+.1f}"
            for y in sorted({x[:4] for x in both})))

    print("\n## 後退の前後12か月で、52・70以上になった最初の日（差＝その日−開始日）")
    print("| 後退の開始 | ≥52 before | ≥52 after | ≥70 before | ≥70 after | 期間中の最大 before / after |")
    print("|---|---|---|---|---|---|")
    for s in ("2001-04-01", "2008-01-01", "2020-03-01"):
        sd = date.fromisoformat(s)
        win = [x for x in both if (sd - timedelta(days=365)).isoformat() <= x <= (sd + timedelta(days=365)).isoformat()]

        def first(series_, th):
            h = next((x for x in win if series_[x] >= th), None)
            return "なし" if h is None else f"{h}（{(date.fromisoformat(h) - sd).days:+d}）"
        print(f"| {s} | {first(before, 52)} | {first(after, 52)} | {first(before, 70)} | {first(after, 70)} | "
              f"{max(before[x] for x in win)} / {max(after[x] for x in win)} |")

    print("\n## 誤報（後退期でない日に52以上）")
    starts = [dd for (dd, v), (_, pv) in zip(usrec[1:], usrec[:-1]) if v == 1 and pv == 0]
    ends = [dd for (dd, v), (_, pv) in zip(usrec[1:], usrec[:-1]) if v == 0 and pv == 1]
    lead, tail = set(), set()
    for s in starts:
        for k in range(366):
            lead.add((date.fromisoformat(s) - timedelta(days=k)).isoformat())
    for e in ends:
        for k in range(31):
            tail.add((date.fromisoformat(e) + timedelta(days=k)).isoformat())
    print("| | 全体（回・日） | 後退の開始前12か月以内・終了直後を除く（回・日） |")
    print("|---|---|---|")
    for lab, ser in (("before", before), ("after", after)):
        out = [x for x in both if ser[x] >= 52 and rec_month.get(x[:7], 0) == 0]
        eps = episodes(out)
        oth = [e for e in eps if not (e[0] in lead or e[1] in lead) and e[0] not in tail]
        print(f"| {lab} | {len(eps)}回・{len(out)}日 | {len(oth)}回・{sum(e[2] for e in oth)}日 |")

    print("\n## Michigan（1997年以降の各月1日）の段ごとの月の割合")
    print("| 段（点数） | before | after |")
    print("|---|---|---|")
    nb = sum(v for (k, _), v in mi_tier.items() if k == "before")
    for sc in (82, 72, 60, 30):
        print(f"| {sc}点 | {100 * mi_tier[('before', sc)] / nb:.1f}% | {100 * mi_tier[('after', sc)] / nb:.1f}% |")
    print("Michigan の最良の段は30点（中立）で、段の数と点数を変えない方式では「拡張」の段（30点未満）に入る月は before・after とも 0%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
