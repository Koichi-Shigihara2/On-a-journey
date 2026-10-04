"""MACRO PULSE 調査（読み取り専用、2026-10-04 指示書M-4 STEP 1）: 景気後退リスクスコアと NBER の景気後退期（FRED USREC）の比較

- スコア: 画面のスコア推移と同じ computeScoreAsOf()（過去の日付は補間あり、先読み除外あり）を、実ブラウザ（東京）で毎日計算する。
  今のゲージ・週次スナップショットが使う 05_main.py の段階関数（補間なし）とは計算が違う
- 後退期: USREC（月次、1＝後退期）。後退の開始日＝USRECが0→1になった月の1日
- 各後退の開始の前後12か月で、スコアが 30・52・70 以上になった最初の日と開始日との差（負＝開始より前）
- 後退期でない日（USREC=0）にスコアが 52・70 以上だった期間（連続する日をまとめて1回）。後退の開始前12か月以内のものは分けて数える
- 8指標の開始（各指標で、値のある行の known_at の最初）。使える指標の数ごとの期間

使い方（リポジトリの根で、FRED_API_KEY を設定して）:
  python scripts/analysis/macro_m4_score_vs_recession.py [--cache DIR]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
from collections import Counter
from datetime import date, timedelta

import pandas as pd
import requests

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EVENTS = os.path.join(REPO, "docs", "market-monitor", "macro-pulse", "data", "05_events.csv")
INDS = ["Yield Curve 10Y-2Y", "HY Spread", "Philadelphia Fed Manufacturing", "Chicago Fed National Activity",
        "Initial Claims 4W MA", "Building Permits", "Michigan Consumer Sentiment", "Sahm Rule Recession Indicator"]
THRESHOLDS = (30, 52, 70)


def fred(series_id: str, key: str, cache: str) -> list:
    os.makedirs(cache, exist_ok=True)
    p = os.path.join(cache, f"fred_{series_id}.json")
    if os.path.exists(p):
        return json.load(open(p, encoding="utf-8"))
    r = requests.get("https://api.stlouisfed.org/fred/series/observations",
                     params={"series_id": series_id, "api_key": key, "file_type": "json"}, timeout=60)
    r.raise_for_status()
    obs = [(o["date"], float(o["value"])) for o in r.json()["observations"] if o["value"] != "."]
    json.dump(obs, open(p, "w", encoding="utf-8"))
    return obs


def daily_scores(start: str, end: str) -> dict:
    port = 8781
    srv = subprocess.Popen([sys.executable, "-m", "http.server", str(port), "--directory", os.path.join(REPO, "docs")],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"http://localhost:{port}/market-monitor/macro-pulse/index.html"
    try:
        for _ in range(50):
            try:
                urllib.request.urlopen(url, timeout=1)
                break
            except Exception:
                time.sleep(0.2)
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            b = p.chromium.launch()
            pg = b.new_context(timezone_id="Asia/Tokyo").new_page()
            pg.goto(url)
            pg.wait_for_function("typeof IND_INDEX!=='undefined' && Object.keys(IND_INDEX).length>5", timeout=60000)
            res = pg.evaluate("""([s, e]) => { const o = {};
                const [y0,mo0,d0] = s.split('-').map(Number), [y1,mo1,d1] = e.split('-').map(Number);
                for (let t = Date.UTC(y0,mo0-1,d0); t <= Date.UTC(y1,mo1-1,d1); t += 86400000) {
                  const ds = new Date(t).toISOString().slice(0,10); const [y,m,d] = ds.split('-').map(Number);
                  o[ds] = computeScoreAsOf(new Date(y, m-1, d, 23, 59, 59)); }
                return o; }""", [start, end])
            b.close()
        return res
    finally:
        srv.terminate()


def episodes(days: list) -> list:
    """連続する日付をまとめて [(開始, 終了, 日数)]。"""
    out = []
    for d in days:
        if out and (date.fromisoformat(d) - date.fromisoformat(out[-1][1])).days == 1:
            out[-1] = (out[-1][0], d, out[-1][2] + 1)
        else:
            out.append((d, d, 1))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=os.path.join(tempfile.gettempdir(), "macro_m4_cache"))
    args = ap.parse_args()
    key = os.environ.get("FRED_API_KEY", "")
    usrec = fred("USREC", key, args.cache)
    rec_month = {d[:7]: v for d, v in usrec}
    starts, ends = [], []
    prev = 0.0
    for d, v in usrec:
        if v == 1 and prev == 0:
            starts.append(d)
        if v == 0 and prev == 1:
            ends.append(d)
        prev = v

    # 指標の開始（値のある行の known_at の最初）
    ev = pd.read_csv(EVENTS, dtype=str).fillna("")
    first = {}
    for ind in INDS:
        r = ev[(ev.indicator == ind) & (ev.actual != "")]
        k = r.known_at.where(r.known_at != "", r.updated_at)
        first[ind] = (r.release_date.min(), k.min()[:10])
    print("## 8指標の開始（値のある行の最古の観測日・最初に使えるようになった日〈known_at〉）")
    print("| 指標 | 最古の観測日 | 最初の known_at |")
    print("|---|---|---|")
    for ind in INDS:
        print(f"| {ind} | {first[ind][0]} | {first[ind][1]} |")
    cut = sorted(v[1] for v in first.values())
    print("\n使える指標の数ごとの期間: " + "・".join(f"{i + 1}指標〜{c}" for i, c in enumerate(cut)))

    start = "1949-01-01"
    end = (date.today() - timedelta(days=1)).isoformat()
    sc = daily_scores(start, end)
    days = sorted(d for d, v in sc.items() if v is not None)
    n_ind = lambda d: sum(1 for c in cut if c <= d)  # noqa: E731

    print("\n## 後退期の開始の前後12か月で、スコアが閾値以上になった最初の日（差＝その日−開始日、日数）")
    print("| 後退の開始 | 終了（USREC=0に戻った月） | その時点で使える指標 | 開始日のスコア | ≥30 | ≥52 | ≥70 | 期間中の最大 |")
    print("|---|---|---|---|---|---|---|---|")
    for i, s in enumerate(starts):
        if s < start:
            continue
        sd = date.fromisoformat(s)
        lo, hi = (sd - timedelta(days=365)).isoformat(), (sd + timedelta(days=365)).isoformat()
        win = [d for d in days if lo <= d <= hi]
        cells = []
        for th in THRESHOLDS:
            hit = next((d for d in win if sc[d] >= th), None)
            if hit is None:
                cells.append("なし")
            else:
                note = "（窓の最初から）" if hit == win[0] else ""
                cells.append(f"{hit}（{(date.fromisoformat(hit) - sd).days:+d}）{note}")
        mx = max(win, key=lambda d: sc[d]) if win else None
        e = ends[i] if i < len(ends) else "継続中"
        print(f"| {s} | {e} | {n_ind(s)} | {sc.get(s)} | {cells[0]} | {cells[1]} | {cells[2]} | "
              f"{sc[mx] if mx else '—'}（{mx}） |")

    print("\n## 後退期でない日にスコアが52・70以上だった期間（誤報の候補）")
    lead_ok = set()
    for s in starts:
        sd = date.fromisoformat(s)
        for k in range(366):
            lead_ok.add((sd - timedelta(days=k)).isoformat())
    # 後退の終了月の1日から30日以内に始まる期間（後退中から続く高止まり）
    tail_ok = set()
    for e_ in ends:
        ed = date.fromisoformat(e_)
        for k in range(31):
            tail_ok.add((ed + timedelta(days=k)).isoformat())
    for th in (52, 70):
        out = [d for d in days if sc[d] >= th and rec_month.get(d[:7], 0) == 0]
        eps = episodes(out)
        pre = [e for e in eps if e[0] in lead_ok or e[1] in lead_ok]
        tail = [e for e in eps if e not in pre and e[0] in tail_ok]
        other = [e for e in eps if e not in pre and e not in tail]
        other8 = [e for e in other if n_ind(e[0]) == 8]
        print(f"\n### ≥{th}: {len(eps)}回・{len(out)}日（後退の開始前12か月以内にかかる {len(pre)}回・{sum(e[2] for e in pre)}日、"
              f"後退の終了直後から続く {len(tail)}回・{sum(e[2] for e in tail)}日、それ以外 {len(other)}回・"
              f"{sum(e[2] for e in other)}日〈うち8指標の時期 {len(other8)}回・{sum(e[2] for e in other8)}日〉）")
        print("| 開始 | 終了 | 日数 | 最大 | 使える指標 | 区分 |")
        print("|---|---|---|---|---|---|")
        for e in eps:
            seg = [d for d in out if e[0] <= d <= e[1]]
            kind = "後退の開始前12か月以内" if e in pre else "後退の終了直後から" if e in tail else "それ以外"
            print(f"| {e[0]} | {e[1]} | {e[2]} | {max(sc[d] for d in seg)} | {n_ind(e[0])} | {kind} |")

    print("\n## 使える指標の数ごとの、スコアの分布（後退期／後退期でない）")
    print("| 使える指標 | 期間 | 日数 | 後退期の平均 | 後退期でない日の平均 | ≥52の日（後退期／でない） |")
    print("|---|---|---|---|---|---|")
    for k in sorted({n_ind(d) for d in days}):
        seg = [d for d in days if n_ind(d) == k]
        r = [sc[d] for d in seg if rec_month.get(d[:7], 0) == 1]
        nr = [sc[d] for d in seg if rec_month.get(d[:7], 0) == 0]
        f = lambda xs: f"{sum(xs) / len(xs):.1f}" if xs else "—"  # noqa: E731
        print(f"| {k} | {seg[0]}〜{seg[-1]} | {len(seg)} | {f(r)} | {f(nr)} | "
              f"{sum(1 for x in r if x >= 52)}／{sum(1 for x in nr if x >= 52)} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
