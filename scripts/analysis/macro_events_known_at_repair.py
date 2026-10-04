"""MACRO PULSE: 05_events.csv の既存の行に known_at・known_at_source を埋める（一度だけの修復、2026-10-04 指示書M-3 STEP 1）

[[MACRO-PULSE-HISTORY-IMPORT-UPDATED-AT-1]]: 2026-03-28/29に一括で取り込んだ過去分の行は updated_at が取り込み日のため、
先読み除外（その時点で使えた行だけを使う）で2026-03-29より前が全て「未取得」になっていた。known_at（その値が使えるように
なった時刻、UTC）を次の順で埋める。updated_at は変えない。
  1. ALFRED（FREDの版の履歴）でその観測の初回公表日が分かる行: 初回公表日の米国東部時間 08:30 → source=alfred
  2. 分からない行（ALFREDの記録の始まりより前の観測、FREDに無い指標、予定の枠の日付の行）:
     観測日＋公表までの日数（macro_alfred_lib.LAG_DAYS。予定の枠の日付の行はその日）の米国東部時間 08:30 → source=estimated
  3. updated_at（UTC）のほうが早い行は updated_at → source=written
既に known_at がある行（M-3以降の実行が書いた行）は変えない。

注意: known_at の時刻（米国東部時間 08:30）は便宜上の値で、日次の系列（T10Y2Y・HY・VIX など）では実際の公表より早い
（ALFREDの初回公表日はほぼ観測日の当日で、値はその日の取引終了後に出る）。日単位の締め（計算日の米国東部時間 23:59:59）で
使う限り結果は変わらない。時刻単位で使う場合は見直す。

あわせて、取り込み分（updated_at=2026-03-28/29）の行の actual が初回公表の値か改定後の値かを数えて表示する（M-3 STEP 1-5）。

05_events.csv は .gitattributes の merge=ours のため、kaihatsu へ統合した後に kaihatsu 上で実行して commit する。
使い方（FRED_API_KEY が必要）:
  python scripts/analysis/macro_events_known_at_repair.py [--cache DIR]          # 確認だけ
  python scripts/analysis/macro_events_known_at_repair.py [--cache DIR] --apply  # 書き換える
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from collections import Counter
from datetime import datetime

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import macro_alfred_lib as A  # noqa: E402

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EVENTS = os.path.join(REPO_ROOT, "docs", "market-monitor", "macro-pulse", "data", "05_events.csv")
IMPORT_DAYS = ("2026-03-28", "2026-03-29")


def updated_utc_str(u: str) -> str:
    try:
        return datetime.strptime(u.strip()[:19], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return ""


def nfp_value(data: dict, obs: str, when: str | None):
    """PAYEMSの水準から、obs月の前月比（人）。whenの時点の版（Noneなら最新の版）。"""
    prev = sorted(d for d in data["versions"] if d < obs)
    if not prev:
        return None
    p = prev[-1]
    if when is None:
        a, b = A.latest_version(data, obs), A.latest_version(data, p)
        return None if not a or not b else round((a[1] - b[1]) * 1000)
    a, b = A.value_at(data, obs, when), A.value_at(data, p, when)
    return None if a is None or b is None else round((a - b) * 1000)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--cache", default=os.path.join(tempfile.gettempdir(), "macro_alfred_cache"))
    args = ap.parse_args()
    key = os.environ.get("FRED_API_KEY", "")
    if not key:
        print("FRED_API_KEY が無い")
        return 1
    ev = pd.read_csv(EVENTS, dtype=str).fillna("")
    for c in ("known_at", "known_at_source"):
        if c not in ev.columns:
            ev[c] = ""
    data = {s: A.fetch_versions(s, key, args.cache) for s in sorted(set(A.SERIES_FOR_INDICATOR.values()))}
    stats = Counter()
    imp = Counter()
    for idx, r in ev.iterrows():
        if r["known_at"].strip():
            stats[(r["indicator"], "既存のまま")] += 1
            continue
        ind, d = r["indicator"], r["release_date"]
        sid = A.SERIES_FOR_INDICATOR.get(ind)
        known, src = None, None
        if sid and d in data[sid]["versions"]:
            fr = A.first_release(data[sid], d)
            if fr:
                known, src = A.et_0830_utc(fr[0]), "alfred"
                if r["updated_at"][:10] in IMPORT_DAYS and r["actual"] != "":
                    a = float(r["actual"])
                    init = nfp_value(data[sid], d, fr[0]) if ind == "NFP" else fr[1]
                    last = nfp_value(data[sid], d, None) if ind == "NFP" else A.latest_version(data[sid], d)[1]
                    eq_i = init is not None and abs(a - init) < 1e-6
                    eq_l = last is not None and abs(a - last) < 1e-6
                    imp[(ind, "初回=最新（改定なし）" if eq_i and eq_l else "初回の値" if eq_i else "改定後の値" if eq_l else "どちらとも違う")] += 1
            else:
                known, src = A.et_0830_utc(A.plus_days(d, A.LAG_DAYS.get(ind, 1))), "estimated"
        elif sid and d[8:] != "01" and ind not in ("Yield Curve 10Y-2Y", "HY Spread", "VIX", "Michigan Inflation 5Y", "Initial Claims 4W MA"):
            # 予定の枠の日付の行（Michigan・Permits・NFP等）: その日を公表日とみなす
            known, src = A.et_0830_utc(d), "estimated"
        else:
            known, src = A.et_0830_utc(A.plus_days(d, A.LAG_DAYS.get(ind, 1))), "estimated"
        u = updated_utc_str(r["updated_at"])
        if u and u < known:
            known, src = u, "written"
        ev.at[idx, "known_at"] = known
        ev.at[idx, "known_at_source"] = src
        stats[(ind, src)] += 1
    print("known_at の根拠（指標ごと）:")
    for ind in sorted({k[0] for k in stats}):
        print(f"  {ind}: " + "・".join(f"{s} {n}" for (i, s), n in sorted(stats.items()) if i == ind))
    tot = Counter()
    for (i, s), n in stats.items():
        tot[s] += n
    print("合計:", dict(tot))
    print("取り込み分（updated_at=2026-03-28/29）の actual と ALFRED の比較（ALFREDに初回公表がある行のみ）:")
    for ind in sorted({k[0] for k in imp}):
        print(f"  {ind}: " + "・".join(f"{s} {n}" for (i, s), n in sorted(imp.items()) if i == ind))
    t2 = Counter()
    for (i, s), n in imp.items():
        t2[s] += n
    print("合計:", dict(t2))
    if not args.apply:
        print("（確認のみ。--apply で書き換える）")
        return 0
    ev.to_csv(EVENTS, index=False, encoding="utf-8")
    print("書き換えた")
    return 0


if __name__ == "__main__":
    sys.exit(main())
