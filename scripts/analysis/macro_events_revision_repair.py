"""MACRO PULSE: 05_events.csv の既存の行を「actual=初回公表の値、revised_actual=最新の値」にする
（一度だけの修復、2026-10-04 指示書M-3 STEP 2、[[MACRO-PULSE-REVISION-NOT-APPLIED-1]]）

観測日に置かれた行（release_date が FRED の観測日）について、ALFRED（FREDの版の履歴）で
  - 初回公表の値が分かる行: actual を初回公表の値にする
  - 最新の版の値が actual（初回公表の値、または初回が分からない行は今の actual）と違う行:
    revised_actual に最新の値、revised_at に最新の版の公表日の米国東部時間 08:30（UTC）を入れる
**近似**: 途中の改定（初回と最新の間の版）は持たない。過去のある日の時点の値は「その日が最新の版の公表日より前なら
初回公表の値、後なら最新の値」になり、途中の版の値は再現しない。
NFP は PAYEMS の水準から前月比（人）を作る（初回: 初回公表日の版での当月−前月、最新: 最新の版での当月−前月）。
予定の枠の日付の行（Michigan・Permits・NFP 等。release_date が観測日でない行）は観測日へ移さず、変えずに件数だけを出す。
FRED に無い指標（CB Consumer Confidence・Conference Board LEI）も変えない。
macro_events_known_at_repair.py（STEP 1）の後に実行する（known_at は変えない）。

05_events.csv は merge=ours のため、kaihatsu へ統合した後に kaihatsu 上で実行して commit する。
使い方（FRED_API_KEY が必要）:
  python scripts/analysis/macro_events_revision_repair.py [--cache DIR]          # 確認だけ
  python scripts/analysis/macro_events_revision_repair.py [--cache DIR] --apply  # 書き換える
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from collections import Counter

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import macro_alfred_lib as A  # noqa: E402
from macro_events_known_at_repair import EVENTS, nfp_value  # noqa: E402


def fmt(v) -> str:
    """整数の値は小数点なしで書く（例: 1394.0 → '1394'）。"""
    f = float(v)
    return str(int(f)) if f.is_integer() else str(v)


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
    for c in ("revised_actual", "revised_at"):
        if c not in ev.columns:
            ev[c] = ""
    data = {s: A.fetch_versions(s, key, args.cache) for s in sorted(set(A.SERIES_FOR_INDICATOR.values()))}
    st = Counter()
    for idx, r in ev.iterrows():
        ind, d = r["indicator"], r["release_date"]
        sid = A.SERIES_FOR_INDICATOR.get(ind)
        if r["actual"] == "":
            st[(ind, "値なし")] += 1
            continue
        if not sid:
            st[(ind, "FREDに無い指標（変えない）")] += 1
            continue
        if d not in data[sid]["versions"]:
            st[(ind, "予定の枠などで観測日でない（変えない）")] += 1
            continue
        fr = A.first_release(data[sid], d)
        lv = A.latest_version(data[sid], d)
        if ind == "NFP":
            init = nfp_value(data[sid], d, fr[0]) if fr else None
            last = nfp_value(data[sid], d, None)
        else:
            init = fr[1] if fr else None
            last = lv[1] if lv else None
        old = float(r["actual"])
        base = old
        if init is not None:
            base = float(init)
            if abs(base - old) > 1e-9:
                ev.at[idx, "actual"] = fmt(init)
                st[(ind, "actualを初回公表の値に変更")] += 1
            else:
                st[(ind, "actualは初回公表の値のまま")] += 1
        else:
            st[(ind, "初回公表が不明（actualは変えない）")] += 1
        if last is not None and abs(float(last) - base) > 1e-9:
            ev.at[idx, "revised_actual"] = fmt(last)
            ev.at[idx, "revised_at"] = A.et_0830_utc(lv[0])
            st[(ind, "revised_actualを設定")] += 1
        else:
            ev.at[idx, "revised_actual"] = ""
            ev.at[idx, "revised_at"] = ""
    for ind in sorted({k[0] for k in st}):
        print(f"  {ind}: " + "・".join(f"{s} {n}" for (i, s), n in sorted(st.items()) if i == ind))
    tot = Counter()
    for (i, s), n in st.items():
        tot[s] += n
    print("合計:", dict(tot))
    if not args.apply:
        print("（確認のみ。--apply で書き換える）")
        return 0
    ev.to_csv(EVENTS, index=False, encoding="utf-8")
    print("書き換えた")
    return 0


if __name__ == "__main__":
    sys.exit(main())
