"""MACRO PULSE: 05_events.csv の CFNAI の行の値を、単月の CFNAI から 3ヶ月移動平均（CFNAIMA3）に置き換える
（一度だけの修復、2026-10-03 指示書M-2 STEP 6-1、[[MACRO-PULSE-CFNAI-MA3-SERIES-1]]）

INDICATOR_CONFIG の fred_id を CFNAIMA3 に変えても、refresh_monthly_indicators() は既に値がある観測月の行を
上書きしないため、既存の行（単月の値、2026-08月分まで）は残り、次の月の発表まで画面・スコアは単月の値のままになる。
この修復は、indicator が "Chicago Fed National Activity" の行の actual を、同じ観測月の CFNAIMA3 の
「その行の updated_at の時点に公表されていた版」（ALFRED）の値に置き換える（Claimsの修復と同じ方式。現在の版を使うと、
後からの改定値が過去のスコア推移に入るため）。release_date・updated_at・その他の列は変えない。
その時点で CFNAIMA3 の該当月が未公表だった行は触らない（一覧に出す）。05_weekly_analysis.csv は変更しない。
FRED_API_KEY が必要。

05_events.csv は .gitattributes の merge=ours のため、kaihatsu へ統合した後に kaihatsu 上で実行して commit する。

使い方:
  python scripts/analysis/macro_cfnai_ma3_rows_repair.py          # 確認だけ
  python scripts/analysis/macro_cfnai_ma3_rows_repair.py --apply  # 書き換える
"""
from __future__ import annotations

import argparse
import os
import sys

import pandas as pd
import requests

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EVENTS = os.path.join(REPO_ROOT, "docs", "market-monitor", "macro-pulse", "data", "05_events.csv")
IND = "Chicago Fed National Activity"


def alfred_versions(api_key: str, since: str) -> dict:
    """観測日 -> [(realtime_start, realtime_end, 値), ...]（since 以降に有効だった版）"""
    r = requests.get("https://api.stlouisfed.org/fred/series/observations", params=dict(
        series_id="CFNAIMA3", api_key=api_key, file_type="json",
        realtime_start=since, realtime_end="9999-12-31"), timeout=120)
    r.raise_for_status()
    out: dict = {}
    for x in r.json()["observations"]:
        if x["value"] != ".":
            out.setdefault(x["date"], []).append((x["realtime_start"], x["realtime_end"], float(x["value"])))
    return out


def value_known_at(versions: list, known_by: str):
    for rs, re_, v in versions:
        if rs <= known_by <= re_:
            return v
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    key = os.environ.get("FRED_API_KEY", "")
    if not key:
        print("FRED_API_KEY が無い")
        return 1
    ev = pd.read_csv(EVENTS, dtype=str).fillna("")
    rows = ev[ev["indicator"] == IND]
    since = min(u[:10] for u in rows["updated_at"] if u)
    vers = alfred_versions(key, since)
    changed, unpublished, same = [], [], 0
    for idx, r in rows.iterrows():
        known_by = r["updated_at"][:10]
        v = value_known_at(vers.get(r["release_date"], []), known_by)
        if v is None:
            unpublished.append((r["release_date"], known_by))
            continue
        if r["actual"] != "" and abs(float(r["actual"]) - v) < 1e-9:
            same += 1
            continue
        changed.append((idx, r["release_date"], known_by, r["actual"], v))
    print(f"CFNAIの行 {len(rows)}件: 置き換え {len(changed)}件・同じ値 {same}件・その時点で未公表 {len(unpublished)}件")
    for d0, kb in unpublished:
        print(f"  未公表で触らない: 観測月 {d0}（updated_at {kb}）")
    for _, d0, kb, old, new in changed[-8:]:
        print(f"  {d0}（{kb}時点の版）: {old} → {new}")
    if not args.apply:
        print("（確認のみ。--apply で書き換える）")
        return 0
    for idx, _, _, _, v in changed:
        ev.at[idx, "actual"] = str(v)
    ev.to_csv(EVENTS, index=False, encoding="utf-8")
    print("書き換えた")
    return 0


if __name__ == "__main__":
    sys.exit(main())
