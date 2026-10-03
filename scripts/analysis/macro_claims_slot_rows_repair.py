"""MACRO PULSE: Initial Claims の「予定の枠」の行を観測日（週末日）へ移す（一度だけの修復、2026-10-03 指示書M-2 STEP 2）

[[MACRO-PULSE-CLAIMS-RELEASE-ID-WRONG-1]]: fred_release_id=321（Empire State Manufacturing Survey）の誤りで、
refresh_monthly_indicators() が最新週の値を毎月15日ごろの予定の枠の行（ic4wsa_YYYY-MM-15 等）へ書いていた。
コードの修正後もこれらの行が残ると、未来日付の行が今日の計算から外れ続け、dedupe_new_rows() が観測日の
正しい行を「同じ値の重複」として捨てる。

各行の値がどの週の観測かは、ALFRED（FREDの版の履歴）で「その行を書いた時点〈updated_at〉までに公表されていた
観測のうち、値が一致するもの」を探して決める。値・updated_at（書き込み時刻＝その値が分かっていた時刻）・
その他の列はそのまま残し、release_date と event_id だけを観測日に変える。観測日の行が既にある場合は枠の行を消す。
05_indicator_schedule.csv の Claims の予定（321由来の月1回の日付）のうち、今日以降の行も消す
（次の update-schedule で180の週次の日付が入る）。05_weekly_analysis.csv（過去の週次スナップショット）は変更しない。

使い方:
  FRED_API_KEY を設定して
  python scripts/analysis/macro_claims_slot_rows_repair.py          # 確認だけ（何も書かない）
  python scripts/analysis/macro_claims_slot_rows_repair.py --apply  # 書き換える
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime

import pandas as pd
import requests

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(REPO_ROOT, "docs", "market-monitor", "macro-pulse", "data")
EVENTS = os.path.join(DATA_DIR, "05_events.csv")
SCHEDULE = os.path.join(DATA_DIR, "05_indicator_schedule.csv")
IND = "Initial Claims 4W MA"


def alfred_first_vintages(api_key: str) -> dict:
    """観測日 -> [(公表日, 値), ...]（その観測の版の履歴）"""
    r = requests.get("https://api.stlouisfed.org/fred/series/observations", params=dict(
        series_id="IC4WSA", api_key=api_key, file_type="json",
        realtime_start="2026-01-01", realtime_end="9999-12-31", observation_start="2025-12-01"), timeout=60)
    r.raise_for_status()
    out: dict = {}
    for x in r.json()["observations"]:
        if x["value"] != ".":
            out.setdefault(x["date"], []).append((x["realtime_start"], float(x["value"])))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    key = os.environ.get("FRED_API_KEY", "")
    if not key:
        print("FRED_API_KEY が無い")
        return 1
    vint = alfred_first_vintages(key)
    ev = pd.read_csv(EVENTS, dtype=str).fillna("")
    claims = ev[ev["indicator"] == IND]
    slot = claims[pd.to_datetime(claims["release_date"]).dt.weekday != 5]  # 観測日は土曜（週末）
    existing = set(claims["release_date"])
    plan = []
    for idx, r in slot.iterrows():
        known_by = r["updated_at"][:10]
        val = float(r["actual"])
        cands = [obs for obs, vs in vint.items()
                 if any(rt <= known_by and abs(v - val) < 1e-6 for rt, v in vs)]
        # 書き込み時点で最新だった観測（最も新しいもの）を採る
        cands = [c for c in cands if c <= known_by]
        if not cands:
            plan.append((idx, r["release_date"], val, None, "判定不能（触らない）"))
            continue
        obs = max(cands)
        action = "削除（観測日の行あり）" if obs in existing else "移動"
        plan.append((idx, r["release_date"], val, obs, action))
    for p in plan:
        print(f"{p[1]}  actual={p[2]:.0f}  → 観測日 {p[3]}  {p[4]}")
    sch = pd.read_csv(SCHEDULE, dtype=str).fillna("")
    today = date.today().isoformat()
    drop_sch = sch[(sch["indicator"] == IND) & (sch["release_date"] >= today)]
    print(f"予定表から消すClaimsの行: {drop_sch['release_date'].tolist()}")
    if not args.apply:
        print("（確認のみ。--apply で書き換える）")
        return 0
    for idx, old, val, obs, action in plan:
        if obs is None:
            continue
        if action.startswith("削除"):
            ev = ev.drop(index=idx)
        else:
            ev.at[idx, "release_date"] = obs
            ev.at[idx, "event_id"] = f"ic4wsa_{obs}"
            existing.add(obs)
    ev = ev.sort_values(["release_date", "indicator"]).reset_index(drop=True)
    ev.to_csv(EVENTS, index=False, encoding="utf-8")
    sch = sch.drop(index=drop_sch.index)
    sch.to_csv(SCHEDULE, index=False, encoding="utf-8")
    print("書き換えた")
    return 0


if __name__ == "__main__":
    sys.exit(main())
