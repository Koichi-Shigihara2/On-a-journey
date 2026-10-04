"""MACRO PULSE: 05_events.csv の「予定の枠」の行（NFP・Building Permits・Michigan Consumer Sentiment）を観測日へ移す
（一度だけの修復、2026-10-04 指示書M-3b STEP 2、[[MACRO-PULSE-SLOT-ROWS-1]]）

枠の行の値がどの観測のものかは、ALFRED で「その行を書いた日（updated_at の米国東部時間の暦日。無ければ前日）に有効だった版の値が
一致する観測」を探して決める（Claims の修復〈macro_claims_slot_rows_repair.py〉と同じ方式）。枠の行の known_at は M-3 STEP 1 の修復で
枠の日付（推定）にしたもので、書いた時刻ではないため照合には使わない。NFP は PAYEMS の水準からその日の版の前月比を作って比べる。
- 候補は書いた日の120日前以降の観測に限る（何十年も前の同じ値の観測と一致させない）
- 一致する観測が無い行・候補が複数の行: 移さない（件数と例を出す）。ただしNFPで一致しない行（2026-10-04の判断）は、発表月の前月を観測月とし、
  その観測月の1日の行が既にあれば枠の行を消し、無ければ枠の行は動かさず known_at＝updated_at（UTC）・known_at_source＝written にする
- 移す先の観測日の行が既にある場合: 値が同じなら、既存の行（known_at＝ALFREDの初回公表、枠の行より早いか同じ）を残して枠の行を消す。
  枠の行の値が既存の行の revised_actual（同じ観測の改定値）と同じ場合も枠の行を消し、既存の行の revised_at を、既存の revised_at と
  枠の行の updated_at（UTC）の早いほうにする（2026-10-04の判断。Permits 枠03-17・05-19）。
  **それ以外で値が違う場合は何も書かずに一覧を出して終了する**（指示書M-3bの停止条件）
- 観測日の行が無い場合: 枠の行を観測日へ移し、M-3 の修復と同じく known_at＝初回公表日の米国東部時間 08:30（alfred）、
  actual＝初回公表の値、revised_actual・revised_at＝最新の版（初回と違うとき）にする。その他の列（sp500_t0 等）は変えない

05_events.csv は merge=ours のため、kaihatsu へ統合した後に kaihatsu 上で実行して commit する。
使い方（FRED_API_KEY が必要）:
  python scripts/analysis/macro_events_slot_rows_repair.py [--cache DIR]          # 確認だけ
  python scripts/analysis/macro_events_slot_rows_repair.py [--cache DIR] --apply  # 書き換える
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import macro_alfred_lib as A  # noqa: E402
from macro_events_known_at_repair import EVENTS, nfp_value  # noqa: E402
from macro_events_revision_repair import fmt  # noqa: E402

TARGETS = {"Building Permits": "PERMIT", "Michigan Consumer Sentiment": "UMCSENT", "NFP": "PAYEMS"}
SLUG = {"Building Permits": "permit", "Michigan Consumer Sentiment": "mich_sent", "NFP": "nfp"}  # make_event_id()と同じ
WINDOW_DAYS = 120  # 候補は書いた日の120日前以降の観測（月次の公表は観測から最大約50日後）
NY = ZoneInfo("America/New_York")


def updated_utc(updated_at: str) -> str:
    return datetime.strptime(updated_at.strip()[:19], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")


def written_day(updated_at: str) -> str:
    u = datetime.strptime(updated_at.strip()[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return u.astimezone(NY).date().isoformat()


def value_of(ind: str, data: dict, obs: str, when):
    return nfp_value(data, obs, when) if ind == "NFP" else (
        A.value_at(data, obs, when) if when else (A.latest_version(data, obs) or (None, None))[1])


def match(ind: str, data: dict, val: float, day: str) -> tuple[list, str]:
    """(候補の観測日, 照合に使った日)。書いた日に無ければ前日の版で探す。候補は書いた日のWINDOW_DAYS日前以降の観測に限る
    （何十年も前の同じ値の観測と一致させない）。"""
    prev = (datetime.fromisoformat(day) - timedelta(days=1)).date().isoformat()
    lo = (datetime.fromisoformat(day) - timedelta(days=WINDOW_DAYS)).date().isoformat()
    for when in (day, prev):
        c = [obs for obs in data["versions"] if lo <= obs <= when
             and (v := value_of(ind, data, obs, when)) is not None and abs(v - val) < 1e-6]
        if c:
            return c, when
    return [], ""


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
    data = {ind: A.fetch_versions(sid, key, args.cache) for ind, sid in TARGETS.items()}
    st = Counter()
    moves, drops, conflicts, skipped = [], [], [], []
    rev_at_updates = []  # (既存の行のidx, 新しいrevised_at, 説明)
    nfp_notes = []
    for idx, r in ev.iterrows():
        ind = r["indicator"]
        if ind not in TARGETS or r["actual"] == "" or r["release_date"] in data[ind]["versions"]:
            continue
        cands, when = match(ind, data[ind], float(r["actual"]), written_day(r["updated_at"]))
        if len(cands) != 1:
            if ind == "NFP" and not cands:
                rd = datetime.fromisoformat(r["release_date"])
                obs_m = (rd.replace(day=1) - timedelta(days=1)).replace(day=1).date().isoformat()  # 発表月の前月の1日
                ex = ev[(ev["indicator"] == ind) & (ev["release_date"] == obs_m)]
                if not ex.empty:
                    drops.append(idx)
                    nfp_notes.append(f"NFP 枠{r['release_date']} 値{r['actual']} → 観測月{obs_m}の行あり（actual={ex.iloc[0]['actual']}）: 枠の行を消す")
                    st[(ind, "一致なし・観測月の行あり（枠の行を消す）")] += 1
                else:
                    ev.at[idx, "known_at"] = updated_utc(r["updated_at"])
                    ev.at[idx, "known_at_source"] = "written"
                    nfp_notes.append(f"NFP 枠{r['release_date']} 値{r['actual']} → 観測月{obs_m}の行なし: 動かさず known_at={ev.at[idx, 'known_at']}（written）")
                    st[(ind, "一致なし・観測月の行なし（動かさずknown_atをwrittenに）")] += 1
                continue
            skipped.append((ind, r["release_date"], r["actual"], r["updated_at"], cands))
            st[(ind, "一致なし（移さない）" if not cands else "候補が複数（移さない）")] += 1
            continue
        obs = cands[0]
        ex = ev[(ev["indicator"] == ind) & (ev["release_date"] == obs)]
        if not ex.empty:
            e = ex.iloc[0]
            rv = str(e.get("revised_actual", "")).strip()
            if abs(float(e["actual"]) - float(r["actual"])) < 1e-6:
                drops.append(idx)
                st[(ind, "観測日の行と同じ値（枠の行を消す）")] += 1
            elif rv and abs(float(rv) - float(r["actual"])) < 1e-6:
                drops.append(idx)
                u = updated_utc(r["updated_at"])
                old = str(e.get("revised_at", "")).strip()
                new = min(x for x in (old, u) if x)
                rev_at_updates.append((ex.index[0], new,
                                       f"{ind} 枠{r['release_date']} 値{r['actual']} → 観測{obs}の行のrevised_actualと同じ: 枠の行を消し、"
                                       f"revised_at {old or '（空）'} → {new}"))
                st[(ind, "観測日の行のrevised_actualと同じ値（枠の行を消し、revised_atを早いほうに）")] += 1
            else:
                conflicts.append((ind, r["release_date"], r["actual"], r["updated_at"], obs, e["actual"],
                                  e.get("revised_actual", ""), e.get("known_at", "")))
                st[(ind, "観測日の行と値が違う（停止）")] += 1
            continue
        moves.append((idx, obs))
        st[(ind, "観測日へ移す")] += 1
    for k in sorted(st):
        print(f"  {k[0]}: {k[1]} {st[k]}")
    if skipped:
        print("移さない行:")
        for s in skipped:
            print(f"    {s[0]} 枠{s[1]} 値{s[2]} updated_at={s[3]} 候補={s[4]}")
    if conflicts:
        print("観測日の行と値が違う行（停止。何も書かない）:")
        for c in conflicts:
            print(f"    {c[0]} 枠{c[1]} 値{c[2]}（updated_at={c[3]}）→ 観測{c[4]}の行 actual={c[5]} revised_actual={c[6]} known_at={c[7]}")
        return 2
    for note in nfp_notes:
        print("    " + note)
    for eidx, new, note in rev_at_updates:
        ev.at[eidx, "revised_at"] = new
        print("    " + note)
    for idx, obs in moves:
        ind = ev.at[idx, "indicator"]
        d = data[ind]
        ev.at[idx, "release_date"] = obs
        ev.at[idx, "event_id"] = f"{SLUG[ind]}_{obs}"
        fr = A.first_release(d, obs)
        lv = A.latest_version(d, obs)
        init = value_of(ind, d, obs, fr[0]) if fr else None
        last = value_of(ind, d, obs, None)
        if fr:
            ev.at[idx, "known_at"] = A.et_0830_utc(fr[0])
            ev.at[idx, "known_at_source"] = "alfred"
        if init is not None:
            ev.at[idx, "actual"] = fmt(init)
        base = init if init is not None else float(ev.at[idx, "actual"])
        if last is not None and lv and abs(float(last) - float(base)) > 1e-9:
            ev.at[idx, "revised_actual"] = fmt(last)
            ev.at[idx, "revised_at"] = A.et_0830_utc(lv[0])
        else:
            ev.at[idx, "revised_actual"] = ""
            ev.at[idx, "revised_at"] = ""
        print(f"    移す: {ind} → {obs} actual={ev.at[idx, 'actual']} revised={ev.at[idx, 'revised_actual']} known_at={ev.at[idx, 'known_at']}")
    ev = ev.drop(index=drops)
    print(f"移す {len(moves)}行・消す {len(drops)}行")
    if not args.apply:
        print("（確認のみ。--apply で書き換える）")
        return 0
    ev.to_csv(EVENTS, index=False, encoding="utf-8")
    print("書き換えた")
    return 0


if __name__ == "__main__":
    sys.exit(main())
