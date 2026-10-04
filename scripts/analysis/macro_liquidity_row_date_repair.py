"""MACRO PULSE: 05_liquidity.csv の日付が1日後になっている行を、実行の米国の日付へ移す
（一度だけの修復、2026-10-04 指示書M-3 STEP 4、[[MACRO-PULSE-LIQUIDITY-ROW-DATE-UTC-SHIFT-1]]）

[[MACRO-PULSE-RUN-DATE-UTC-SHIFT-1]]（2026-10-03修正）の前は、日次の実行が起動時刻のUTCの日付を使っていたため、cronの遅れで
UTCの0時をまたいだ実行は行に「実行の米国の日付＋1日」を付けていた。git履歴（05_liquidity.csvの全版）で、各行の中身
（元の9列: date〜sp500）を最後に書いたcommitを求め、それがgithub-actions[bot]で、行の日付＝そのcommitの時刻の米国の日付
（米国東部時間17時前なら前日）＋1日の行を、1日前へ移す。行の中身は変えない（日付だけ）。

重複する日付: 移す先の日付に、移さない行が既にある場合は書き換えずに一覧を出して終了する（指示書M-3の停止条件）。
移す行どうしは、日付の新しい順に1日ずつずらすため重ならない。

05_liquidity.csv は merge=ours のため、kaihatsu へ統合した後に kaihatsu 上で実行して commit する。
使い方（リポジトリの根で実行）:
  python scripts/analysis/macro_liquidity_row_date_repair.py           # 確認だけ
  python scripts/analysis/macro_liquidity_row_date_repair.py --apply   # 書き換える
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import subprocess
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

REL = "docs/market-monitor/macro-pulse/data/05_liquidity.csv"
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BASE = ("date", "m2", "hy_spread", "fed_balance", "tga", "rrp", "net_liquidity", "reserve_balance", "sp500")
BOT = "github-actions[bot]"
NY = ZoneInfo("America/New_York")


def git(*args: str) -> str:
    env = {**os.environ, "MSYS_NO_PATHCONV": "1"}
    return subprocess.run(["git", *args], capture_output=True, text=True, encoding="utf-8", cwd=REPO_ROOT,
                          env=env, check=True).stdout


def us_date_of(commit_iso: str) -> date:
    t = datetime.fromisoformat(commit_iso).astimezone(NY)
    return t.date() if t.hour >= 17 else t.date() - timedelta(days=1)


def last_writers() -> dict:
    """{行の日付: (commitの時刻, 作者)}。行の中身（BASE列）が変わった最後のcommit。"""
    log = git("log", "--reverse", "--format=%H|%cI|%an", "HEAD", "--", REL).strip().splitlines()
    last, prev = {}, {}
    for line in log:
        h, ci, an = line.split("|", 2)
        rows = {r["date"]: tuple(r.get(k, "") for k in BASE)
                for r in csv.DictReader(io.StringIO(git("show", f"{h}:{REL}"))) if r.get("date")}
        for d, v in rows.items():
            if prev.get(d) != v:
                last[d] = (ci, an)
        prev = rows
    return last


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    path = os.path.join(REPO_ROOT, REL)
    with open(path, encoding="utf-8", newline="") as f:
        rd = csv.DictReader(f)
        cols = rd.fieldnames
        rows = list(rd)
    writers = last_writers()
    moves = {}
    for r in rows:
        ci, an = writers.get(r["date"], (None, None))
        if an != BOT:
            continue
        us = us_date_of(ci)
        if (date.fromisoformat(r["date"]) - us).days == 1:
            moves[r["date"]] = us.isoformat()
    dates = {r["date"] for r in rows}
    conflicts = sorted(t for s, t in moves.items() if t in dates and t not in moves)
    print(f"行数 {len(rows)}・移す行 {len(moves)}"
          + (f"（{min(moves)}〜{max(moves)}）" if moves else ""))
    print("月ごと:", dict(sorted(Counter(d[:7] for d in moves).items())))
    if conflicts:
        print(f"重複する日付（移す先に移さない行がある）: {len(conflicts)}件 → 書き換えずに終了")
        for t in conflicts:
            src = [s for s, x in moves.items() if x == t][0]
            print(f"  {src} → {t}（{t}の行を最後に書いた: {writers.get(t)}）")
        return 2
    vacated = sorted(d for d in moves if d not in moves.values())
    print("移した後に行が無くなる日付:", vacated)
    if not args.apply:
        print("（確認のみ。--apply で書き換える）")
        return 0
    for r in rows:
        if r["date"] in moves:
            r["date"] = moves[r["date"]]
    rows.sort(key=lambda r: r["date"])
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    print("書き換えた")
    return 0


if __name__ == "__main__":
    sys.exit(main())
