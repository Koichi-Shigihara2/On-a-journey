"""market_data.json の旧キー comments_history を消す（[[MARKETPULSE-REVIEW-EPIC-1]] 順3・型S、2026-11-27）。

各エントリの comments_history は「そのエントリの summary ＋ 直前の最大11エントリの summary（新しい順）」の複製で、
ファイルの56%（6.0MB）を占めていた。画面が前のエントリの summary から作る（index.html の commentsHistoryOf）ので、
旧キーは要らない。消す前に、保存済みの履歴が「前のエントリの summary から作った履歴」と1件ずつ一致することを確かめ、
1件でも違えば何も書かずに止まる（値を失わないため）。

使い方: python scripts/migrate_market_data_comments_history.py [--check] [パス]
  --check: 検査だけ（書かない）。終了コード 0＝すべて一致、1＝食い違いあり
書き手は collect_and_send.py の1か所のまま（旧キーはもう書かない）。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, List, Tuple

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_PATH = os.path.join(REPO, "docs", "market-monitor", "market-pulse", "data", "market_data.json")
KEY = "comments_history"
HISTORY_N = 11     # 前のエントリを最大11件（collect_and_send.py が書いていた件数）


def derive(entries: List[dict], i: int) -> List[dict]:
    """entries[i] の履歴: そのエントリの summary ＋ 直前の最大11エントリの summary（新しい順、summary が空のものは除く）。画面と同じ規則"""
    prev = [{"date": p.get("date", ""), "summary": p.get("summary", "")}
            for p in reversed(entries[max(0, i - HISTORY_N):i]) if p.get("summary")]
    return [{"date": entries[i].get("date"), "summary": entries[i].get("summary")}] + prev


def mismatches(entries: List[dict]) -> List[Tuple[int, str]]:
    return [(i, str(e.get("date", ""))[:10]) for i, e in enumerate(entries) if KEY in e and e[KEY] != derive(entries, i)]


def strip(entries: List[dict]) -> int:
    """旧キーを消す（その場で）。食い違いがあれば ValueError（何も変えない）。消した件数を返す"""
    bad = mismatches(entries)
    if bad:
        raise ValueError(f"保存済みの履歴が前のエントリの summary と違う（{len(bad)}件、先頭 {bad[:3]}）。消さない")
    n = 0
    for e in entries:
        if KEY in e:
            del e[KEY]
            n += 1
    return n


def main(argv: Any = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("path", nargs="?", default=DEFAULT_PATH)
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args(argv)
    raw = open(args.path, "rb").read()
    entries = json.loads(raw.decode("utf-8"))
    crlf = b"\r\n" in raw
    bad = mismatches(entries)
    have = sum(1 for e in entries if KEY in e)
    print(f"{len(entries)}エントリ・旧キーあり {have}・食い違い {len(bad)}")
    if bad:
        print(f"食い違い: {bad[:5]}")
        return 1
    if args.check:
        return 0
    n = strip(entries)
    text = json.dumps(entries, ensure_ascii=False, indent=2)
    if crlf:
        text = text.replace("\n", "\r\n")
    with open(args.path, "wb") as f:
        f.write(text.encode("utf-8"))
    print(f"旧キーを消した: {n}エントリ（{len(raw) / 1e6:.1f}MB → {len(text.encode('utf-8')) / 1e6:.1f}MB）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
