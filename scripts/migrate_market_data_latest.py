"""market_data.json の履歴から sector_rotation.trail を消し、latest.json（最新のエントリだけ）を作る
（[[MARKETPULSE-REVIEW-EPIC-1]] 順3の残り・型S、2026-11-27）。

trail（毎日8週分の軌跡。ファイルの22%・1.05MB）は、最新以外のエントリでは誰も読まない（画面・検査は最新のエントリだけから読む）。
日足から sector_rotation.sector_rotation(as_of) で作り直せるので、消す前に、**そのエントリの trail が日足から作り直した trail と
1つずつ一致すること**を確かめ、一致しないエントリの trail は消さずに残して報告する（情報を失わない）。
最新のエントリは何も変えず、そのまま latest.json に書く。

使い方: python scripts/migrate_market_data_latest.py [--check] [データのフォルダ]
  --check: 検査だけ（書かない）。終了コード 0＝すべて作り直せる、1＝作り直せないエントリあり
書き手は collect_and_send.py の1か所のまま（以後は毎回 trail の整理と latest.json を書く）。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Callable, List, Tuple

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (REPO, os.path.join(REPO, "src", "market", "market_pulse")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from common import market_pulse_data as mpd  # noqa: E402


def not_reproducible(entries: List[dict], get_series: Callable) -> List[Tuple[int, str]]:
    """最新以外のエントリで、保存した trail が日足から作り直した trail と違うもの（[(番号, 日付)]）"""
    import sector_rotation as sr
    bad = []
    for i, e in enumerate(entries[:-1]):
        r = e.get("sector_rotation")
        if not r:
            continue
        new = sr.sector_rotation(get_series, r["as_of"])
        for s, v in (r.get("sectors") or {}).items():
            if "trail" not in v:
                continue
            nv = (new.get("sectors") or {}).get(s)
            if not nv or nv.get("trail") != v["trail"]:
                bad.append((i, str(e.get("date", ""))[:10]))
                break
    return bad


def main(argv: Any = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("data_dir", nargs="?", default=mpd.DATA_DIR)
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args(argv)
    hp = mpd.history_path(args.data_dir)
    raw = open(hp, "rb").read()
    entries = json.loads(raw.decode("utf-8"))
    from common.market_data.reader import get_price_series_as_of as _md
    bad = not_reproducible(entries, lambda sym, as_of, days: _md(sym, as_of, days=days))
    have = sum(1 for e in entries[:-1] for v in ((e.get("sector_rotation") or {}).get("sectors") or {}).values() if "trail" in v)
    print(f"{len(entries)}エントリ・最新以外の trail あり {have}セクター・日足から作り直せない {len(bad)}エントリ")
    if bad:
        print(f"作り直せない（trail を残す）: {bad[:5]}")
    if args.check:
        return 1 if bad else 0
    keep = {i for i, _ in bad}
    saved = []
    for i in keep:
        saved.append((i, {s: v.get("trail") for s, v in ((entries[i].get("sector_rotation") or {}).get("sectors") or {}).items()}))
    n = mpd.strip_old_trails(entries)
    for i, tr in saved:                       # 作り直せないエントリは trail を戻す
        for s, t in tr.items():
            if t is not None:
                entries[i]["sector_rotation"]["sectors"][s]["trail"] = t
                n -= 1
    text = json.dumps(entries, ensure_ascii=False, indent=2)
    if b"\r\n" in raw:
        text = text.replace("\n", "\r\n")
    with open(hp, "wb") as f:
        f.write(text.encode("utf-8"))
    latest = json.dumps(entries[-1], ensure_ascii=False, indent=2)
    with open(mpd.latest_path(args.data_dir), "wb") as f:
        f.write(latest.encode("utf-8"))
    print(f"trail を消した: {n}セクター（{len(raw) / 1e6:.1f}MB → {len(text.encode('utf-8')) / 1e6:.1f}MB）。latest.json を書いた（{len(latest.encode('utf-8')) / 1e3:.0f}KB）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
