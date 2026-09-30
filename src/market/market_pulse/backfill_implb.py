"""market_data.jsonの既存エントリに、実装B（指示書㉖）の要素を付け足す一過性ツール。

- 全エントリ（S&P500の終値日があるもの）: sector_rotation・semis_m7・commodity_fx を、そのエントリのS&P500の終値日以前の
  daily/の終値だけで計算して追加し（先読みなし）、段階1（SOX・M7のタグ）・段階5（Strongのセクター名）を計算し直し、段階6を追加する
- 段階7（監視銘柄）は最新のエントリだけに付ける（監視銘柄の一覧は現在のもので、過去の時点の一覧は残っていないため）
- 他のキー・他の段階は変更しない

使い方: python src/market/market_pulse/backfill_implb.py [--dry-run]
"""
import argparse
import json
import os
import sys

_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _DIR)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(_DIR))))

import collect_and_send as cs  # noqa: E402
import sector_rotation as sr  # noqa: E402
from stage_conclusions import stage1, stage5, stage6, stage7  # noqa: E402
from common.market_data.reader import get_price_series_as_of, get_attributes  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    with open(cs.JSON_PATH, encoding="utf-8") as f:
        data = json.load(f)
    get = lambda sym, as_of, days: get_price_series_as_of(sym, as_of, days=days)  # noqa: E731
    n = 0
    for i, e in enumerate(data):
        ind = e.get("indicators") or {}
        spx = ind.get("S&P500") if isinstance(ind.get("S&P500"), dict) else None
        if not spx or not spx.get("date") or "stage_conclusions" not in e:
            continue
        as_of = spx["date"]
        rot = sr.sector_rotation(get, as_of)
        semis = sr.semis_m7(get, as_of, spx.get("change_percent"))
        e["sector_rotation"] = rot
        e["semis_m7"] = semis
        e["commodity_fx"] = sr.breakdown(get, as_of)
        sc = e["stage_conclusions"]
        sc["1"] = stage1(ind, semis)
        sc["5"] = stage5(ind, rot)
        sc["6"] = stage6(semis)
        if i == len(data) - 1:
            e["watch_list"] = sr.watch_list(cs.REPO_ROOT, rot, get_attributes)
            sc["7"] = stage7(e["watch_list"])
        n += 1
    print(f"実装Bの要素を追加したエントリ: {n}件（全{len(data)}件）。最新: {data[-1]['date']}")
    for k in ("1", "5", "6", "7"):
        print(f"  最新の段階{k}: {(data[-1]['stage_conclusions'].get(k) or {}).get('line')}")
    if args.dry_run:
        return
    with open(cs.JSON_PATH, "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("保存しました")


if __name__ == "__main__":
    main()
