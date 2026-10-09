"""market_data.jsonの最新エントリに、実装C（指示書㉗）の要素（ニュースの見出し・予定・先物とドル円の最新値）を付ける一過性ツール。

ニュース・予定・先物の最新値は、その時点で取得したものしか残らない（過去のエントリの時点の値は取り直せない）ため、最新のエントリだけに
付け、取得時刻（fetched_at）と付けた旨（backfilled_at）を記録する。過去のエントリには付けない。data_quality・段階0を計算し直す。

使い方: python src/market/market_pulse/backfill_implc.py [--dry-run] [--only headlines]
  --only headlines: 見出しだけを取り直す（予定・先物の最新値はエントリの値を残す。2026-10-09、[[MARKETPULSE-HEADLINES-NHK-STALE-1]]・
  [[MARKETPULSE-HEADLINES-JA-1]]の修正の反映用）。見出しの翻訳と関係の判定でGrokを1回呼ぶ（XAI_API_KEYがあるとき）
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _DIR)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(_DIR))))

import collect_and_send as cs  # noqa: E402
from stage_conclusions import build_stage_conclusions  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", choices=["headlines"], help="この要素だけを取り直す")
    args = ap.parse_args()
    with open(cs.JSON_PATH, encoding="utf-8") as f:
        data = json.load(f)
    L = data[-1]
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if args.only == "headlines":
        import news_headlines as nh
        new = {"headlines": nh.annotate_headlines(nh.fetch_headlines())}
        implc = {k: L.get(k) or {} for k in ("calendar", "futures")}
        implc.update(new)
    else:
        new = implc = cs.compute_implc({"watch_list": L.get("watch_list")})
    for k, v in new.items():
        v["backfilled_at"] = stamp
        L[k] = v
    # data_qualityと段階0だけを計算し直す（他の段階は変更しない）
    r = build_stage_conclusions(L.get("indicators") or {}, L.get("asset_flow"), (L.get("sentiment") or {}).get("breadth"),
                                (L.get("data_quality") or {}).get("expected_close_date"),
                                implb={"sector_rotation": L.get("sector_rotation")}, implc=implc)
    L["data_quality"] = r["data_quality"]
    L["stage_conclusions"]["0"] = r["stages"]["0"]
    rel = implc["headlines"].get("relevance") or {}
    print(f"最新エントリ {L['date']} に付けた: 見出し{len(implc['headlines'].get('items') or [])}件（{implc['headlines']['status']}、"
          f"関係の判定 {rel.get('status')} true{rel.get('true')}・false{rel.get('false')}）、"
          f"予定{len(implc['calendar'].get('events') or [])}件（{implc['calendar']['status']}）、先物{implc['futures']['status']}、"
          f"data_quality={L['data_quality']['status']}")
    if args.dry_run:
        return
    with open(cs.JSON_PATH, "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("保存しました")


if __name__ == "__main__":
    main()
