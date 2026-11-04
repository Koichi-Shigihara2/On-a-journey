"""見出しの6時間ごとの取得（[[MARKETPULSE-MOVE-POSTMORTEM-EPIC-1]] ①、2026-11-01）。

`.github/workflows/Market_Pulse_Headlines.yml`（00:40・06:40・12:40・18:40 UTC、土日も含む）から動く。
news_headlines.fetch_headlines() で3本の配信元を取り、headlines_store.accumulate() で日ごとのファイルに積み足す。
Grokは呼ばない（訳・判定は夜の Market Pulse の回が写す）。新しい記事も変更も無ければ何も書かない。
全配信元が失敗・停止なら終了コード1（System Health の [J] が失敗として数える）。

使い方:
  python src/market/market_pulse/headlines_fetch.py                 # docs/market-monitor/market-pulse/data/headlines/ に積み足す
  python src/market/market_pulse/headlines_fetch.py --data-dir DIR  # 別の場所に積み足す（手元の確認用）
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if _REPO_ROOT not in sys.path:   # ワークフローは python src/market/market_pulse/headlines_fetch.py で直接動かし、
    sys.path.insert(1, _REPO_ROOT)  # news_headlines が関数の中で common.external_api を読む（2026-11-04、COMMON-PATH-GUARD-1の後の退行）

import headlines_store as hs  # noqa: E402
import news_headlines as nh  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", default=hs.HEADLINES_DIR, help="積み足す場所（既定は docs/.../data/headlines）")
    args = ap.parse_args(argv)
    h = nh.fetch_headlines()
    st = hs.sources_status(h)
    print(f"[INFO] 見出しの取得 {h['fetched_at']}: status={h['status']} 配信元={st} 切る前の件数={len(h['all_items'])}")
    r = hs.store_from_fetch(h, "scheduled", data_dir=args.data_dir)
    if r["written"]:
        print(f"[INFO] 積み足し: 新しい記事 {r['new']}件（{r['by_date']}）・変更 {r['updated']}件・書いたファイル {r['written']}")
    else:
        print("[INFO] 新しい記事も変更も無い。何も書かない")
    if h["status"] == "failed":
        print("::error::見出しの配信元がすべて失敗・停止")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
