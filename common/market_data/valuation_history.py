"""yfinance の倍率のスナップショットの履歴（common/market_data/valuation_history/{T}.json）。
[[HYPECORE-STAGE-TIMESCALE-1]] 段B-4（2026-11-05、チャット側の決定4）

フォワードPERは過去の値が取れない（yfinance は今の値だけ）。Market Data Weekly が属性（attributes/{T}.json）を取った後に、
同じ値（forward_pe と、同時に取れる trailing_pe・price_to_sales・ev_to_ebitda）を取得の日付つきで積む。
追記だけで上書きしない（同じ日付の行が既にあれば足さない）。新しい取得はしない（属性のファイルを読むだけ）。
対象は登録簿（retired 以外）の銘柄。

使い方: python common/market_data/valuation_history.py            # 登録簿の全銘柄
        python common/market_data/valuation_history.py NVDA AAPL  # 指定した銘柄
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

ATTR_DIR = os.path.join(_REPO, "common", "market_data", "attributes")
OUT_DIR = os.path.join(_REPO, "common", "market_data", "valuation_history")
KEYS = ("forward_pe", "trailing_pe", "price_to_sales", "ev_to_ebitda")


def _load(path: str) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def append(symbol: str, attr_dir: str = ATTR_DIR, out_dir: str = OUT_DIR) -> bool:
    """属性のファイルの今の値を1行積む。積んだら True（属性が無い・同じ日付の行がある・値が全部 None なら False）"""
    a = _load(os.path.join(attr_dir, f"{symbol}.json"))
    fetched = a.get("fetched_at")
    if not fetched:
        return False
    row = {"date": fetched[:10], "fetched_at": fetched, **{k: a.get(k) for k in KEYS}}
    if all(row[k] is None for k in KEYS):
        return False
    path = os.path.join(out_dir, f"{symbol}.json")
    cur = _load(path) or {"symbol": symbol, "records": []}
    if any(r.get("date") == row["date"] for r in cur.get("records") or []):
        return False
    cur["records"] = sorted((cur.get("records") or []) + [row], key=lambda r: r["date"])
    os.makedirs(out_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cur, f, ensure_ascii=False, indent=1)
        f.write("\n")
    return True


def main(argv: Optional[List[str]] = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv:
        symbols = [s.upper() for s in argv]
    else:
        from common.sec_data.tickers import get_all_rows   # 登録簿の読み方の1か所
        symbols = sorted({r["ticker"].upper() for r in get_all_rows() if r.get("status") != "retired"})
    n = sum(1 for s in symbols if append(s))
    print(f"倍率のスナップショットの履歴: {len(symbols)}銘柄のうち {n}銘柄に1行積んだ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
