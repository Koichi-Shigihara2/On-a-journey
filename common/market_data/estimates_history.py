"""アナリストの EPS 予想の改定の週1の履歴（common/market_data/estimates_history/{T}.json）。
[[EPS-REVISION-OUTLOOK-MARK-1]]（2026-11-28、Koichi の決定）

Market Data Weekly が属性（attributes/{T}.json の estimates）を取った後に、その今の値を取得の日付つきで1行積む（valuation_history と同じ形）。
追記だけで上書きしない（同じ日付の行が既にあれば足さない）。新しい取得はしない（属性のファイルを読む）。
1行 = 4期間（0q・+1q・0y・+1y）それぞれの {期末日 end, 改定の件数（7日・30日の上げ・下げ）, 予想 EPS（現在・7・30・60・90日前）} と、
元の決算期の日付（fiscal）。**期間 0q・+1q は決算を発表すると1四半期進む**ので、変化を比べるときは同じ期末日 end の間だけで計算する
（期末日が替わった回は「切り替わり」として比べない）。市場の画面（market_data.json）には入れない。

目的は「見通しの悪化の確認（遅れて追う印）」。予測とは書かない。先に効くかの評価は、次の決算シーズン（2027年2月ごろ）を過ぎてから、貯まった履歴で行う。
表示の実装は、履歴の保存が回り始めた後（stock.html の GUIDANCE のブロックの下・TANUKI SCORE に1列。BUY チェックリストには出さない）。

使い方: python common/market_data/estimates_history.py            # 属性に予想がある銘柄すべて
        python common/market_data/estimates_history.py NVDA AAPL  # 指定した銘柄
"""
from __future__ import annotations

import glob
import json
import os
import sys
from typing import Any, Dict, List, Optional

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

ATTR_DIR = os.path.join(_REPO, "common", "market_data", "attributes")
OUT_DIR = os.path.join(_REPO, "common", "market_data", "estimates_history")
PERIODS = ("0q", "+1q", "0y", "+1y")
REV_KEYS = (("up7", "up_7d"), ("dn7", "down_7d"), ("up30", "up_30d"), ("dn30", "down_30d"))
TREND_KEYS = (("e0", "current"), ("e7", "ago_7d"), ("e30", "ago_30d"), ("e60", "ago_60d"), ("e90", "ago_90d"))


def _load(path: str) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def build_row(attrs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """属性のファイルの estimates から1行を作る。予想が無い・期が1つも取れない銘柄は None"""
    est = attrs.get("estimates") or {}
    fetched = est.get("fetched_at") or attrs.get("fetched_at")
    if not est or not fetched:
        return None
    rev, trend, ends = est.get("eps_revisions") or {}, est.get("eps_trend") or {}, est.get("period_ends") or {}
    periods: Dict[str, Any] = {}
    for p in PERIODS:
        cell: Dict[str, Any] = {}
        if ends.get(p):
            cell["end"] = ends[p]
        for short, long_ in REV_KEYS:
            if (rev.get(p) or {}).get(long_) is not None:
                cell[short] = int(rev[p][long_])
        for short, long_ in TREND_KEYS:
            v = (trend.get(p) or {}).get(long_)
            if v is not None:
                cell[short] = round(float(v), 4)
        if any(k in cell for k in ("up30", "e0")):
            periods[p] = cell
    if not periods:
        return None
    row: Dict[str, Any] = {"date": fetched[:10], "fetched_at": fetched, "p": periods}
    if est.get("fiscal"):
        row["fiscal"] = est["fiscal"]
    return row


def _dump(path: str, symbol: str, records: List[Dict[str, Any]]) -> None:
    """1行を1行に書く（毎週の差分が増えた1行だけになるように）"""
    lines = ",\n".join("  " + json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in records)
    text = ('{\n "symbol": ' + json.dumps(symbol) + ',\n "description": ' + json.dumps(
        "p は期間（0q・+1q・0y・+1y）ごとに end＝期末日・up7/dn7/up30/dn30＝予想の上げ・下げの件数（7日・30日）・e0/e7/e30/e60/e90＝予想EPS（現在・7・30・60・90日前）。"
        "期間は決算で1四半期進むので、変化は同じ end の間だけで比べる", ensure_ascii=False) + ',\n "records": [\n' + lines + "\n ]\n}\n")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def append(symbol: str, attr_dir: str = ATTR_DIR, out_dir: str = OUT_DIR) -> bool:
    """属性のファイルの今の予想を1行積む。積んだら True（属性が無い・予想が無い・同じ日付の行がある場合は False）"""
    row = build_row(_load(os.path.join(attr_dir, f"{symbol}.json")))
    if row is None:
        return False
    path = os.path.join(out_dir, f"{symbol}.json")
    cur = _load(path) or {"symbol": symbol, "records": []}
    if any(r.get("date") == row["date"] for r in cur.get("records") or []):
        return False
    records = sorted((cur.get("records") or []) + [row], key=lambda r: r["date"])
    _dump(path, symbol, records)
    return True


def main(argv: Optional[List[str]] = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv:
        symbols = [s.upper() for s in argv]
    else:
        symbols = sorted(os.path.basename(p)[:-5] for p in glob.glob(os.path.join(ATTR_DIR, "*.json")))
    done, withdata = 0, 0
    for s in symbols:
        if build_row(_load(os.path.join(ATTR_DIR, f"{s}.json"))) is not None:
            withdata += 1
        if append(s):
            done += 1
    print(f"EPS予想の改定の履歴: 予想のある{withdata}銘柄のうち {done}銘柄に1行積んだ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
