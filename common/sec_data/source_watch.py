"""SECの取得元の変化・ばらつきの検知（[[EXTERNAL-SOURCE-CHANGE-WATCH-1]] B1・B2、2026-11-04）。直さずに検知して知らせる。

SEC_Data_Update（update.py）が銘柄ごとにLayer3を作った直後に tag_items() を呼び、実行の最後に cik_items() と一緒に
RESULT_FILE（common/sec_data/data/_source_watch.json）へ書く。System Health の [P] ExternalChange（common/external_watch.py）が読み、
台帳（config/external_change_acknowledged.json）に無い新しい件だけを⚠️にする。銘柄ごとの事情は台帳に記録する（ここに銘柄の一覧は書かない）。

件の種類（kind）と台帳の match:
  cik          CIKの切り替え（config/cik_lookup.csv と SECの company_tickers.json の食い違い・ティッカーが無い）。match「<手元のCIK>→<SECのCIK>」
               submissions の社名の履歴（former_names）は合図にしない（今の社名の「最後の日」が直近の申告の日になる銘柄があり、表記の変更でも出る）
  tag_gap      4a 途切れ: 前の年の同じ四半期（350〜380日前）に0でない値があり、最新の申告の期末に値も候補のタグも無い（KEY_FIELDS だけ）。match「<項目>」
  tag_switch   4b 切り替え: 最新の期末の値のタグが前の4期の多数と違い、値が前の期の SWITCH_RATIO 倍の外。match「<項目> <前のタグ>→<今のタグ>」
  tag_outside  4c 候補外: 最新の申告の当期に、項目に当たる候補外のタグ（OUTSIDE_TAGS の完全な名前）の値があり、その項目に値が無い。match「<項目> <タグ>」
  dup_period   同じ期の期末の数日違い（1〜6日）の2件（TTMの窓〈最新の期末から DUP_WINDOW_DAYS 日〉の中だけ）。match「<項目> <期末>/<期末>」
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(os.path.dirname(_DIR))
RESULT_FILE = os.path.join(_DIR, "data", "_source_watch.json")
CIK_LOOKUP = os.path.join(_REPO, "config", "cik_lookup.csv")

def _item(kind: str, ticker: str, match: str, detail: str) -> Dict[str, str]:
    return {"kind": kind, "ticker": ticker, "match": match, "detail": detail}


def cik_items(company_tickers: Dict[str, Any], lookup_path: str = CIK_LOOKUP) -> List[Dict[str, str]]:
    """config/cik_lookup.csv（status が retired 以外）と SECの company_tickers.json の食い違い"""
    by: Dict[str, List[str]] = {}
    for e in (company_tickers or {}).values():
        by.setdefault(str(e.get("ticker", "")).upper(), []).append(str(e.get("cik_str")).zfill(10))
    from .tickers import get_all_rows   # 登録簿の読み方の1か所（cik_lookup.csv を直接読まない）
    out = []
    for r in get_all_rows(lookup_path):
        if r.get("status") == "retired":
            continue
        t, c = r["ticker"].upper(), str(r["cik"]).zfill(10)
        if t not in by:
            out.append(_item("cik", t, f"{c}→なし", f"SECの company_tickers.json に {t} が無い（手元のCIK {c}）"))
        elif c not in by[t]:
            out.append(_item("cik", t, f"{c}→{by[t][0]}",
                             f"手元のCIK {c} と SECの company_tickers.json の {'・'.join(by[t])} が違う（法人の再編・合併・SPAC。"
                             f"cik_history.json・cik_lookup.csv の変更は人が判断）"))
    return out


def write(items_by_ticker: Dict[str, List[Dict[str, str]]], cik: Optional[List[Dict[str, str]]], full_run: bool,
          path: str = RESULT_FILE, now: Optional[datetime] = None) -> Dict[str, Any]:
    """結果を書く。銘柄を絞った実行は、その銘柄の件だけを置き換える（ほかの銘柄・CIKは前の結果を残す）"""
    now = now or datetime.now(timezone.utc)
    old = load(path)
    tags = {} if full_run else dict(old.get("tags") or {})
    tags.update(items_by_ticker)
    doc = {"generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ") if full_run else old.get("generated_at"),
           "updated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
           "cik": cik if cik is not None else (old.get("cik") or []),
           "cik_checked_at": now.strftime("%Y-%m-%dT%H:%M:%SZ") if cik is not None else old.get("cik_checked_at"),
           "tags": {t: tags[t] for t in sorted(tags) if tags[t]}}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=1)
        f.write("\n")
    return doc


def load(path: str = RESULT_FILE) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f) or {}
    except (OSError, ValueError):
        return {}


def all_items(path: str = RESULT_FILE) -> List[Dict[str, str]]:
    d = load(path)
    return list(d.get("cik") or []) + [i for items in (d.get("tags") or {}).values() for i in items]
