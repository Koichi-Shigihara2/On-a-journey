"""外の変化・ばらつきの検知を集める（System Health の [P] ExternalChange、[[EXTERNAL-SOURCE-CHANGE-WATCH-1]]、2026-11-04）。

原則: 外の変化は直さずに検知して知らせる。各検知の件を ここで集め、台帳（LEDGER_PATH）に無い新しい件だけを⚠️にする
（重大度は common/fetch_failure.py の表の「台帳に無い外の変化」〈judge_external〉）。銘柄ごとの事情は台帳に理由と一緒に記録する。

件の出どころ:
  - SECの取得元（common/sec_data/source_watch.py の結果 common/sec_data/data/_source_watch.json。SEC_Data_Update が書く）:
    cik・tag_gap・tag_switch・tag_outside・dup_period
  - 先物の限月の欄の異常（common/market_data/futures_roll.py の contract_anomalies、日足から読むたびに作る）: contract
  - MACRO PULSEのFOMCの予定のキャッシュの古さ（common/macro_data/fomc_calendar.py）は台帳を使わない（古さは直れば消える）
台帳の形は config/warn_acknowledged.json と同じ: {"acknowledged": [{"kind", "ticker", "match", "acknowledged_date", "comment"}]}。
(kind, ticker, match) が一致した件を確認済みとする
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LEDGER_PATH = os.path.join(_REPO, "config", "external_change_acknowledged.json")

KIND_LABELS = {
    "cik": "CIKの切り替え",
    "tag_gap": "タグの途切れ",
    "tag_switch": "タグの切り替え＋値の飛び",
    "tag_outside": "候補外のタグ",
    "dup_period": "期末の数日違いの重複",
    "contract": "先物の限月の欄の異常",
}


def load_ledger(path: str = LEDGER_PATH) -> List[Dict[str, Any]]:
    try:
        with open(path, encoding="utf-8") as f:
            return (json.load(f) or {}).get("acknowledged") or []
    except (OSError, ValueError):
        return []


def collect(source_watch_path: Optional[str] = None, daily_dir: Optional[str] = None) -> List[Dict[str, str]]:
    """全部の件（台帳で分ける前）"""
    from common.sec_data import source_watch
    items = source_watch.all_items(source_watch_path or source_watch.RESULT_FILE)
    try:
        from common.market_data import futures_roll
        items += futures_roll.contract_anomalies(daily_dir=daily_dir)
    except ImportError:
        pass
    return items


def split(items: List[Dict[str, str]], ledger: List[Dict[str, Any]]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """(台帳に無い新しい件, 確認済みの件)"""
    keys = {(a.get("kind"), a.get("ticker"), a.get("match")) for a in ledger}
    new, ack = [], []
    for i in items:
        (ack if (i["kind"], i["ticker"], i["match"]) in keys else new).append(i)
    return new, ack


def summarize(new: List[Dict[str, str]], limit: int = 4) -> str:
    """新しい件の短い表示（種類ごとの件数と先頭の数件）"""
    by: Dict[str, List[str]] = {}
    for i in new:
        by.setdefault(i["kind"], []).append(f"{i['ticker']} {i['match']}")
    parts = []
    for k, v in by.items():
        head = "・".join(v[:limit]) + (f" ほか{len(v) - limit}" if len(v) > limit else "")
        parts.append(f"{KIND_LABELS.get(k, k)} {len(v)}件（{head}）")
    return " / ".join(parts)
