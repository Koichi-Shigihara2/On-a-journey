"""STONKS SILO の対象の選び方（[[DEFICIT-QUALITY-ADOPT-1]]、2026-11-12）。

手で書く cik_lookup.csv の stonks_silo フラグも、手で書く例外（pinned・excluded）もやめ、**規則だけ**で決める:

  1. 収益の部品（common/profitability.py）が出す**月末ごとの営業利益の符号**（operating_state_history。直近4か月末）を、
     **stage_v2 の持続の条件（persist、判定が2か月の月末で続けて同じになったときだけ変える）**に通した最後の状態が「赤字」なら対象。新しい閾値は無い
  2. 符号の根拠は、その月末までに申告された TTM 営業利益。TTM が作れない月は:
       - 直前の状態を保つ（持続の条件の「判定不能は直前を保つ」）。印 "held"
       - 履歴の最初から取れない銘柄は、取れる最新の年次の営業利益の符号を使う。印 "annual"
       - どちらも無い銘柄は対象外。印 "no_data"
  3. 金融・資産の重い業種は対象外（common/sector_master.py::is_financial_ticker。業種のマスターの判定）
  4. status が retired・provisioning の銘柄は除く（common/sec_data/tickers.py と同じ）

入力: docs/common/profitability.json の operating_state_history（SEC_Data_Update の後・Market Data Weekly で作り直す。新しい銘柄は
register_ticker の Step 1.7）。履歴が1銘柄も無いときは止める（空の対象で STONKS を走らせない）。
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

PROFITABILITY_PATH = os.path.join(_REPO, "docs", "common", "profitability.json")

LOSS, PROFIT, UNKNOWN = "loss", "profit", "unknown"
MARK_HELD, MARK_ANNUAL, MARK_NO_DATA = "held", "annual", "no_data"
_TO_V2 = {LOSS: "赤字", PROFIT: "黒字", UNKNOWN: "判定不能"}     # stage_v2.persist の入力（判定不能は直前の状態を保つ）


def persisted_state(history: List[List[str]]) -> Optional[str]:
    """月末ごとの符号（古い順。行は [月末, 状態, 根拠]）を持続の条件に通した最後の状態。"loss"・"profit"・None（判定できない）"""
    from common.stage_v2 import persist
    if not history:
        return None
    seq = [_TO_V2.get(row[1], "判定不能") for row in history]
    last = persist(seq)[-1]
    return {"赤字": LOSS, "黒字": PROFIT}.get(last)


def marks(history: List[List[str]]) -> List[str]:
    """判定に使った根拠の印。held＝TTMが作れない月があり直前の状態を保った、annual＝TTMが作れず年次の符号を使った月がある、
    no_data＝どの月も判定できない（対象外）"""
    out: List[str] = []
    states = [row[1] for row in history or []]
    bases = [row[2] if len(row) > 2 else "ttm" for row in history or []]
    if not history or all(s == UNKNOWN for s in states):
        return [MARK_NO_DATA]
    if "annual" in bases:
        out.append(MARK_ANNUAL)
    first_known = next(i for i, s in enumerate(states) if s != UNKNOWN)
    if any(s == UNKNOWN for s in states[first_known:]):
        out.append(MARK_HELD)
    return out


def _load_profitability(profitability_path: Optional[str]) -> Dict[str, Any]:
    try:
        with open(profitability_path or PROFITABILITY_PATH, encoding="utf-8") as f:
            return json.load(f).get("tickers") or {}
    except (OSError, ValueError):
        return {}


def targets_with_marks(profitability_path: Optional[str] = None, csv_path: Optional[str] = None,
                       include_provisioning: bool = False, is_financial=None) -> Dict[str, List[str]]:
    """STONKS の対象の銘柄と、判定に使った根拠の印（空リスト＝TTMだけで判定）"""
    from common.sec_data import tickers as T
    if is_financial is None:
        from common.sector_master import is_financial_ticker as is_financial
    prof = _load_profitability(profitability_path)
    if not any((e or {}).get("operating_state_history") for e in prof.values()):
        # 月末の履歴が1銘柄も無い（common/profitability.py が古い形のまま、または profitability.json が無い）。
        # 空の対象で STONKS が走ると結果が空になるので、止める
        raise RuntimeError("docs/common/profitability.json に operating_state_history が無い。common/profitability.py --all で作り直す")
    invalid = {"retired"} if include_provisioning else T._INVALID_STATUSES
    valid = {r["ticker"] for r in T._load(csv_path) if r.get("status", "").strip().lower() not in invalid}
    out: Dict[str, List[str]] = {}
    for t in sorted(valid):
        hist = (prof.get(t) or {}).get("operating_state_history") or []
        if is_financial(t):
            continue
        if persisted_state(hist) == LOSS:
            out[t] = marks(hist)
    return out


def targets(profitability_path: Optional[str] = None, csv_path: Optional[str] = None, include_provisioning: bool = False,
            is_financial=None) -> List[str]:
    """STONKS の対象の銘柄（並べ替え済み）。include_provisioning=True は登録処理中の銘柄も含める（CLI引数の明示指定の検証用）"""
    return sorted(targets_with_marks(profitability_path, csv_path, include_provisioning, is_financial))


def explain(ticker: str, profitability_path: Optional[str] = None, is_financial=None) -> Dict[str, Any]:
    """1銘柄の判定の中身（登録の検証・確認用）"""
    if is_financial is None:
        from common.sector_master import is_financial_ticker as is_financial
    t = ticker.upper()
    hist = (_load_profitability(profitability_path).get(t) or {}).get("operating_state_history") or []
    if is_financial(t):
        return {"ticker": t, "target": False, "reason": "金融・資産の重い業種（業種のマスター）", "history": hist, "marks": []}
    st = persisted_state(hist)
    mk = marks(hist) if hist else [MARK_NO_DATA]
    return {"ticker": t, "target": st == LOSS, "reason": f"持続の条件を通した状態={st}", "history": hist, "marks": mk}
