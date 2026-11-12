"""STONKS SILO の対象の選び方（[[DEFICIT-QUALITY-ADOPT-1]]、2026-11-12）。

手で書く cik_lookup.csv の stonks_silo フラグをやめ、**収益の部品（common/profitability.py）の月末ごとの TTM 営業利益の符号**を、
**stage_v2 の持続の条件（persist、判定が2か月の月末で続けて同じになったときだけ変える）**に通して決める。新しい閾値は作らない。

  対象 = 持続の条件を通した最後の状態が「赤字」（TTM 営業利益<0。業種の区分に依らない＝区分なしのバイオも赤字なら対象）
         ＋ config/stonks_target.json の pinned（TTM の営業利益が取れない既知の銘柄。理由つき）
         − excluded（金融など、枠組みが合わない銘柄。理由つき）
  status が retired・provisioning の銘柄は除く（common/sec_data/tickers.py と同じ）

入力: docs/common/profitability.json の operating_state_history（SEC_Data_Update の後・Market Data Weekly で作り直す。
新しい銘柄は register_ticker の Step 1.7）。profitability.json に無い銘柄は対象にならない（pinned を除く）。
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

CONFIG_PATH = os.path.join(_REPO, "config", "stonks_target.json")
PROFITABILITY_PATH = os.path.join(_REPO, "docs", "common", "profitability.json")

LOSS, PROFIT, UNKNOWN = "loss", "profit", "unknown"
_TO_V2 = {LOSS: "赤字", PROFIT: "黒字", UNKNOWN: "判定不能"}     # stage_v2.persist の入力（判定不能は直前の状態を保つ）


def resolve_config_path() -> str:
    """config/stonks_target.json の絶対パス（report_consistency_check.py CHECK-34の_CONFIG_LOADER_REGISTRYから存在確認される）"""
    return CONFIG_PATH


def load_config(path: Optional[str] = None) -> Dict[str, Dict[str, str]]:
    with open(path or CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f)
    return {"excluded": cfg.get("excluded") or {}, "pinned": cfg.get("pinned") or {}}


def persisted_state(history: List[List[str]]) -> Optional[str]:
    """月末ごとの符号（古い順）を持続の条件に通した最後の状態。"loss"・"profit"・None（判定できない）"""
    from common.stage_v2 import persist
    if not history:
        return None
    seq = [_TO_V2.get(state, "判定不能") for _, state in history]
    last = persist(seq)[-1]
    return {"赤字": LOSS, "黒字": PROFIT}.get(last)


def targets(profitability_path: Optional[str] = None, config: Optional[Dict[str, Any]] = None,
            csv_path: Optional[str] = None, include_provisioning: bool = False) -> List[str]:
    """STONKS の対象の銘柄（並べ替え済み）。include_provisioning=True は登録処理中の銘柄も含める（CLI引数の明示指定の検証用）"""
    from common.sec_data import tickers as T
    cfg = config if config is not None else load_config()
    try:
        with open(profitability_path or PROFITABILITY_PATH, encoding="utf-8") as f:
            prof = json.load(f).get("tickers") or {}
    except (OSError, ValueError):
        prof = {}
    if not any((e or {}).get("operating_state_history") for e in prof.values()):
        # 月末の履歴が1銘柄も無い（common/profitability.py が古い形のまま、または profitability.json が無い）。
        # 空の対象で STONKS が走ると結果が空になるので、止める
        raise RuntimeError("docs/common/profitability.json に operating_state_history が無い。common/profitability.py --all で作り直す")
    invalid = {"retired"} if include_provisioning else T._INVALID_STATUSES
    valid = {r["ticker"] for r in T._load(csv_path) if r.get("status", "").strip().lower() not in invalid}
    out = set()
    for t in valid:
        if t in cfg["excluded"]:
            continue
        if t in cfg["pinned"]:
            out.add(t)
            continue
        if persisted_state((prof.get(t) or {}).get("operating_state_history") or []) == LOSS:
            out.add(t)
    return sorted(out)


def explain(ticker: str, profitability_path: Optional[str] = None, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """1銘柄の判定の中身（登録の検証・確認用）"""
    cfg = config if config is not None else load_config()
    t = ticker.upper()
    try:
        with open(profitability_path or PROFITABILITY_PATH, encoding="utf-8") as f:
            hist = ((json.load(f).get("tickers") or {}).get(t) or {}).get("operating_state_history") or []
    except (OSError, ValueError):
        hist = []
    if t in cfg["excluded"]:
        return {"ticker": t, "target": False, "reason": "excluded: " + cfg["excluded"][t], "history": hist}
    if t in cfg["pinned"]:
        return {"ticker": t, "target": True, "reason": "pinned: " + cfg["pinned"][t], "history": hist}
    st = persisted_state(hist)
    return {"ticker": t, "target": st == LOSS, "reason": f"持続の条件を通した状態={st}", "history": hist}
