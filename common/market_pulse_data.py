"""Market Pulse の保存データ market_data.json の読み書きの共通部品（[[MARKETPULSE-REVIEW-EPIC-1]] 順3・型S、2026-11-27）。

market_data.json は1日1エントリの配列（履歴。画面が全部を読む）。**最新のエントリだけ**を読む所（TANUKI SCORE の画面・
TANUKI のパイプライン・ポートフォリオのスナップショット・System Health・検査）が、履歴ぜんぶ（7MB超）を読んでいた。
同じ書き手（collect_and_send.py::save_data_to_json_and_csv の1か所）が、最新のエントリだけの latest.json も書く。

- load_latest(): latest.json を読む。無ければ market_data.json の最後のエントリ（latest.json を作る前の環境・手元用）
- strip_old_trails(): 最新以外のエントリから sector_rotation の trail（毎日8週分の軌跡）を消す。trail は日足から
  sector_rotation.sector_rotation(as_of) で作り直せる（146エントリすべてで一致を確認）ので、履歴に複製しない。
  象限・RS-Ratio・Momentum・騰落（1日・5日）は残す。画面・検査は trail を最新のエントリだけから読む
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(_REPO, "docs", "market-monitor", "market-pulse", "data")
HISTORY_NAME = "market_data.json"
LATEST_NAME = "latest.json"


def history_path(data_dir: Optional[str] = None) -> str:
    return os.path.join(data_dir or DATA_DIR, HISTORY_NAME)


def latest_path(data_dir: Optional[str] = None) -> str:
    return os.path.join(data_dir or DATA_DIR, LATEST_NAME)


def load_latest(data_dir: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """最新のエントリ（dict）。latest.json があればそれ、無ければ market_data.json の最後のエントリ。どちらも無い・空・壊れていれば None"""
    lp = latest_path(data_dir)
    if os.path.exists(lp):
        try:
            with open(lp, encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, dict) and d:
                return d
        except (OSError, ValueError):
            pass
    hp = history_path(data_dir)
    if os.path.exists(hp):
        try:
            with open(hp, encoding="utf-8") as f:
                arr = json.load(f)
            if isinstance(arr, list) and arr and isinstance(arr[-1], dict):
                return arr[-1]
        except (OSError, ValueError):
            pass
    return None


def strip_old_trails(entries: List[dict]) -> int:
    """最新（最後）以外のエントリの sector_rotation.sectors.*.trail を消す（その場で）。消したセクターの数を返す"""
    n = 0
    for e in entries[:-1]:
        for v in ((e.get("sector_rotation") or {}).get("sectors") or {}).values():
            if isinstance(v, dict) and "trail" in v:
                del v["trail"]
                n += 1
    return n
