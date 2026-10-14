"""
common/method_change.py
計算方法の変更の印（[[LAYER3-RETENTION-PERIOD-1]]、2026-10-14新設）

計算方法を変えて公開データを作り直した実行の、判定・評価の履歴に印を残す。
相場の変化による判定の変化と、計算方法の変更による変化を、履歴とDiscordの通知で区別するため。

印の渡し方（どちらか、両方なら和集合）:
  1. config/method_change.json の "changes" に {"id": "<BACKLOGの項目ID>", "date": "YYYY-MM-DD"} を足す。
     その日（JST）に動いた実行すべてに印が付く。ワークフローの連鎖（workflow_run）は
     入力を引き継げないため、連鎖する作り直しにはこちらを使う
  2. 環境変数 METHOD_CHANGE（カンマ区切りの項目ID）。手元での実行・単発の実行向け

印は、履歴の1件（dict）に "method_change": "ID1,ID2" として付く（stamp()）。
"""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timedelta, timezone

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(_REPO_ROOT, "config", "method_change.json")
ENV_NAME = "METHOD_CHANGE"
KEY = "method_change"
_ID_RE = re.compile(r"^[A-Z0-9]+(?:-[A-Z0-9]+)*-\d+$")
_JST = timezone(timedelta(hours=9))


def _today_jst() -> date:
    return datetime.now(_JST).date()


def _valid(ids) -> list[str]:
    out: list[str] = []
    for i in ids:
        i = (i or "").strip()
        if i and _ID_RE.match(i) and i not in out:
            out.append(i)
    return out


def active_method_changes(today: date | None = None, config_path: str | None = None) -> list[str]:
    """今日（JST）の実行に付ける計算方法の変更の項目IDの一覧（無ければ空）。"""
    today = today or _today_jst()
    ids: list[str] = _valid((os.environ.get(ENV_NAME) or "").split(","))
    try:
        with open(config_path or CONFIG_PATH, encoding="utf-8") as f:
            cfg = json.load(f)
    except (OSError, json.JSONDecodeError):
        cfg = {}
    for c in cfg.get("changes") or []:
        if isinstance(c, dict) and c.get("date") == today.isoformat():
            ids += _valid([c.get("id")])
    return _valid(ids)


def stamp(entry: dict, today: date | None = None, config_path: str | None = None) -> dict:
    """entryに今日の計算方法の変更の印を付けて返す（無ければそのまま）。"""
    ids = active_method_changes(today, config_path)
    if ids:
        entry[KEY] = ",".join(ids)
    return entry
