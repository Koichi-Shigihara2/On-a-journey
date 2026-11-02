"""
common/method_change.py
計算方法の変更の印（[[LAYER3-RETENTION-PERIOD-1]]、2026-10-14新設。[[METHOD-CHANGE-PUBLISH-TIMING-1]]、2026-11-02 改訂）

計算方法を変えて公開データを作り直した実行の、判定・評価の履歴に印を残す。
相場の変化による判定の変化と、計算方法の変更による変化を、履歴とDiscordの通知で区別するため。

config/method_change.json の "changes" の1件は、次の2つの形のどちらか:
  1. 新しい形（2026-11-02〜）: {"id": "<BACKLOGの項目ID>", "committed_at": "<ISO 8601>",
                                 "scope": [{"ticker", "fields", "before", "after"}, ...]}
     各出力は、前回の出力の "method_change_applied"（読んだIDの一覧）と比べ、一覧に無いIDを
     初めて読んだ実行でだけ、scopeの銘柄に印を付ける。日付（JST）では決めない（実行の無い日をまたいでも付く）。
     scopeは変更のときの全銘柄のシミュレーション（株価・市場データを固定した前後）から
     scripts/method_change_scope.py が作る（手で書かない）。scopeが無い件は全銘柄。
     同じ項目で2回目の変更を出すときは、IDの末尾に番号を足す（例: FCF-DEFINITION-SINGLE-SOURCE-1-2）
  2. 古い形（〜2026-11-01の件）: {"id": "...", "date": "YYYY-MM-DD"} … その日（JST）に動いた実行の全銘柄に印
  ほかに、環境変数 METHOD_CHANGE（カンマ区切りの項目ID）は、その実行の全銘柄に印（手元・単発の実行向け）。

印は、出力（dict）に "method_change": "ID1,ID2" として付く。新しい形の件が1件でもあれば、
出力に "method_change_applied": [読んだID] も書く（次の実行の比べる相手）。
呼び出し側は、書き換える前の同じ出力（無ければNone）を prev に渡す（prev を渡さない呼び出しは古い形と環境変数だけ）。
"""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(_REPO_ROOT, "config", "method_change.json")
ENV_NAME = "METHOD_CHANGE"
KEY = "method_change"
APPLIED_KEY = "method_change_applied"
_ID_RE = re.compile(r"^[A-Z0-9]+(?:-[A-Z0-9]+)*-\d+$")
_JST = timezone(timedelta(hours=9))
_UNSET = object()
ALL = "ALL"   # scope_for の戻り値: 全銘柄・全項目（scopeの無い件・古い形・環境変数）


def _today_jst() -> date:
    return datetime.now(_JST).date()


def _valid(ids) -> list[str]:
    out: list[str] = []
    for i in ids:
        i = (i or "").strip()
        if i and _ID_RE.match(i) and i not in out:
            out.append(i)
    return out


def _load(config_path: Optional[str] = None) -> dict:
    try:
        with open(config_path or CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f) or {}
    except (OSError, json.JSONDecodeError):
        return {}


def _changes(cfg: dict) -> list[dict]:
    return [c for c in cfg.get("changes") or [] if isinstance(c, dict)]


def _is_new_form(c: dict) -> bool:
    return "committed_at" in c and "date" not in c


def _scope_tickers(c: dict) -> Optional[set[str]]:
    """新しい形の件のscopeの銘柄（scopeが無ければNone＝全銘柄）"""
    if "scope" not in c or c.get("scope") is None:
        return None
    return {str(s.get("ticker", "")).upper() for s in c.get("scope") or [] if isinstance(s, dict)}


def active_method_changes(today: date | None = None, config_path: str | None = None) -> list[str]:
    """古い形（日付）と環境変数で、今日（JST）の実行の全銘柄に付ける項目IDの一覧（無ければ空）。"""
    today = today or _today_jst()
    ids: list[str] = _valid((os.environ.get(ENV_NAME) or "").split(","))
    for c in _changes(_load(config_path)):
        if not _is_new_form(c) and c.get("date") == today.isoformat():
            ids += _valid([c.get("id")])
    return _valid(ids)


def new_form_ids(config_path: str | None = None) -> list[str]:
    """新しい形の件のIDの一覧（"method_change_applied" に書く）"""
    return _valid([c.get("id") for c in _changes(_load(config_path)) if _is_new_form(c)])


def marks(prev: Any, ticker: Optional[str] = None, today: date | None = None,
          config_path: str | None = None) -> tuple[list[str], Optional[list[str]]]:
    """(印を付けるID, 出力に書く "method_change_applied")。

    prev: 書き換える前の同じ出力（dict。ファイルが無ければNone）。
      - Noneなら比べる相手が無い（新しい銘柄・新しいファイル）ので、新しい形の件では印を付けない
      - "method_change_applied" の無い出力は、新しい形の件を1件も読んでいない
    ticker: 銘柄ごとの出力なら銘柄。全銘柄をまとめた出力ならNone（scopeの銘柄が1つでもあれば付ける）
    """
    cfg = _load(config_path)
    ids = active_method_changes(today, config_path)
    new_form = [c for c in _changes(cfg) if _is_new_form(c) and _valid([c.get("id")])]
    applied = _valid([c.get("id") for c in new_form])
    if not applied:
        return ids, None
    if isinstance(prev, dict):
        seen = set(prev.get(APPLIED_KEY) or [])
        t = (ticker or "").upper()
        for c in new_form:
            if c["id"] in seen:
                continue
            tickers = _scope_tickers(c)
            if tickers is None or (ticker is None and tickers) or (ticker is not None and t in tickers):
                ids.append(c["id"])
    return _valid(ids), applied


def stamp(entry: dict, today: date | None = None, config_path: str | None = None, *,
          prev: Any = _UNSET, ticker: Optional[str] = None) -> dict:
    """entryに計算方法の変更の印を付けて返す（無ければそのまま）。

    prev を渡さない呼び出しは、古い形（日付）と環境変数の印だけ（2026-11-01までの動き）。
    """
    if prev is _UNSET:
        ids, applied = active_method_changes(today, config_path), None
    else:
        ids, applied = marks(prev, ticker, today, config_path)
    return apply(entry, ids, applied)


def apply(entry: dict, ids: list[str], applied: Optional[list[str]]) -> dict:
    """marks() の結果を entry に書く（同じ実行の複数の出力に同じ印を付けるとき）。前の印は消す"""
    entry.pop(KEY, None)
    if ids:
        entry[KEY] = ",".join(ids)
    if applied is not None:
        entry[APPLIED_KEY] = list(applied)
    return entry


def read_json(path: str) -> Optional[dict]:
    """書き換える前の出力（prev）を読む。無い・読めないときはNone"""
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def scope_for(change_id: str, ticker: str, config_path: str | None = None) -> Any:
    """その件のその銘柄のscope（{"fields","before","after"}）。

    ALL: scopeの無い新しい形・古い形・環境変数・configに無いID（全項目を計算方法の分とする）。
    None: scopeがあり、その銘柄が入っていない。
    新しい形の件で同じIDが複数あれば、最後の件。
    """
    found = None
    for c in _changes(_load(config_path)):
        if c.get("id") == change_id and _is_new_form(c):
            found = c
    if found is None or found.get("scope") is None:
        return ALL
    t = (ticker or "").upper()
    for s in found.get("scope") or []:
        if isinstance(s, dict) and str(s.get("ticker", "")).upper() == t:
            return {"fields": list(s.get("fields") or []), "before": dict(s.get("before") or {}),
                    "after": dict(s.get("after") or {})}
    return None
