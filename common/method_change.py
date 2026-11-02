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
# 前回の出力が見つからず、新しい形の件の印を決められなかった出力に付ける（rccのCHECK-67が全件で付いていればWARN）
PREV_MISSING_KEY = "method_change_prev_missing"
# 「前回の出力」は前回公開したもの（docs/配下）から読む（2026-11-03。HypeCoreは手元の書き出し先
# src/value/hypecore/data/〈.gitignore、CIに無い〉から読んでいて、CIで毎回「前回の出力が無い」になり印が付かなかった）。
# {種類: (リポジトリの根からのフォルダ, ファイル名〈{T}は銘柄〉)}
PUBLISHED = {
    "tanuki": ("docs/value-monitor/tanuki_valuation/data", "{T}/latest.json"),
    "hypecore": ("docs/value-monitor/hypecore/data", "{T}_poc.json"),
    "stonks": ("docs/value-monitor/stonks-silo/data", "results.json"),
    "rotce": ("docs/common/sec_data/rotce", "{T}.json"),
    "rotce_summary": ("docs/common/sec_data/rotce", "_summary.json"),
}
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
        return apply(entry, active_method_changes(today, config_path), None)
    ids, applied = marks(prev, ticker, today, config_path)
    return apply(entry, ids, applied, prev_missing=prev is None)


def apply(entry: dict, ids: list[str], applied: Optional[list[str]], prev_missing: bool = False) -> dict:
    """marks() の結果を entry に書く（同じ実行の複数の出力に同じ印を付けるとき）。前の印は消す。
    prev_missing: 前回の出力が無かった（新しい形の件があるときだけ PREV_MISSING_KEY を書く）"""
    entry.pop(KEY, None)
    entry.pop(PREV_MISSING_KEY, None)
    if ids:
        entry[KEY] = ",".join(ids)
    if applied is not None:
        entry[APPLIED_KEY] = list(applied)
        if prev_missing:
            entry[PREV_MISSING_KEY] = True
    return entry


def previous_output(kind: str, ticker: Optional[str] = None, base_dir: Optional[str] = None) -> Optional[dict]:
    """前回公開した出力（PUBLISHEDの場所）。base_dir は書き出し先を変えられる書き手（TANUKI・ROTCEのテスト等）が渡す"""
    folder, name = PUBLISHED[kind]
    d = base_dir or os.path.join(_REPO_ROOT, folder)
    return read_json(os.path.join(d, name.format(T=(ticker or "").upper())))


def check_previous_outputs(root: Optional[str] = None, config_path: str | None = None) -> list[str]:
    """CHECK-67（WARN）: 新しい形の件があるのに、ある種類の公開の出力が全件とも「前回の出力が無い」（PREV_MISSING_KEY）なら、
    印を付けられていない（前回の出力を読む場所の誤り等）。種類ごとに1行。判定の1か所（rccが呼ぶ）"""
    import glob
    if not new_form_ids(config_path):
        return []
    root = root or _REPO_ROOT
    out = []
    for kind, (folder, name) in PUBLISHED.items():
        paths = glob.glob(os.path.join(root, folder, name.replace("{T}", "*")))
        if kind == "rotce":
            paths = [p for p in paths if not os.path.basename(p).startswith("_")]
        docs = [d for d in (read_json(p) for p in paths) if d is not None]
        if not docs:
            continue
        missing = sum(1 for d in docs if d.get(PREV_MISSING_KEY))
        if missing == len(docs):
            out.append(f"  [WARN-67 計算方法の変更の前回の出力] {kind}: 公開の出力 {len(docs)}件すべてで前回の出力が見つからず、"
                       f"新しい形の件の印を付けられていない（{folder}）。common/method_change.py の PUBLISHED と書き手の読み方を確かめる")
    return out


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
