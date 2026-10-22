"""
common/macro_data/reader.py

FRED統合層の読み取りAPI（新DB構築プロジェクト フェーズ1、BACKLOG
[[MACRODATA-LAYER-CONSTRUCTION-1]]）。fetcher.pyが書き込む
series/{SERIES_ID}.jsonを読み取る唯一のモジュールとして新設する。

現時点でこのモジュールを参照する本番消費者は存在しない（グリーンフィールド
実装。`05_main.py`・`collect_and_send.py`側の切替は次段階で行う）。データが
一切存在しない系列に対する呼び出しは、例外ではなくNone/空リストを返す
（common/market_data/reader.pyと同じエラー耐性方針）。

reader.py内で外部API呼び出しは一切行わない（ファイル読み取りのみ）。
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .fetcher import MACRO_DATA_DIR

__all__ = [
    "get_latest",
    "get_series",
    "get_value_as_of",
]


def _resolve_base_dir(base_dir: Optional[str]) -> str:
    return base_dir if base_dir is not None else MACRO_DATA_DIR


def _load_json(path: str, default: Any) -> Any:
    """JSON読み込み。ファイル不在・パース失敗時は例外を投げずdefaultを返す
    （エラー耐性。fetcher.py未実行の系列への呼び出しがここで吸収される）。
    """
    if not os.path.exists(path):
        return json.loads(json.dumps(default))
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"   [WARN] {path} 読み込み失敗（初期値で継続）: {e}")
        return json.loads(json.dumps(default))


def _load_series_payload(series_id: str, base_dir: str) -> Dict[str, Any]:
    path = os.path.join(base_dir, "series", f"{series_id}.json")
    return _load_json(path, default={"series_id": series_id, "records": []})


def _to_date_str(date: Any) -> str:
    return date.isoformat()[:10] if hasattr(date, "isoformat") else str(date)[:10]


def get_latest(series_id: str, base_dir: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """series/{SERIES_ID}.jsonの最新エントリ（value/as_of/fetched_at/
    source/source_detail）を返す。データが存在しない場合はNoneを返す。
    """
    base = _resolve_base_dir(base_dir)
    records = _load_series_payload(series_id, base).get("records", [])
    if not records:
        return None
    latest = dict(sorted(records, key=lambda r: r.get("as_of", ""))[-1])
    return latest


def get_series(series_id: str, start: Optional[Any] = None, end: Optional[Any] = None,
               base_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """期間内の全エントリを観測日（as_of）昇順で返す。

    start/end は 'YYYY-MM-DD' 文字列または date/datetime を受け付ける
    （両端を含む、省略時は無制限）。観測日（as_of）を含む生データそのもの
    を返すため、消費側（VXN50日MA等の「直近N件」ロジック）が期間の
    連続性を自前で検証できる。データが一切存在しない系列は空リストを
    返す（例外は発生させない）。
    """
    base = _resolve_base_dir(base_dir)
    records = _load_series_payload(series_id, base).get("records", [])
    if not records:
        return []

    start_str = _to_date_str(start) if start is not None else None
    end_str = _to_date_str(end) if end is not None else None

    filtered = [
        r for r in records
        if r.get("as_of")
        and (start_str is None or r["as_of"] >= start_str)
        and (end_str is None or r["as_of"] <= end_str)
    ]
    filtered.sort(key=lambda r: r["as_of"])
    return filtered


def get_value_as_of(series_id: str, date: Any, lookback_days: int = 45,
                     base_dir: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """指定日（date）以前で最も新しい観測値を、lookback_days日以内の
    ルックバック窓で探索して返す。

    デフォルト45日は月次系列を確実に拾える幅（common/market_data/
    reader.py::get_price_on_or_after()系のAPIパターン: 起点から固定日数の
    ウィンドウでレコードを検索、を踏襲。ただし本APIは「date以前」を探す
    点が方向として逆であり、05_import_history.py::_lookup_ctx()〈2026-10-23に削除〉の
    「target_date以前・lookback日以内の直近値」パターンにより近い）。

    窓内に該当データが見つからない場合、または系列自体のデータが
    存在しない場合はNoneを返す（例外は発生させない）。
    """
    base = _resolve_base_dir(base_dir)
    target_str = _to_date_str(date)
    target = datetime.strptime(target_str, "%Y-%m-%d").date()
    cutoff_str = (target - timedelta(days=lookback_days)).isoformat()

    records = _load_series_payload(series_id, base).get("records", [])
    candidates = [
        r for r in records
        if r.get("as_of") and cutoff_str <= r["as_of"] <= target_str
    ]
    if not candidates:
        return None

    chosen = dict(max(candidates, key=lambda r: r["as_of"]))
    return chosen
