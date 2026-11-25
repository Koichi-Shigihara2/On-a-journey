"""手書きの成長率の設定の古さの印（[[TANUKI-UPSIDE-MAGNITUDE-INFLATED-1]] 6-1、2026-11-25）。

背景: 成長率が `config/segment_config.json` の手書き（セグメント別の重み×成長率。`growth.source=segment_weighted` かつ
`segment_configured=True`）で決まる銘柄は、更新の仕組みが無く、会計年度が古いまま使われる（docs/architecture/
HAND_SET_GROWTH_SURVEY_2026-11-25.md）。値は置き換えない（置き換えるとかえって上がる銘柄が多い）。
最後の更新から STALE_DAYS を超えたら、画面と report.txt に古さの印を出すだけ（表示だけ。理論株価・判定は変えない）。

最後の更新日は、設定の銘柄ごとのブロックの `updated`（YYYY-MM-DD）。成長率・重みを手で変えたらこの日付も更新する。
判定はこの関数の1か所だけ。銘柄名は書かない。
"""
from __future__ import annotations

import json
import os
from datetime import date
from typing import Optional

STALE_DAYS = 180

STALE = "stale"        # 印を付ける
FRESH = "fresh"        # 手書きだが180日以内
UNKNOWN = "unknown"    # 手書きだが更新日が記録されていない
NOT_HANDSET = "none"   # 手書きではない（XBRLのセグメント・過去の数字の平均）

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CONFIG = os.path.join(_ROOT, "config", "segment_config.json")


def _d(v) -> Optional[date]:
    try:
        return date.fromisoformat(str(v)[:10])
    except Exception:
        return None


def updated_date(ticker: str, config: Optional[dict] = None) -> Optional[str]:
    if config is None:
        try:
            with open(_CONFIG, encoding="utf-8") as f:
                config = json.load(f)
        except Exception:
            return None
    return (config.get(ticker) or {}).get("updated")


def evaluate(growth_source: Optional[str], segment_configured: Optional[bool], updated: Optional[str], today: date) -> dict:
    out: dict = {"status": NOT_HANDSET, "updated": None, "age_days": None, "label": None}
    if growth_source != "segment_weighted" or not segment_configured:
        return out
    u = _d(updated)
    if u is None:
        out["status"] = UNKNOWN
        out["label"] = "手書きの成長率の更新日が記録されていない"
        return out
    age = (today - u).days
    out["updated"] = u.isoformat()
    out["age_days"] = age
    if age > STALE_DAYS:
        out["status"] = STALE
        out["label"] = f"手書きの成長率は{age}日前（{u.isoformat()}）の設定"
    else:
        out["status"] = FRESH
    return out


def build(ticker: str, growth_source: Optional[str], segment_configured: Optional[bool], today: Optional[date] = None,
          config: Optional[dict] = None) -> dict:
    return evaluate(growth_source, segment_configured, updated_date(ticker, config), today or date.today())


def report_line(h: Optional[dict]) -> Optional[str]:
    if not h:
        return None
    if h.get("status") == STALE:
        return f"Handset_Growth_Age: ⚠️ {h['label']}（{STALE_DAYS}日を超えた。会社の見通しの変化・新しい決算は反映されていない）"
    if h.get("status") == UNKNOWN:
        return f"Handset_Growth_Age: {h['label']}"
    return None
