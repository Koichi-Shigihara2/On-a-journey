"""決算の発表後で、まだLayer3に取り込んでいない銘柄の印（[[EARNINGS-REFLECTION-LAG-1]] 案B、2026-11-25）。

背景: SECを取りに行くのは日曜の SEC_Data_Update だけで、提出からLayer3に入るまで中央値4.5日かかる
（docs/architecture/EARNINGS_REFLECTION_LAG_STEP0.md）。その間、画面の数字は前の四半期のままなので、
「最新の決算は反映前」と表示する。表示だけで、理論株価・判定・stageは変えない（method_changeには登録しない）。

判定（この関数の1か所だけ。銘柄名は書かない）:
  発表日 ≤ 今日、かつ Layer3の最新の四半期の期末 < その発表の四半期の期末。
  - 発表日: Yahooの予定日（common/market_data の calendar.earnings_date）のうち今日以前で一番新しい日。
    8-K Item 2.02 の提出日が手元にあれば（item_202_dates）それを優先して確かめに使う（追加の取得はしない。今は手元に無い）
  - 発表の四半期の期末: Layer3の最新の期末の3か月後（その期末が発表日より前なら、その期の決算の発表と見なす）
  - 予定日が取れない銘柄は印を付けない（status="no_date"）。古すぎる予定日（MAX_AGE_DAYS超）も付けない
"""
from __future__ import annotations

import calendar
import json
import os
from datetime import date
from typing import Any, Iterable, Optional

MAX_AGE_DAYS = 90   # 発表日からこの日数を超えた予定日は、取り込みの遅れではなく予定日の古さと見て印を付けない

PENDING = "pending"        # 印を付ける
NONE = "none"              # 印なし（発表前・取り込み済み）
NO_DATE = "no_date"        # 予定日が取れず、印を付けられない
NO_LAYER3 = "no_layer3"    # Layer3の最新の期末が分からず、印を付けられない

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _d(v: Any) -> Optional[date]:
    try:
        return date.fromisoformat(str(v)[:10])
    except Exception:
        return None


def add_quarter(end: date) -> date:
    """期末の3か月後（月末なら月末、52/53週の会社は数日ずれる。発表日との比較にだけ使う）"""
    m = end.month + 3
    y = end.year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    last = calendar.monthrange(y, m)[1]
    day = last if end.day == calendar.monthrange(end.year, end.month)[1] else min(end.day, last)
    return date(y, m, day)


def layer3_latest_period_end(ticker: str, today: Optional[date] = None) -> Optional[str]:
    """Layer3（normalized の Assets＝貸借対照表の期末）の最新の期末。無ければNone"""
    p = os.path.join(_ROOT, "common", "sec_data", "normalized", f"{ticker}_quarterly_normalized.json")
    try:
        with open(p, encoding="utf-8") as f:
            pts = (json.load(f).get("fields") or {}).get("Assets") or []
    except Exception:
        return None
    ends = [e.get("end") for e in pts if e.get("end") and (today is None or (e.get("filed") or "") <= today.isoformat())]
    return max(ends) if ends else None


def evaluate(earnings_dates: Optional[Iterable[Any]], layer3_end: Optional[str], today: date,
             item_202_dates: Optional[Iterable[Any]] = None) -> dict:
    """印の判定。戻り値の status が PENDING のときだけ画面に印を出す"""
    out: dict = {"status": NONE, "earnings_date": None, "earnings_source": None, "layer3_period_end": layer3_end}
    cands = []
    src = "yahoo_calendar"
    if item_202_dates:
        cands = [x for x in (_d(v) for v in item_202_dates) if x and x <= today]
        if cands:
            src = "8-K_2.02"
    if not cands:
        ds = [x for x in (_d(v) for v in (earnings_dates or [])) if x]
        if not ds:
            out["status"] = NO_DATE
            return out
        cands = [x for x in ds if x <= today]
        src = "yahoo_calendar"
    if not cands:
        return out                                   # 予定日はあるが全部先（発表前）
    e = max(cands)
    out["earnings_date"] = e.isoformat()
    out["earnings_source"] = src
    if (today - e).days > MAX_AGE_DAYS:
        out["status"] = NO_DATE                      # 古すぎる予定日は使わない
        out["note"] = f"予定日が{MAX_AGE_DAYS}日より古い"
        return out
    le = _d(layer3_end)
    if le is None:
        out["status"] = NO_LAYER3
        return out
    nxt = add_quarter(le)
    out["announced_period_end_est"] = nxt.isoformat()
    if le < nxt < e:                                 # Layer3の最新の期末の次の四半期は、発表日より前に終わっている＝その決算が未反映
        out["status"] = PENDING
        out["label"] = f"最新の決算（{e.strftime('%m-%d')} 発表）は未反映。次の取り込みは日曜"
    return out


def build(ticker: str, today: Optional[date] = None, calendar_dict: Optional[dict] = None,
          prev: Optional[dict] = None) -> dict:
    """銘柄の印。calendar_dict を渡さなければ common.market_data.reader.get_calendar から読む"""
    today = today or date.today()
    if calendar_dict is None:
        try:
            from common.market_data.reader import get_calendar
            calendar_dict = get_calendar(ticker) or {}
        except Exception:
            calendar_dict = {}
    dates = list(calendar_dict.get("earnings_date") or [])
    # Yahooは発表の後に予定日を次の四半期へ進めることがある。前回の出力で印が付いていた発表日は、取り込まれるまで引き継ぐ
    if prev is None:
        prev = _load_prev(ticker)
    if prev and prev.get("status") == PENDING and prev.get("earnings_date"):
        dates.append(prev["earnings_date"])
    return evaluate(dates, layer3_latest_period_end(ticker, today), today)


def _load_prev(ticker: str) -> Optional[dict]:
    p = os.path.join(_ROOT, "docs", "value-monitor", "tanuki_valuation", "data", ticker, "latest.json")
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f).get("earnings_pending")
    except Exception:
        return None


def report_line(ep: Optional[dict]) -> Optional[str]:
    """report.txt の1行。印が付くとき・予定日が取れず付けられないときだけ"""
    if not ep:
        return None
    if ep.get("status") == PENDING:
        return f"Earnings_Pending: ⚠️ {ep['label']}（Layer3の最新の期末 {ep['layer3_period_end']}）"
    if ep.get("status") == NO_DATE:
        return "Earnings_Pending: 決算の予定日が取れないため、決算の反映前の印を付けられません"
    return None
