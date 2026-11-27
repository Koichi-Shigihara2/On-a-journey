"""値の日付と鮮度の判定（1か所。[[MARKETPULSE-REVIEW-EPIC-1]] 順1・型L、2026-11-27）。

Market Pulse の data_quality・画面（値ごとの日付と「古い」の印）・System Health が、同じこの部品を読む。
手書きの鮮度の一覧（stage_conclusions.DQ_INDICATORS など）が入力の一覧と別に管理され、日経・FRED系列・HYスプレッドが
一覧に無いために「古い日付を黙って使う」ことが見えなかった（1-a・3-b・MACROPULSE-DATA-LAG-AND-REGIME-DRIFT-1）。

入力の種類（kind）ごとに「期待する基準日」を決める:
  us_close   : 直近に引けたNYSEの取引日（呼び出し側が渡す。nyse_latest_closed_day）
  jpx        : 日本の取引所の直近の取引日のうち、引け（15:30 JST）から JPX_CONFIRM_LAG 経ったもの。
               ^N225 はYahooの日足の終値が翌日以降にならないと入らない（実測: 引けの約15時間後は無し・約39時間後は有り。
               2026-11-24・25の足が定時の取得〈06:28 JST〉で「終値が無い」→ 翌日の定時で保存）。構造上の遅れなので、
               その分を見込んだ日付と比べる（見込みより古ければ「古い」）
  fred_daily : FREDの日次系列（HYスプレッド・SP500・DGS*）。FREDは1営業日遅れで公表されるので、期待する終値日の前のNYSEの取引日

日付の比べ方は judge() の1か所。日付を持たない入力は「日付なし（unknown）」とし、黙って新しい扱いにしない。
カレンダーの判定は common/market_data/calendar_universe.py（決める所）に任せる。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional
from zoneinfo import ZoneInfo

from common.market_data.calendar_universe import exchange_trading_days, nyse_trading_days_ending

KIND_US_CLOSE = "us_close"
KIND_JPX = "jpx"
KIND_FRED_DAILY = "fred_daily"
KINDS = (KIND_US_CLOSE, KIND_JPX, KIND_FRED_DAILY)

JPX_CLOSE_HOUR, JPX_CLOSE_MINUTE = 15, 30            # 東京の引け（JST）
JPX_CONFIRM_LAG = timedelta(hours=39)                # Yahooの日足の終値が入るまで（実測の上限。定時の取得は引けの約15時間後）
_TZ_TYO = ZoneInfo("Asia/Tokyo")

# 入力名 → kind（ここに無い入力は us_close）。画面・data_quality・System Health が同じ表を読む
SOURCE_KINDS: Dict[str, str] = {
    "日経平均": KIND_JPX,
    "HYスプレッド": KIND_FRED_DAILY,
    "S&P500（FRED）": KIND_FRED_DAILY,
}


def kind_of(name: str) -> str:
    return SOURCE_KINDS.get(name, KIND_US_CLOSE)


def _iso(d: Any) -> str:
    return d if isinstance(d, str) else d.strftime("%Y-%m-%d")


def jpx_expected_date(now_utc: datetime) -> Optional[str]:
    """引け（15:30 JST）から JPX_CONFIRM_LAG 経った、日本の直近の取引日。見つからなければ None"""
    for day in reversed(exchange_trading_days("JPX")):
        y, m, d = (int(x) for x in day.split("-"))
        close = datetime(y, m, d, JPX_CLOSE_HOUR, JPX_CLOSE_MINUTE, tzinfo=_TZ_TYO).astimezone(timezone.utc)
        if close + JPX_CONFIRM_LAG <= now_utc:
            return day
    return None


def _previous_nyse_day(day: str) -> Optional[str]:
    days = nyse_trading_days_ending(day, 2)     # day を含む直近2取引日（day が取引日でなければ day より前の2日）
    if not days:
        return None
    return days[0] if days[-1] == day and len(days) == 2 else days[-1]


def expected_as_of(kind: str, now_utc: datetime, us_expected_close: Optional[str]) -> Optional[str]:
    """kind の入力が、いま持っているはずの基準日（'YYYY-MM-DD'）。出せなければ None"""
    if kind == KIND_JPX:
        return jpx_expected_date(now_utc)
    if kind == KIND_FRED_DAILY:
        return _previous_nyse_day(us_expected_close) if us_expected_close else None
    return us_expected_close


def judge(as_of: Optional[str], expected: Optional[str]) -> Dict[str, Any]:
    """status: fresh（期待する日以降）・stale（期待する日より古い）・unknown（日付が無い、または期待する日が出せない）。
    lag_days は期待する日との暦日の差（古いときだけ正）"""
    if not as_of:
        return {"as_of": None, "expected": expected, "status": "unknown", "lag_days": None, "reason": "日付なし"}
    if not expected:
        return {"as_of": as_of, "expected": None, "status": "unknown", "lag_days": None, "reason": "期待する日を出せない"}
    lag = (datetime.fromisoformat(expected) - datetime.fromisoformat(as_of)).days
    return {"as_of": as_of, "expected": expected, "status": "stale" if lag > 0 else "fresh",
            "lag_days": max(lag, 0), "reason": None}


def assess(elements: Dict[str, Optional[str]], now_utc: datetime, us_expected_close: Optional[str],
           kinds: Optional[Dict[str, str]] = None) -> Dict[str, Dict[str, Any]]:
    """elements: {入力名: 基準日}。戻り値: {入力名: judge() の結果 + kind}"""
    out: Dict[str, Dict[str, Any]] = {}
    for name, as_of in elements.items():
        kind = (kinds or {}).get(name) or kind_of(name)
        res = judge(as_of, expected_as_of(kind, now_utc, us_expected_close))
        res["kind"] = kind
        out[name] = res
    return out


def stale_names(assessed: Dict[str, Dict[str, Any]]) -> Iterable[str]:
    return sorted(k for k, v in assessed.items() if v.get("status") == "stale")
