"""Market Pulse 実装C（指示書㉗、2026-09-30）: 段階8の先物・ドル円の最新値（取得時刻つき）。

米国: S&P500先物（ES=F）・NASDAQ100先物（NQ=F）、日本: 日経平均CME先物（円建て、NIY=F）・ドル円（JPY=X）。
yfinanceの15分足の最新の足の終値と、その足の時刻・取得時刻を記録する。前日比はYahooの前日終値（fast_info.previousClose）との比で、
その値も記録する（先物の前日終値は清算値）。data_qualityの判定からは外し（設計書6章）、取得時刻を表示する。
取得に失敗した銘柄は推測で埋めず、failedに記録して「取得できず」と表示する。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

SYMBOLS = [
    {"symbol": "ES=F", "name": "S&P500先物", "region": "米国"},
    {"symbol": "NQ=F", "name": "NASDAQ100先物", "region": "米国"},
    {"symbol": "NIY=F", "name": "日経平均先物（CME、円建て）", "region": "日本"},
    {"symbol": "JPY=X", "name": "ドル円", "region": "日本"},
]


def _default_quote(symbol: str) -> Dict[str, Any]:
    """yfinanceの15分足の最新の足と前日終値。"""
    import yfinance as yf
    t = yf.Ticker(symbol)
    h = t.history(period="5d", interval="15m", auto_adjust=False).dropna(how="all")
    if h.empty or h["Close"].iloc[-1] != h["Close"].iloc[-1]:
        raise RuntimeError("15分足が空")
    prev = t.fast_info.get("previousClose")
    info = {}
    try:
        info = t.info or {}
    except Exception:
        pass
    return {"value": float(h["Close"].iloc[-1]), "bar_time": h.index[-1].to_pydatetime(),
            "previous_close": float(prev) if prev else None, "contract": info.get("underlyingSymbol")}


def _provisional_kind(symbol: str, bar_time: datetime) -> Optional[str]:
    """最新の足が属する日足がまだ確定していなければ、その種類（"清算前"・"日中"、fetcher.expected_provisional_kind）。"""
    try:
        from common.market_data.fetcher import expected_provisional_kind
        return expected_provisional_kind(symbol, bar_time.astimezone(timezone.utc).strftime("%Y-%m-%d"))
    except Exception as e:
        print(f"[WARN] 暫定の種類の判定失敗（{symbol}）: {e}")
        return None


def snapshot(quote: Callable[[str], Dict[str, Any]] = _default_quote, now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    items, failed = [], []
    for s in SYMBOLS:
        try:
            q = quote(s["symbol"])
            prev = q.get("previous_close")
            items.append({**s, "value": round(q["value"], 4), "previous_close": round(prev, 4) if prev else None,
                          "change_pct": round((q["value"] / prev - 1) * 100, 2) if prev else None,
                          "bar_time_utc": q["bar_time"].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                          "contract": q.get("contract"), "status": "ok"})
            # 取引中の15分足の値は確定前（先物は清算前、ドル円は日の区切り前）。種類は日足の区切りの定義から判定する
            kind = _provisional_kind(s["symbol"], q["bar_time"])
            if kind:
                items[-1].update(provisional=True, provisional_kind=kind)
        except Exception as ex:
            print(f"[WARN] 先物・ドル円の最新値の取得に失敗: {s['symbol']} ({type(ex).__name__}: {ex})")
            items.append({**s, "value": None, "previous_close": None, "change_pct": None, "bar_time_utc": None,
                          "contract": None, "status": "取得できず"})
            failed.append(s["symbol"])
    status = "ok" if not failed else ("failed" if len(failed) == len(SYMBOLS) else "partial")
    return {"fetched_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "items": items, "failed": failed, "status": status}
