"""Market Pulse の5分足の日ごとの保存（[[MARKETPULSE-MOVE-POSTMORTEM-EPIC-1]] ②、2026-11-01）。

大きく動いた日の事後分析（③の材料M・X「動きが集中した時間帯」「夜間と日中の変化」）のため、10本の5分足を
`docs/market-monitor/market-pulse/data/intraday/YYYY-MM-DD.json`（米国東部〈ET〉の日付ごと、10本を1ファイル）に残す。
Yahoo の5分足は「今日から60日（暦日）」より前を返さないため、毎晩残しておかないと消える。

- 書き込みはこのファイルの update() の1か所。夜の Market Pulse の回（collect_and_send.py）から呼ぶ。最初の一回は
  `python src/market/market_pulse/intraday_store.py --start 2026-09-04` を手元で実行して commit した。
- 形（チャット側の決定 B）: 銘柄ごとに列の配列 {"t": ["HH:MM"（ET）], "o", "h", "l", "c", "v"}、整形なし。1日 約60〜75KB。
- 時刻はETにそろえる（^VIX・^TNX はシカゴ時間で返る）。その ET の日付の足を全部入れる（夜間の先物・ドル・原油・金を含む）。
- 対象の日: 直近 RECENT_TRADING_DAYS（10）取引日の最初の日から、その回の時点の ET の今日までの全部の暦日（土日・休場日も、
  先物などの夜の足があるため）。ただし ET の今日は、NYSE の引けの後だけ（引けの前は書かない）。10本とも足の無い取引日でない日
  （土曜など）はファイルを作らない。取得の start は今日から59日より前にしない（Yahoo が受け付けない）。
- 取り直し: ファイルが無い日と「足りない日」だけを取り直す。足りない日 = 取得に失敗した銘柄がある日、NYSE の取引日なのに
  足が無い銘柄がある日、ET でその日がまだ終わっていない日（引けの後に書いた今日の分。夜の先物の足が後から増える）。
  そろった日（ほかの全部）は読み直さない。
- 取り直した日は、銘柄ごとに今あるものと今回のもののうち足の多い方を残す（足を減らさない）。取れなかった銘柄は missing に
  理由（fetch_error・no_bars）を記録し、推測で埋めない。
- 何度動かしても同じ: 中身は足と missing だけで、作った時刻のような毎回変わる値を持たない。中身が同じなら書かない。
- 取得は start/end で指定する（日曜の period='1d' は先物・ドルが空になるため）。prepost=True（HYG の時間外も入る）。
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.market_data import calendar_universe as cu  # noqa: E402

INTRADAY_DIR = os.path.join(REPO_ROOT, "docs", "market-monitor", "market-pulse", "data", "intraday")
TICKERS = ["^GSPC", "^IXIC", "^VIX", "^TNX", "DX-Y.NYB", "CL=F", "GC=F", "HYG", "ES=F", "NQ=F"]
INTERVAL = "5m"
RECENT_TRADING_DAYS = 10
MAX_LOOKBACK_DAYS = 59   # Yahoo の5分足は今日から60日（暦日）より前の start を受け付けない（10本とも「5m data not available」、2026-11-01 確認）
ET = ZoneInfo("America/New_York")
FIELDS = ("o", "h", "l", "c", "v")


def _num(x) -> Optional[float]:
    """有効数字6桁にそろえる（同じ足から毎回同じ値。NaN は None）。"""
    if x is None:
        return None
    x = float(x)
    if math.isnan(x):
        return None
    return float(f"{x:.6g}")


def split_by_et_date(bars: List[Dict[str, Any]]) -> Dict[str, Dict[str, list]]:
    """足（{"ts": tz付き datetime, open, high, low, close, volume}）を ET の日付ごとの列の配列に分ける。時刻の順。
    tz の無い足は使わない（どの時刻帯か分からないため）。同じ時刻の足が2つあれば後のものを使う。"""
    by: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for b in bars:
        ts = b["ts"]
        if ts.tzinfo is None:
            continue
        et = ts.astimezone(ET)
        by.setdefault(et.strftime("%Y-%m-%d"), {})[et.strftime("%H:%M")] = b
    out = {}
    for d, rows in by.items():
        keys = sorted(rows)
        out[d] = {"t": keys,
                  "o": [_num(rows[k].get("open")) for k in keys], "h": [_num(rows[k].get("high")) for k in keys],
                  "l": [_num(rows[k].get("low")) for k in keys], "c": [_num(rows[k].get("close")) for k in keys],
                  "v": [int(rows[k]["volume"]) if _num(rows[k].get("volume")) is not None else None for k in keys]}
    return out


def _default_fetch(symbol: str, start: date, end: date) -> List[Dict[str, Any]]:
    """yfinance の5分足（start〜end の前日まで、ET の日付の境目を含むよう呼び出し側が1日広げる）。失敗は例外。
    yfinance 1.2 は raise_errors が非推奨で、失敗しても空の表を返すことがある。対象は必ず取引日を含む（直近10取引日）ので、
    範囲全体で足が0本なら取得の失敗とみなす（fetch_error。yfinance の設定〈yf.config〉はほかの取得に響くので変えない）。"""
    import yfinance as yf
    df = yf.Ticker(symbol).history(start=start.isoformat(), end=end.isoformat(), interval=INTERVAL,
                                   prepost=True, auto_adjust=False)
    if df.empty:
        raise RuntimeError(f"{symbol}: {start}〜{end} の5分足が0本")
    return [{"ts": ts.to_pydatetime(), "open": r["Open"], "high": r["High"], "low": r["Low"], "close": r["Close"],
             "volume": r["Volume"]} for ts, r in df.iterrows()]


def _load(date_et: str, data_dir: str = INTRADAY_DIR) -> Optional[Dict[str, Any]]:
    if not os.path.exists(os.path.join(data_dir, f"{date_et}.json")):
        return None
    with open(os.path.join(data_dir, f"{date_et}.json"), encoding="utf-8") as f:
        return json.load(f)


def is_complete(doc: Optional[Dict[str, Any]], date_et: str, now: datetime) -> bool:
    """そろった日か（取り直さない）。ファイルが無い・取得に失敗した銘柄がある・NYSEの取引日に足の無い銘柄がある・
    ET でその日がまだ終わっていない、のどれかなら False。"""
    if doc is None:
        return False
    if now.astimezone(ET).strftime("%Y-%m-%d") <= date_et:
        return False
    missing = doc.get("missing") or {}
    if any(v == "fetch_error" for v in missing.values()):
        return False
    if missing and cu.is_nyse_trading_day(date_et):
        return False
    return all(t in (doc.get("tickers") or {}) or t in missing for t in TICKERS)


def target_dates(now: datetime, start: Optional[date] = None) -> List[str]:
    """対象の ET の日付（暦日）。start を省くと直近 RECENT_TRADING_DAYS 取引日の最初の日から。
    ET の今日は NYSE の引けの後だけ（取引日でなければ、今日はまだ終わっていないので入れない）。"""
    today = now.astimezone(ET).date()
    if start is None:
        days = cu.nyse_trading_days_ending(today, RECENT_TRADING_DAYS)
        start = date.fromisoformat(days[0])
    out = []
    d = start
    while d < today:
        out.append(d.isoformat())
        d += timedelta(days=1)
    close = cu.nyse_close_utc(today.isoformat())
    if close is not None and now >= close:
        out.append(today.isoformat())
    return out


def update(now: Optional[datetime] = None, data_dir: str = INTRADAY_DIR, *, start: Optional[date] = None,
           fetch: Callable[[str, date, date], List[Dict[str, Any]]] = _default_fetch) -> Dict[str, Any]:
    """足りない日だけ取り直して書く。返り値 {"written": [日付], "skipped_complete": 数, "missing": {日付: {銘柄: 理由}}}。"""
    now = now or datetime.now(timezone.utc)
    dates = [d for d in target_dates(now, start) if not is_complete(_load(d, data_dir), d, now)]
    if not dates:
        return {"written": [], "skipped_complete": len(target_dates(now, start)), "missing": {}}
    # 1銘柄1回の取得で全部の対象の日を覆う（ET の日付の境目のため、UTC で前後に1日広げる）
    lo = max(date.fromisoformat(dates[0]) - timedelta(days=1), now.date() - timedelta(days=MAX_LOOKBACK_DAYS))
    hi = date.fromisoformat(dates[-1]) + timedelta(days=2)
    got: Dict[str, Optional[Dict[str, Dict[str, list]]]] = {}
    for sym in TICKERS:
        try:
            got[sym] = split_by_et_date(fetch(sym, lo, hi))
        except Exception as e:
            print(f"[WARN] 5分足の取得に失敗: {sym}（{type(e).__name__}: {str(e)[:120]}）")
            got[sym] = None
    written, missing_all = [], {}
    for date_et in dates:
        d = date_et
        old = _load(date_et, data_dir) or {}
        tickers = dict(old.get("tickers") or {})
        missing: Dict[str, str] = {}
        for sym in TICKERS:
            new = (got[sym] or {}).get(d)
            cur = tickers.get(sym)
            if new and (cur is None or len(new["t"]) > len(cur["t"])):
                tickers[sym] = new
            elif cur is None:
                missing[sym] = "fetch_error" if got[sym] is None else "no_bars"
        doc = {"date_et": d, "interval": INTERVAL, "tz": "America/New_York",
               "tickers": {k: tickers[k] for k in TICKERS if k in tickers}, "missing": missing}
        if not doc["tickers"] and set(missing.values()) == {"no_bars"} and not cu.is_nyse_trading_day(d):
            continue   # 取引所がどれも開いていない日（土曜など）はファイルを作らない
        if missing:
            missing_all[d] = missing
        if doc == old:
            continue
        os.makedirs(data_dir, exist_ok=True)
        with open(os.path.join(data_dir, f"{date_et}.json"), "w", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(doc, ensure_ascii=False, separators=(",", ":")) + "\n")
        written.append(d)
    return {"written": written, "skipped_complete": len(target_dates(now, start)) - len(dates), "missing": missing_all}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--start", help="最初の日（ET、YYYY-MM-DD）。省くと直近10取引日")
    ap.add_argument("--data-dir", default=INTRADAY_DIR)
    a = ap.parse_args(argv)
    r = update(start=date.fromisoformat(a.start) if a.start else None, data_dir=a.data_dir)
    print(f"[INFO] 5分足: 書いた {len(r['written'])}日（{r['written'][:3]}…{r['written'][-3:]}）・そろい済み {r['skipped_complete']}日")
    for d, m in r["missing"].items():
        print(f"  {d} 欠け: {m}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
