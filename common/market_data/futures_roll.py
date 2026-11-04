"""先物の連続系列（=F）の限月の切り替えの検知と印（[[EXTERNAL-SOURCE-CHANGE-WATCH-1]] B3、2026-11-04。統合した [[MARKETDATA-FUTURES-ROLL-1]]）。

Yahooの連続系列（CL=F・GC=F・ES=F・NQ=F・NIY=F）は限月の乗り換えの日に前日比が不連続になる。日足の行には2026-09-29から
限月（contract〈YahooのunderlyingSymbol〉・contract_close・contract_prev_close）を記録している（fetcher.py）。

印（行の _contract_roll。値は変えない。日足の書き込みの1つの関数 write_daily_rows の印だけを変える経路〈reason=MARK_ONLY〉で付ける）:
  "confirmed"  行の contract が前の行から変わった日（どちらも月の記号つき）。その日の前日比は、新しい限月の contract_prev_close と比べる
  "possible"   contract を記録する前（CONTRACT_SINCE より前）で、ES=F・NQ=F の満期の日（3・6・9・12月の第3金曜。Yahooの連続系列は
               満期の日に乗り換える）に、前日比と指数（POSSIBLE_RULES の相手）の前日比の差が閾値を超えた日。確かめられないので「可能性」
異常（System Health の [P] ExternalChange に出す。common/external_watch.py が読む）:
  contract の欄が、その銘柄で最初に限月を記録した日（CL=F・GC=F は2026-09-29、ES=F・NQ=F は10-01）より後の行で欠けている・
  月の記号が無い（2026-10-30 の ES=F・NQ=F の「ES.CME」「NQ.CME」）
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_DIR))
if _REPO_ROOT not in sys.path:   # ワークフローは python common/market_data/futures_roll.py で直接動かす
    sys.path.insert(0, _REPO_ROOT)
FUTURES = ("CL=F", "GC=F", "ES=F", "NQ=F", "NIY=F")
CONTRACT_SINCE = "2026-09-29"          # 行に限月を記録し始めた日
MARK_KEY = "_contract_roll"
CONFIRMED, POSSIBLE = "confirmed", "possible"
# 月の記号つきの限月（例: CLZ26.NYM・ESZ26.CME・GCZ26.CMX・NIYZ26.CME）
_MONTH_CODE = re.compile(r"^[A-Z]+[FGHJKMNQUVXZ]\d{2}\.[A-Z]+$")
# "possible" を探す先物と相手・前日比の差の閾値（GC=F〈GLD〉・NIY=F〈日経平均〉は相手と引けの時刻が違い誤報が多いので使わない。
# CL=F は相手になるETFが日足に無い）
POSSIBLE_RULES = {"ES=F": ("^GSPC", 0.006), "NQ=F": ("^NDX", 0.008)}


def _load(symbol: str, daily_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    path = os.path.join(daily_dir or os.path.join(_DIR, "daily"), f"{symbol}.json")
    try:
        with open(path, encoding="utf-8") as f:
            return [r for r in json.load(f).get("records", []) if r.get("date")]
    except (OSError, ValueError):
        return []


def has_month_code(contract: Optional[str]) -> bool:
    return bool(contract) and bool(_MONTH_CODE.match(contract))


def _third_friday(y: int, m: int) -> date:
    d = date(y, m, 15)
    while d.weekday() != 4:
        d += timedelta(1)
    return d


def is_quarterly_expiry(d: str) -> bool:
    x = date.fromisoformat(d)
    return x.month in (3, 6, 9, 12) and x == _third_friday(x.year, x.month)


def desired_marks(symbol: str, records: List[Dict[str, Any]], proxy: Optional[List[Dict[str, Any]]] = None) -> Dict[str, str]:
    """{日付: "confirmed"|"possible"}（付けるべき印）"""
    out: Dict[str, str] = {}
    rs = [r for r in records if r.get("close")]
    for a, b in zip(rs, rs[1:]):
        if has_month_code(a.get("contract")) and has_month_code(b.get("contract")) and a["contract"] != b["contract"]:
            out[b["date"]] = CONFIRMED
    rule = POSSIBLE_RULES.get(symbol)
    if rule and proxy:
        pm = {r["date"]: r["close"] for r in proxy if r.get("close")}
        for a, b in zip(rs, rs[1:]):
            if b["date"] >= CONTRACT_SINCE or not is_quarterly_expiry(b["date"]) or a["date"] not in pm or b["date"] not in pm:
                continue
            gap = (b["close"] / a["close"] - 1) - (pm[b["date"]] / pm[a["date"]] - 1)
            if abs(gap) > rule[1]:
                out[b["date"]] = POSSIBLE
    return out


def contract_anomalies(daily_dir: Optional[str] = None) -> List[Dict[str, str]]:
    """最初に限月を記録した日より後の行で、限月の欄の欠け・月の記号の無い限月（[P] の件。external_watch の形）"""
    out = []
    for s in FUTURES:
        rs = _load(s, daily_dir)
        since = min((r["date"] for r in rs if r.get("contract")), default=None)   # 銘柄ごとに最初に限月を記録した日
        if since is None:
            continue
        for r in rs:
            if r["date"] < since or not r.get("close") or r.get("_no_yahoo_bar"):
                continue
            c = r.get("contract")
            if not has_month_code(c):
                out.append({"kind": "contract", "ticker": s, "match": r["date"],
                            "detail": f"{s} {r['date']}: 限月の欄が{'無い' if not c else f'月の記号の無い「{c}」'}（前日比を同じ限月で計算できない日）"})
    return out


def apply_marks(base_dir: Optional[str] = None, symbols=FUTURES, dry_run: bool = False) -> Dict[str, Dict[str, str]]:
    """印の無い行に印を付ける（write_daily_rows の MARK_ONLY。値は変えない）。{銘柄: {日付: 印}}（付けた・付けるもの）"""
    from common.market_data.fetcher import MARK_ONLY, write_daily_rows
    base_dir = base_dir or _DIR
    daily_dir = os.path.join(base_dir, "daily")
    done: Dict[str, Dict[str, str]] = {}
    for s in symbols:
        rs = _load(s, daily_dir)
        rule = POSSIBLE_RULES.get(s)
        want = desired_marks(s, rs, _load(rule[0], daily_dir) if rule else None)
        by = {r["date"]: r for r in rs}
        rows = [{**by[d], MARK_KEY: m} for d, m in sorted(want.items()) if by[d].get(MARK_KEY) != m]
        if rows:
            done[s] = {r["date"]: r[MARK_KEY] for r in rows}
            if not dry_run:
                write_daily_rows(s, rows, base_dir, source="限月の切り替えの印", reason=MARK_ONLY)
    return done


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="先物の限月の切り替えの印を付ける（値は変えない）")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    res = apply_marks(dry_run=a.dry_run)
    for s, m in res.items():
        print(f"{s}: {len(m)}行 {m}")
    print("印を付ける行なし" if not res else ("（--dry-run: 書いていない）" if a.dry_run else "書いた"))
