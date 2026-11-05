"""倍率の履歴の部品（docs/common/multiples/{T}.json）。[[HYPECORE-STAGE-TIMESCALE-1]] 段B-4（2026-11-05）

決まり（チャット側の決定 2026-11-05）:
  1. 銘柄ごとのファイル。月末ごと（2021-01〜前の月末。今月の途中は作らない）
  2. 全種類（PER・EV/売上・EV/粗利・5年平均の利益の倍率・P/TBV）を根拠つきで持つ。どれを使うかは定義v2（段C）が決める
  3. HON型（株価の系列の分割の調整と config/split_history.yaml の比の食い違い）は機械的に検知して台帳
     （config/split_price_ledger.json）に載せ、その分割の日より前の月の倍率は判定不能
  4. フォワードPERは週1回の値の保存を今から始める（common/market_data/valuation_history.py）

時点:
  - 株価: common/market_data/daily/{T}.json のその月末の終値（直前の取引日、7日まで。足なしの印の行は使わない）
  - 株数: その月末までに申告された最新の期末の希薄化後株式数を、今の基準に分割換算（ROTCEと共通の
    common/sec_data/split_adjust.py::shares_by_end）。申告日は Layer3 の first_filed
  - 損益・CF・BSの財務: Layer3 の first_filed がその月末以前の値（first_val）。TTM・年次は収益の段階の部品
    （common/profitability.py）と同じ関数
  - P/TBV の TCE（有形自己資本）は ROTCE の規則（common/sec_data/rotce.py::tce_as_of、TCEの規則を二重にしない）で、
    その月末までに申告された値（first_val）だけから作る（2026-11-06。それまでは ROTCE の最新の申告の値だった）
業種の中の位置（L）: 部品には持たない（定義v2の側で、毎月の全銘柄の値から peer_percentile() で作る。業種のマスターの写像で比べる相手を決める）。
理由: 順位は全銘柄に依存するので、1銘柄の作り直しでほかの銘柄のファイルまで変わる。部品は銘柄ごとの値だけにし、軽くする

使い方:
  python common/multiples.py --all            # 全銘柄（と分割の検知の台帳）
  python common/multiples.py --ticker NVDA    # 1銘柄だけ（登録の手順の Step 1.8）
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import math
import os
import sys
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Optional

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from common.profitability import annual_points, quarterly_points, ttm_series  # noqa: E402

OUT_DIR = os.path.join(_REPO, "docs", "common", "multiples")
LEDGER_PATH = os.path.join(_REPO, "config", "split_price_ledger.json")
DAILY_DIR = os.path.join(_REPO, "common", "market_data", "daily")
TANUKI_DIR = os.path.join(_REPO, "docs", "value-monitor", "tanuki_valuation", "data")
VALUATION_HISTORY_DIR = os.path.join(_REPO, "common", "market_data", "valuation_history")
SNAPSHOT_MAX_AGE_DAYS = 31        # 月末の時点で使える yfinance の週1回の値の古さ
CALENDAR_SYMBOL = "^GSPC"
START_MONTH = "2021-01"
PRICE_MAX_STALE_DAYS = 7
SHARES_LAG_IF_NO_FILED = 45
NI_AVG_YEARS = 5
NI_AVG_MIN_YEARS = 3
# 分割の検知: その時点に記録した株価（TANUKI の history.json の current_price）と今の日足の終値の比が、
# 分割の比から期待される比（1/比）からこの倍率以上外れたら食い違い（HON: 期待2倍・実際1.049倍）
SPLIT_MISMATCH_TOLERANCE = 1.3
SCHEMA_VERSION = "1.0"
MULTIPLE_KEYS = ("per", "ev_sales", "ev_gross_profit", "pe_5y_avg_earnings", "p_tbv")


def _d(s: str) -> date:
    return date.fromisoformat(s[:10])


def _load(path: str) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _daily_rows(symbol: str, daily_dir: str = DAILY_DIR) -> List[dict]:
    recs = _load(os.path.join(daily_dir, f"{symbol}.json")).get("records") or []
    return sorted((r for r in recs if r.get("close") is not None and r.get("date") and not r.get("_no_yahoo_bar")),
                  key=lambda r: r["date"])


def month_ends(calendar_rows: List[dict], today: Optional[str] = None, start: str = START_MONTH) -> List[tuple]:
    """(月, その月の最後の取引日)。前の月末まで（今月は作らない）"""
    today = today or date.today().isoformat()
    cur = today[:7]
    last: Dict[str, str] = {}
    for r in calendar_rows:
        m = r["date"][:7]
        if start <= m < cur:
            last[m] = max(last.get(m, ""), r["date"])
    return sorted(last.items())


def close_on_or_before(rows: List[dict], d: str) -> Optional[dict]:
    best = None
    for r in rows:
        if r["date"] > d:
            break
        best = r
    if best is None or (_d(d) - _d(best["date"])).days > PRICE_MAX_STALE_DAYS:
        return None
    return best


# ── HON型の分割の検知 ─────────────────────────────────────────────────
def detect_split_price(ticker: str, splits: List[dict], daily: List[dict], recorded: List[tuple],
                       since: str = "2021-01-01") -> List[Dict[str, Any]]:
    """分割ごとに、その時点に記録した株価（recorded: [(日時, 価格)]）と今の日足の終値を比べる。
    status: ok（期待どおり）・mismatch（食い違い→その前の月は判定不能）・unverifiable（分割より前の記録が無い）"""
    out = []
    for sp in sorted(splits or [], key=lambda x: str(x.get("date"))):
        sd, ratio = str(sp["date"]), float(sp["ratio"])
        if sd < since:
            continue
        e: Dict[str, Any] = {"ticker": ticker, "split_date": sd, "ratio": ratio, "expected_factor": round(1 / ratio, 6)}
        before = [(t, p) for t, p in recorded if t[:10] < sd and p]
        if not before:
            out.append({**e, "status": "unverifiable", "reason": "分割より前にその時点の株価の記録が無い"})
            continue
        t_rec, p_rec = max(before)
        row = close_on_or_before(daily, (_d(t_rec[:10]) - timedelta(days=1)).isoformat())
        if row is None:
            out.append({**e, "status": "unverifiable", "reason": "記録の日の日足が無い"})
            continue
        obs = row["close"] / p_rec
        bad = abs(math.log(obs * ratio)) > math.log(SPLIT_MISMATCH_TOLERANCE)
        out.append({**e, "status": "mismatch" if bad else "ok", "recorded_at": t_rec, "recorded_price": p_rec,
                    "daily_date": row["date"], "daily_close": row["close"], "observed_factor": round(obs, 6),
                    "reason": ("今の日足の終値がその時点の株価の{:.3f}倍で、分割の比から期待される{:.3f}倍と違う"
                               "（株価の系列の調整と split_history の比が合わない。分社と同時の併合等）").format(obs, 1 / ratio)
                    if bad else ""})
    return out


def recorded_prices(ticker: str, tanuki_dir: str = TANUKI_DIR) -> List[tuple]:
    """その時点に記録した株価（TANUKI VALUATION の history.json の date・current_price。2026-05-30から）"""
    h = _load(os.path.join(tanuki_dir, ticker, "history.json"))
    rows = h if isinstance(h, list) else []
    return [(r["date"], r["current_price"]) for r in rows if r.get("date") and r.get("current_price")]


def build_ledger(tickers: List[str], daily_dir: str = DAILY_DIR, tanuki_dir: str = TANUKI_DIR,
                 splits_all: Optional[Dict[str, list]] = None) -> Dict[str, Any]:
    from common.sec_data.split_adjust import load_split_history
    splits_all = splits_all if splits_all is not None else load_split_history()
    entries = []
    for t in tickers:
        if splits_all.get(t):
            entries += detect_split_price(t, splits_all[t], _daily_rows(t, daily_dir), recorded_prices(t, tanuki_dir))
    return {"_description": "株価の系列の分割の調整と config/split_history.yaml の比の食い違い（common/multiples.py が機械で作る）。"
                            "status mismatch の銘柄は、その分割の日より前の月の倍率を判定不能にする",
            "entries": entries}


def load_ledger(path: str = LEDGER_PATH) -> List[Dict[str, Any]]:
    return _load(path).get("entries") or []


# ── 1銘柄の計算 ───────────────────────────────────────────────────────
def _stock_at(entries: List[dict], d: str) -> Optional[tuple]:
    """BSの値（その時点までに申告された最新の期末、first_val）"""
    best: Dict[str, tuple] = {}
    for e in entries or []:
        ff = e.get("first_filed") or e.get("filed")
        v = e.get("first_val") if e.get("first_val") is not None else e.get("val")
        if not ff or v is None or ff > d:
            continue
        if e["end"] not in best or ff < best[e["end"]][0]:
            best[e["end"]] = (ff, float(v))
    if not best:
        return None
    end = max(best)
    return end, best[end][1]


def _latest_ttm(series: Dict[str, float]) -> Optional[tuple]:
    if not series:
        return None
    e = max(series)
    return e, series[e]


def snapshot_at(records: List[dict], d: str) -> Optional[dict]:
    """月末 d 以前の直近の yfinance の週1回の値（common/market_data/valuation_history.py が積む。SNAPSHOT_MAX_AGE_DAYS まで）"""
    before = [r for r in records or [] if r.get("date") and r["date"] <= d]
    if not before:
        return None
    r = max(before, key=lambda x: x["date"])
    return r if (_d(d) - _d(r["date"])).days <= SNAPSHOT_MAX_AGE_DAYS else None


def compute(ticker: str, store: Dict[str, Any], prices: List[dict], months: List[tuple],
            splits: List[dict], tce_at_date: Callable[[str], Dict[str, Any]], ledger: List[Dict[str, Any]],
            snapshots: Optional[List[dict]] = None) -> Dict[str, Any]:
    """tce_at_date(月末) → {"end", "tce"}（その月末までに申告された値のTCE、rotce.tce_as_of）"""
    from common.sec_data.split_adjust import shares_by_end
    fields = (store or {}).get("fields") or {}
    ent = lambda f: (fields.get(f) or {}).get("entries") or []
    sh_entries = ent("shares_diluted")
    ends = sorted({e["end"] for e in sh_entries})
    with contextlib.redirect_stdout(io.StringIO()):
        shares = shares_by_end(store, ends, splits) if ends else {}
    sh_filed: Dict[str, str] = {}
    for e in sh_entries:
        ff = e.get("first_filed") or e.get("filed")
        if ff and (e["end"] not in sh_filed or ff < sh_filed[e["end"]]):
            sh_filed[e["end"]] = ff
    blocked = sorted(x["split_date"] for x in ledger if x.get("ticker") == ticker and x.get("status") == "mismatch")
    rows = []
    for m, d in months:
        r: Dict[str, Any] = {"month": m, "date": d}
        bad = [s for s in blocked if d < s]
        if bad:
            r.update({k: None for k in MULTIPLE_KEYS}, undetermined="split_price_mismatch",
                     reason=f"分割の日（{bad[0]}）より前（株価の系列の調整と分割の比が合わない、config/split_price_ledger.json）")
            rows.append(r)
            continue
        px = close_on_or_before(prices, d)
        avail = [e for e in shares if (sh_filed.get(e) or (_d(e) + timedelta(days=SHARES_LAG_IF_NO_FILED)).isoformat()) <= d]
        if px is None or not avail:
            r.update({k: None for k in MULTIPLE_KEYS}, undetermined="no_price" if px is None else "no_shares",
                     reason="その月末の終値が無い" if px is None else "その月末までに申告された株数が無い")
            rows.append(r)
            continue
        se = max(avail)
        mcap = px["close"] * shares[se]["shares"]
        debt = sum((_stock_at(ent(f), d) or (None, 0.0))[1] for f in ("short_term_debt", "long_term_debt"))
        cash = sum((_stock_at(ent(f), d) or (None, 0.0))[1] for f in ("cash_and_equivalents", "short_term_investments"))
        ev = mcap + debt - cash
        ni = _latest_ttm(ttm_series(quarterly_points(ent("net_income"), d)))
        rev = _latest_ttm(ttm_series(quarterly_points(ent("revenue"), d)))
        gp = _latest_ttm(ttm_series(quarterly_points(ent("gross_profit"), d)))
        a_ni = list(annual_points(ent("net_income"), d).items())[-NI_AVG_YEARS:]
        ni5 = (sum(v for _, v in a_ni) / len(a_ni)) if len(a_ni) >= NI_AVG_MIN_YEARS else None
        t = tce_at_date(d) or {}
        tce = t.get("tce")
        pos = lambda num, den: (num / den) if (den is not None and den > 0 and num is not None and num > 0) else None
        r.update({
            "price": px["close"], "price_date": px["date"], "shares": shares[se]["shares"], "shares_end": se,
            "shares_source": shares[se].get("source"), "market_cap": mcap, "enterprise_value": ev,
            "ttm_end": ni[0] if ni else None, "ttm_net_income": ni[1] if ni else None,
            "ttm_revenue": rev[1] if rev else None, "ttm_gross_profit": gp[1] if gp else None,
            "net_income_5y_avg": ni5, "net_income_5y_years": len(a_ni), "tce": tce, "tce_end": t.get("end"),
            "per": pos(mcap, ni[1] if ni else None),
            "ev_sales": pos(ev, rev[1] if rev else None),
            "ev_gross_profit": pos(ev, gp[1] if gp else None),
            "pe_5y_avg_earnings": pos(mcap, ni5),
            "p_tbv": pos(mcap, tce),
            "undetermined": None,
        })
        snap = snapshot_at(snapshots or [], d)
        # フォワードPERは過去の値が取れないので、2026-11-01から積んだ yfinance の週1回の値（スナップショット、参考）
        r["forward_pe_snapshot"] = snap.get("forward_pe") if snap else None
        r["forward_pe_snapshot_date"] = snap.get("date") if snap else None
        rows.append(r)
    return {"_schema_version": SCHEMA_VERSION, "ticker": ticker,
            "_description": "月末ごとの倍率（common/multiples.py が機械で作る）。値が None は計算できない（分母が0以下・赤字等）か判定不能（undetermined）。"
                            "どれを使うかは定義v2が決める。読み手は [[MULTIPLES-ADOPT-1]] で寄せる",
            "months": rows}


def peer_percentile(values: Dict[str, Optional[float]], ticker: str, sector_of: Dict[str, Optional[str]],
                    min_peers: int = 5) -> Optional[float]:
    """業種の中の位置（0〜100）。values: その月・その倍率の全銘柄の値。同じ sector（業種のマスター）が min_peers 未満なら全銘柄"""
    v = values.get(ticker)
    if v is None:
        return None
    same = [x for t, x in values.items() if x is not None and sector_of.get(t) == sector_of.get(ticker)]
    peers = same if len(same) >= min_peers else [x for x in values.values() if x is not None]
    if len(peers) < 2:
        return None
    return 100.0 * sum(1 for x in peers if x < v) / (len(peers) - 1)


# ── 全銘柄・保存 ─────────────────────────────────────────────────────
def _tce_fn(ticker: str, store: Dict[str, Any]) -> Callable[[str], Dict[str, Any]]:
    """月末 → その月末までに申告された値のTCE（ROTCE の規則、rotce.tce_as_of）。会社のfactsは1回だけ読む"""
    from common.sec_data import rotce
    from common.sec_data.layer3_builder import load_company_facts
    cf = load_company_facts(ticker)

    def at(d: str) -> Dict[str, Any]:
        try:
            return rotce.tce_as_of(store, cf, d)
        except Exception:  # noqa: BLE001  TCEが作れない月は P/TBV を出さない
            return {}
    return at


def _default_store(ticker: str) -> Optional[Dict[str, Any]]:
    import logging
    from common.sec_data.layer3_builder import build_ticker_store
    logging.disable(logging.CRITICAL)
    try:
        return build_ticker_store(ticker)
    finally:
        logging.disable(logging.NOTSET)


def build(tickers: Optional[List[str]] = None, out_dir: str = OUT_DIR, ledger_path: str = LEDGER_PATH,
          today: Optional[str] = None, store_fn: Callable = _default_store, tce_fn: Callable = _tce_fn,
          daily_dir: str = DAILY_DIR, tanuki_dir: str = TANUKI_DIR, lookup_path: Optional[str] = None,
          splits_all: Optional[Dict[str, list]] = None) -> Dict[str, Any]:
    """作り直してファイルに書く。tickers=None は全銘柄（と台帳）。戻り値は件数の要約"""
    from common import sector_master as sm
    from common.sec_data.split_adjust import load_split_history
    splits_all = splits_all if splits_all is not None else load_split_history()
    registered = sm.registered_tickers(lookup_path)
    targets = registered if tickers is None else sorted({t.upper() for t in tickers})
    if tickers is None:
        ledger = build_ledger(registered, daily_dir, tanuki_dir, splits_all)
        _write_if_changed(ledger_path, ledger)
    else:
        # 1銘柄の経路: その銘柄の分割だけ検知し、台帳のほかの銘柄の行は残す
        cur = _load(ledger_path) or {"_description": "", "entries": []}
        mine = build_ledger(targets, daily_dir, tanuki_dir, splits_all)["entries"]
        cur["entries"] = [e for e in cur.get("entries") or [] if e.get("ticker") not in set(targets)] + mine
        cur["_description"] = cur.get("_description") or build_ledger([], daily_dir, tanuki_dir, splits_all)["_description"]
        _write_if_changed(ledger_path, cur)
    ledger_entries = load_ledger(ledger_path)
    months = month_ends(_daily_rows(CALENDAR_SYMBOL, daily_dir), today)
    os.makedirs(out_dir, exist_ok=True)
    summary = {"tickers": 0, "months": len(months), "undetermined": {}, "written": 0}
    for t in targets:
        store = store_fn(t) or {}
        snaps = _load(os.path.join(VALUATION_HISTORY_DIR, f"{t}.json")).get("records") or []
        out = compute(t, store, _daily_rows(t, daily_dir), months, splits_all.get(t, []), tce_fn(t, store), ledger_entries, snaps)
        if _write_if_changed(os.path.join(out_dir, f"{t}.json"), out):
            summary["written"] += 1
        summary["tickers"] += 1
        for r in out["months"]:
            if r.get("undetermined"):
                summary["undetermined"][r["undetermined"]] = summary["undetermined"].get(r["undetermined"], 0) + 1
    return summary


def _write_if_changed(path: str, data: Dict[str, Any]) -> bool:
    if _load(path) == json.loads(json.dumps(data, default=str)):
        return False
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1, default=str)
        f.write("\n")
    return True


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="倍率の履歴（docs/common/multiples/{T}.json）を作る")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true")
    g.add_argument("--ticker", action="append")
    args = ap.parse_args(argv)
    s = build(None if args.all else args.ticker)
    mism = [f"{e['ticker']} {e['split_date']}" for e in load_ledger() if e.get("status") == "mismatch"]
    print(f"倍率の履歴: {s['tickers']}銘柄 × {s['months']}か月、書いた {s['written']}、判定不能の月 {s['undetermined']}、"
          f"分割の食い違い {mism or 'なし'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
