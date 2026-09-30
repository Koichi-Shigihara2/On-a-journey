"""Market Pulse 実装B（指示書㉖、2026-09-30）: セクター4象限・半導体とM7・監視銘柄。

設計書 docs/architecture/MARKET_PULSE_REDESIGN.md の1章（段階5〜7）・3章（セクター4象限）のとおり。
入力は common/market_data/daily/ の終値（as_of日以前だけを使うため、過去のエントリに当てはめても先読みにならない）と、
監視銘柄の一覧（ポートフォリオの保有銘柄＋TANUKI TAILの監視銘柄）・属性（yfinanceのsector）・TANUKI SCORE・HypeCore。
"""
from __future__ import annotations

import json
import os
from typing import Any, Callable, Dict, List, Optional, Tuple

SECTORS = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC"]
SECTOR_JA = {"XLK": "情報技術", "XLF": "金融", "XLE": "エネルギー", "XLV": "ヘルスケア", "XLI": "資本財",
             "XLY": "一般消費財", "XLP": "生活必需品", "XLU": "公益", "XLB": "素材", "XLRE": "不動産", "XLC": "通信"}
# yfinanceのsector名 → セクターETF
SECTOR_MAP = {"Technology": "XLK", "Financial Services": "XLF", "Energy": "XLE", "Healthcare": "XLV",
              "Industrials": "XLI", "Consumer Cyclical": "XLY", "Consumer Defensive": "XLP", "Utilities": "XLU",
              "Basic Materials": "XLB", "Real Estate": "XLRE", "Communication Services": "XLC"}
RATIO_WEEKS = 13      # 3章の推奨: RS-Ratio 13週
MOMENTUM_LAG = 4      # 3章の推奨: RS-Momentum 4週
TRAIL_WEEKS = 8
M7 = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA"]
# 段階6（設計書1章、指示書㉓で正負対称の8文言に修正）
S6_SOX_PT = 1.0
S6_M7_PT = 0.5
# 段階1の事実タグ
TAG_SOX_PCT = 2.0
TAG_M7_PT = 1.0
# 段階6の商品・為替の内訳（結論1行には使わない）
BREAKDOWN = [("WTI原油", "CL=F"), ("金", "GC=F"), ("ドル円", "JPY=X"), ("ドル指数", "DX-Y.NYB")]

GetSeries = Callable[[str, str, int], List[Dict[str, Any]]]   # reader.get_price_series_as_of互換 (symbol, as_of, days)


def _closes(get_series: GetSeries, symbol: str, as_of: str, days: int) -> Dict[str, float]:
    out = {}
    for r in get_series(symbol, as_of, days):
        c = r.get("close")
        if not r.get("_gap") and isinstance(c, (int, float)) and c == c and c > 0:
            out[r["date"]] = float(c)
    return out


def _week_key(day: str) -> str:
    """週の区切り（その週の金曜日の日付）。"""
    from datetime import date, timedelta
    d = date.fromisoformat(day)
    return (d + timedelta(days=(4 - d.weekday()) % 7)).isoformat()


def weekly_last(closes: Dict[str, float]) -> Dict[str, Tuple[str, float]]:
    """{週の金曜日: (その週の最後の取引日, 終値)}（当週は途中でもその時点の最後の終値）。"""
    out: Dict[str, Tuple[str, float]] = {}
    for d in sorted(closes):
        out[_week_key(d)] = (d, closes[d])
    return out


def quadrant(ratio: float, mom: float) -> str:
    if ratio >= 100:
        return "Strong" if mom >= 100 else "Weakening"
    return "Improving" if mom >= 100 else "Weak"


QUADRANT_JA = {"Strong": "強い", "Weakening": "弱まりつつある", "Weak": "弱い", "Improving": "強まりつつある"}


def rs_ratio_momentum(sector_weekly: List[float], spy_weekly: List[float], n: int = RATIO_WEEKS,
                      lag: int = MOMENTUM_LAG) -> List[Tuple[Optional[float], Optional[float]]]:
    """週次の終値（同じ週どうし）から、各週の(RS-Ratio, RS-Momentum)を返す（計算できない週はNone）。
    RS = セクター÷SPY、RS-Ratio = RS ÷ RSのn週単純平均 × 100、RS-Momentum = RS-Ratio ÷ lag週前のRS-Ratio × 100。"""
    rs = [a / b for a, b in zip(sector_weekly, spy_weekly)]
    ratio: List[Optional[float]] = []
    for i in range(len(rs)):
        if i + 1 < n:
            ratio.append(None)
        else:
            ratio.append(rs[i] / (sum(rs[i + 1 - n:i + 1]) / n) * 100)
    out = []
    for i, r in enumerate(ratio):
        prev = ratio[i - lag] if i >= lag else None
        out.append((r, None if r is None or prev is None else r / prev * 100))
    return out


def sector_rotation(get_series: GetSeries, as_of: str) -> Dict[str, Any]:
    """段階5のセクター4象限（3章）。as_of（S&P500の終値日）以前の終値だけを使う。"""
    days = (RATIO_WEEKS + MOMENTUM_LAG + TRAIL_WEEKS + 4) * 5 + 10
    spy = _closes(get_series, "SPY", as_of, days)
    spy_w = weekly_last(spy)
    res: Dict[str, Any] = {"as_of": as_of, "ratio_weeks": RATIO_WEEKS, "momentum_lag_weeks": MOMENTUM_LAG,
                           "sectors": {}, "excluded": []}
    for s in SECTORS:
        c = _closes(get_series, s, as_of, days)
        if as_of not in c:
            res["excluded"].append(s)   # 当日の終値が無いセクターは象限の計算から外す（設計書3章）
            continue
        w = weekly_last(c)
        weeks = [k for k in sorted(w) if k in spy_w]
        pts = rs_ratio_momentum([w[k][1] for k in weeks], [spy_w[k][1] for k in weeks])
        trail = [{"week": weeks[i], "date": w[weeks[i]][0], "ratio": round(r, 2), "momentum": round(m, 2)}
                 for i, (r, m) in enumerate(pts) if r is not None and m is not None][-TRAIL_WEEKS:]
        if not trail:
            res["excluded"].append(s)
            continue
        last = trail[-1]
        ds = sorted(c)
        prev = ds[-2] if len(ds) >= 2 else None
        res["sectors"][s] = {"name": SECTOR_JA[s], "ratio": last["ratio"], "momentum": last["momentum"],
                             "quadrant": quadrant(last["ratio"], last["momentum"]), "trail": trail,
                             "change_pct": round((c[as_of] / c[prev] - 1) * 100, 2) if prev else None}
    by_q: Dict[str, List[str]] = {q: [] for q in ("Strong", "Improving", "Weakening", "Weak")}
    for s, v in sorted(res["sectors"].items(), key=lambda kv: -kv[1]["momentum"]):
        by_q[v["quadrant"]].append(s)
    res["by_quadrant"] = by_q
    return res


# ─────────────────────────────────────────────────────────
#  段階1・6: 半導体（SOX）とM7（均等加重）
# ─────────────────────────────────────────────────────────

def _day_change(c: Dict[str, float], day: str) -> Optional[Tuple[str, float]]:
    ds = [d for d in sorted(c) if d <= day]
    if not ds or ds[-1] != day or len(ds) < 2:
        return None
    return ds[-2], (c[day] / c[ds[-2]] - 1) * 100


def semis_m7(get_series: GetSeries, as_of: str, spx_pct: Optional[float]) -> Dict[str, Any]:
    """SOXの前日比、M7均等加重（7銘柄の前日比の単純平均、7銘柄とも同じ前営業日と比べられる場合だけ）と、S&P500との差。"""
    out: Dict[str, Any] = {"as_of": as_of}
    sox_c = _closes(get_series, "^SOX", as_of, 10)
    sox = _day_change(sox_c, as_of)
    out["sox_pct"] = None if sox is None else round(sox[1], 2)
    out["sox_value"] = round(sox_c[as_of], 2) if sox is not None else None
    out["sox_date"] = as_of if sox is not None else None
    chg = {}
    for t in M7:
        x = _day_change(_closes(get_series, t, as_of, 10), as_of)
        if x is not None:
            chg[t] = x
    prevs = {v[0] for v in chg.values()}
    if len(chg) == len(M7) and len(prevs) == 1:
        out["m7_pct"] = round(sum(v[1] for v in chg.values()) / len(M7), 2)
        out["m7_members"] = {t: round(v[1], 2) for t, v in chg.items()}
    else:
        out["m7_pct"] = None
        out["m7_missing"] = sorted(set(M7) - set(chg))
    out["sp500_pct"] = spx_pct
    out["sox_vs_sp500_pt"] = None if out["sox_pct"] is None or spx_pct is None else round(out["sox_pct"] - spx_pct, 2)
    out["m7_vs_sp500_pt"] = None if out["m7_pct"] is None or spx_pct is None else round(out["m7_pct"] - spx_pct, 2)
    return out


def s6_label(sox_rel: Optional[float], m7_rel: Optional[float]) -> Optional[str]:
    """段階6（設計書1章、正負対称）。"""
    if sox_rel is None or m7_rel is None:
        return None
    su, sd = sox_rel >= S6_SOX_PT, sox_rel <= -S6_SOX_PT
    mu, md = m7_rel >= S6_M7_PT, m7_rel <= -S6_M7_PT
    if (su and md) or (sd and mu):
        return "半導体とM7が逆方向"
    if su and mu:
        return "半導体・M7がけん引"
    if sd and md:
        return "半導体・M7が重し"
    if su:
        return "半導体がけん引"
    if sd:
        return "半導体が重し"
    if mu:
        return "M7がけん引"
    if md:
        return "M7が重し"
    return "特定分野の突出なし"


def breakdown(get_series: GetSeries, as_of: str) -> List[Dict[str, Any]]:
    """段階6の商品・為替の内訳（原油・金・ドル円・ドル指数の前日比）。"""
    rows = []
    for name, sym in BREAKDOWN:
        c = _closes(get_series, sym, as_of, 10)
        ds = [d for d in sorted(c) if d <= as_of]
        if len(ds) < 2:
            rows.append({"name": name, "symbol": sym, "value": None, "change_pct": None, "date": None})
            continue
        d, p = ds[-1], ds[-2]
        rows.append({"name": name, "symbol": sym, "value": round(c[d], 4), "change_pct": round((c[d] / c[p] - 1) * 100, 2),
                     "date": d, "prev_date": p})
    return rows


# ─────────────────────────────────────────────────────────
#  段階7: 資金が向かっているセクターにいる監視銘柄
# ─────────────────────────────────────────────────────────

def watch_tickers(repo_root: str) -> Dict[str, List[str]]:
    """監視銘柄 = ポートフォリオの保有銘柄 ∪ TANUKI TAILの監視銘柄。"""
    held: List[str] = []
    try:
        pf = json.load(open(os.path.join(repo_root, "docs", "portfolio", "data", "portfolio.json"), encoding="utf-8"))
        held = sorted({t for b in (pf.get("brokers") or {}).values() for t in (b.get("positions") or {})})
    except Exception as e:
        print(f"[WARN] 保有銘柄の読込失敗: {e}")
    tail: List[str] = []
    try:
        from src.tail.edgar_rss_monitor import get_monitored_tickers
        tail = sorted(set(get_monitored_tickers()))
    except Exception as e:
        print(f"[WARN] TAILの監視銘柄の読込失敗: {e}")
    return {"held": held, "tail": tail, "all": sorted(set(held) | set(tail))}


def _tanuki_score(repo_root: str, t: str) -> Tuple[Optional[str], Optional[str]]:
    """(TANUKI SCOREの分類, 計算日時)。Market Pulseより前の夜の計算結果を読むため、計算日時も返す（指示書㉗）。"""
    try:
        d = json.load(open(os.path.join(repo_root, "docs", "value-monitor", "tanuki_valuation", "data", t, "latest.json"),
                           encoding="utf-8"))
        return d.get("tanuki_score"), d.get("calculation_date")
    except Exception:
        return None, None


def _hype_phase(repo_root: str, t: str) -> Tuple[Optional[str], Optional[str]]:
    """(HypeCoreのPhase, 計算日時)。"""
    try:
        d = json.load(open(os.path.join(repo_root, "docs", "value-monitor", "hypecore", "data", f"{t}_poc.json"),
                           encoding="utf-8"))
        m = (d.get("monthly") or [])[-1]
        return m.get("stage_label"), d.get("generated_at") or d.get("generated")
    except Exception:
        return None, None


def _date_range(values: List[Optional[str]]) -> Optional[List[str]]:
    ds = sorted({v[:10] for v in values if v})
    return [ds[0], ds[-1]] if ds else None


def watch_list(repo_root: str, rotation: Dict[str, Any], get_attributes: Callable[[str], Optional[dict]]) -> Dict[str, Any]:
    """監視銘柄ごとのセクター・象限・TANUKI SCORE・HypeCoreのPhase。資金が向かっているセクター
    （Strong・Improving＝RS-Momentum≥100）にいる銘柄を、セクターのMomentumの高い順→ティッカー順に並べる。
    順位に銘柄の評価は使わない（推奨に読めないようにする）。"""
    wt = watch_tickers(repo_root)
    sectors = rotation.get("sectors") or {}
    rows = []
    for t in wt["all"]:
        attr = get_attributes(t) or {}
        etf = SECTOR_MAP.get(attr.get("sector"))
        sec = sectors.get(etf) if etf else None
        score, score_at = _tanuki_score(repo_root, t)
        phase, phase_at = _hype_phase(repo_root, t)
        rows.append({"ticker": t, "held": t in wt["held"], "tail": t in wt["tail"], "sector": attr.get("sector"),
                     "sector_etf": etf, "sector_name": SECTOR_JA.get(etf) if etf else None,
                     "quadrant": sec["quadrant"] if sec else None, "momentum": sec["momentum"] if sec else None,
                     "tanuki_score": score, "tanuki_calculated_at": score_at,
                     "hype_phase": phase, "hype_calculated_at": phase_at})
    flowing = [r for r in rows if r["quadrant"] in ("Strong", "Improving")]
    flowing.sort(key=lambda r: (-r["momentum"], r["ticker"]))
    others = sorted((r for r in rows if r not in flowing), key=lambda r: r["ticker"])
    # 表の見出しに出す計算日（TANUKI SCORE・HypeCoreはMarket Pulseより前の夜の計算結果、指示書㉗）
    calc = {"tanuki_score": _date_range([r["tanuki_calculated_at"] for r in rows]),
            "hype_phase": _date_range([r["hype_calculated_at"] for r in rows])}
    return {"tickers": wt, "flowing": [r["ticker"] for r in flowing], "rows": flowing + others, "calculated": calc}


def stage7_line(wl: Dict[str, Any]) -> str:
    f = wl.get("flowing") or []
    head = "資金が向かっているセクターにいる監視銘柄："
    if not f:
        return head + "なし"
    return head + "・".join(f[:2]) + (f" ほか{len(f) - 2}銘柄" if len(f) > 2 else "")
