"""理論株価の分母・時価総額の株数を決める1か所（[[TANUKI-SHARES-SINGLE-SOURCE-1]]、2026-11-13の決定）。

株数 = 加重平均の希薄化後株数 Wd（Layer3・分割換算済み）× (1 + 発行の分 ÷ 同じ期の基本の加重平均 Wb)
  発行の分 = (最新の表紙 c1 − 前の期の表紙 c0) − 加重平均に既に入っている分。買い戻し（c1<c0）は引く。閾値は使わない。
  加重平均に既に入っている分 = (c1−c0) と同じ向きで clip(Wb−c0, 0, c1−c0)（増加）／ clip(Wb−c0, c1−c0, 0)（減少）
  → 発行が無ければ補正は0。表紙と加重平均の差のうち、発行でも希薄化後でもない分（TSLA型）は入らない（c1−c0にだけ基づくため）

読み手は common/multiples.py の部品（docs/common/multiples/{T}.json の current の行）を通してこの株数を読む:
TANUKI の分母（data_fetcher._determine_diluted_shares）・時価総額（multiples.at_price）・STONKS SILO・HypeCore が同じ値。
DILUTION の share_count_change は加重平均の希薄化後株数の系列（同じ Layer3・shares_by_end）で、補正前の Wd を使う（CAGR の端点に表紙の補正を混ぜない）。
"""
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

COVER_MAX_DAYS = 120            # 表紙の日付が期末から120日以内
COVER_KEY = "EntityCommonStockSharesOutstanding"
SOURCE_PLAIN = "weighted"       # 補正なし（Wd）
SOURCE_ADJUSTED = "weighted_plus_issuance"
_FORMS = ("10-Q", "10-K", "10-K/A", "10-Q/A")


def _d(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def corrected_shares(wd: float, wb: float, c0: float, c1: float) -> Dict[str, float]:
    """補正後の株数。純粋な関数（入力は全て同じ単位の株数）。issued は符号つき（買い戻しは負）。
    不変条件: |issued| ≤ |c1−c0|（二重に数えない）"""
    inc = c1 - c0
    if inc >= 0:
        in_avg = min(max(wb - c0, 0.0), inc)        # c0→c1の増加のうち、加重平均が既に含む分
    else:
        in_avg = max(min(wb - c0, 0.0), inc)        # 減少のうち、加重平均が既に含む分
    issued = inc - in_avg
    return {"shares": wd * (1.0 + issued / wb), "issued": issued, "in_average": in_avg, "change": inc}


def _q90(g: dict, key: str, end: str) -> Optional[float]:
    best = None
    for x in (g.get(key) or {}).get("units", {}).get("shares", []):
        if x.get("form") not in ("10-Q", "10-K") or not x.get("start") or x.get("end") != end:
            continue
        if not 80 <= (_d(x["end"]) - _d(x["start"])).days <= 100:
            continue
        if best is None or x.get("filed", "") > best.get("filed", ""):
            best = x
    return best["val"] if best else None


def basic_in_units_of_wd(cf: Dict[str, Any], end: str) -> Optional[float]:
    """同じ期の Wb÷Wd（company_facts の同じ期の四半期の値どうしの比）。LOAR型（基本だけ千株単位）は比で補う"""
    g = ((cf or {}).get("facts") or {}).get("us-gaap") or {}
    wb = _q90(g, "WeightedAverageNumberOfSharesOutstandingBasic", end)
    wd = _q90(g, "WeightedAverageNumberOfDilutedSharesOutstanding", end)
    if not wb or not wd:
        return None
    r = wb / wd
    return r * 1000 if r < 0.01 else r


def cover_series(cf: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """表紙の発行済（dei）。期末（報告日）ごとに最新の申告の値。同じ申告に複数値（種類株）があれば None"""
    arr = (((cf or {}).get("facts") or {}).get("dei") or {}).get(COVER_KEY, {}).get("units", {}).get("shares", [])
    by: Dict[str, list] = {}
    for x in arr:
        if x.get("form") in _FORMS and x.get("end") and x.get("val"):
            by.setdefault(x["end"], []).append(x)
    out: Dict[str, Optional[float]] = {}
    for e, xs in by.items():
        lf = max(x.get("filed", "") for x in xs)
        vals = {x["val"] for x in xs if x.get("filed", "") == lf}
        out[e] = float(vals.pop()) if len(vals) == 1 else None
    return out


def base_shares(wd: Optional[float], period_end: Optional[str], wd_source: Optional[str],
                cf: Optional[Dict[str, Any]], splits: Optional[List[dict]] = None) -> Dict[str, Any]:
    """分母にする株数と、その印。補正できない時は Wd のまま（shares_note に理由）。

    戻り値: shares / shares_wd / shares_basis(weighted | weighted_plus_issuance) / shares_issued / shares_note /
            cover_shares・cover_date / outstanding_gap_pct（表紙÷採用の株数−1、%。TSLA型の差の印。表示だけ）"""
    out: Dict[str, Any] = {"shares": wd, "shares_wd": wd, "shares_basis": SOURCE_PLAIN, "shares_issued": 0.0,
                           "shares_note": None, "cover_shares": None, "cover_date": None, "outstanding_gap_pct": None}
    if not wd or not period_end:
        out["shares_note"] = "no_shares"
        return out
    ser = cover_series(cf or {})
    ends = sorted(ser)
    latest = ends[-1] if ends else None
    if latest and ser[latest]:
        out.update(cover_shares=ser[latest], cover_date=latest)
        out["outstanding_gap_pct"] = (ser[latest] / wd - 1) * 100
    if wd_source != "quarterly_weighted":
        out["shares_note"] = f"wd_not_quarterly:{wd_source}"          # 年度の平均・Q4が無い銘柄は印だけ（決定3）
        return out
    if not latest:
        out["shares_note"] = "no_cover"
        return out
    c1 = ser[latest]
    if c1 is None:
        out["shares_note"] = "cover_multi_class"
        return out
    if not (0 < (_d(latest) - _d(period_end)).days <= COVER_MAX_DAYS):
        out["shares_note"] = f"cover_not_after_period_end:{latest}"
        return out
    if any(period_end < s.get("split_date", "") for s in (splits or [])):
        out["shares_note"] = "split_after_period_end"
        return out
    prev = [e for e in ends if e < latest and e <= (_d(period_end) + dt.timedelta(days=60)).isoformat()]
    if not prev or ser[prev[-1]] is None:
        out["shares_note"] = "no_previous_cover"
        return out
    c0 = ser[prev[-1]]
    r = basic_in_units_of_wd(cf, period_end)
    if r is None:
        out["shares_note"] = "no_basic_weighted"
        return out
    res = corrected_shares(wd, wd * r, c0, c1)
    out.update(shares=res["shares"], shares_issued=res["issued"],
               shares_basis=SOURCE_ADJUSTED if res["issued"] else SOURCE_PLAIN, previous_cover_shares=c0,
               previous_cover_date=prev[-1])
    out["outstanding_gap_pct"] = (c1 / out["shares"] - 1) * 100
    return out
