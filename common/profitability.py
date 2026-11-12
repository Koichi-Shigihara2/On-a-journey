"""収益の段階・黒字化の見込み・構造的赤字・赤字の質の点数の部品。[[HYPECORE-STAGE-TIMESCALE-1]] 段B-2（2026-11-05）

決まり（チャット側の決定 2026-11-05）:
  1. 収益の段階は TTM の営業利益（GAAP）。プラスなら「黒字」
  2. 黒字化の見込みは、TTM の営業利益率の直近8四半期（8点）の回帰の傾きで、最新の実際の率から2年（8四半期）以内に0に届くか。
     届けば「黒字化目前」、届かない・上向きでない・点が足りない・当てはまり（R²）が MIN_R2 未満（見込み不明）なら「赤字」
  3. 構造的赤字（FCF の5点・2点の平均がともに0以下）は収益の段階と別の項目。判定は TANUKI の
     src/value/tanuki_valuation/calculator/adjustments.py::is_structural_deficit を呼ぶだけ（定義は1つ）。
     TANUKI の下限・Policy A は今のまま（この部品を読まない）
  4. 業種のマスター（config/sector_master.json）で区分なし・金融の銘柄は収益の段階を「区分なし」（金融は P/TBV÷ROTCE の側）
  赤字の質の点数（STONKS SILO の「良い赤字・悪い赤字」）の関数は deficit_quality_verdict に移した。STONKS はそれを呼ぶ
  5. 黒字化の予測（会計）はこの部品の1つだけ（[[DEFICIT-QUALITY-ADOPT-1]]、2026-11-12）。STONKS SILO の画面・TANUKI の report.txt は
     breakeven_view() で読む。STONKS の営業CF黒字化（ocf_breakeven）はキャッシュの軸として別の名前で残す
  6. STONKS の対象は operating_state_history（月末ごとの TTM 営業利益の符号）を stage_v2 の持続の条件で通した結果
     （common/stonks_target.py）。この部品は月末ごとの符号を出すだけで、持続の閾値は持たない
  7. 粗利率は粗利のフィールドが無ければ 売上−売上原価 から導出する（STONKS の gross_margin_derived と同じ）

時点: 財務は Layer3（common/sec_data/layer3_builder.py::build_ticker_store、ファイルに保存しない）の値を、その期の値が最初に
申告された書類の提出日（first_filed）で、時点（as_of）までに使えたものだけ・その時点の値（first_val）で使う（定義v2 §3）。

出力: docs/common/profitability.json（今の値）。作り直しは SEC_Data_Update の後と業種のマスターの作り直しの後（Market Data Weekly）。
新しい銘柄は register_ticker の Step 1.7（--ticker）。今は読み手を変えない（[[DEFICIT-QUALITY-ADOPT-1]] で寄せる）。

使い方:
  python common/profitability.py --all            # 登録簿（retired 以外）の全銘柄
  python common/profitability.py --ticker NVDA    # 1銘柄だけ作り直して足す
  python common/profitability.py --ticker NVDA --as-of 2025-06-30 --print   # 時点を指定して表示だけ（書かない）
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

OUTPUT_PATH = os.path.join(_REPO, "docs", "common", "profitability.json")
SCHEMA_VERSION = "1.0"

QUARTER_DAYS = (60, 125)          # 1四半期の長さ（52・53週の16週の第4四半期〈112日〉を含む）
STEP_DAYS = (70, 125)             # 続く四半期の期末の間隔
BREAKEVEN_POINTS = 8              # 黒字化の見込みの回帰に使う TTM の点（直近8四半期）
NEAR_HORIZON_QUARTERS = 8         # 2年以内に0に届けば「黒字化目前」
# 回帰の当てはまり（決定係数R²）の下限。これ未満は「赤字（見込み不明）」（2026-11-05、チャット側の指示で導入）。
# 理由: 2026-11-05の全銘柄（赤字で8点がそろう21銘柄）のR²は 0.997〜0.599 の群（SITM・S・SPIR・RKLB・GTLB・ONDS・AVAV・RBRK・RDW・
# KULR・FROG・ESTC・BBAI）と 0.393 以下の群（SOUN・IONQ・JOBY・NET・QBTS・LYFT・ZS・RCAT）に分かれ、間（0.4〜0.6）に銘柄が無い。
# 0.5 はその切れ目。R²が低いのは率が直線に乗らない（売上がほぼ0の期を含みJOBYの率が数千%から動く等）ときで、傾きで伸ばした時期に意味が無い
MIN_R2 = 0.5
BREAKEVEN_METHOD = "ttm_operating_margin_ols_8q"
HISTORY_MONTHS = 4                # 月末ごとの TTM 営業利益の符号を何か月ぶん出すか（持続の条件〈2か月〉を通すのに足りる数）
FCF_POINTS = 5                    # 構造的赤字の5点の平均（TANUKI の TTM の系列と同じく、毎年同じ期末の点）

STAGE_PROFIT, STAGE_NEAR, STAGE_LOSS, STAGE_UNCLASSIFIED = "profit", "near_profit", "loss", "unclassified"
STAGE_LABELS = {STAGE_PROFIT: "黒字", STAGE_NEAR: "黒字化目前", STAGE_LOSS: "赤字", STAGE_UNCLASSIFIED: "区分なし"}


# ── 赤字の質の点数（STONKS SILO の「① 良い赤字 vs 悪い赤字」。src/value/stonks_silo/analyzer.py から移した） ──────
def deficit_quality_verdict(
    net_income: Optional[float],
    cagr_3yr: Optional[float],
    rev_growth: dict,
    rnd_ratio: Optional[float],
    sm_ratio: Optional[float],
    gross_margin: Optional[float],
) -> Tuple[str, str, float]:
    """
    総合判定。スコアリング方式で0-100点を算出。

    スコア構成:
      売上成長  (40pt): CAGR >50%=40, >30%=30, >20%=20, >10%=10, else=0
      投資姿勢  (30pt): R&D+SM >60%=30, >40%=20, >20%=10, else=5
      粗利率    (20pt): >70%=20, >50%=15, >30%=8,  else=0
      赤字状況  (10pt): 純赤字=5 (赤字でも投資中), 純黒字=10
    """
    is_profitable = net_income is not None and net_income > 0

    score = 0.0
    reasons = []

    # 売上成長 (40pt)
    if cagr_3yr is not None:
        if cagr_3yr >= 50:
            score += 40; reasons.append(f"売上CAGR +{cagr_3yr:.0f}% (超高成長)")
        elif cagr_3yr >= 30:
            score += 30; reasons.append(f"売上CAGR +{cagr_3yr:.0f}% (高成長)")
        elif cagr_3yr >= 20:
            score += 20; reasons.append(f"売上CAGR +{cagr_3yr:.0f}% (成長中)")
        elif cagr_3yr >= 10:
            score += 10; reasons.append(f"売上CAGR +{cagr_3yr:.0f}% (緩成長)")
        else:
            reasons.append(f"売上CAGR {cagr_3yr:.0f}% (成長鈍化)")
    else:
        reasons.append("成長率計算不可")

    # 投資姿勢 (30pt) — R&D + S&M 合計対Revenue
    invest_ratio = 0.0
    if rnd_ratio is not None:
        invest_ratio += rnd_ratio
    if sm_ratio is not None:
        invest_ratio += sm_ratio
    if invest_ratio >= 60:
        score += 30; reasons.append(f"R&D+SM {invest_ratio:.0f}% (積極投資)")
    elif invest_ratio >= 40:
        score += 20; reasons.append(f"R&D+SM {invest_ratio:.0f}% (投資中)")
    elif invest_ratio >= 20:
        score += 10; reasons.append(f"R&D+SM {invest_ratio:.0f}% (標準)")
    elif invest_ratio > 0:
        score += 5; reasons.append(f"R&D+SM {invest_ratio:.0f}% (低投資)")
    else:
        reasons.append("投資比率計算不可")

    # 粗利率 (20pt)
    if gross_margin is not None:
        if gross_margin >= 70:
            score += 20; reasons.append(f"粗利率 {gross_margin:.0f}% (高収益構造)")
        elif gross_margin >= 50:
            score += 15; reasons.append(f"粗利率 {gross_margin:.0f}% (良好)")
        elif gross_margin >= 30:
            score += 8;  reasons.append(f"粗利率 {gross_margin:.0f}% (普通)")
        else:
            reasons.append(f"粗利率 {gross_margin:.0f}% (低い)")
    else:
        reasons.append("粗利率計算不可")

    # 黒字状況 (10pt)
    if is_profitable:
        score += 10; reasons.append("純利益黒字")
    elif net_income is not None:
        score += 5

    # 最終判定
    if is_profitable:
        verdict = "PROFITABLE"
    elif score >= 65:
        verdict = "GOOD_DEFICIT"
    elif score >= 35:
        verdict = "WATCH"
    else:
        verdict = "BAD_DEFICIT"

    return verdict, " / ".join(reasons), round(score, 1)


# ── 構造的赤字（TANUKI の定義を呼ぶ） ───────────────────────────────────────
_ADJ = None


def is_structural_deficit(fcf_5yr_avg: Optional[float], fcf_2yr_avg: Optional[float]) -> bool:
    """src/value/tanuki_valuation/calculator/adjustments.py::is_structural_deficit を呼ぶ（定義はそこの1か所）"""
    global _ADJ
    if _ADJ is None:
        path = os.path.join(_REPO, "src", "value", "tanuki_valuation", "calculator", "adjustments.py")
        spec = importlib.util.spec_from_file_location("_tanuki_adjustments_for_profitability", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _ADJ = mod
    return _ADJ.is_structural_deficit(fcf_5yr_avg, fcf_2yr_avg)


# ── その時点で使えた値 ──────────────────────────────────────────────────
def _d(s: str) -> date:
    return date.fromisoformat(s[:10])


def _usable(e: Dict[str, Any], as_of: str) -> Optional[Tuple[str, float]]:
    ff = e.get("first_filed") or e.get("filed")
    v = e.get("first_val") if e.get("first_val") is not None else e.get("val")
    if not ff or v is None or ff > as_of:
        return None
    return ff, float(v)


def quarterly_points(entries: List[Dict[str, Any]], as_of: str) -> Dict[str, float]:
    """1四半期の値（年次・累計を除く）を期末ごとに。その時点までに最初に申告された値（first_val）"""
    best: Dict[str, Tuple[str, float]] = {}
    for e in entries or []:
        if e.get("is_annual") or e.get("is_ytd"):
            continue
        pd_ = e.get("period_days") or 0
        if not (QUARTER_DAYS[0] <= pd_ <= QUARTER_DAYS[1]):
            continue
        u = _usable(e, as_of)
        if u and (e["end"] not in best or u[0] < best[e["end"]][0]):
            best[e["end"]] = u
    return {k: v[1] for k, v in sorted(best.items())}


def annual_points(entries: List[Dict[str, Any]], as_of: str) -> Dict[str, float]:
    best: Dict[str, Tuple[str, float]] = {}
    for e in entries or []:
        if not e.get("is_annual"):
            continue
        u = _usable(e, as_of)
        if u and (e["end"] not in best or u[0] < best[e["end"]][0]):
            best[e["end"]] = u
    return {k: v[1] for k, v in sorted(best.items())}


def ttm_series(quarters: Dict[str, float]) -> Dict[str, float]:
    """続く4四半期の合計を、最後の四半期の期末ごとに"""
    ends = sorted(quarters)
    out = {}
    for i in range(3, len(ends)):
        chain = ends[i - 3:i + 1]
        if all(STEP_DAYS[0] <= (_d(b) - _d(a)).days <= STEP_DAYS[1] for a, b in zip(chain, chain[1:])):
            out[ends[i]] = sum(quarters[e] for e in chain)
    return out


def month_ends_upto(as_of: str, n: int = HISTORY_MONTHS) -> List[str]:
    """as_of 以前の最後の n 個の月末（古い順）。as_of が月末ならそれを含む"""
    d = _d(as_of)
    nxt = date(d.year + (d.month == 12), (d.month % 12) + 1, 1)
    last = nxt - timedelta(days=1)
    cur = last if d == last else date(d.year, d.month, 1) - timedelta(days=1)
    out = []
    for _ in range(n):
        out.append(cur.isoformat())
        cur = date(cur.year, cur.month, 1) - timedelta(days=1)
    return list(reversed(out))


def operating_state(fields: Dict[str, Any], as_of: str) -> Tuple[str, str]:
    """その時点で使えた値だけの営業利益の符号と、その根拠。(状態, 根拠)
    状態: "loss"（<0）・"profit"（≥0）・"unknown"。根拠: "ttm"（TTM営業利益）・"annual"（TTMが作れないので、取れる最新の年次の営業利益の符号）・
    "none"（どちらも取れない）。業種の区分には依らない（STONKS の対象の選び方〈営業利益<0〉。区分なしのバイオも赤字なら "loss"）"""
    ent = lambda f: (fields.get(f) or {}).get("entries") or []
    oi = ttm_series(quarterly_points(ent("operating_income"), as_of))
    if oi:
        return ("loss" if oi[max(oi)] < 0 else "profit"), "ttm"
    ann = annual_points(ent("operating_income"), as_of)
    if ann:
        return ("loss" if ann[max(ann)] < 0 else "profit"), "annual"
    return "unknown", "none"


def _ols(ys: List[float]) -> Tuple[float, float, float]:
    """(傾き, 切片, 決定係数R²)"""
    n = len(ys)
    xs = list(range(n))
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    syy = sum((y - my) ** 2 for y in ys)
    slope = sxy / sxx
    r2 = (sxy * sxy) / (sxx * syy) if syy > 0 else 0.0
    return slope, my - slope * mx, r2


def breakeven(margins: List[Tuple[str, float]]) -> Dict[str, Any]:
    """TTM の営業利益率（古い順、直近 BREAKEVEN_POINTS 点）の回帰の傾きで、最新の実際の率から0に届く時期。
    起点は回帰の線の上の点ではなく最新の実際の値（線は0超・実際は負、をなくす。2026-11-05）。
    当てはまり（R²）が MIN_R2 未満なら見込み不明（LOW_FIT）"""
    pts = margins[-BREAKEVEN_POINTS:]
    out: Dict[str, Any] = {"method": BREAKEVEN_METHOD, "basis_ends": [e for e, _ in pts],
                           "margins": [round(m, 4) for _, m in pts], "slope_per_quarter": None, "r2": None,
                           "quarters_to_zero": None, "year": None, "reason": None}
    if len(pts) < BREAKEVEN_POINTS:
        out["reason"] = "NO_DATA"
        return out
    slope, _intercept, r2 = _ols([m for _, m in pts])
    out["slope_per_quarter"] = round(slope, 5)
    out["r2"] = round(r2, 3)
    if slope <= 0:
        out["reason"] = "NO_TREND"
        return out
    if r2 < MIN_R2:
        out["reason"] = "LOW_FIT"
        return out
    last = pts[-1][1]
    q = max(-last / slope, 0.0)                         # 最新の実際の率から傾きで伸ばして0に届くまでの四半期
    out["quarters_to_zero"] = round(q, 2)
    out["year"] = (_d(pts[-1][0]) + timedelta(days=round(q * 91.3))).year
    out["reason"] = "PREDICTED" if q <= NEAR_HORIZON_QUARTERS else "TOO_FAR"
    return out


def _same_quarter_points(series: Dict[str, float], latest: str, n: int) -> List[Tuple[str, float]]:
    """latest と、その1年前・2年前…（±20日）の点（新しい順）"""
    out = []
    for k in range(n):
        target = _d(latest) - timedelta(days=round(365.25 * k))
        cands = [e for e in series if abs((_d(e) - target).days) <= 20]
        if not cands:
            break
        e = min(cands, key=lambda x: abs((_d(x) - target).days))
        out.append((e, series[e]))
    return out


# ── 1銘柄の計算 ───────────────────────────────────────────────────────
def compute(ticker: str, as_of: str, store: Dict[str, Any], sector_entry: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """1銘柄・1時点。store は build_ticker_store() の戻り値（テストでは作った値）"""
    from common.sec_data.fcf import free_cash_flow, ttm_capex
    fields = (store or {}).get("fields") or {}
    ent = lambda f: (fields.get(f) or {}).get("entries") or []

    q_oi = quarterly_points(ent("operating_income"), as_of)
    q_rev = quarterly_points(ent("revenue"), as_of)
    oi_ttm, rev_ttm = ttm_series(q_oi), ttm_series(q_rev)
    ends = sorted(set(oi_ttm) & set(rev_ttm))
    margins = [(e, oi_ttm[e] / rev_ttm[e]) for e in ends if rev_ttm[e] and rev_ttm[e] > 0]

    res: Dict[str, Any] = {"as_of": as_of, "ttm_end": None, "ttm_operating_income": None, "ttm_revenue": None,
                           "operating_margin": None, "revenue_stage": None, "revenue_stage_label": None,
                           "stage_reason": "", "breakeven": None}
    sector_entry = sector_entry or {}
    if ends:
        last = ends[-1]
        res.update(ttm_end=last, ttm_operating_income=oi_ttm[last], ttm_revenue=rev_ttm[last],
                   operating_margin=round(oi_ttm[last] / rev_ttm[last], 4) if rev_ttm[last] else None)
    if sector_entry.get("unclassified"):
        stage, why = STAGE_UNCLASSIFIED, f"業種のマスター: {sector_entry['unclassified']}"
    elif (sector_entry.get("financial") or {}).get("value"):
        stage, why = STAGE_UNCLASSIFIED, "業種のマスター: 金融・資産の重い業種（P/TBV÷ROTCE の側）"
    elif not ends:
        stage, why = STAGE_UNCLASSIFIED, "TTMの営業利益・売上を作れない（4四半期がそろわない）"
    elif oi_ttm[ends[-1]] > 0:
        stage, why = STAGE_PROFIT, f"TTMの営業利益がプラス（{ends[-1]}）"
    else:
        be = breakeven(margins)
        res["breakeven"] = be
        if be["reason"] == "PREDICTED":
            stage, why = STAGE_NEAR, (f"最新のTTMの営業利益率から直近8点の回帰の傾きで{be['quarters_to_zero']}四半期後に0"
                                      f"（{be['year']}年、R² {be['r2']}）")
        else:
            stage, why = STAGE_LOSS, {"NO_DATA": "TTMの営業利益率の点が8点に足りない", "NO_TREND": "TTMの営業利益率が上向きでない",
                                      "LOW_FIT": f"見込み不明（TTMの営業利益率が直線に乗らない、R² {be['r2']} < {MIN_R2}）",
                                      "TOO_FAR": f"0に届くのが2年より先（{be['quarters_to_zero']}四半期後）"}[be["reason"]]
    res.update(revenue_stage=stage, revenue_stage_label=STAGE_LABELS[stage], stage_reason=why)
    res["operating_state_history"] = [[me, *operating_state(fields, me)] for me in month_ends_upto(as_of)]

    # 構造的赤字（FCF、TANUKI の TTM の系列と同じく毎年同じ期末の5点）
    ocf_ttm = ttm_series(quarterly_points(ent("operating_cash_flow"), as_of))
    capex_ttm = ttm_series(quarterly_points(ent("capital_expenditure"), as_of))
    fcf = {e: free_cash_flow(ocf_ttm[e], ttm_capex(capex_ttm[e])) for e in ocf_ttm if e in capex_ttm}
    fcf = {e: v for e, v in fcf.items() if v is not None}
    sd: Dict[str, Any] = {"value": None, "fcf_points": [], "fcf_5yr_avg": None, "fcf_2yr_avg": None}
    if fcf:
        pts = _same_quarter_points(fcf, max(fcf), FCF_POINTS)
        vals = [v for _, v in pts]
        sd["fcf_points"] = [[e, v] for e, v in pts]
        sd["fcf_5yr_avg"] = sum(vals) / len(vals)
        sd["fcf_2yr_avg"] = sum(vals[:2]) / 2 if len(vals) >= 2 else 0.0      # TANUKI の _calc_fcf_2yr_avg と同じく2点未満は0
        sd["value"] = is_structural_deficit(sd["fcf_5yr_avg"], sd["fcf_2yr_avg"])
    res["structural_deficit"] = sd

    # 赤字の質の点数（年次、その時点で申告済みの値）
    a_rev = annual_points(ent("revenue"), as_of)
    yrs = sorted(a_rev)[-4:]
    dq: Dict[str, Any] = {"verdict": None, "score": None, "reason": "", "fiscal_year_end": None}
    if yrs:
        last = yrs[-1]
        rev = a_rev[last]
        cagr = None
        if len(yrs) >= 4 and a_rev[yrs[0]] and a_rev[yrs[0]] > 0 and rev and rev > 0:
            cagr = ((rev / a_rev[yrs[0]]) ** (1 / 3) - 1) * 100
        growth = {}
        for a, b in zip(yrs, yrs[1:]):
            growth[b[:4]] = (a_rev[b] / a_rev[a] - 1) * 100 if a_rev[a] and a_rev[a] > 0 else None
        ratio = lambda f: (annual_points(ent(f), as_of).get(last) / rev * 100) if rev and rev > 0 and \
            annual_points(ent(f), as_of).get(last) is not None else None

        def gross_ratio():
            """粗利率（%）。粗利のフィールドが無い年は 売上−売上原価 から導出（STONKS の gross_margin_derived と同じ）"""
            if not rev or rev <= 0:
                return None
            gp = annual_points(ent("gross_profit"), as_of).get(last)
            if gp is None:
                cogs = annual_points(ent("cost_of_revenue"), as_of).get(last)
                gp = (rev - cogs) if cogs is not None else None
            return gp / rev * 100 if gp is not None else None
        ni = annual_points(ent("net_income"), as_of).get(last)
        v, why, sc = deficit_quality_verdict(ni, cagr, growth, ratio("research_and_development"),
                                             ratio("selling_and_marketing"), gross_ratio())
        dq.update(verdict=v, score=sc, reason=why, fiscal_year_end=last)
    res["deficit_quality"] = dq
    return res


def structural_deficit_kind(entry: Optional[Dict[str, Any]]) -> str:
    """構造的赤字（FCF）の種類（表示だけ）。"capex_profit"＝収益の段階が黒字でFCFだけ構造的に負（AMZN・CEG型。設備投資が営業CFを上回る）、
    "cash_loss"＝営業利益も赤字（または区分なし等）。TANUKI の下限・Policy A はこの種類に依らず今のまま"""
    return "capex_profit" if (entry or {}).get("revenue_stage") == STAGE_PROFIT else "cash_loss"


# ── 読み手（STONKS の画面・TANUKI の report.txt）が黒字化の予測を読む1か所 ───────────────
def load_entry(ticker: str, path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """docs/common/profitability.json のその銘柄の行。無ければ None"""
    try:
        with open(path or OUTPUT_PATH, encoding="utf-8") as f:
            return (json.load(f).get("tickers") or {}).get(ticker.upper())
    except (OSError, ValueError):
        return None


def breakeven_view(entry: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """黒字化の見込み（会計）の見せ方。{"state": achieved|predicted|unknown, "year", "reason", "r2", "basis"}。
    achieved = TTM の営業利益がプラス。predicted = 年が出る（黒字化目前、または2年より先でも年を出せるもの）。
    reason は部品の理由コード（PREDICTED・TOO_FAR・NO_TREND・NO_DATA・LOW_FIT）か ACHIEVED・UNCLASSIFIED・NO_ENTRY"""
    if not entry:
        return {"state": "unknown", "year": None, "reason": "NO_ENTRY", "r2": None, "basis": BREAKEVEN_METHOD}
    stage = entry.get("revenue_stage")
    be = entry.get("breakeven") or {}
    if stage == STAGE_PROFIT:
        return {"state": "achieved", "year": None, "reason": "ACHIEVED", "r2": None, "basis": BREAKEVEN_METHOD}
    if stage == STAGE_UNCLASSIFIED and not be:
        return {"state": "unknown", "year": None, "reason": "UNCLASSIFIED", "r2": None, "basis": BREAKEVEN_METHOD}
    year = be.get("year")
    return {"state": "predicted" if year else "unknown", "year": year, "reason": be.get("reason") or "NO_DATA",
            "r2": be.get("r2"), "basis": BREAKEVEN_METHOD}


# ── 全銘柄・保存 ─────────────────────────────────────────────────────
def _default_store(ticker: str) -> Optional[Dict[str, Any]]:
    import logging
    from common.sec_data.layer3_builder import build_ticker_store
    logging.disable(logging.CRITICAL)
    try:
        return build_ticker_store(ticker)
    finally:
        logging.disable(logging.NOTSET)


def build(tickers: Optional[List[str]] = None, as_of: Optional[str] = None, path: str = OUTPUT_PATH,
          store_fn: Callable[[str], Optional[Dict[str, Any]]] = _default_store,
          sector_master_path: Optional[str] = None, lookup_path: Optional[str] = None,
          now: Optional[str] = None) -> Dict[str, Any]:
    """作り直して返す（書かない）。tickers を渡すとその銘柄だけ作り直し、ほかは前回のまま"""
    from common import sector_master as sm
    now = now or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    as_of = as_of or now[:10]
    master = sm.load(sector_master_path or sm.MASTER_PATH).get("tickers") or {}
    registered = sm.registered_tickers(lookup_path)
    prev = _load(path).get("tickers") or {}
    if tickers is None:
        targets, entries = registered, {}
    else:
        targets = sorted({t.upper() for t in tickers})
        entries = {t: e for t, e in prev.items() if t in set(registered)}
    for t in targets:
        store = store_fn(t)
        entries[t] = compute(t, as_of, store or {}, master.get(t))
    return {
        "_schema_version": SCHEMA_VERSION,
        "_description": "収益の段階・黒字化の見込み・構造的赤字・赤字の質の点数（common/profitability.py が機械で作る）。"
                        "読み手は [[DEFICIT-QUALITY-ADOPT-1]] で寄せる（今は読む処理なし）",
        "generated_at": now,
        "rules": {"revenue_stage": "TTMの営業利益（GAAP）", "breakeven_method": BREAKEVEN_METHOD,
                  "breakeven_points": BREAKEVEN_POINTS, "near_horizon_quarters": NEAR_HORIZON_QUARTERS,
                  "breakeven_start": "最新の実際のTTMの営業利益率", "breakeven_min_r2": MIN_R2,
                  "structural_deficit": "TTMのFCFの毎年同じ期末の5点の平均と直近2点の平均（is_structural_deficit）",
                  "time_basis": "Layer3 の first_filed がその時点以前の値（first_val）"},
        "tickers": {t: entries[t] for t in sorted(entries)},
    }


def _load(path: str) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def write(out: Dict[str, Any], path: str = OUTPUT_PATH) -> bool:
    """中身（generated_at と各銘柄の as_of 以外）が前回と同じなら書かない。書いたら True"""
    def strip(d):
        d = {k: v for k, v in d.items() if k != "generated_at"}
        d["tickers"] = {t: {k: v for k, v in e.items() if k != "as_of"} for t, e in (d.get("tickers") or {}).items()}
        return d
    prev = _load(path)
    if prev and strip(prev) == strip(out):
        return False
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
        f.write("\n")
    return True


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="収益の段階・黒字化の見込み（docs/common/profitability.json）を作る")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true")
    g.add_argument("--ticker", action="append")
    ap.add_argument("--as-of", help="時点（YYYY-MM-DD）。--print と一緒に使う（過去の時点はファイルに書かない）")
    ap.add_argument("--print", action="store_true", help="表示だけ（書かない）")
    args = ap.parse_args(argv)
    if args.as_of and not args.print:
        ap.error("--as-of は --print と一緒に使う（公開のファイルは今の時点だけ）")
    out = build(None if args.all else args.ticker, as_of=args.as_of)
    if args.print:
        print(json.dumps({t: out["tickers"][t] for t in (args.ticker or out["tickers"])}, ensure_ascii=False, indent=1))
        return 0
    wrote = write(out)
    from collections import Counter
    c = Counter(e["revenue_stage_label"] for e in out["tickers"].values())
    print(f"収益の段階: {len(out['tickers'])}銘柄 {dict(c)}、構造的赤字 "
          f"{sum(1 for e in out['tickers'].values() if e['structural_deficit']['value'])} → {'書いた' if wrote else '変化なし（書かない）'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
