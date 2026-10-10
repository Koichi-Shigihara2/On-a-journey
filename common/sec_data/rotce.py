"""
common/sec_data/rotce.py

ROTCE（有形普通株主資本利益率）・P/TBV（株価有形純資産倍率）の共有モジュール
（[[ROTCE-PTBV-1]]、2026-10-10）。参考表示専用で、TANUKI SCOREの判定（category・
funda/timing・tanuki_score・matrix・トラップ判定）やTANUKI VALUATIONの計算には使わない。
表示先: TANUKI SCOREの散布図（銘柄間比較）・EPS Analyzer個別ページ（自社の過去との比較）。

定義:
  TCE   = stockholders_equity
          − minority_interest（純資産が非支配持分込みのタグの期末日だけ）
          − preferred_stock − goodwill − intangible_assets_excl_goodwill
  ROTCE = その四半期の普通株主帰属純利益 × 4 ÷（前の四半期末と当四半期末のTCEの平均）
          （[[ROTCE-QUARTERLY-ANNUALIZED-1]]、2026-10-11。会社発表のROTCE〈SOFI等〉と同じ四半期の年率。
          分子はcompany_factsのNetIncomeLossAvailableToCommonStockholdersDiluted→Basicの単独四半期
          〈暗黙のQ4を含む〉、無い四半期はLayer3の純利益〈net_income_source="net_income"〉。
          前の四半期末〈70〜120日前の純資産の期末日〉のTCEが無い・0以下なら当四半期末のTCEだけで計算し、
          rotce_basis="quarter_end_only"。両方そろえば rotce_basis="quarter_average"）
  ROTCE（参考、TTM）= rotce_ttm: Layer3の四半期純利益を連続4期足したTTM純利益 ÷（4四半期前と当四半期末のTCEの平均）
          （2026-10-10の当初の定義。4四半期前のTCEが無い・0以下なら当四半期末だけ、rotce_ttm_basis="end_only"）
  P/TBV = 時価総額 ÷ 期末のTCE
          四半期: 期末日（または直前の取引日）の終値 × その期の希薄化後株式数
          直近:   最新の終値 × 最新四半期の希薄化後株式数（TCEは最新四半期）
  成分を初めて申告した期末日より前の期は0と仮定し、assumed_zeroを付ける。自社比の母数から外すのは、
  無形資産を0と仮定した期だけ（ASSUMED_ZERO_EXCLUDED_FIELDS、excluded_from_percentile=true）。
  優先配当は、普通株主帰属純利益のタグがある四半期は引かれている。タグが無く純利益で代用した四半期は引いていない。

データ: Layer3（layer3_builder.build_ticker_store）・日次株価（common/market_data/daily/）・
分割履歴（config/split_history.yaml、株式数の未調整の値を split_adjust.py で換算）。

自社の過去との比較: 直近のROTCEが正（赤字でない）で、四半期のROTCE・P/TBVが両方そろう期がMIN_QUARTERS_FOR_PERCENTILE以上あるときだけ、
直近値が過去の四半期分布の何パーセンタイルにあるかを出し、目安（自社比割安・中立・自社比割高）を付ける。

出力: docs/common/sec_data/rotce/{TICKER}.json と _summary.json（TANUKI SCOREの散布図用）。
会社発表のROTCE: config/company_reported_metrics.jsonに登録した銘柄は、{TICKER}.jsonのcompany_reportedに
会社の値・こちらの計算値・差・ガイダンス・長期目標と、直近の四半期末が登録済みか（latest_registered）を書く。
実行: python common/sec_data/rotce.py [TICKER ...]（引数なしは common/sec_data/config.py の全銘柄）
"""
import contextlib
import io
import json
import os
import sys
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

if __name__ == "__main__":
    sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")))

from common.sec_data.layer3_builder import (
    build_ticker_store, extract_field_raw_entries, get_field_entries, load_company_facts,
)
from common.sec_data.q4_implied import build_q4_implied_entries
from common.sec_data.split_adjust import adjust_share_points, load_split_history
from common.sec_data.tag_definitions import (
    INTANGIBLE_EXCL_TAG, INTANGIBLE_FINITE_TAG, INTANGIBLE_INCL_GOODWILL_TAG, INTANGIBLE_INDEFINITE_TAG,
    derive_intangible_resolved_facts,
)

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
OUTPUT_DIR = os.path.join(REPO_ROOT, "docs", "common", "sec_data", "rotce")
# 会社発表のROTCE（出典付きの登録、[[ROTCE-QUARTERLY-ANNUALIZED-1]]）。個別ページに並べて出し、CHECK-61が登録漏れを見る
COMPANY_REPORTED_PATH = os.path.join(REPO_ROOT, "config", "company_reported_metrics.json")
COMPANY_REPORTED_METRIC = "ROTCE"

# 自社の過去との比較（しきい値はパーセンタイル、0〜100）
MIN_QUARTERS_FOR_PERCENTILE = 8
CHEAP_PTBV_MAX_PCTL = 25     # P/TBVがこれ以下 かつ
CHEAP_ROTCE_MIN_PCTL = 50    # ROTCEがこれ以上 → 自社比割安
RICH_PTBV_MIN_PCTL = 75      # P/TBVがこれ以上 かつ
RICH_ROTCE_MAX_PCTL = 50     # ROTCEがこれ以下 → 自社比割高
SIGNAL_CHEAP = "cheap"
SIGNAL_NEUTRAL = "neutral"
SIGNAL_RICH = "rich"

# 期末日に成分が無いとき、補ってよい年次（10-K）の値の古さ（日）
ANNUAL_FILL_MAX_DAYS = 365
# TTMの4四半期が連続とみなす間隔（日）・期首（4四半期前）とみなす間隔（日）
QUARTER_GAP_DAYS = (70, 120)   # 上限120日: PEPの第4四半期は16週（112日）
YEAR_GAP_DAYS = (340, 390)
# 期末日の終値として使える直前の取引日の古さ（日）
PRICE_MAX_STALE_DAYS = 7
# 株式数の外れ値: 分割換算後の中央値からこの倍率以上離れた値は使わない
# （CIX 2026-06-30: 12,329株〈前後の期は約1,232万株、千株単位での申告ミスとみられる〉）
SHARES_OUTLIER_RATIO = 20.0

SE_TAG = "StockholdersEquity"
SE_NCI_TAG = "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"
WEIGHTED_DILUTED_TAG = "WeightedAverageNumberOfDilutedSharesOutstanding"
# ROTCEの分子（普通株主帰属純利益）。希薄化後を優先（会社発表のROTCEは希薄化後の調整後純利益）
COMMON_NI_DILUTED_TAG = "NetIncomeLossAvailableToCommonStockholdersDiluted"
COMMON_NI_BASIC_TAG = "NetIncomeLossAvailableToCommonStockholdersBasic"
NI_SOURCE_BY_TAG = {COMMON_NI_DILUTED_TAG: "common_diluted", COMMON_NI_BASIC_TAG: "common_basic"}
NI_SOURCE_FALLBACK = "net_income"
ANNUALIZE_QUARTERS = 4
DEDUCTION_FIELDS = ("goodwill", "intangible_assets_excl_goodwill", "preferred_stock")
TAG_BY_FIELD = {"goodwill": "Goodwill", "preferred_stock": "PreferredStockValue",
                "minority_interest": "MinorityInterest"}

# 理由コード
R_NO_LAYER3 = "no_layer3"
R_INSUFFICIENT_QUARTERS = "insufficient_quarters"   # 連続4四半期の純利益が無い／比較用の四半期が8期未満
R_EQUITY_MISSING = "equity_missing"
R_COMPONENT_MISSING = "component_missing"
R_TCE_NONPOSITIVE = "tce_nonpositive"
R_PRICE_MISSING = "price_missing"
R_SHARES_MISSING = "shares_missing"
R_INTANGIBLE_ONLY_INCL = "intangible_only_including_goodwill_tag"
R_PREFERRED_TEMPORARY_EQUITY = "preferred_stock_classified_as_temporary_equity"
R_ROTCE_NONPOSITIVE = "rotce_nonpositive"   # 直近のROTCE≤0（赤字）→ 目安を付けない

# 0と仮定した期（assumed_zero）のうち、自社比の母数から外す成分。のれん・優先株は存在すれば
# 貸借対照表に独立した行として出るため、初申告より前を0とするのは実態どおり（ALABは2025年の
# 買収で初めてのれんが生じた）。無形資産は注記にしか出ないことがあり、申告していない期間にも
# 残高があった例がある（AAPL 2018〜2025年）ため、0の仮定が外れうる
ASSUMED_ZERO_EXCLUDED_FIELDS = frozenset({"intangible_assets_excl_goodwill"})

# 成分ごとの生タグ（初めて申告した期末日の判定に使う。無形資産は派生概念の期末日）
RAW_TAG_BY_FIELD = {"goodwill": "Goodwill", "preferred_stock": "PreferredStockValue",
                    "minority_interest": "MinorityInterest"}
TEMPORARY_EQUITY_TAGS = (
    "TemporaryEquityCarryingAmountAttributableToParent",
    "TemporaryEquityCarryingAmountIncludingPortionAttributableToNoncontrollingInterests",
)


def _d(s: str) -> date:
    return date.fromisoformat(s)


def _days(a: str, b: str) -> int:
    return (_d(b) - _d(a)).days


def _stock_by_end(store: dict, field: str) -> Dict[str, dict]:
    """Layer3のstockフィールドを期末日→エントリにする（同じ期末日は最新filed）"""
    out: Dict[str, dict] = {}
    for e in get_field_entries(store, field):
        if e.get("val") is None:
            continue
        cur = out.get(e["end"])
        if cur is None or (e.get("filed") or "") > (cur.get("filed") or ""):
            out[e["end"]] = e
    return dict(sorted(out.items()))


def _quarterly_flow(store: dict, field: str) -> Dict[str, dict]:
    """Layer3のflowフィールドの単独四半期（暗黙のQ4を含む）を期末日→エントリにする"""
    out: Dict[str, dict] = {}
    for e in get_field_entries(store, field):
        if e.get("is_annual") or e.get("is_ytd") or e.get("val") is None:
            continue
        out[e["end"]] = e
    return dict(sorted(out.items()))


def _common_ni_quarterly(company_facts: Optional[dict], ticker: str = "") -> Dict[str, dict]:
    """普通株主帰属純利益の単独四半期（暗黙のQ4を含む）を期末日→エントリにする

    Layer3のflowと同じ抽出（希薄化後→基本の候補を期末日ごとに優先順位でマージ、YTD→単独四半期）と
    Q4の逆算（年次−Q1〜Q3）を使う。各エントリのsource_tagに採用したタグが入る。
    """
    if not company_facts:
        return {}
    field_def = {"category": "flow", "unit": "USD", "candidates": [COMMON_NI_DILUTED_TAG, COMMON_NI_BASIC_TAG]}
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        entries, _ = extract_field_raw_entries(company_facts, field_def, "net_income", ticker)
        annual = [e for e in entries if e.get("is_annual")]
        quarterly = [e for e in entries if not e.get("is_annual") and not e.get("is_ytd")]
        q4 = build_q4_implied_entries(annual, quarterly, "net_income")
    out = {e["end"]: e for e in quarterly if e.get("val") is not None}
    for e in q4:
        if e["end"] not in out and e.get("val") is not None:
            out[e["end"]] = e
    return dict(sorted(out.items()))


def _previous_quarter_end(equity_ends: List[str], q: str) -> Optional[str]:
    """qの前の四半期末（純資産の期末日のうち、qの70〜120日前で最も新しいもの）"""
    prev = [e for e in equity_ends if e < q and QUARTER_GAP_DAYS[0] <= _days(e, q) <= QUARTER_GAP_DAYS[1]]
    return max(prev) if prev else None


def component_at(by_end: Dict[str, dict], q: str, fy_ends: frozenset = frozenset(),
                 first_reported: Optional[str] = None) -> Dict[str, Any]:
    """期末日qの成分の値。戻り値 {"val", "filled_from"（年次で補った期末日）, "missing", "assumed_zero"}

    - qの値があればその値
    - 無ければ、q以前ANNUAL_FILL_MAX_DAYS日以内の年度末（fy_ends）の値で補う（filled_from）。
      年度末の値は後の10-Qが比較期間として再掲したもの（form=10-Q）でも年次の値として扱う
    - その銘柄がこの成分を一度も申告していない、またはqが初めて申告した期末日
      （first_reported、company_factsの全履歴）より前なら0（該当なし）
    - 直前の申告値が0なら0（残高がなくなって申告をやめた）
    - それ以外（申告していたが直近の値が無い）は推測せず missing
    assumed_zero: qが初めて申告した期末日より前で0と仮定した（実際は0でない可能性がある。
    AAPLの無形資産のように、申告していない期間にも残高があった例がある）
    """
    if q in by_end:
        return {"val": by_end[q]["val"], "filled_from": None, "missing": False}
    if first_reported is not None and q < first_reported:
        return {"val": 0, "filled_from": None, "missing": False, "assumed_zero": True}
    if not by_end:
        return {"val": 0, "filled_from": None, "missing": False}
    annual = [e for end, e in by_end.items()
              if end < q and end in fy_ends and _days(end, q) <= ANNUAL_FILL_MAX_DAYS]
    if annual:
        e = max(annual, key=lambda x: x["end"])
        return {"val": e["val"], "filled_from": e["end"], "missing": False}
    prior = [end for end in by_end if end < q]
    if prior and by_end[max(prior)]["val"] == 0:
        return {"val": 0, "filled_from": None, "missing": False}
    return {"val": None, "filled_from": None, "missing": True}


def tce_at(series: Dict[str, Dict[str, dict]], q: str, intangible_tags: Dict[str, str],
           unavailable: frozenset = frozenset(), fy_ends: frozenset = frozenset(),
           first_reported: Optional[Dict[str, Optional[str]]] = None) -> Dict[str, Any]:
    """期末日qのTCEと内訳。計算できなければ tce=None と reason

    unavailable: 値があっても使えない成分（ASTS型の無形資産）。常にmissing扱い
    fy_ends・first_reported: component_at()へ渡す（年度末の期末日・成分ごとの初申告の期末日）
    """
    first_reported = first_reported or {}
    se = series["stockholders_equity"].get(q)
    if se is None:
        return {"tce": None, "reason": R_EQUITY_MISSING}
    se_tag = se.get("source_tag")
    parts: Dict[str, Any] = {"stockholders_equity": se["val"], "stockholders_equity_tag": se_tag}
    filled: Dict[str, str] = {}
    missing: List[str] = []
    assumed_zero: List[str] = []
    fields = list(DEDUCTION_FIELDS) + (["minority_interest"] if se_tag == SE_NCI_TAG else [])
    for f in fields:
        c = ({"val": None, "filled_from": None, "missing": True} if f in unavailable
             else component_at(series[f], q, fy_ends, first_reported.get(f)))
        parts[f] = c["val"]
        if c["filled_from"] and c["val"]:  # 0で補った場合（優先株0等）は値が変わらないので記録しない
            filled[f] = c["filled_from"]
        if c["missing"]:
            missing.append(f)
        if c.get("assumed_zero"):
            assumed_zero.append(f)
    src_end = filled.get("intangible_assets_excl_goodwill", q)
    parts["intangible_assets_excl_goodwill_tag"] = intangible_tags.get(src_end) if parts.get(
        "intangible_assets_excl_goodwill") else None
    out: Dict[str, Any] = {"components": parts, "filled_components": filled}
    if assumed_zero:
        out["assumed_zero"] = assumed_zero
    if missing:
        out.update(tce=None, reason=R_COMPONENT_MISSING, missing_components=missing)
        return out
    tce = se["val"] - sum(parts[f] or 0 for f in fields)
    out["tce"] = tce
    if tce <= 0:
        out["reason"] = R_TCE_NONPOSITIVE
    return out


def _intangible_tags_by_end(company_facts: Optional[dict]) -> Dict[str, str]:
    """無形資産の期末日→採用タグ（同じ期末日に複数accnがあれば最新filed）"""
    if not company_facts:
        return {}
    us_gaap = (company_facts.get("facts") or {}).get("us-gaap") or {}
    best: Dict[str, dict] = {}
    for f in derive_intangible_resolved_facts(us_gaap):
        if not str(f.get("form", "")).startswith(("10-K", "10-Q")):
            continue
        cur = best.get(f["end"])
        if cur is None or (f.get("filed") or "") > (cur.get("filed") or ""):
            best[f["end"]] = f
    return {end: f["source_tags"] for end, f in best.items()}


def _raw_instants(company_facts: Optional[dict], tag: str) -> List[dict]:
    us_gaap = ((company_facts or {}).get("facts") or {}).get("us-gaap") or {}
    facts = ((us_gaap.get(tag) or {}).get("units") or {}).get("USD") or []
    return [f for f in facts if not f.get("start") and f.get("end") and f.get("val") is not None
            and str(f.get("form", "")).startswith(("10-K", "10-Q"))]


def _first_reported(company_facts: Optional[dict]) -> Dict[str, Optional[str]]:
    """成分ごとに、company_factsの全履歴で初めて申告された期末日（10-K/10-Q）"""
    out: Dict[str, Optional[str]] = {}
    for f, tag in RAW_TAG_BY_FIELD.items():
        ends = [x["end"] for x in _raw_instants(company_facts, tag)]
        out[f] = min(ends) if ends else None
    us_gaap = ((company_facts or {}).get("facts") or {}).get("us-gaap") or {}
    ends = [x["end"] for x in derive_intangible_resolved_facts(us_gaap)
            if str(x.get("form", "")).startswith(("10-K", "10-Q"))]
    out["intangible_assets_excl_goodwill"] = min(ends) if ends else None
    return out


def _temporary_equity_preferred_ends(company_facts: Optional[dict]) -> set:
    """PreferredStockValueが同じ期末日のメザニン（一時資本）の簿価と同額の期末日。

    その優先株は純資産の外にあり（CELH 2022年: 824,488千ドル）、純資産から引くと二重の控除になる。
    """
    te = {(x["end"], x["val"]) for tag in TEMPORARY_EQUITY_TAGS for x in _raw_instants(company_facts, tag)}
    return {x["end"] for x in _raw_instants(company_facts, "PreferredStockValue")
            if x["val"] and (x["end"], x["val"]) in te}


def _has_only_including_goodwill_tag(company_facts: Optional[dict]) -> bool:
    us_gaap = ((company_facts or {}).get("facts") or {}).get("us-gaap") or {}
    return INTANGIBLE_INCL_GOODWILL_TAG in us_gaap and not any(
        t in us_gaap for t in (INTANGIBLE_EXCL_TAG, INTANGIBLE_FINITE_TAG, INTANGIBLE_INDEFINITE_TAG))


def _shares_by_end(store: dict, ends: List[str], splits: List[dict]) -> Dict[str, Dict[str, Any]]:
    """各期末日の希薄化後株式数（分割の未調整分を換算済み）と出所

    優先: ①その四半期の加重平均（10-Q）②年次の加重平均（年度末の四半期、10-K）
          ③その期末日の発行済株式数（CommonStockSharesOutstanding、Layer3のフォールバック）
    分割換算後の中央値からSHARES_OUTLIER_RATIO倍以上離れた値（千株単位での申告ミス等）は使わず、
    同じ期末日の次の候補を試す（ONDS 2025-12-31: 年次の加重平均221,769→発行済380,763,481）。
    """
    entries = [e for e in get_field_entries(store, "shares_diluted") if e.get("val")]
    cands_by_end: Dict[str, List[tuple]] = {}
    for q in ends:
        at = [e for e in entries if e["end"] == q]
        groups = [
            ("quarterly_weighted", [e for e in at if not e.get("is_annual") and e.get("source_tag") == WEIGHTED_DILUTED_TAG]),
            ("annual_weighted", [e for e in at if e.get("is_annual")]),
            ("period_end_outstanding", [e for e in at if not e.get("is_annual") and e.get("source_tag") != WEIGHTED_DILUTED_TAG]),
        ]
        cands = [(src, float(max(es, key=lambda e: e.get("filed") or "")["val"])) for src, es in groups if es]
        if cands:
            cands_by_end[q] = cands
    if not cands_by_end:
        return {}
    qs = list(cands_by_end)
    primary = [cands_by_end[q][0][1] for q in qs]
    adjusted = adjust_share_points(list(zip(qs, primary)), splits) if splits else list(primary)
    med = sorted(adjusted)[len(adjusted) // 2]

    def _ok(v: float) -> bool:
        return med > 0 and 1 / SHARES_OUTLIER_RATIO < v / med < SHARES_OUTLIER_RATIO

    out: Dict[str, Dict[str, Any]] = {}
    for q, raw, adj in zip(qs, primary, adjusted):
        if _ok(adj):
            out[q] = {"shares": adj, "source": cands_by_end[q][0][0], "split_adjusted": abs(adj - raw) > 0.5}
            continue
        alt = next(((src, v) for src, v in cands_by_end[q][1:] if _ok(v)), None)
        if alt:
            out[q] = {"shares": alt[1], "source": alt[0], "split_adjusted": False,
                      "replaced_outlier": cands_by_end[q][0][1]}
    return out


def _load_prices(repo_root: str, ticker: str) -> List[dict]:
    path = os.path.join(repo_root, "common", "market_data", "daily", f"{ticker}.json")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        recs = json.load(f).get("records") or []
    return sorted((r for r in recs if r.get("close") is not None and r.get("date")), key=lambda r: r["date"])


def _close_on_or_before(prices: List[dict], q: str) -> Optional[dict]:
    best = None
    for r in prices:
        if r["date"] > q:
            break
        best = r
    if best is None or _days(best["date"], q) > PRICE_MAX_STALE_DAYS:
        return None
    return best


def percentile_of(value: float, history: List[float]) -> float:
    """historyの中でのvalueのパーセンタイル（0〜100、同値は半分として数える）"""
    below = sum(1 for v in history if v < value)
    equal = sum(1 for v in history if v == value)
    return 100.0 * (below + 0.5 * equal) / len(history)


def signal_for(ptbv_pctl: float, rotce_pctl: float) -> str:
    if ptbv_pctl <= CHEAP_PTBV_MAX_PCTL and rotce_pctl >= CHEAP_ROTCE_MIN_PCTL:
        return SIGNAL_CHEAP
    if ptbv_pctl >= RICH_PTBV_MIN_PCTL and rotce_pctl <= RICH_ROTCE_MAX_PCTL:
        return SIGNAL_RICH
    return SIGNAL_NEUTRAL


def compute_ticker(ticker: str, repo_root: str = REPO_ROOT, store: Optional[dict] = None,
                   prices: Optional[List[dict]] = None, splits: Optional[List[dict]] = None,
                   company_facts: Optional[dict] = None) -> Dict[str, Any]:
    """1銘柄のROTCE・P/TBV（四半期履歴・直近値・自社比のパーセンタイル）を返す"""
    ticker = ticker.upper()
    out: Dict[str, Any] = {"ticker": ticker, "reference_only": True, "history": [], "notes": []}
    if store is None:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            store = build_ticker_store(ticker)
    if store is None:
        out["current"] = {"reason": R_NO_LAYER3}
        out["percentile"] = {"n_quarters": 0, "signal": None, "reason": R_NO_LAYER3}
        return out
    if company_facts is None:
        company_facts = load_company_facts(ticker)
    if prices is None:
        prices = _load_prices(repo_root, ticker)
    if splits is None:
        splits = load_split_history().get(ticker, [])

    series = {f: _stock_by_end(store, f) for f in
              ("stockholders_equity", "goodwill", "intangible_assets_excl_goodwill",
               "preferred_stock", "minority_interest")}
    intangible_tags = _intangible_tags_by_end(company_facts)
    unavailable: frozenset = frozenset()
    if _has_only_including_goodwill_tag(company_facts):
        # ASTS型: IntangibleAssetsNetIncludingGoodwillしか無い（銘柄で意味が違うため使わず、
        # 無形資産が不明＝TCEを計算しない）
        out["notes"].append(R_INTANGIBLE_ONLY_INCL)
        unavailable = frozenset({"intangible_assets_excl_goodwill"})
    temp_eq_ends = _temporary_equity_preferred_ends(company_facts)
    if temp_eq_ends:
        # 優先株がメザニンに分類されている期末日は控除しない（0として扱う）
        out["notes"].append(R_PREFERRED_TEMPORARY_EQUITY)
        series["preferred_stock"] = {
            end: ({**e, "val": 0} if end in temp_eq_ends else e) for end, e in series["preferred_stock"].items()}
    first_reported = _first_reported(company_facts)

    ni = _quarterly_flow(store, "net_income")
    ends = list(ni)
    # 年度末の期末日（Layer3の年次の純利益・株式数の期末日）
    fy_ends = frozenset(e["end"] for f in ("net_income", "shares_diluted")
                        for e in get_field_entries(store, f) if e.get("is_annual"))
    shares = _shares_by_end(store, ends, splits)
    tce_cache: Dict[str, Dict[str, Any]] = {}

    def _tce(q: str) -> Dict[str, Any]:
        if q not in tce_cache:
            tce_cache[q] = tce_at(series, q, intangible_tags, unavailable, fy_ends, first_reported)
        return tce_cache[q]

    common_ni = _common_ni_quarterly(company_facts, ticker)
    equity_ends = list(series["stockholders_equity"])

    for i in range(len(ends)):
        q = ends[i]
        window = ends[i - 3:i + 1] if i >= 3 else []
        ttm_ok = bool(window) and all(
            QUARTER_GAP_DAYS[0] <= _days(window[k], window[k + 1]) <= QUARTER_GAP_DAYS[1] for k in range(3))
        cn = common_ni.get(q)
        row: Dict[str, Any] = {
            "end": q,
            "quarter_net_income": cn["val"] if cn else ni[q]["val"],
            "net_income_source": NI_SOURCE_BY_TAG.get(cn.get("source_tag"), "common") if cn else NI_SOURCE_FALLBACK,
            "ttm_net_income": sum(ni[x]["val"] for x in window) if ttm_ok else None,
        }
        t = _tce(q)
        row.update(tce=t.get("tce"), components=t.get("components"),
                   filled_from_annual=bool(t.get("filled_components")),
                   filled_components=t.get("filled_components") or {})
        if t.get("missing_components"):
            row["missing_components"] = t["missing_components"]
        if t.get("assumed_zero"):
            row["assumed_zero"] = t["assumed_zero"]
            if ASSUMED_ZERO_EXCLUDED_FIELDS.intersection(t["assumed_zero"]):
                row["excluded_from_percentile"] = True
        sh = shares.get(q)
        px = _close_on_or_before(prices, q)
        row.update(price=px["close"] if px else None, price_date=px["date"] if px else None,
                   shares=sh["shares"] if sh else None, shares_source=sh["source"] if sh else None)
        if t.get("reason"):
            row["reason"] = t["reason"]
            out["history"].append(row)
            continue
        # 主表示: 四半期の年率（前の四半期末と当四半期末のTCEの平均）
        prev = _previous_quarter_end(equity_ends, q)
        tp = _tce(prev).get("tce") if prev else None
        annualized = row["quarter_net_income"] * ANNUALIZE_QUARTERS
        if tp is not None and tp > 0:
            row.update(tce_prev_quarter=tp, prev_quarter_end=prev,
                       rotce=annualized / ((t["tce"] + tp) / 2), rotce_basis="quarter_average")
        else:
            row.update(tce_prev_quarter=tp, prev_quarter_end=prev,
                       rotce=annualized / t["tce"], rotce_basis="quarter_end_only")
        # 参考: TTM純利益 ÷（4四半期前と当四半期末のTCEの平均）
        if ttm_ok:
            begin = ends[i - 4] if i >= 4 and YEAR_GAP_DAYS[0] <= _days(ends[i - 4], q) <= YEAR_GAP_DAYS[1] else None
            tb = _tce(begin).get("tce") if begin else None
            if tb is not None and tb > 0:
                row.update(tce_begin=tb, rotce_ttm=row["ttm_net_income"] / ((t["tce"] + tb) / 2),
                           rotce_ttm_basis="average")
            else:
                row.update(tce_begin=tb, rotce_ttm=row["ttm_net_income"] / t["tce"], rotce_ttm_basis="end_only")
        if px and sh:
            row["market_cap"] = px["close"] * sh["shares"]
            row["ptbv"] = row["market_cap"] / t["tce"]
        else:
            row["reason"] = R_PRICE_MISSING if not px else R_SHARES_MISSING
        out["history"].append(row)

    out["current"] = _current(out["history"], prices, ends)
    out["percentile"] = _percentile(out["history"], out["current"])
    latest_comp = (out["history"][-1].get("components") or {}) if out["history"] else {}
    out["tags"] = {
        "stockholders_equity": latest_comp.get("stockholders_equity_tag"),
        "intangible_assets_excl_goodwill": latest_comp.get("intangible_assets_excl_goodwill_tag"),
        **{f: tag for f, tag in TAG_BY_FIELD.items() if series[f]},
    }
    return out


def _current(history: List[dict], prices: List[dict], ends: List[str]) -> Dict[str, Any]:
    """直近値: 最新四半期のTCE・ROTCEと、最新の終値×最新四半期の株式数"""
    if not history:
        return {"reason": R_INSUFFICIENT_QUARTERS, "quarter_end": ends[-1] if ends else None}
    h = history[-1]
    cur: Dict[str, Any] = {
        "quarter_end": h["end"], "quarter_net_income": h.get("quarter_net_income"),
        "net_income_source": h.get("net_income_source"), "tce": h.get("tce"),
        "tce_prev_quarter": h.get("tce_prev_quarter"), "prev_quarter_end": h.get("prev_quarter_end"),
        "rotce": h.get("rotce"), "rotce_basis": h.get("rotce_basis"),
        "ttm_net_income": h.get("ttm_net_income"), "rotce_ttm": h.get("rotce_ttm"),
        "rotce_ttm_basis": h.get("rotce_ttm_basis"),
        "filled_from_annual": h.get("filled_from_annual", False),
        "filled_components": h.get("filled_components") or {},
    }
    if h.get("reason") in (R_EQUITY_MISSING, R_COMPONENT_MISSING, R_TCE_NONPOSITIVE):
        cur["reason"] = h["reason"]
        if h.get("missing_components"):
            cur["missing_components"] = h["missing_components"]
        return cur
    last = prices[-1] if prices else None
    if last is None:
        cur["reason"] = R_PRICE_MISSING
        return cur
    # 最新の希薄化後株式数（最新四半期に使える値が無ければ、使える直近の四半期の値）
    sh_row = next((r for r in reversed(history) if r.get("shares")), None)
    if sh_row is None:
        cur["reason"] = R_SHARES_MISSING
        return cur
    cur.update(price=last["close"], price_date=last["date"], shares=sh_row["shares"],
               shares_quarter_end=sh_row["end"], market_cap=last["close"] * sh_row["shares"])
    cur["ptbv"] = cur["market_cap"] / h["tce"]
    return cur


def _percentile(history: List[dict], current: Dict[str, Any]) -> Dict[str, Any]:
    """自社の過去の四半期との比較。無形資産を0と仮定した期（excluded_from_percentile）は母数から外す"""
    pts = [h for h in history if h.get("rotce") is not None and h.get("ptbv") is not None
           and not h.get("excluded_from_percentile")]
    n_assumed = sum(1 for h in history if h.get("rotce") is not None and h.get("ptbv") is not None
                    and h.get("excluded_from_percentile"))
    res: Dict[str, Any] = {"n_quarters": len(pts), "n_excluded_assumed_zero": n_assumed, "signal": None}
    if current.get("ptbv") is None or current.get("rotce") is None:
        res["reason"] = current.get("reason") or R_INSUFFICIENT_QUARTERS
        return res
    if current["rotce"] <= 0:
        # 赤字（ROTCE≤0）の銘柄は、マイナス同士の比較で「自社比割安」等になるため目安の対象外
        res["reason"] = R_ROTCE_NONPOSITIVE
        return res
    if len(pts) < MIN_QUARTERS_FOR_PERCENTILE:
        res["reason"] = R_INSUFFICIENT_QUARTERS
        return res
    res["ptbv"] = percentile_of(current["ptbv"], [h["ptbv"] for h in pts])
    res["rotce"] = percentile_of(current["rotce"], [h["rotce"] for h in pts])
    res["first_quarter"] = pts[0]["end"]
    res["last_quarter"] = pts[-1]["end"]
    res["signal"] = signal_for(res["ptbv"], res["rotce"])
    return res


def load_company_reported(path: str = COMPANY_REPORTED_PATH) -> Dict[str, dict]:
    """config/company_reported_metrics.jsonのROTCEの登録を 銘柄→登録 にする（ファイルが無ければ空）"""
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    return {m["ticker"].upper(): m for m in doc.get("metrics") or []
            if m.get("metric") == COMPANY_REPORTED_METRIC and m.get("ticker")}


def company_reported_block(entry: dict, history: List[dict], current: Dict[str, Any]) -> Dict[str, Any]:
    """会社発表のROTCEと、同じ四半期末のこちらの計算値・差（こちら−会社、小数）

    latest_registered: こちらの直近の四半期末（current.quarter_end）の会社発表の値が登録済みか。
    Falseなら個別ページに「未登録」と出し、CHECK-61がWARNにする。
    """
    ours = {h["end"]: h.get("rotce") for h in history}
    values = []
    for v in sorted(entry.get("values") or [], key=lambda x: x["quarter_end"]):
        c = ours.get(v["quarter_end"])
        values.append({**v, "computed": c,
                       "diff": (c - v["value"]) if c is not None and v.get("value") is not None else None})
    latest_q = current.get("quarter_end")
    return {
        "metric": COMPANY_REPORTED_METRIC,
        "definition": entry.get("definition"), "source": entry.get("source"),
        "guidance": entry.get("guidance"), "long_term_target": entry.get("long_term_target"),
        "values": values,
        "latest_quarter_end": latest_q,
        "latest_registered": latest_q is not None and any(v["quarter_end"] == latest_q for v in values),
        "last_registered_quarter_end": values[-1]["quarter_end"] if values else None,
    }


def unregistered_latest_quarters(reported: Optional[Dict[str, dict]] = None,
                                 out_dir: str = OUTPUT_DIR) -> List[Dict[str, Any]]:
    """CHECK-61用: 会社発表を登録している銘柄のうち、直近の四半期末（出力済みの{TICKER}.jsonの
    current.quarter_end）の値が未登録のもの。出力が無い銘柄は reason="no_output" で返す"""
    reported = load_company_reported() if reported is None else reported
    out: List[Dict[str, Any]] = []
    for t, entry in sorted(reported.items()):
        path = os.path.join(out_dir, f"{t}.json")
        if not os.path.exists(path):
            out.append({"ticker": t, "reason": "no_output"})
            continue
        with open(path, encoding="utf-8") as f:
            q = ((json.load(f).get("current") or {}).get("quarter_end"))
        registered = sorted(v["quarter_end"] for v in entry.get("values") or [])
        if q and q not in registered:
            out.append({"ticker": t, "reason": "unregistered", "latest_quarter_end": q,
                        "last_registered_quarter_end": registered[-1] if registered else None})
    return out


def _summary_row(r: Dict[str, Any]) -> Dict[str, Any]:
    c, p = r.get("current") or {}, r.get("percentile") or {}
    return {
        "rotce": c.get("rotce"), "ptbv": c.get("ptbv"), "tce": c.get("tce"),
        "quarter_end": c.get("quarter_end"), "price_date": c.get("price_date"),
        "filled_from_annual": c.get("filled_from_annual", False),
        "filled_components": c.get("filled_components") or {},
        "rotce_basis": c.get("rotce_basis"), "net_income_source": c.get("net_income_source"),
        "rotce_ttm": c.get("rotce_ttm"),
        "reason": c.get("reason"), "missing_components": c.get("missing_components"),
        "n_quarters": p.get("n_quarters", 0), "n_excluded_assumed_zero": p.get("n_excluded_assumed_zero", 0),
        "ptbv_pctl": p.get("ptbv"), "rotce_pctl": p.get("rotce"),
        "signal": p.get("signal"), "signal_reason": p.get("reason"),
        "notes": r.get("notes") or [],
    }


def build_all(tickers: List[str], repo_root: str = REPO_ROOT, out_dir: str = OUTPUT_DIR) -> Dict[str, Any]:
    """全銘柄を計算して {TICKER}.json と _summary.json を書き出す"""
    os.makedirs(out_dir, exist_ok=True)
    splits_all = load_split_history()
    reported = load_company_reported()
    generated_at = datetime.now().isoformat(timespec="seconds")
    rows: Dict[str, Any] = {}
    for t in tickers:
        try:
            r = compute_ticker(t, repo_root, splits=splits_all.get(t.upper(), []))
        except Exception as e:  # 1銘柄の失敗で全体を止めない
            r = {"ticker": t.upper(), "reference_only": True, "history": [], "notes": [],
                 "current": {"reason": "exception", "error": repr(e)[:300]},
                 "percentile": {"n_quarters": 0, "signal": None, "reason": "exception"}}
        if r["ticker"] in reported:
            r["company_reported"] = company_reported_block(reported[r["ticker"]], r["history"], r["current"])
        r["generated_at"] = generated_at
        with open(os.path.join(out_dir, f"{r['ticker']}.json"), "w", encoding="utf-8") as f:
            json.dump(r, f, ensure_ascii=False, indent=1)
        rows[r["ticker"]] = _summary_row(r)
    summary = {
        "generated_at": generated_at,
        "reference_only": True,
        "thresholds": {
            "min_quarters": MIN_QUARTERS_FOR_PERCENTILE,
            "cheap": {"ptbv_pctl_max": CHEAP_PTBV_MAX_PCTL, "rotce_pctl_min": CHEAP_ROTCE_MIN_PCTL},
            "rich": {"ptbv_pctl_min": RICH_PTBV_MIN_PCTL, "rotce_pctl_max": RICH_ROTCE_MAX_PCTL},
            "annual_fill_max_days": ANNUAL_FILL_MAX_DAYS,
        },
        "tickers": rows,
    }
    with open(os.path.join(out_dir, "_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=1)
    return summary


def main(argv: List[str]) -> int:
    from common.sec_data.config import get_all
    tickers = [a.upper() for a in argv] if argv else get_all()
    summary = build_all(tickers)
    rows = summary["tickers"].values()
    ok = sum(1 for r in rows if r["rotce"] is not None and r["ptbv"] is not None)
    print(f"ROTCE/P/TBV: {ok}/{len(summary['tickers'])}銘柄で算出 → {OUTPUT_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
