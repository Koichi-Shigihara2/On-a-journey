"""SECの取得元の変化・ばらつきの検知（[[EXTERNAL-SOURCE-CHANGE-WATCH-1]] B1・B2、2026-11-04）。直さずに検知して知らせる。

SEC_Data_Update（update.py）が銘柄ごとにLayer3を作った直後に tag_items() を呼び、実行の最後に cik_items() と一緒に
RESULT_FILE（common/sec_data/data/_source_watch.json）へ書く。System Health の [P] ExternalChange（common/external_watch.py）が読み、
台帳（config/external_change_acknowledged.json）に無い新しい件だけを⚠️にする。銘柄ごとの事情は台帳に記録する（ここに銘柄の一覧は書かない）。

件の種類（kind）と台帳の match:
  cik          CIKの切り替え（config/cik_lookup.csv と SECの company_tickers.json の食い違い・ティッカーが無い）。match「<手元のCIK>→<SECのCIK>」
               submissions の社名の履歴（former_names）は合図にしない（今の社名の「最後の日」が直近の申告の日になる銘柄があり、表記の変更でも出る）
  tag_gap      4a 途切れ: 前の年の同じ四半期（350〜380日前）に0でない値があり、最新の申告の期末に値も候補のタグも無い（KEY_FIELDS だけ）。match「<項目>」
  tag_switch   4b 切り替え: 最新の期末の値のタグが前の4期の多数と違い、値が前の期の SWITCH_RATIO 倍の外。match「<項目> <前のタグ>→<今のタグ>」
  tag_outside  4c 候補外: 最新の申告の当期に、項目に当たる候補外のタグ（OUTSIDE_TAGS の完全な名前）の値があり、その項目に値が無い。match「<項目> <タグ>」
  dup_period   同じ期の期末の数日違い（1〜6日）の2件（TTMの窓〈最新の期末から DUP_WINDOW_DAYS 日〉の中だけ）。match「<項目> <期末>/<期末>」
"""
from __future__ import annotations

import json
import os
import re
from collections import Counter
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(os.path.dirname(_DIR))
RESULT_FILE = os.path.join(_DIR, "data", "_source_watch.json")
CIK_LOOKUP = os.path.join(_REPO, "config", "cik_lookup.csv")
CONCEPT_DEFS = os.path.join(_REPO, "config", "sec_concept_definitions.json")

# 出力（理論株価・判定・funda・Stonks・ROTCE・P/TBV等）に効く項目（4aの対象）
KEY_FIELDS = (
    "revenue", "operating_cash_flow", "capital_expenditure", "stock_based_compensation", "depreciation_and_amortization",
    "net_income", "operating_income", "gross_profit", "cash_and_equivalents", "short_term_debt", "long_term_debt",
    "stockholders_equity", "shares_diluted", "goodwill", "intangible_assets_excl_goodwill", "preferred_stock",
    "minority_interest", "rpo", "short_term_investments", "total_liabilities", "eps_diluted",
)
# 4c: 項目に当たる候補外のタグ（完全な名前の表。部分一致は別の項目〈非支配持分の増減等〉まで拾うので使わない）。
# 前受収益（ContractWithCustomerLiability*）はLayer3の deferred_revenue を読む処理が無いので入れない
OUTSIDE_TAGS = {
    "stock_based_compensation": ("StockOptionPlanExpense", "RestrictedStockExpense", "EmployeeBenefitsAndShareBasedCompensation"),
    "preferred_stock": ("ConvertiblePreferredStockNonredeemableOrRedeemableIssuerOptionValue", "PreferredStockValueOutstanding",
                        "PreferredStockIncludingAdditionalPaidInCapital",
                        "PreferredStockIncludingAdditionalPaidInCapitalNetOfDiscount"),
    "intangible_assets_excl_goodwill": ("IndefiniteLivedTrademarks", "IndefiniteLivedIntangibleAssetsExcludingGoodwill"),
    "minority_interest": ("MinorityInterest", "RedeemableNoncontrollingInterestEquityCarryingAmount"),
}
SWITCH_RATIO = 1.5          # 4b: 前の期の 1/1.5〜1.5 倍の外
DUP_WINDOW_DAYS = 730       # 重複を数える窓（TTMの系列の範囲）
FORMS = ("10-K", "10-Q")


def _days(a: str, b: str) -> int:
    return (date.fromisoformat(b) - date.fromisoformat(a)).days


def latest_filing(company_facts: Dict[str, Any]):
    """最新の10-K・10-Q（提出日の最大）の (accn, 当期の期末, その申告の当期の0でない値を持つタグの集合)"""
    gaap = (company_facts or {}).get("facts", {}).get("us-gaap") or {}
    best = None
    for d in gaap.values():
        for u in (d.get("units") or {}).values():
            for e in u:
                if e.get("form") in FORMS and e.get("accn") and (best is None or e.get("filed", "") > best[0]):
                    best = (e["filed"], e["accn"])
    if best is None:
        return None, None, set()
    accn = best[1]
    ends = [e["end"] for d in gaap.values() for u in (d.get("units") or {}).values() for e in u if e.get("accn") == accn and e.get("end")]
    end = max(ends) if ends else None
    tags = {tag for tag, d in gaap.items() for u in (d.get("units") or {}).values() for e in u
            if e.get("accn") == accn and e.get("end") == end and e.get("val") not in (None, 0)}
    return accn, end, tags


def _item(kind: str, ticker: str, match: str, detail: str) -> Dict[str, str]:
    return {"kind": kind, "ticker": ticker, "match": match, "detail": detail}


def tag_items(ticker: str, store: Dict[str, Any], company_facts: Dict[str, Any],
              defs: Optional[Dict[str, Any]] = None) -> List[Dict[str, str]]:
    """4a・4b・4c・重複の件（Layer3のストアと company_facts から）"""
    from .layer3_builder import get_field_entries
    if defs is None:
        with open(CONCEPT_DEFS, encoding="utf-8") as f:
            defs = json.load(f)["fields"]
    accn, end, in_latest = latest_filing(company_facts)
    if not end or not store:
        return []
    out: List[Dict[str, str]] = []
    for field, fd in defs.items():
        cands = set(fd.get("candidates") or [])
        ents = [e for e in get_field_entries(store, field) if not e.get("is_ytd") and e.get("val") is not None]
        at_end = [e for e in ents if e["end"] == end]
        prev = sorted((e for e in ents if e["end"] < end and _days(e["end"], end) <= 400), key=lambda e: e["end"])
        if field in KEY_FIELDS:
            yago = [e for e in ents if 350 <= _days(e["end"], end) <= 380 and e.get("val") not in (0, 0.0)]
            if yago and not at_end and not (cands & in_latest):
                out.append(_item("tag_gap", ticker, field,
                                 f"{field}: {yago[-1]['end']} に {yago[-1].get('source_tag')} の値があり、最新の申告（{accn}、期末 {end}）に値も候補のタグも無い"))
        if at_end and prev:
            cur = at_end[0]
            same = [e for e in prev if bool(e.get("is_annual")) == bool(cur.get("is_annual"))]
            mode = Counter(e.get("source_tag") for e in prev[-4:]).most_common(1)[0][0]
            lt = cur.get("source_tag")
            if lt and mode and lt != mode and "cf_lines" not in (lt, mode) and same and same[-1]["val"]:
                ratio = abs(cur["val"]) / abs(same[-1]["val"])
                if ratio > SWITCH_RATIO or ratio < 1 / SWITCH_RATIO:
                    out.append(_item("tag_switch", ticker, f"{field} {mode}→{lt}",
                                     f"{field}: 期末 {end} のタグが {mode}→{lt}、値 {same[-1]['val']:,.0f}→{cur['val']:,.0f}（{ratio:.2f}倍）"))
        win = [e for e in ents if not e.get("is_implied") and 0 <= _days(e["end"], end) <= DUP_WINDOW_DAYS]
        for ann in (False, True):
            es = sorted({e["end"] for e in win if bool(e.get("is_annual")) == ann})
            for a, b in zip(es, es[1:]):
                if 0 < _days(a, b) <= 6:
                    out.append(_item("dup_period", ticker, f"{field} {a}/{b}",
                                     f"{field}: {'年次' if ann else '四半期'}の期末 {a} と {b} の2件（同じ期の期末の数日違い）"))
        if field in OUTSIDE_TAGS and not at_end:
            for tg in OUTSIDE_TAGS[field]:
                if tg in in_latest and tg not in cands:
                    out.append(_item("tag_outside", ticker, f"{field} {tg}",
                                     f"{field}: 最新の申告（{accn}、期末 {end}）に候補外のタグ {tg} の値があり、この項目に値が無い"))
    return out


def cik_items(company_tickers: Dict[str, Any], lookup_path: str = CIK_LOOKUP) -> List[Dict[str, str]]:
    """config/cik_lookup.csv（status が retired 以外）と SECの company_tickers.json の食い違い"""
    by: Dict[str, List[str]] = {}
    for e in (company_tickers or {}).values():
        by.setdefault(str(e.get("ticker", "")).upper(), []).append(str(e.get("cik_str")).zfill(10))
    from .tickers import get_all_rows   # 登録簿の読み方の1か所（cik_lookup.csv を直接読まない）
    out = []
    for r in get_all_rows(lookup_path):
        if r.get("status") == "retired":
            continue
        t, c = r["ticker"].upper(), str(r["cik"]).zfill(10)
        if t not in by:
            out.append(_item("cik", t, f"{c}→なし", f"SECの company_tickers.json に {t} が無い（手元のCIK {c}）"))
        elif c not in by[t]:
            out.append(_item("cik", t, f"{c}→{by[t][0]}",
                             f"手元のCIK {c} と SECの company_tickers.json の {'・'.join(by[t])} が違う（法人の再編・合併・SPAC。"
                             f"cik_history.json・cik_lookup.csv の変更は人が判断）"))
    return out


def write(items_by_ticker: Dict[str, List[Dict[str, str]]], cik: Optional[List[Dict[str, str]]], full_run: bool,
          path: str = RESULT_FILE, now: Optional[datetime] = None) -> Dict[str, Any]:
    """結果を書く。銘柄を絞った実行は、その銘柄の件だけを置き換える（ほかの銘柄・CIKは前の結果を残す）"""
    now = now or datetime.now(timezone.utc)
    old = load(path)
    tags = {} if full_run else dict(old.get("tags") or {})
    tags.update(items_by_ticker)
    doc = {"generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ") if full_run else old.get("generated_at"),
           "updated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
           "cik": cik if cik is not None else (old.get("cik") or []),
           "cik_checked_at": now.strftime("%Y-%m-%dT%H:%M:%SZ") if cik is not None else old.get("cik_checked_at"),
           "tags": {t: tags[t] for t in sorted(tags) if tags[t]}}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=1)
        f.write("\n")
    return doc


def load(path: str = RESULT_FILE) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f) or {}
    except (OSError, ValueError):
        return {}


def all_items(path: str = RESULT_FILE) -> List[Dict[str, str]]:
    d = load(path)
    return list(d.get("cik") or []) + [i for items in (d.get("tags") or {}).values() for i in items]
