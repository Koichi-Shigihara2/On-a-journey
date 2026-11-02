"""
common/sec_data/finance_lease.py
ファイナンス・リースの負債を純有利子負債に1回だけ入れる（[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段1、2026-11-02）

parser.py の long_term_debt・short_term_debt の候補にファイナンス・リースの負債のタグは無く、
リースを含むタグ（LongTermDebtAndCapitalLeaseObligations系）が選ばれた銘柄だけ純有利子負債に入っていた。
規則（チャット側の決定、2026-11-02）:
  1. 負債に選ばれた値が、リースを含むタグの値（Noncurrent、またはNoncurrent＋Current、IncludingCurrentMaturities）と
     同じ期末で一致するなら、もう入っているので足さない（タグ名でなく値で判定。HONの LongTermDebt 14,169M は
     LongTermDebtAndCapitalLeaseObligations 10,386M＋同Current 3,783M と一致し、10-Kでもリースは負債の行にある）
  2. 入っていなければ FinanceLeaseLiabilityNoncurrent＋FinanceLeaseLiabilityCurrent（無ければ FinanceLeaseLiability）を足す。
     値は負債と同じ期末を優先し、無ければ基準日（負債の期末、分からなければ最新の決算期末）から400日以内の最新の値
     （10-Kにしか出さない会社のため。案B）。使った値の期末を記録する
  オペレーティング・リースは入れない。
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional

LTD_LEASE_TAGS = ("LongTermDebtAndCapitalLeaseObligations",)
LTD_LEASE_CURRENT_TAGS = ("LongTermDebtAndCapitalLeaseObligationsCurrent",)
LTD_LEASE_TOTAL_TAGS = ("LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities",)
FL_NONCURRENT, FL_CURRENT, FL_TOTAL = "FinanceLeaseLiabilityNoncurrent", "FinanceLeaseLiabilityCurrent", "FinanceLeaseLiability"
WINDOW_DAYS = 400
_FORMS = ("10-K", "10-Q", "10-K/A", "10-Q/A")
_OCF = "NetCashProvidedByUsedInOperatingActivities"


def _instants(us_gaap: dict, tag: str) -> dict[str, float]:
    """{期末: 値}（時点の値、同じ期末は最後の申告）"""
    out: dict[str, tuple[float, str]] = {}
    for r in ((us_gaap.get(tag) or {}).get("units") or {}).get("USD") or []:
        if r.get("form") in _FORMS and "start" not in r and r.get("val") is not None:
            if r["end"] not in out or (r.get("filed") or "") > out[r["end"]][1]:
                out[r["end"]] = (r["val"], r.get("filed") or "")
    return {e: v[0] for e, v in out.items()}


def _same(a: Optional[float], b: Optional[float]) -> bool:
    return a is not None and b is not None and abs(a - b) <= max(1.0, abs(b) * 1e-6)


def _debt_end(us_gaap: dict, ltd: Optional[float], std: Optional[float], debt_tags: list[str]) -> Optional[str]:
    """負債に選ばれた値と同じ値の期末（新しい方）。値が無ければNone"""
    ends = []
    for val in (ltd, std):
        if not val:
            continue
        for tag in debt_tags:
            ends += [e for e, v in _instants(us_gaap, tag).items() if _same(v, val)]
    return max(ends) if ends else None


def lease_in_debt(us_gaap: dict, ltd: Optional[float], end: Optional[str]) -> Optional[str]:
    """負債の値がリースを含むタグと一致するなら、その理由の文（含まなければNone）"""
    if not ltd or not end:
        return None
    nc = next((v for t in LTD_LEASE_TAGS for e, v in _instants(us_gaap, t).items() if e == end), None)
    cu = next((v for t in LTD_LEASE_CURRENT_TAGS for e, v in _instants(us_gaap, t).items() if e == end), None)
    tot = next((v for t in LTD_LEASE_TOTAL_TAGS for e, v in _instants(us_gaap, t).items() if e == end), None)
    if _same(nc, ltd):
        return "LongTermDebtAndCapitalLeaseObligationsと一致"
    if nc is not None and _same(nc + (cu or 0), ltd):
        return "LongTermDebtAndCapitalLeaseObligations＋Currentと一致"
    if _same(tot, ltd):
        return "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturitiesと一致"
    return None


def finance_lease_liability(company_facts: Optional[dict], ltd: Optional[float], std: Optional[float],
                            debt_tags: list[str]) -> dict[str, Any]:
    """純有利子負債に足すファイナンス・リースの負債。
    Returns: {"value": 足す額（0なら足さない）, "end": 使った値の期末, "basis": "same_end"|"latest_within_400d"|None,
              "tags": 使ったタグ, "debt_end": 負債の期末, "included_in_debt": 理由の文 or None}
    """
    out: dict[str, Any] = {"value": 0.0, "end": None, "basis": None, "tags": [], "debt_end": None, "included_in_debt": None}
    us = ((company_facts or {}).get("facts") or {}).get("us-gaap") or {}
    if not us:
        return out
    end = _debt_end(us, ltd, std, debt_tags)
    out["debt_end"] = end
    reason = lease_in_debt(us, ltd, end)
    if reason:
        out["included_in_debt"] = reason
        return out
    nc, cu, tot = _instants(us, FL_NONCURRENT), _instants(us, FL_CURRENT), _instants(us, FL_TOTAL)

    def value_at(e: str) -> Optional[tuple[float, list[str]]]:
        if e in nc or e in cu:
            return (nc.get(e) or 0) + (cu.get(e) or 0), [t for t, d in ((FL_NONCURRENT, nc), (FL_CURRENT, cu)) if e in d]
        if e in tot:
            return tot[e], [FL_TOTAL]
        return None

    if end and value_at(end):
        v, tags = value_at(end)
        out.update(value=float(v), end=end, basis="same_end", tags=tags)
        return out
    ref = end or max(_instants_any_end(us), default=None)
    if not ref:
        return out
    ref_d = date.fromisoformat(ref)
    for e in sorted(set(nc) | set(cu) | set(tot), reverse=True):
        if e > ref:
            continue
        if (ref_d - date.fromisoformat(e)).days > WINDOW_DAYS:
            break
        got = value_at(e)
        if got:
            out.update(value=float(got[0]), end=e, basis="latest_within_400d", tags=got[1])
            break
    return out


def _instants_any_end(us_gaap: dict) -> list[str]:
    """最新の決算期末の候補（営業CFの期末）"""
    return [r["end"] for r in ((us_gaap.get(_OCF) or {}).get("units") or {}).get("USD") or [] if r.get("form") in _FORMS]
