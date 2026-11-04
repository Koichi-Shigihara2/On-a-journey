"""[[EXTERNAL-SOURCE-CHANGE-WATCH-1]] B2（2026-11-04）: タグの途切れ・切り替え＋値の飛び・候補外のタグ・期末の数日違いの重複（common/sec_data/source_watch.py）"""
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from common import external_watch as ew
from common.sec_data import source_watch as sw


# ── B2 ───────────────────────────────────────────────────────────────
def _e(end, val, tag, ann=False, start=None):
    return {"end": end, "start": start or end, "val": val, "source_tag": tag, "is_annual": ann, "is_ytd": False}


def _facts(accn, end, tags):
    gaap = {t: {"units": {"USD": [{"accn": accn, "form": "10-Q", "filed": "2026-08-01", "end": end, "val": 5}]}} for t in tags}
    gaap.setdefault("Revenues", {"units": {"USD": [{"accn": accn, "form": "10-Q", "filed": "2026-08-01", "end": end, "val": 1}]}})
    return {"facts": {"us-gaap": gaap}}


DEFS = {"rpo": {"candidates": ["RevenueRemainingPerformanceObligation"]},
        "revenue": {"candidates": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"]},
        "stock_based_compensation": {"candidates": ["ShareBasedCompensation"]},
        "net_income": {"candidates": ["NetIncomeLoss"]}}


def test_tag_gap_switch_outside_dup():
    store = {"fields": {
        "rpo": {"entries": [_e("2025-06-30", 100.0, "RevenueRemainingPerformanceObligation"), _e("2026-03-31", 110.0, "RevenueRemainingPerformanceObligation")]},
        "revenue": {"entries": [_e(d, 100.0, "Revenues") for d in ("2025-09-30", "2025-12-31", "2026-03-31")]
                    + [_e("2026-06-30", 200.0, "RevenueFromContractWithCustomerExcludingAssessedTax")]},
        "stock_based_compensation": {"entries": [_e("2025-06-30", 3.0, "ShareBasedCompensation")]},
        "net_income": {"entries": [_e("2025-09-30", 1.0, "NetIncomeLoss"), _e("2025-10-02", 1.0, "NetIncomeLoss"), _e("2026-06-30", 1.0, "NetIncomeLoss")]},
    }}
    items = sw.tag_items("X", store, _facts("a1", "2026-06-30", ["StockOptionPlanExpense"]), DEFS)
    kinds = {(i["kind"], i["match"]) for i in items}
    assert ("tag_gap", "rpo") in kinds                                                    # 前の年の同じ四半期に値、今は無い
    assert ("tag_switch", "revenue Revenues→RevenueFromContractWithCustomerExcludingAssessedTax") in kinds   # 2.0倍
    assert ("tag_outside", "stock_based_compensation StockOptionPlanExpense") in kinds   # 候補外のタグに値、項目に値なし
    assert ("dup_period", "net_income 2025-09-30/2025-10-02") in kinds


def test_tag_switch_without_jump_is_not_reported():
    store = {"fields": {"revenue": {"entries": [_e(d, 100.0, "Revenues") for d in ("2025-09-30", "2025-12-31", "2026-03-31")]
                                    + [_e("2026-06-30", 110.0, "RevenueFromContractWithCustomerExcludingAssessedTax")]}}}
    assert not sw.tag_items("X", store, _facts("a1", "2026-06-30", []), {"revenue": DEFS["revenue"]})


def test_saved_source_watch_matches_rules():
    d = sw.load()
    assert d.get("generated_at") and isinstance(d.get("tags"), dict)
    assert {i["kind"] for i in sw.all_items()} <= set(ew.KIND_LABELS)


def test_tag_gap_only_when_rules_cannot_fill():
    """4aは、決めた補いの規則（bs_lines.covered_by_rules）でも埋まらない件だけ（2026-11-04 夜）。
    無形資産・のれん・短期投資で直近の10-Kの年度末の値がある（LLY・META・TSLA型）は出ない／借入金は年次の値があっても出る（CRWV型）"""
    defs = {"short_term_investments": {"candidates": ["ShortTermInvestments"]}, "goodwill": {"candidates": ["Goodwill"]},
            "long_term_debt": {"candidates": ["LongTermDebtNoncurrent"]}}
    fy = {"fp": "FY", "form": "10-K"}
    store = {"fields": {
        "short_term_investments": {"entries": [_e("2025-06-30", 100.0, "ShortTermInvestments"),
                                               {**_e("2025-12-31", 120.0, "ShortTermInvestments"), **fy}]},
        "goodwill": {"entries": [_e("2025-06-30", 5.0, "Goodwill"), {**_e("2025-12-31", 5.0, "Goodwill"), **fy}]},
        "long_term_debt": {"entries": [_e("2025-06-30", 900.0, "LongTermDebtNoncurrent"),
                                       {**_e("2025-12-31", 950.0, "LongTermDebtNoncurrent"), **fy}]},
    }}
    got = {(i["kind"], i["match"]) for i in sw.tag_items("X", store, _facts("a1", "2026-06-30", []), defs)}
    assert ("tag_gap", "short_term_investments") not in got and ("tag_gap", "goodwill") not in got
    assert ("tag_gap", "long_term_debt") in got
