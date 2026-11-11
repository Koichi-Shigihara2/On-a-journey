"""[[PARSER-LAYER3-SINGLE-SOURCE-1]] 段0（2026-11-11）: Layer3の定義の修正。

1. total_liabilities の候補に LiabilitiesAndStockholdersEquity（＝資産合計）を入れない。Liabilities が無い期末日は
   資産合計 − 純資産 − 非支配持分（minority_interest。無ければ0）で埋める
2. D&A: 部分概念（Depreciation・AmortizationOfIntangibleAssets）は、両方ある期は足す。優先順位は 合計のタグ ＞ 2つの合計 ＞ 片方だけ
3. 保持期間は読み手が要る長さから導く（retention.py。手で書かない）
"""
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from common.sec_data import layer3_builder as L  # noqa: E402
from common.sec_data import retention  # noqa: E402


def test_total_liabilities_candidates_exclude_assets_total():
    d = json.load(open(os.path.join(REPO, "config", "sec_concept_definitions.json"), encoding="utf-8"))
    assert d["fields"]["total_liabilities"]["candidates"] == ["Liabilities"]


def _e(end, val, **kw):
    return dict({"end": end, "start": "", "val": val, "first_val": val, "first_filed": "2026-01-01", "is_ytd": False,
                 "is_annual": False, "filed": "2026-01-01", "source_tag": "x"}, **kw)


def test_total_liabilities_identity_fills_missing_ends_only():
    f = {
        "total_assets": {"entries": [_e("2025-12-31", 104_816), _e("2026-03-31", 110_000)]},
        "stockholders_equity": {"entries": [_e("2025-12-31", 32_169), _e("2026-03-31", 30_000)]},
        "minority_interest": {"entries": [_e("2025-12-31", 2_106)]},
        "total_liabilities": {"source_tag": "Liabilities", "category": "stock", "entries": [_e("2026-03-31", 77_000)]},
    }
    L._derive_total_liabilities(f)
    by = {e["end"]: e for e in f["total_liabilities"]["entries"]}
    assert by["2025-12-31"]["val"] == 104_816 - 32_169 - 2_106       # 非支配持分も引く
    assert by["2025-12-31"]["derived"] is True and by["2025-12-31"]["source_tag"].startswith("identity:")
    assert by["2026-03-31"]["val"] == 77_000                          # 申告の値は上書きしない


def test_total_liabilities_identity_subtracts_temporary_equity():
    f = {
        "total_assets": {"entries": [_e("2019-12-31", 226_839)]},
        "stockholders_equity": {"entries": [_e("2019-12-31", 0)]},
    }
    facts = {"facts": {"us-gaap": {"TemporaryEquityCarryingAmountAttributableToParent": {"units": {"USD": [
        {"end": "2019-12-31", "val": 202_558}]}}}}}
    L._derive_total_liabilities(f, facts)
    assert f["total_liabilities"]["entries"][0]["val"] == 226_839 - 202_558


def test_total_liabilities_identity_skips_without_equity_or_nonpositive():
    f = {
        "total_assets": {"entries": [_e("2025-12-31", 100), _e("2026-03-31", 100)]},
        "stockholders_equity": {"entries": [_e("2026-03-31", 100)]},          # 負債0以下は作らない
    }
    L._derive_total_liabilities(f)
    assert f["total_liabilities"]["entries"] == []


def test_partial_sum_series_priority_and_values():
    dep = [_e("2025-12-31", 100, source_tag="Depreciation", is_annual=True), _e("2025-09-30", 20, source_tag="Depreciation")]
    amo = [_e("2025-12-31", 300, source_tag="AmortizationOfIntangibleAssets", is_annual=True)]
    tot = [_e("2025-12-31", 999, source_tag="DepreciationAndAmortization", is_annual=True)]
    series, used = L._add_partial_sum_series(
        [tot, dep, amo], ["DepreciationAndAmortization", "Depreciation", "AmortizationOfIntangibleAssets"],
        L.PARTIAL_SUM_FIELDS["depreciation_and_amortization"])
    assert used == ["DepreciationAndAmortization", "Depreciation+AmortizationOfIntangibleAssets",
                    "Depreciation", "AmortizationOfIntangibleAssets"]
    merged = L._merge_normalized_by_priority(series)
    by = {(e["end"], e["is_annual"]): e for e in merged}
    assert by[("2025-12-31", True)]["val"] == 999                         # 合計のタグが先
    assert by[("2025-09-30", False)]["val"] == 20                         # 片方しか無い期はそのまま


def test_partial_sum_used_when_no_total_tag():
    dep = [_e("2025-12-31", 100, source_tag="Depreciation", is_annual=True)]
    amo = [_e("2025-12-31", 300, source_tag="AmortizationOfIntangibleAssets", is_annual=True)]
    series, used = L._add_partial_sum_series([dep, amo], ["Depreciation", "AmortizationOfIntangibleAssets"],
                                             L.PARTIAL_SUM_FIELDS["depreciation_and_amortization"])
    (e,) = L._merge_normalized_by_priority(series)
    assert e["val"] == 400 and e["first_val"] == 400 and e["partial_sum"] is True


def test_retention_is_derived_from_reader_needs():
    from common.sec_data import quarterly as q
    assert q._QUARTERLY_YEARS == retention.QUARTERLY_YEARS == 7              # TTM 6点: (6-1)+1+0.75 → 7
    assert q._ANNUAL_YEARS == retention.ANNUAL_YEARS == retention.ROE_AVG_YEARS + 1 == 11
    from common.sec_data.reader import SECReader
    import inspect
    assert inspect.signature(SECReader.get_roe_avg_detail).parameters["years"].default == retention.ROE_AVG_YEARS
