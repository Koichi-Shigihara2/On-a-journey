"""
tests/test_fcf_definition_single_source.py

[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段1・段1b（2026-11-02）:
- FCF＝営業CF − |CapEx| の1か所（common/sec_data/fcf.py）。リースの支払いを足さない
- ファイナンス・リースの負債（common/sec_data/finance_lease.py）: 負債の値がリースを含むタグと一致すれば足さない（HON型）、
  同じ期末の値を優先、無ければ400日以内の最新（10-Kだけの会社）、オペレーティング・リースは入れない
- fact_overrides.json の fill_missing（LYFTのCapEx、独自タグでcompanyfactsに無い値）
- 段1b: 売上×8%の下限は構造的赤字（5年・2年とも≤0）のときだけ。Policy Aは下限の値をDCFの基準に使ったときだけ

実行方法:
    venv/Scripts/python.exe -m pytest tests/test_fcf_definition_single_source.py -v
"""

import json
import os
import sys

import pytest

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "src", "value", "tanuki_valuation"))

from common.sec_data.fcf import free_cash_flow  # noqa: E402
from common.sec_data.finance_lease import finance_lease_liability  # noqa: E402
from common.sec_data import ttm_calculator  # noqa: E402
from calculator.adjustments import adjust_fcf, is_structural_deficit  # noqa: E402


class TestFreeCashFlow:
    def test_ocf_minus_abs_capex(self):
        assert free_cash_flow(1000, -300) == 700
        assert free_cash_flow(1000, 300) == 700

    def test_lease_payments_are_not_added(self):
        """2026-11-01までの式（OCF − max(0, |CapEx| − |リースの支払い|)）はリースの支払いを足していた（TSLA +82M等）"""
        assert free_cash_flow(18_710, 15_407) == 3_303

    def test_missing_is_none(self):
        assert free_cash_flow(None, 100) is None and free_cash_flow(100, None) is None

    def test_old_definitions_are_gone(self):
        import common.sec_data.layer3_builder as lb
        assert not hasattr(lb, "calc_fcf") and not hasattr(ttm_calculator, "_calc_fcf")


def _inst(end, val, form="10-Q", filed=None):
    return {"end": end, "val": val, "form": form, "filed": filed or end}


def _facts(**tags):
    return {"facts": {"us-gaap": {t: {"units": {"USD": rows}} for t, rows in tags.items()}}}


DEBT_TAGS = ["LongTermDebtNoncurrent", "LongTermDebt", "LongTermDebtAndCapitalLeaseObligations", "LongTermDebtCurrent"]


class TestFinanceLease:
    def test_added_when_debt_excludes_lease(self):
        f = _facts(LongTermDebtNoncurrent=[_inst("2026-09-30", 40_000)],
                   FinanceLeaseLiabilityNoncurrent=[_inst("2026-09-30", 60_000)],
                   FinanceLeaseLiabilityCurrent=[_inst("2026-09-30", 11_000)])
        r = finance_lease_liability(f, 40_000, None, DEBT_TAGS)
        assert (r["value"], r["end"], r["basis"], r["included_in_debt"]) == (71_000, "2026-09-30", "same_end", None)

    def test_not_added_when_debt_value_matches_lease_inclusive_tags(self):
        """HON型: 選ばれたタグ名（LongTermDebt）にリースが無くても、値がリースを含むタグの合計と一致すれば足さない"""
        f = _facts(LongTermDebt=[_inst("2026-09-30", 14_169)],
                   LongTermDebtAndCapitalLeaseObligations=[_inst("2026-09-30", 10_386)],
                   LongTermDebtAndCapitalLeaseObligationsCurrent=[_inst("2026-09-30", 3_783)],
                   FinanceLeaseLiability=[_inst("2026-09-30", 72)])
        r = finance_lease_liability(f, 14_169, None, DEBT_TAGS)
        assert r["value"] == 0 and r["included_in_debt"]

    def test_latest_within_400_days_when_only_in_10k(self):
        f = _facts(LongTermDebtNoncurrent=[_inst("2026-09-30", 30_000)],
                   FinanceLeaseLiabilityNoncurrent=[_inst("2025-12-31", 1_000, "10-K"), _inst("2024-12-31", 900, "10-K")],
                   FinanceLeaseLiabilityCurrent=[_inst("2025-12-31", 184, "10-K")])
        r = finance_lease_liability(f, 30_000, None, DEBT_TAGS)
        assert (r["value"], r["end"], r["basis"]) == (1_184, "2025-12-31", "latest_within_400d")

    def test_too_old_is_not_added(self):
        f = _facts(LongTermDebtNoncurrent=[_inst("2026-09-30", 30_000)],
                   FinanceLeaseLiability=[_inst("2024-12-31", 500, "10-K")])
        assert finance_lease_liability(f, 30_000, None, DEBT_TAGS)["value"] == 0

    def test_operating_lease_is_not_used(self):
        f = _facts(LongTermDebtNoncurrent=[_inst("2026-09-30", 30_000)],
                   OperatingLeaseLiability=[_inst("2026-09-30", 9_999)])
        assert finance_lease_liability(f, 30_000, None, DEBT_TAGS)["value"] == 0


class TestFactOverridesFillMissing:
    def _extracted(self):
        return {"capital_expenditure": {"annual": {2024: None}, "_annual_provenance": {}}}

    def test_fill_missing_fills_none_year(self, monkeypatch):
        import common.sec_data.parser as parser_mod
        monkeypatch.setattr(parser_mod, "_load_fact_overrides", lambda: {"LYFT": {"2024": {"reason": "r", "fields": {
            "capital_expenditure": {"value": 83_470_000, "source_accn": "A", "source_filed": "2026-02-11", "fill_missing": True}}}}})
        ex = self._extracted()
        parser_mod.SECParser()._apply_fact_overrides("LYFT", ex)
        assert ex["capital_expenditure"]["annual"][2024] == 83_470_000
        assert ex["capital_expenditure"]["_annual_provenance"][2024]["override_applied"] is True

    def test_without_fill_missing_none_year_is_kept(self, monkeypatch):
        import common.sec_data.parser as parser_mod
        monkeypatch.setattr(parser_mod, "_load_fact_overrides", lambda: {"X": {"2024": {"reason": "r", "fields": {
            "capital_expenditure": {"value": 1, "source_accn": "A", "source_filed": "x"}}}}})
        ex = self._extracted()
        parser_mod.SECParser()._apply_fact_overrides("X", ex)
        assert ex["capital_expenditure"]["annual"][2024] is None

    def test_repo_lyft_entries_have_citation(self):
        with open(os.path.join(_ROOT, "common", "sec_data", "fact_overrides.json"), encoding="utf-8") as f:
            lyft = json.load(f)["LYFT"]
        assert sorted(lyft) == ["2023", "2024", "2025"]
        for y, v in lyft.items():
            ov = v["fields"]["capital_expenditure"]
            assert ov["fill_missing"] is True and ov["source_accn"] and ov["citation"]


class TestStructuralDeficitFloor:
    def test_condition(self):
        assert is_structural_deficit(-1, -1) and is_structural_deficit(0, 0)
        assert not is_structural_deficit(-1, 5) and not is_structural_deficit(5, -1)

    def test_floor_only_when_both_averages_nonpositive(self):
        r = adjust_fcf(fcf_avg=-556e6, latest_revenue=717e9, fcf_2yr_avg=-18e9)
        assert r.method == "revenue_floor" and r.adjusted_fcf == pytest.approx(717e9 * 0.08)

    def test_no_floor_when_two_year_average_positive(self):
        """BROS・COHR・RCAT・S型: 5年平均は負だが2年平均は正 → 下限を付けない（DCFの基準は2年平均）"""
        r = adjust_fcf(fcf_avg=-26e6, latest_revenue=1.5e9, fcf_2yr_avg=84e6)
        assert r.method == "none" and r.floor_applied == 0 and r.adjusted_fcf == -26e6


class TestPolicyAUsesFloorInBase:
    def test_policy_a_does_not_fire_without_floor_in_base(self, tmp_path):
        sys.path.insert(0, os.path.dirname(__file__))
        from test_pipeline_logic import _make_pipe, _write_stonks_json
        pipe = _make_pipe(tmp_path)
        _write_stonks_json(tmp_path, {})
        valuation = {"upside_percent": -5.0,
                     "components": {"current_price": 100.0, "diluted_shares": None,
                                    "fcf_floor_applied": 1_000_000, "fcf_floor_used_in_base": False},
                     "fcf_base": {"base_fcf": 500_000_000}, "financial_health": {}, "fcf_estimation": {}}
        r = pipe._compute_tanuki_score("BROS", valuation)
        assert r["rounded_by_policy"] != "A"
