"""
tests/test_tangible_equity_tags.py

[[ROTCE-PTBV-1]]（2026-10-10）: 有形普通株主資本（TCE）の控除項目
（のれん・のれんを除く無形資産・優先株・非支配持分）の取り込み。

- のれんを除く無形資産は、期末日ごとにIntangibleAssetsNetExcludingGoodwill（EXCL）を優先し、
  無い期末日だけFiniteLived＋IndefiniteLived（有限＋無期限）を使う。EXCLと内訳は足さない
- IntangibleAssetsNetIncludingGoodwill（INCL）しか無い銘柄（ASTS型）はNone
- EXCLが同じaccnの有限＋無期限より小さい期末日（PEP型、EXCLが合計ではない）は有限＋無期限
- Layer2（parser.py）とLayer3（layer3_builder.py）で同じ値になる
- この項目だけの年度・四半期のファイルは作らない

実行方法:
    python -m pytest tests/test_tangible_equity_tags.py -v
"""

from common.sec_data import layer3_builder as lb
from common.sec_data.parser import SECParser
from common.sec_data.tag_definitions import (
    INTANGIBLE_EXCL_TAG, INTANGIBLE_FINITE_TAG, INTANGIBLE_INDEFINITE_TAG,
    INTANGIBLE_INCL_GOODWILL_TAG, INTANGIBLE_RESOLVED_DERIVED, INTANGIBLE_SUM_DERIVED,
    derive_intangible_resolved_facts, with_derived_intangibles,
)

ACCN = "0000000000-25-000001"


def _inst(end, val, accn=ACCN, form="10-K", fy=2025, fp="FY"):
    """instant fact（start_dateを持たない）"""
    return {"end": end, "val": val, "accn": accn, "fy": fy, "fp": fp, "form": form,
            "filed": end + "T00:00:00"}


def _facts(**tags):
    return {"facts": {"us-gaap": {t: {"units": {"USD": es}} for t, es in tags.items()}}}


def _resolved_by_end(company_facts):
    us_gaap = with_derived_intangibles(company_facts)["facts"]["us-gaap"]
    facts = (us_gaap.get(INTANGIBLE_RESOLVED_DERIVED) or {}).get("units", {}).get("USD", [])
    return {f["end"]: (f["val"], f["source_tags"]) for f in facts}


def _layer2_annual(company_facts, year=2025):
    """parser.pyの抽出（_extract_values_best_candidate）でのintangible_assets_excl_goodwill"""
    parser = SECParser()
    us_gaap = with_derived_intangibles(company_facts)["facts"]["us-gaap"]
    result = parser._extract_values_best_candidate(
        us_gaap, list(parser.TANGIBLE_EQUITY_MAPPING["intangible_assets_excl_goodwill"]),
        fiscal_end_month=12, anchor_month=12, anchor_day=31,
        field_name="intangible_assets_excl_goodwill",
    )
    return result.get("annual", {}).get(year)


def _layer3_by_end(company_facts):
    """layer3_builder.pyの抽出（sec_concept_definitions.jsonの定義）でのintangible_assets_excl_goodwill"""
    field_def = lb.load_concept_definitions()["fields"]["intangible_assets_excl_goodwill"]
    cf = with_derived_intangibles(company_facts)
    entries, _ = lb.extract_field_raw_entries(cf, field_def, "intangible_assets_excl_goodwill", "TEST")
    return {e["end"]: e["val"] for e in entries}


END = "2025-12-31"


class TestIntangiblePriority:
    def test_excl_preferred_when_breakdown_coexists(self):
        # EXCL（合計）と内訳（有限＋無期限）が併存 → EXCLを採用（内訳の合計がEXCLより小さくても）
        cf = _facts(**{
            INTANGIBLE_EXCL_TAG: [_inst(END, 1_000)],
            INTANGIBLE_FINITE_TAG: [_inst(END, 700)],
        })
        assert _resolved_by_end(cf)[END] == (1_000, INTANGIBLE_EXCL_TAG)
        assert _layer2_annual(cf) == 1_000
        assert _layer3_by_end(cf)[END] == 1_000

    def test_no_double_count(self):
        # EXCL=有限＋無期限のとき、EXCLと内訳を足した2,000にはならない
        cf = _facts(**{
            INTANGIBLE_EXCL_TAG: [_inst(END, 1_000)],
            INTANGIBLE_FINITE_TAG: [_inst(END, 600)],
            INTANGIBLE_INDEFINITE_TAG: [_inst(END, 400)],
        })
        assert _resolved_by_end(cf)[END] == (1_000, INTANGIBLE_EXCL_TAG)
        assert _layer2_annual(cf) == 1_000
        assert _layer3_by_end(cf)[END] == 1_000

    def test_finite_plus_indefinite_when_no_excl(self):
        cf = _facts(**{
            INTANGIBLE_FINITE_TAG: [_inst(END, 600)],
            INTANGIBLE_INDEFINITE_TAG: [_inst(END, 400)],
        })
        val, tags = _resolved_by_end(cf)[END]
        assert val == 1_000
        assert tags == f"{INTANGIBLE_FINITE_TAG}+{INTANGIBLE_INDEFINITE_TAG}"
        assert _layer2_annual(cf) == 1_000
        assert _layer3_by_end(cf)[END] == 1_000

    def test_only_one_breakdown_tag_is_used_alone(self):
        cf = _facts(**{INTANGIBLE_INDEFINITE_TAG: [_inst(END, 400)]})
        assert _resolved_by_end(cf)[END] == (400, INTANGIBLE_INDEFINITE_TAG)

    def test_only_including_goodwill_tag_is_none(self):
        # ASTS型: INCL（のれん込み）しか無い → 派生概念を作らずNone（INCL−のれんは使わない）
        cf = _facts(**{
            INTANGIBLE_INCL_GOODWILL_TAG: [_inst(END, 1_500)],
            "Goodwill": [_inst(END, 500)],
        })
        us_gaap = with_derived_intangibles(cf)["facts"]["us-gaap"]
        assert INTANGIBLE_RESOLVED_DERIVED not in us_gaap
        assert INTANGIBLE_SUM_DERIVED not in us_gaap
        assert _layer2_annual(cf) is None
        assert _layer3_by_end(cf) == {}

    def test_excl_below_breakdown_sum_uses_sum(self):
        # PEP型: EXCL 500に対し有限1,219＋無期限13,847（合計が内訳より小さいことは定義上ない）
        cf = _facts(**{
            INTANGIBLE_EXCL_TAG: [_inst(END, 500)],
            INTANGIBLE_FINITE_TAG: [_inst(END, 1_219)],
            INTANGIBLE_INDEFINITE_TAG: [_inst(END, 13_847)],
        })
        assert _resolved_by_end(cf)[END][0] == 15_066
        assert _layer2_annual(cf) == 15_066
        assert _layer3_by_end(cf)[END] == 15_066

    def test_priority_is_per_period_end(self):
        # EXCLがある期末日はEXCL、無い期末日は内訳（期末日ごとに優先順位を解決する）
        q3 = "2025-09-30"
        facts = derive_intangible_resolved_facts({
            INTANGIBLE_EXCL_TAG: {"units": {"USD": [_inst(END, 1_000)]}},
            INTANGIBLE_FINITE_TAG: {"units": {"USD": [
                _inst(END, 700), _inst(q3, 650, accn="0000000000-25-000002", form="10-Q", fp="Q3"),
            ]}},
        })
        by_end = {f["end"]: (f["val"], f["source_tags"]) for f in facts}
        assert by_end == {END: (1_000, INTANGIBLE_EXCL_TAG), q3: (650, INTANGIBLE_FINITE_TAG)}

    def test_original_company_facts_not_modified(self):
        cf = _facts(**{INTANGIBLE_FINITE_TAG: [_inst(END, 600)]})
        with_derived_intangibles(cf)
        assert set(cf["facts"]["us-gaap"]) == {INTANGIBLE_FINITE_TAG}


class TestLayer2PeriodData:
    def test_tag_sources_recorded_and_no_new_periods(self):
        parser = SECParser()
        extracted = {
            "total_assets": {"annual": {2025: 10_000}, "quarterly": {}},
            "goodwill": {"annual": {2025: 300, 2019: 250}, "quarterly": {}},
            "intangible_assets_excl_goodwill": {
                "annual": {2025: 1_000}, "quarterly": {},
                "_source_tags_by_val": {1_000: INTANGIBLE_EXCL_TAG},
            },
        }
        # TCE控除項目だけの年度（2019）はファイルを作らない
        assert parser._get_available_years(extracted) == [2025]
        data = parser._build_period_data(extracted, 2025, is_annual=True)
        assert data["bs"]["goodwill"] == 300
        assert data["bs"]["intangible_assets_excl_goodwill"] == 1_000
        assert data["bs_tag_sources"] == {
            "goodwill": "Goodwill",
            "intangible_assets_excl_goodwill": INTANGIBLE_EXCL_TAG,
        }

    def test_layer3_fields_defined_as_stock(self):
        fields = lb.load_concept_definitions()["fields"]
        for name in ("goodwill", "intangible_assets_excl_goodwill", "preferred_stock", "minority_interest"):
            assert fields[name]["category"] == "stock"
        assert fields["intangible_assets_excl_goodwill"]["candidates"] == [INTANGIBLE_RESOLVED_DERIVED]
