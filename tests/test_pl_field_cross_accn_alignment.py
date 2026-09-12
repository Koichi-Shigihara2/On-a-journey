"""
tests/test_pl_field_cross_accn_alignment.py

[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案a・b・eの回帰テスト。

SECParser._align_cost_of_revenue_to_revenue_period()（案a・b）が、
「revenue − cost_of_revenue ≠ gross_profit」という数学的矛盾が現に
存在する年度についてのみ、revenueと同一accn・同一期間のcost_of_revenue
候補で矛盾が厳密に解消する場合に限り置換することを確認する
（欠損穴埋め型のゲート条件、既存の正しい値・矛盾のない年度には一切触れない）。

SECParser._align_revenue_and_cost_to_gross_profit_own_accn()（案e）が、
gross_profitをアンカーにrevenue・cost_of_revenue両方を是正するケースも
[[CRM-REVENUE-COGS-TAG-COVERAGE-GAP-1]]対応（_REVENUE_ALIGNMENT_
CANDIDATES・_COST_OF_REVENUE_ALIGNMENT_CANDIDATESへのSalesRevenue
ServicesNet・CostOfServices追加）を機に併せてカバーする。

実行方法:
    python -m pytest tests/test_pl_field_cross_accn_alignment.py -v
"""

from common.sec_data.parser import SECParser


def _extracted(revenue: dict, cost_of_revenue: dict, gross_profit: dict) -> dict:
    """{year: {"val":..., "accn":...}} からextracted構造を組み立てる"""
    def _build(field_map):
        annual = {y: v["val"] for y, v in field_map.items()}
        prov = {y: {"accn": v["accn"], "filed": v.get("filed", ""),
                     "is_own_data": v.get("is_own_data", True), "fy_tag": y}
                for y, v in field_map.items()}
        return {"annual": annual, "quarterly": {}, "_annual_provenance": prov}

    def _build_gp(field_map):
        annual = {y: v["val"] for y, v in field_map.items()}
        return {"annual": annual, "quarterly": {}, "_annual_provenance": {}}

    return {
        "revenue": _build(revenue),
        "cost_of_revenue": _build(cost_of_revenue),
        "gross_profit": _build_gp(gross_profit),
    }


def _us_gaap_entry(accn, start, end, val, tag="Revenues"):
    return {tag: {"units": {"USD": [
        {"accn": accn, "start": start, "end": end, "val": val, "filed": "2020-01-01"}
    ]}}}


def _merge_us_gaap(*dicts):
    merged = {}
    for d in dicts:
        for tag, tagdata in d.items():
            merged.setdefault(tag, {"units": {"USD": []}})
            merged[tag]["units"]["USD"].extend(tagdata["units"]["USD"])
    return merged


def test_aligns_cost_of_revenue_when_mismatch_exists_and_resolves_exactly():
    """revenue/cost_of_revenueが別accnで矛盾が存在し、revenueと同一accn・
    同一期間の正しいCostOfRevenue候補で矛盾が厳密に解消する場合、
    そちらへ置換される（CRM実データ相当）"""
    extracted = _extracted(
        revenue={2013: {"val": 3050195000, "accn": "accn_rev_2013"}},
        cost_of_revenue={2013: {"val": 968428000, "accn": "accn_cogs_wrong"}},
        gross_profit={2013: {"val": 2366616000}},
    )
    us_gaap = _merge_us_gaap(
        _us_gaap_entry("accn_rev_2013", "2012-02-01", "2013-01-31", 3050195000, tag="Revenues"),
        _us_gaap_entry("accn_rev_2013", "2012-02-01", "2013-01-31", 683579000, tag="CostOfRevenue"),
    )
    parser = SECParser()
    parser._align_cost_of_revenue_to_revenue_period(extracted, us_gaap)

    assert extracted["cost_of_revenue"]["annual"][2013] == 683579000
    prov = extracted["cost_of_revenue"]["_annual_provenance"][2013]
    assert prov["accn"] == "accn_rev_2013"
    assert prov["accn_aligned"] is True


def test_does_not_touch_years_with_no_existing_mismatch():
    """revenue − cost_of_revenue == gross_profitが既に成立している年度は、
    revenue/cost_of_revenueのaccnが異なっていても一切触れない
    （ゲート条件: 実データ検証でGOOGL(2008)等の巻き添えを防止するために追加）"""
    extracted = _extracted(
        revenue={2008: {"val": 21796000000, "accn": "accn_rev"}},
        cost_of_revenue={2008: {"val": 8621506000, "accn": "accn_cogs_other"}},
        gross_profit={2008: {"val": 13174494000}},  # rev - cogs と厳密一致（矛盾なし）
    )
    us_gaap = _merge_us_gaap(
        _us_gaap_entry("accn_rev", "2007-01-01", "2008-01-01", 21796000000, tag="Revenues"),
        _us_gaap_entry("accn_rev", "2007-01-01", "2008-01-01", 8622000000, tag="CostOfRevenue"),
    )
    parser = SECParser()
    parser._align_cost_of_revenue_to_revenue_period(extracted, us_gaap)

    assert extracted["cost_of_revenue"]["annual"][2008] == 8621506000
    prov = extracted["cost_of_revenue"]["_annual_provenance"][2008]
    assert "accn_aligned" not in prov


def test_realigns_same_accn_different_period_when_it_resolves_mismatch():
    """revenue/cost_of_revenueが既に同一accnでも、cost_of_revenueだけが
    異なる期間（隣接年度の比較列）を誤って参照しており、revenueと同一
    期間の候補で矛盾が厳密に解消する場合は置換する（CRM(2009-2013)実データ
    相当。「accnが一致＝同じ期間を見ている」という当初の前提が誤りだったと
    判明したための2026-09-09拡張。1つのaccnに複数の(start,end)期間が
    混在するケースへの対応）"""
    extracted = _extracted(
        revenue={2020: {"val": 1000, "accn": "accn_a"}},
        cost_of_revenue={2020: {"val": 400, "accn": "accn_a"}},  # accn_a内の別期間の値を誤採用
        gross_profit={2020: {"val": 999}},  # 矛盾あり(1000-400=600 != 999)
    )
    us_gaap = _merge_us_gaap(
        _us_gaap_entry("accn_a", "2019-01-01", "2020-01-01", 1000, tag="Revenues"),
        # accn_a内・revenueと同一期間の正しい候補（1000-1=999=gross_profit）
        _us_gaap_entry("accn_a", "2019-01-01", "2020-01-01", 1, tag="CostOfRevenue"),
    )
    parser = SECParser()
    parser._align_cost_of_revenue_to_revenue_period(extracted, us_gaap)

    assert extracted["cost_of_revenue"]["annual"][2020] == 1
    prov = extracted["cost_of_revenue"]["_annual_provenance"][2020]
    assert prov["accn"] == "accn_a"
    assert prov["accn_aligned"] is True


def test_does_not_touch_same_accn_when_replacement_does_not_resolve():
    """revenue/cost_of_revenueが同一accnで矛盾があっても、同一期間の候補に
    置換して尚矛盾が解消しない場合は現状維持する（同一accnへのゲート緩和後も
    「厳密解消のみ採用」という安全条件自体は変わらないことの回帰確認）"""
    extracted = _extracted(
        revenue={2020: {"val": 1000, "accn": "accn_a"}},
        cost_of_revenue={2020: {"val": 400, "accn": "accn_a"}},
        gross_profit={2020: {"val": 999}},  # 矛盾あり(1000-400=600 != 999)
    )
    us_gaap = _merge_us_gaap(
        _us_gaap_entry("accn_a", "2019-01-01", "2020-01-01", 1000, tag="Revenues"),
        # 置換しても 1000-500=500 != 999 のため採用されない
        _us_gaap_entry("accn_a", "2019-01-01", "2020-01-01", 500, tag="CostOfRevenue"),
    )
    parser = SECParser()
    parser._align_cost_of_revenue_to_revenue_period(extracted, us_gaap)

    assert extracted["cost_of_revenue"]["annual"][2020] == 400
    prov = extracted["cost_of_revenue"]["_annual_provenance"][2020]
    assert "accn_aligned" not in prov


def test_keeps_current_value_when_no_candidate_in_revenue_accn():
    """矛盾は存在するが、revenueのaccn内に一致するcost_of_revenue候補が
    存在しない場合は現状維持"""
    extracted = _extracted(
        revenue={2019: {"val": 5000, "accn": "accn_rev"}},
        cost_of_revenue={2019: {"val": 2000, "accn": "accn_other"}},
        gross_profit={2019: {"val": 9999}},  # 矛盾あり(5000-2000=3000 != 9999)
    )
    us_gaap = _merge_us_gaap(
        _us_gaap_entry("accn_rev", "2018-01-01", "2019-01-01", 5000, tag="Revenues"),
        # accn_rev内にCostOfRevenue等のタグが一切存在しない
    )
    parser = SECParser()
    parser._align_cost_of_revenue_to_revenue_period(extracted, us_gaap)

    assert extracted["cost_of_revenue"]["annual"][2019] == 2000
    prov = extracted["cost_of_revenue"]["_annual_provenance"][2019]
    assert prov["accn"] == "accn_other"


def test_keeps_current_value_when_candidate_does_not_resolve_mismatch():
    """矛盾は存在し、revenueのaccn内に候補も見つかるが、置換しても矛盾が
    解消しない場合は採用しない（KULR型の巻き添え防止と同じ設計）"""
    extracted = _extracted(
        revenue={2017: {"val": 36556000000, "accn": "accn_rev"}},
        cost_of_revenue={2017: {"val": 27994000000, "accn": "accn_other"}},
        gross_profit={2017: {"val": 8562000000}},  # 36556-27994=8562 (real, matches)
    )
    # ここでは意図的に gross_profit と矛盾する状態を作る
    extracted["gross_profit"]["annual"][2017] = 999999999
    us_gaap = _merge_us_gaap(
        _us_gaap_entry("accn_rev", "2016-01-01", "2017-01-01", 36556000000, tag="Revenues"),
        # accn_rev内の候補(31118000000)を採用しても 36556000000-31118000000=5438000000
        # であり gross_profit(999999999)とは一致しない → 採用しない
        _us_gaap_entry("accn_rev", "2016-01-01", "2017-01-01", 31118000000, tag="CostOfRevenue"),
    )
    parser = SECParser()
    parser._align_cost_of_revenue_to_revenue_period(extracted, us_gaap)

    assert extracted["cost_of_revenue"]["annual"][2017] == 27994000000


def test_handles_multiple_years_independently():
    """複数年度が混在する場合、矛盾があり解消可能な年度のみ個別に置換される"""
    extracted = _extracted(
        revenue={
            2019: {"val": 5000, "accn": "accn_rev_2019"},
            2020: {"val": 6000, "accn": "accn_rev_2020"},
        },
        cost_of_revenue={
            2019: {"val": 2000, "accn": "accn_rev_2019"},  # 既に一致（対象外）
            2020: {"val": 2500, "accn": "accn_other_2020"},  # 矛盾あり（対象）
        },
        gross_profit={
            2019: {"val": 3000},  # 5000-2000=3000 (矛盾なし)
            2020: {"val": 3700},  # 6000-2500=3500 != 3700 (矛盾あり)
        },
    )
    us_gaap = _merge_us_gaap(
        _us_gaap_entry("accn_rev_2019", "2018-01-01", "2019-01-01", 5000, tag="Revenues"),
        _us_gaap_entry("accn_rev_2020", "2019-01-01", "2020-01-01", 6000, tag="Revenues"),
        # 6000-2300=3700=gross_profit なので矛盾が解消する
        _us_gaap_entry("accn_rev_2020", "2019-01-01", "2020-01-01", 2300, tag="CostOfRevenue"),
    )
    parser = SECParser()
    parser._align_cost_of_revenue_to_revenue_period(extracted, us_gaap)

    assert extracted["cost_of_revenue"]["annual"][2019] == 2000  # 変化なし
    assert extracted["cost_of_revenue"]["annual"][2020] == 2300  # 置換された
    assert extracted["cost_of_revenue"]["_annual_provenance"][2020]["accn_aligned"] is True


def test_noop_when_fields_absent():
    """revenue/cost_of_revenue/gross_profitのいずれかのフィールド自体が
    存在しない場合は何もしない（例外を送出しない）"""
    parser = SECParser()
    extracted = {"revenue": {"annual": {2020: 100}, "_annual_provenance": {}}}
    parser._align_cost_of_revenue_to_revenue_period(extracted, {})  # no KeyError


def test_ignores_non_annual_duration_candidates():
    """矛盾は存在するが、revenueのaccn内の候補タグが期間長340-380日でない
    （四半期等）場合は対象外として現状維持する"""
    extracted = _extracted(
        revenue={2022: {"val": 900, "accn": "accn_rev"}},
        cost_of_revenue={2022: {"val": 300, "accn": "accn_other"}},
        gross_profit={2022: {"val": 999}},  # 矛盾あり(900-300=600 != 999)
    )
    us_gaap = _merge_us_gaap(
        _us_gaap_entry("accn_rev", "2021-01-01", "2022-01-01", 900, tag="Revenues"),
        _us_gaap_entry("accn_rev", "2021-10-01", "2022-01-01", 250, tag="CostOfRevenue"),  # 92日、四半期相当
    )
    parser = SECParser()
    parser._align_cost_of_revenue_to_revenue_period(extracted, us_gaap)

    assert extracted["cost_of_revenue"]["annual"][2022] == 300


def test_skips_when_gross_profit_is_none():
    """gross_profitが未確定（derived前でNone）の年度は、比較不能として
    対象外とする（backfill前のderived候補との巻き添え比較を避けるための
    ゲート）"""
    extracted = _extracted(
        revenue={2009: {"val": 3734300000, "accn": "accn_rev"}},
        cost_of_revenue={2009: {"val": 1823673000, "accn": "accn_other"}},
        gross_profit={},  # 2009年度のgross_profitが未確定
    )
    us_gaap = _merge_us_gaap(
        _us_gaap_entry("accn_rev", "2008-01-01", "2009-01-01", 3734300000, tag="Revenues"),
        _us_gaap_entry("accn_rev", "2008-01-01", "2009-01-01", 1865828000, tag="CostOfRevenue"),
    )
    parser = SECParser()
    parser._align_cost_of_revenue_to_revenue_period(extracted, us_gaap)

    assert extracted["cost_of_revenue"]["annual"][2009] == 1823673000


def test_finds_cost_of_services_tag_in_revenue_accn_crm_2017_style():
    """[[CRM-REVENUE-COGS-TAG-COVERAGE-GAP-1]]: revenueと同一accn・同一
    期間にCostOfServices（_COST_OF_REVENUE_ALIGNMENT_CANDIDATES追加分、
    CRM自身のFY2017本人filingが使うタグ）しか存在しない場合でも発見でき、
    矛盾が厳密に解消するなら採用する（CRM(2017)実データ相当：
    8391984000-2234039000=6157945000=gross_profit）"""
    extracted = _extracted(
        revenue={2017: {"val": 8391984000, "accn": "accn_crm_2017"}},
        cost_of_revenue={2017: {"val": 2234000000, "accn": "accn_other"}},  # 別accnの丸め値
        gross_profit={2017: {"val": 6157945000}},
    )
    us_gaap = _merge_us_gaap(
        _us_gaap_entry("accn_crm_2017", "2016-02-01", "2017-01-31", 8391984000, tag="Revenues"),
        _us_gaap_entry("accn_crm_2017", "2016-02-01", "2017-01-31", 2234039000, tag="CostOfServices"),
    )
    parser = SECParser()
    parser._align_cost_of_revenue_to_revenue_period(extracted, us_gaap)

    assert extracted["cost_of_revenue"]["annual"][2017] == 2234039000
    prov = extracted["cost_of_revenue"]["_annual_provenance"][2017]
    assert prov["accn"] == "accn_crm_2017"
    assert prov["accn_aligned"] is True


class TestAlignRevenueAndCostToGrossProfitOwnAccn:
    """[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案e
    （_align_revenue_and_cost_to_gross_profit_own_accn）の回帰テスト。

    既存のテストファイルは案a・bのみを対象としており案eは未カバーだった
    ため、[[CRM-REVENUE-COGS-TAG-COVERAGE-GAP-1]]対応（
    _REVENUE_ALIGNMENT_CANDIDATES・_COST_OF_REVENUE_ALIGNMENT_CANDIDATES
    へのSalesRevenueServicesNet・CostOfServices追加）を機に新設する。
    """

    @staticmethod
    def _extracted_with_gp_accn(revenue, cost_of_revenue, gross_profit_val, gp_accn, year):
        """gross_profit自身のaccnを持つextracted構造を組み立てる
        （案eはgp_prov["accn"]をアンカーとして参照するため、共通ヘルパー
        _extracted()の_build_gp（provenance空）では検証できない）"""
        def _build(field_map):
            annual = {y: v["val"] for y, v in field_map.items()}
            prov = {y: {"accn": v["accn"], "filed": v.get("filed", ""),
                         "is_own_data": v.get("is_own_data", False), "fy_tag": y}
                    for y, v in field_map.items()}
            return {"annual": annual, "quarterly": {}, "_annual_provenance": prov}

        return {
            "revenue": _build(revenue),
            "cost_of_revenue": _build(cost_of_revenue),
            "gross_profit": {
                "annual": {year: gross_profit_val},
                "quarterly": {},
                "_annual_provenance": {year: {"accn": gp_accn, "filed": "", "is_own_data": True, "fy_tag": year}},
            },
        }

    def test_finds_sales_revenue_services_net_and_cost_of_services_crm_2018_style(self):
        """revenue・cost_of_revenueが両方とも別accn（FY2019比較列相当）
        から採用されており、gross_profitの採用元accn（CRM自身のFY2018
        本人filing相当）にSalesRevenueServicesNet・CostOfServices
        （今回追加した拡張候補タグ）の両方が存在し、その差がgross_profitと
        厳密に一致する場合、revenue・cost_of_revenue両方をそちらへ置換する
        （CRM(2018)実データ相当:
        10480012000-2773522000=7706490000=gross_profit）"""
        extracted = self._extracted_with_gp_accn(
            revenue={2018: {"val": 10540000000, "accn": "accn_fy2019_restated"}},
            cost_of_revenue={2018: {"val": 2773000000, "accn": "accn_fy2019_restated"}},
            gross_profit_val=7706490000,
            gp_accn="accn_crm_own_2018",
            year=2018,
        )
        us_gaap = _merge_us_gaap(
            _us_gaap_entry("accn_crm_own_2018", "2017-02-01", "2018-01-31", 7706490000, tag="GrossProfit"),
            _us_gaap_entry("accn_crm_own_2018", "2017-02-01", "2018-01-31", 10480012000, tag="SalesRevenueServicesNet"),
            _us_gaap_entry("accn_crm_own_2018", "2017-02-01", "2018-01-31", 2773522000, tag="CostOfServices"),
        )
        parser = SECParser()
        parser._align_revenue_and_cost_to_gross_profit_own_accn(extracted, us_gaap)

        assert extracted["revenue"]["annual"][2018] == 10480012000
        assert extracted["cost_of_revenue"]["annual"][2018] == 2773522000
        rev_prov = extracted["revenue"]["_annual_provenance"][2018]
        cogs_prov = extracted["cost_of_revenue"]["_annual_provenance"][2018]
        assert rev_prov["accn"] == "accn_crm_own_2018"
        assert rev_prov["gp_anchor_realigned"] is True
        assert rev_prov["gp_anchor_realigned_tag"] == "SalesRevenueServicesNet"
        assert cogs_prov["accn"] == "accn_crm_own_2018"
        assert cogs_prov["gp_anchor_realigned"] is True
        assert cogs_prov["gp_anchor_realigned_tag"] == "CostOfServices"

    def test_does_not_apply_when_candidates_do_not_resolve_mismatch(self):
        """gp採用元accnにSalesRevenueServicesNet・CostOfServicesが存在
        しても、その差がgross_profitと厳密に一致しない場合は現状維持する
        （新規追加タグがゲート条件自体を緩めていないことの確認）"""
        extracted = self._extracted_with_gp_accn(
            revenue={2018: {"val": 10540000000, "accn": "accn_fy2019_restated"}},
            cost_of_revenue={2018: {"val": 2773000000, "accn": "accn_fy2019_restated"}},
            gross_profit_val=7706490000,
            gp_accn="accn_crm_own_2018",
            year=2018,
        )
        us_gaap = _merge_us_gaap(
            _us_gaap_entry("accn_crm_own_2018", "2017-02-01", "2018-01-31", 7706490000, tag="GrossProfit"),
            _us_gaap_entry("accn_crm_own_2018", "2017-02-01", "2018-01-31", 10480012000, tag="SalesRevenueServicesNet"),
            # 差し替えても 10480012000-2700000000=7780012000 != 7706490000 のため不採用
            _us_gaap_entry("accn_crm_own_2018", "2017-02-01", "2018-01-31", 2700000000, tag="CostOfServices"),
        )
        parser = SECParser()
        parser._align_revenue_and_cost_to_gross_profit_own_accn(extracted, us_gaap)

        assert extracted["revenue"]["annual"][2018] == 10540000000
        assert extracted["cost_of_revenue"]["annual"][2018] == 2773000000

    def test_does_not_touch_years_with_no_existing_mismatch(self):
        """revenue - cost_of_revenue == gross_profitが既に成立している
        場合、gp採用元accnにSalesRevenueServicesNet・CostOfServicesが
        存在していても一切触れない（ゲート条件: 矛盾のある年度のみ対象）"""
        extracted = self._extracted_with_gp_accn(
            revenue={2018: {"val": 7706490000 + 2773522000, "accn": "accn_crm_own_2018"}},
            cost_of_revenue={2018: {"val": 2773522000, "accn": "accn_crm_own_2018"}},
            gross_profit_val=7706490000,
            gp_accn="accn_crm_own_2018",
            year=2018,
        )
        us_gaap = _merge_us_gaap(
            _us_gaap_entry("accn_crm_own_2018", "2017-02-01", "2018-01-31", 7706490000, tag="GrossProfit"),
            _us_gaap_entry("accn_crm_own_2018", "2017-02-01", "2018-01-31", 99999999999, tag="SalesRevenueServicesNet"),
            _us_gaap_entry("accn_crm_own_2018", "2017-02-01", "2018-01-31", 1, tag="CostOfServices"),
        )
        parser = SECParser()
        parser._align_revenue_and_cost_to_gross_profit_own_accn(extracted, us_gaap)

        assert extracted["revenue"]["annual"][2018] == 7706490000 + 2773522000
        assert extracted["cost_of_revenue"]["annual"][2018] == 2773522000


class TestRealDataCrmRevenueCogsGrossProfit:
    """common/sec_data/data/CRM/配下の実データ（company_facts.json・
    submissions.json）を使った統合テスト。
    [[CRM-REVENUE-COGS-TAG-COVERAGE-GAP-1]]の修正後、CHECK-46が検知した
    CRM(2018)のGP-COGS不整合（乖離0.5741%、$60,510,000）が解消され、かつ
    副次的に判明したCRM(2017)の丸め誤差（$39,000、WARN-46閾値0.1%未満で
    従来は不発火）も解消されることを確認する。"""

    def test_crm_2018_revenue_cost_of_revenue_gross_profit_reconcile_exactly(self):
        parser = SECParser()
        parsed = parser.parse_company_facts("CRM")
        annual_2018 = parsed["annual"][2018]
        pl = annual_2018["pl"]
        assert pl["revenue"] == 10480012000
        assert pl["cost_of_revenue"] == 2773522000
        assert pl["gross_profit"] == 7706490000
        assert pl["revenue"] - pl["cost_of_revenue"] == pl["gross_profit"]

        prov = annual_2018["pl_provenance"]
        assert prov["revenue"]["accn"] == prov["cost_of_revenue"]["accn"] == prov["gross_profit"]["accn"]
        assert prov["revenue"]["is_own_data"] is True
        assert prov["cost_of_revenue"]["is_own_data"] is True
        assert prov["revenue"]["gp_anchor_realigned"] is True
        assert prov["cost_of_revenue"]["gp_anchor_realigned"] is True

    def test_crm_2017_revenue_cost_of_revenue_gross_profit_reconcile_exactly(self):
        parser = SECParser()
        parsed = parser.parse_company_facts("CRM")
        annual_2017 = parsed["annual"][2017]
        pl = annual_2017["pl"]
        assert pl["revenue"] == 8391984000
        assert pl["cost_of_revenue"] == 2234039000
        assert pl["gross_profit"] == 6157945000
        assert pl["revenue"] - pl["cost_of_revenue"] == pl["gross_profit"]
