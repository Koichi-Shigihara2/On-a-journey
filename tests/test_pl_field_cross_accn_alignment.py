"""
tests/test_pl_field_cross_accn_alignment.py

[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案bの回帰テスト。

SECParser._align_cost_of_revenue_to_revenue_period()が、
「revenue − cost_of_revenue ≠ gross_profit」という数学的矛盾が現に
存在する年度についてのみ、revenueと同一accn・同一期間のcost_of_revenue
候補で矛盾が厳密に解消する場合に限り置換することを確認する
（欠損穴埋め型のゲート条件、既存の正しい値・矛盾のない年度には一切触れない）。

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
