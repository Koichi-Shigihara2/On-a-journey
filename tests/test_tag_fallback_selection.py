"""
tests/test_tag_fallback_selection.py

LLY-CAPEX-STALE-1 Phase 2a の回帰テスト。
「候補タグ群の中で最初に条件を満たしたものを採用して打ち切る」方式から
「最小件数を満たす候補の中から最新end日が最も新しいものを採用する」方式への
選定ロジック転換（quarterly.py::_select_best_candidate・
parser.py::_extract_values_best_candidate）と、タグリスト統合
（common/sec_data/tag_definitions.py）を検証する。

実行方法:
    python -m pytest tests/test_tag_fallback_selection.py -v
"""

from common.sec_data.quarterly import build_raw_table, _select_best_candidate
from common.sec_data.parser import SECParser
from common.sec_data.tag_definitions import TAG_CANDIDATES


def _entry(start, end, val, fp, form="10-Q", filed=None):
    return {
        "start": start, "end": end, "val": val, "accn": "0000000000-00-000000",
        "fp": fp, "fy": int(end[:4]), "form": form, "filed": filed or (end + "T00:00:00"),
    }


# ─────────────────────────────────────────────
# tag_definitions.py
# ─────────────────────────────────────────────

def test_capital_expenditure_includes_lly_new_tag():
    """LLYが2023年以降申告する新タグが候補リストに含まれていること"""
    assert "PaymentsToAcquireOtherPropertyPlantAndEquipment" in TAG_CANDIDATES["CAPITAL_EXPENDITURE"]


# ─────────────────────────────────────────────
# quarterly.py::_select_best_candidate
# ─────────────────────────────────────────────

def test_select_best_candidate_prefers_freshest_end_over_first_match():
    """最小件数を満たす候補が複数ある場合、リスト順ではなく最新end日を採用する（LLY型）"""
    company_facts = {
        "facts": {"us-gaap": {
            # 旧タグ: 件数(4)は満たすが2022年で申告停止（stale）
            "STALE_TAG": {"units": {"USD": [
                _entry("2022-01-01", "2022-03-31", 100, "Q1"),
                _entry("2022-01-01", "2022-06-30", 110, "Q2"),
                _entry("2022-01-01", "2022-09-30", 120, "Q3"),
                _entry("2022-01-01", "2022-12-31", 130, "Q1"),
            ]}},
            # 新タグ: 件数も多く、最新end日が新しい
            "FRESH_TAG": {"units": {"USD": [
                _entry("2025-01-01", "2025-03-31", 500, "Q1"),
                _entry("2025-01-01", "2025-06-30", 510, "Q2"),
                _entry("2025-01-01", "2025-09-30", 520, "Q3"),
                _entry("2026-01-01", "2026-03-31", 530, "Q1"),
            ]}},
        }}
    }
    result, source_tag = _select_best_candidate(
        company_facts, "USD", "PRIMARY_MISSING", [],
        ("STALE_TAG", "FRESH_TAG"), min_count=4,
    )
    ends = sorted(e["end"] for e in result)
    assert ends[-1] == "2026-03-31", f"最新end日が新しい候補(FRESH_TAG)を採用すべき: {ends}"
    assert source_tag == "FRESH_TAG", "採用概念名(source_tag)がFRESH_TAGであるべき"


def test_select_best_candidate_falls_back_to_most_count_when_none_qualify():
    """最小件数を満たす候補が皆無の場合は従来通り最多件数の候補を採用する"""
    company_facts = {
        "facts": {"us-gaap": {
            "SPARSE_A": {"units": {"USD": [
                _entry("2024-01-01", "2024-03-31", 10, "Q1"),
            ]}},
            "SPARSE_B": {"units": {"USD": [
                _entry("2024-01-01", "2024-03-31", 20, "Q1"),
                _entry("2024-01-01", "2024-06-30", 21, "Q2"),
            ]}},
        }}
    }
    result, source_tag = _select_best_candidate(
        company_facts, "USD", "PRIMARY_MISSING", [],
        ("SPARSE_A", "SPARSE_B"), min_count=4,
    )
    assert len(result) == 2, "件数がより多い候補(SPARSE_B)を採用すべき"
    assert source_tag == "SPARSE_B"


def test_select_best_candidate_ties_prefer_priority_order():
    """同着（end日が同一）の場合は優先順位（primary→fallback順）を維持する"""
    company_facts = {
        "facts": {"us-gaap": {
            "SECOND_TAG": {"units": {"USD": [
                _entry("2025-01-01", "2025-03-31", 999, "Q1"),
                _entry("2025-01-01", "2025-06-30", 999, "Q2"),
                _entry("2025-01-01", "2025-09-30", 999, "Q3"),
                _entry("2025-01-01", "2025-12-31", 999, "Q1"),
            ]}},
        }}
    }
    primary_processed = [
        {"end": "2025-03-31", "val": 100, "is_annual": False},
        {"end": "2025-06-30", "val": 101, "is_annual": False},
        {"end": "2025-09-30", "val": 102, "is_annual": False},
        {"end": "2025-12-31", "val": 103, "is_annual": False},
    ]
    result, source_tag = _select_best_candidate(
        company_facts, "USD", "PRIMARY_TAG", primary_processed,
        ("SECOND_TAG",), min_count=4,
    )
    assert result is primary_processed, "同着の場合はprimaryを維持すべき"
    assert source_tag is None, "primary採用時はsource_tagがNoneであるべき（provenance不要）"


def test_build_raw_table_ltdebt_flyw_scenario_uses_fresher_tag():
    """FLYW型（[[SCHEMA-NORMALIZED-ISSUES-1]]③派生、2026-09-23）:
    primary(LongTermDebtNoncurrent)は件数(min_count=4)を満たすが2022年で
    申告停止、fallback(LongTermDebt)の方が新しいデータを持つ場合、件数条件
    だけでは_select_best_candidate()が起動されず陳腐化したprimaryの値
    （最新end=2022-12-31、val=0）がそのまま採用されてしまっていた
    （実データ照合済み: EDGAR原本のBS本体では2025-09-30時点のLong-term debt
    は$15,000千、LongTermDebtタグでのみ申告されている）。
    最新end日の新旧比較を追加し、fallbackの方が新しい場合は起動対象と
    することで、この陳腐化ケースを検知できるようにする。
    """
    us_gaap = {}
    for concept in ["NetCashProvidedByUsedInOperatingActivities", "NetIncomeLoss",
                     "Revenues", "GrossProfit", "StockholdersEquity", "Assets"]:
        us_gaap[concept] = {"units": {"USD": [
            _entry("2025-01-01", "2025-12-31", 1000, "FY", form="10-K"),
        ]}}
    # primary: 件数(6)はmin_count(4)を満たすが2022-12-31で申告停止（陳腐化）
    us_gaap["LongTermDebtNoncurrent"] = {"units": {"USD": [
        _entry("2021-01-01", "2021-09-30", 25933000, "Q3"),
        _entry("2021-01-01", "2021-12-31", 25939000, "FY", form="10-K"),
        _entry("2022-01-01", "2022-03-31", 25939000, "Q1"),
        _entry("2022-01-01", "2022-06-30", 25939000, "Q2"),
        _entry("2022-01-01", "2022-09-30", 25939000, "Q3"),
        _entry("2022-01-01", "2022-12-31", 0, "FY", form="10-K"),
    ]}}
    # fallback: primaryより新しいデータを持つ（実際の直近BS本体値）
    us_gaap["LongTermDebt"] = {"units": {"USD": [
        _entry("2021-01-01", "2021-09-30", 25933000, "Q3"),
        _entry("2021-01-01", "2021-12-31", 25939000, "FY", form="10-K"),
        _entry("2024-01-01", "2024-12-31", 0, "Q3"),
        _entry("2025-01-01", "2025-03-31", 60000000, "Q1"),
        _entry("2025-01-01", "2025-06-30", 60000000, "Q2"),
        _entry("2025-01-01", "2025-09-30", 15000000, "Q3"),
    ]}}
    company_facts = {"facts": {"us-gaap": us_gaap}}

    raw = build_raw_table("FLYWTEST", company_facts)
    ltdebt = raw["fields"]["LTDebt"]
    latest = max(ltdebt, key=lambda e: e["end"])
    assert latest["end"] == "2025-09-30", (
        "陳腐化したprimary(LongTermDebtNoncurrent)ではなく、"
        f"より新しいfallback(LongTermDebt)を採用すべき: {latest}"
    )
    assert latest["val"] == 15000000


def test_select_best_candidate_stale_check_does_not_break_lly_capex_count_route():
    """LLY-CAPEX-STALE-1回帰確認: 件数不足経由（use_min）で既に起動していた
    ケースが、新設したend日比較(stale)の追加によって壊れないこと"""
    us_gaap = {}
    for concept in ["NetCashProvidedByUsedInOperatingActivities", "NetIncomeLoss",
                     "Revenues", "GrossProfit", "StockholdersEquity", "Assets"]:
        us_gaap[concept] = {"units": {"USD": [
            _entry("2025-01-01", "2025-12-31", 1000, "FY", form="10-K"),
        ]}}
    us_gaap["PaymentsToAcquireProductiveAssets"] = {"units": {"USD": [
        _entry("2022-01-01", "2022-03-31", 365400000, "Q1"),
        _entry("2022-01-01", "2022-06-30", 736400000, "Q2"),
        _entry("2022-01-01", "2022-09-30", 1353600000, "Q3"),
        _entry("2021-01-01", "2021-09-30", 1018400000, "Q3"),
    ]}}
    us_gaap["PaymentsToAcquireOtherPropertyPlantAndEquipment"] = {"units": {"USD": [
        _entry("2025-01-01", "2025-03-31", 1509500000, "Q1"),
        _entry("2025-01-01", "2025-06-30", 3206600000, "Q2"),
        _entry("2025-01-01", "2025-09-30", 5294300000, "Q3"),
        _entry("2026-01-01", "2026-03-31", 2326000000, "Q1"),
    ]}}
    company_facts = {"facts": {"us-gaap": us_gaap}}
    raw = build_raw_table("LLYTEST2", company_facts)
    capex = raw["fields"]["CapEx"]
    q_only = [e for e in capex if not e.get("is_annual")]
    latest = max(q_only, key=lambda e: e["end"])
    assert latest["end"] == "2026-03-31"
    assert latest["val"] == 2326000000


def test_build_raw_table_lly_capex_uses_new_tag():
    """LLYを模した company_facts（旧タグ4件+新タグ多数）でCapExが新タグの最新値を反映すること"""
    us_gaap = {}
    # OCF等、他の必須フィールドはNVDA同様の最小構成にしておく（KeyErrorを避ける）
    for concept in ["NetCashProvidedByUsedInOperatingActivities", "NetIncomeLoss",
                     "Revenues", "GrossProfit", "StockholdersEquity", "Assets"]:
        us_gaap[concept] = {"units": {"USD": [
            _entry("2025-01-01", "2025-12-31", 1000, "FY", form="10-K"),
        ]}}
    us_gaap["PaymentsToAcquireProductiveAssets"] = {"units": {"USD": [
        _entry("2022-01-01", "2022-03-31", 365400000, "Q1"),
        _entry("2022-01-01", "2022-06-30", 736400000, "Q2"),
        _entry("2022-01-01", "2022-09-30", 1353600000, "Q3"),
        _entry("2021-01-01", "2021-09-30", 1018400000, "Q3"),
    ]}}
    us_gaap["PaymentsToAcquireOtherPropertyPlantAndEquipment"] = {"units": {"USD": [
        _entry("2025-01-01", "2025-03-31", 1509500000, "Q1"),
        _entry("2025-01-01", "2025-06-30", 3206600000, "Q2"),
        _entry("2025-01-01", "2025-09-30", 5294300000, "Q3"),
        _entry("2026-01-01", "2026-03-31", 2326000000, "Q1"),
    ]}}

    company_facts = {"facts": {"us-gaap": us_gaap}}
    raw = build_raw_table("LLYTEST", company_facts)
    capex = raw["fields"]["CapEx"]
    q_only = [e for e in capex if not e.get("is_annual")]
    assert q_only, "CapEx四半期データが取得できていない"
    latest = max(q_only, key=lambda e: e["end"])
    assert latest["end"] == "2026-03-31", f"新タグの最新四半期を採用すべき: {latest}"
    assert latest["val"] == 2326000000


# ─────────────────────────────────────────────
# parser.py::_extract_values_best_candidate / _extract_single_key
# ─────────────────────────────────────────────

def test_parser_capital_expenditure_lly_scenario():
    """LLY型: 旧タグはquarterlyのみでannualなし、新タグはannual/quarterly両方 → 新タグ採用"""
    us_gaap = {
        "PaymentsToAcquireProductiveAssets": {"units": {"USD": [
            _entry("2022-01-01", "2022-03-31", 365400000, "Q1"),
            _entry("2022-01-01", "2022-06-30", 736400000, "Q2"),
            _entry("2022-01-01", "2022-09-30", 1353600000, "Q3"),
        ]}},
        "PaymentsToAcquireOtherPropertyPlantAndEquipment": {"units": {"USD": [
            _entry("2023-01-01", "2023-12-31", 3448000000, "FY", form="10-K"),
            _entry("2024-01-01", "2024-12-31", 5058000000, "FY", form="10-K"),
            _entry("2025-01-01", "2025-12-31", 7841000000, "FY", form="10-K"),
            _entry("2026-01-01", "2026-03-31", 2326000000, "Q1"),
        ]}},
    }
    parser = SECParser()
    result = parser._extract_values_best_candidate(
        us_gaap,
        ["PaymentsToAcquirePropertyPlantAndEquipment",
         "PaymentsToAcquireProductiveAssets",
         "PaymentsForCapitalImprovements",
         "PaymentsToAcquireOtherPropertyPlantAndEquipment"],
        fiscal_end_month=12,
    )
    assert result["annual"] == {2023: 3448000000, 2024: 5058000000, 2025: 7841000000}
    assert result["quarterly"].get("2026Q1") == 2326000000


def test_parser_best_candidate_no_annual_data_falls_back_gracefully():
    """annualデータを持つ候補が皆無の場合は空を返す（従来のall-or-nothing挙動を維持）"""
    us_gaap = {
        "QUARTERLY_ONLY_TAG": {"units": {"USD": [
            _entry("2022-01-01", "2022-03-31", 100, "Q1"),
        ]}},
    }
    parser = SECParser()
    result = parser._extract_values_best_candidate(
        us_gaap, ["QUARTERLY_ONLY_TAG"], fiscal_end_month=12,
    )
    assert result["annual"] == {}
    assert result["quarterly"] == {"2022Q1": 100}


def test_parser_missing_all_keys_returns_empty():
    parser = SECParser()
    result = parser._extract_values_best_candidate({}, ["NOT_PRESENT"], fiscal_end_month=12)
    assert result == {"annual": {}, "quarterly": {}}
