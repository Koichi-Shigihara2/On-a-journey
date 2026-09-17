"""
tests/test_fcf_bottom_up_capex_concept.py

[[FCF-CONVRATE-LOWER-DIVERGENCE-1]]ボトムアップFCF移行の一環。

LYFTはCAPITAL_EXPENDITURE標準候補4タグ（PaymentsToAcquireProperty
PlantAndEquipment等）を一度も申告しておらず、実際に計上しているのは
候補リスト外のCapitalizedComputerSoftwareAdditionsのみ（company_facts.json
で実測確認済み）。quarterly.py::TICKER_RESTRICTIONS["LYFT"]["capex_concept"]
でLYFT限定オーバーライドとし、parser.py::_parse_company_facts()（の
capital_expenditure抽出箇所）でそのタグのみを使用するよう配線した。

グローバル候補リストへ追加しなかった理由: 他21銘柄（AAPL/AMZN/APP等）も
同タグを申告しており、うちAPPは既存のquarterly.py::TICKER_RESTRICTIONS
["APP"]["exclude"]=["CapEx"]（TTM側限定の除外設定）と意図せず干渉する
リスクがあるため。

あわせて決済/フロート型事業（LYFT/PAYS/FLYW）の運転資本を可視化する
開示専用フィールド（insurance_reserves/restricted_cash、DCF計算には
未使用）の追加も回帰テストする。

実行方法:
    python -m pytest tests/test_fcf_bottom_up_capex_concept.py -v
"""

from common.sec_data.parser import SECParser
from common.sec_data.quarterly import TICKER_RESTRICTIONS
from common.sec_data.tag_definitions import TAG_CANDIDATES


def _entry(start, end, val, fy, accn="0000000000-00-000001", fp="FY", form="10-K", filed=None):
    return {
        "start": start, "end": end, "val": val, "accn": accn,
        "fp": fp, "fy": fy, "form": form, "filed": filed or (end + "T00:00:00"),
    }


class TestLyftCapexConceptOverride:
    def test_lyft_has_capex_concept_override(self):
        assert TICKER_RESTRICTIONS["LYFT"]["capex_concept"] == "CapitalizedComputerSoftwareAdditions"

    def test_standard_candidates_yield_nothing_for_lyft_shaped_data(self):
        """標準4候補タグのいずれも申告されていない場合、
        capex_concept未適用のままだとcapital_expenditureは抽出できない
        （LYFTの実データ形状を模したケース、これが本タスク着手前の状態）"""
        us_gaap = {
            "CapitalizedComputerSoftwareAdditions": {"units": {"USD": [
                _entry("2022-01-01", "2022-12-31", 12_100_000, fy=2022),
            ]}},
        }
        parser = SECParser()
        result = parser._extract_values_best_candidate(
            us_gaap, list(TAG_CANDIDATES["CAPITAL_EXPENDITURE"]), fiscal_end_month=12,
            anchor_month=12, anchor_day=31, field_name="capital_expenditure",
        )
        assert 2022 not in result["annual"]

    def test_with_capex_concept_override_lyft_value_is_extracted(self):
        """capex_concept適用後（xbrl_keysを単一タグに絞る、parser.py本体の
        配線と同じ操作）はLYFTのソフトウェア資産化費用が抽出できる"""
        us_gaap = {
            "CapitalizedComputerSoftwareAdditions": {"units": {"USD": [
                _entry("2022-01-01", "2022-12-31", 12_100_000, fy=2022),
            ]}},
        }
        parser = SECParser()
        result = parser._extract_values_best_candidate(
            us_gaap, ["CapitalizedComputerSoftwareAdditions"],  # capex_concept適用後の状態
            fiscal_end_month=12, anchor_month=12, anchor_day=31,
            field_name="capital_expenditure",
        )
        assert result["annual"][2022] == 12_100_000


class TestFloatDisclosureFields:
    """[[FCF-CONVRATE-LOWER-DIVERGENCE-1]]: 決済/フロート型事業の運転資本
    開示用フィールド（DCF計算には未使用、report.txt参考表示専用）"""

    def test_insurance_reserves_and_restricted_cash_registered_as_instant_fields(self):
        parser = SECParser()
        assert "insurance_reserves" in parser.INSTANT_FACT_FIELDS
        assert "restricted_cash" in parser.INSTANT_FACT_FIELDS

    def test_insurance_reserves_extracted_from_accrued_insurance_current(self):
        entry = _entry("2025-12-31", "2025-12-31", 2_180_426_000, fy=2025)
        entry.pop("start")  # instant fact（start_dateを持たない）を模す
        us_gaap = {"AccruedInsuranceCurrent": {"units": {"USD": [entry]}}}
        parser = SECParser()
        result = parser._extract_values_best_candidate(
            us_gaap, ["AccruedInsuranceCurrent"], fiscal_end_month=12,
            anchor_month=12, anchor_day=31, field_name="insurance_reserves",
        )
        assert result["annual"][2025] == 2_180_426_000

    def test_restricted_cash_priority_order_prefers_first_available_tag(self):
        e1 = _entry("2025-12-31", "2025-12-31", 143_917_060, fy=2025)
        e1.pop("start")
        e2 = _entry("2025-12-31", "2025-12-31", 164_984_711, fy=2025)
        e2.pop("start")
        us_gaap = {
            "RestrictedCash": {"units": {"USD": [e1]}},
            "RestrictedCashAndCashEquivalents": {"units": {"USD": [e2]}},
        }
        parser = SECParser()
        result = parser._extract_values_best_candidate(
            us_gaap, list(parser.XBRL_MAPPING["restricted_cash"]),
            fiscal_end_month=12, anchor_month=12, anchor_day=31,
            field_name="restricted_cash",
        )
        assert result["annual"][2025] == 143_917_060  # 優先タグ（RestrictedCash）を採用
