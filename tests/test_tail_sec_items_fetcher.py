"""
tests/test_tail_sec_items_fetcher.py

TANUKI TAIL — src/tail/sec_items_fetcher.py のユニットテスト（[[TAIL-SEC-ITEMS-1]]）。
Item境界抽出（TOC誤検知除外・Part II境界限定）・「変更なし」検知・
保存ロジックをネットワークアクセスなしで検証する。

実行方法:
    python -m pytest tests/test_tail_sec_items_fetcher.py -v
"""

import sys
import os
import json
import re

_TAIL_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "tail"))
sys.path.insert(0, _TAIL_DIR)

import sec_items_fetcher as sif  # noqa: E402


class TestLooksLikeTocEntry:
    def test_toc_entry_with_pagenum_and_next_item_is_detected(self):
        text = "Item 1A.\nRisk Factors\n12\nItem 1B.\nUnresolved Staff Comments\n62\n"
        next_res = [re.compile(r"(?i)item\s+1b[\.\s]"), re.compile(r"(?i)item\s+2[\.\s]")]
        assert sif._looks_like_toc_entry(text, 0, next_res) is True

    def test_real_heading_with_prose_is_not_detected(self):
        text = "ITEM 1A. RISK FACTORS\nInvesting in our common stock involves a high degree of risk."
        next_res = [re.compile(r"(?i)item\s+1b[\.\s]"), re.compile(r"(?i)item\s+2[\.\s]")]
        assert sif._looks_like_toc_entry(text, 0, next_res) is False

    def test_pagenum_without_next_item_nearby_is_not_toc(self):
        text = "Item 1A.\nRisk Factors\n12\nSome unrelated prose continues here without a marker."
        next_res = [re.compile(r"(?i)item\s+1b[\.\s]"), re.compile(r"(?i)item\s+2[\.\s]")]
        assert sif._looks_like_toc_entry(text, 0, next_res) is False


class TestExtractItemSection:
    def test_skips_toc_and_finds_real_section(self):
        toc = "Item 1A.\nRisk Factors\n12\nItem 1B.\nUnresolved Staff Comments\n62\n"
        filler = "x" * 500
        real = "ITEM 1A. RISK FACTORS\nInvesting in our stock involves risk. " + "y" * 300
        boundary = "ITEM 1B. UNRESOLVED STAFF COMMENTS\nNone."
        text = toc + filler + real + boundary

        item_re = re.compile(r"(?i)item\s+1a[\.\s]")
        next_res = [re.compile(r"(?i)item\s+1b[\.\s]")]
        anchor_re = re.compile(r"(?i)risk\s+factors")

        start, seg = sif.extract_item_section(text, item_re, next_res, anchor_re)
        assert start == text.index("ITEM 1A. RISK FACTORS")
        assert "Investing in our stock involves risk" in seg
        assert "UNRESOLVED STAFF COMMENTS" not in seg

    def test_restrict_after_re_excludes_part1_occurrence(self):
        """Part Iの同一Item番号（例: Item1=Financial Statements）を
        PART II境界指定で除外できること"""
        part1_body = "Item 1. Financial Statements\n" + "a" * 200
        part2_divider = "\nPART II\nOTHER INFORMATION\n"
        part2_body = "Item 1. Legal Proceedings\nWe are subject to various claims. " + "b" * 200
        boundary = "Item 1A. Risk Factors\n..."
        text = part1_body + part2_divider + part2_body + boundary

        item_re = re.compile(r"(?i)item\s+1[\.\s](?!a)")
        next_res = [re.compile(r"(?i)item\s+1a[\.\s]")]
        anchor_re = re.compile(r"(?i)legal\s+proceedings")
        restrict_re = re.compile(r"(?i)part\s+ii\b")

        start, seg = sif.extract_item_section(
            text, item_re, next_res, anchor_re, restrict_after_re=restrict_re,
        )
        assert start == text.index("Item 1. Legal Proceedings")
        assert "Financial Statements" not in seg
        assert "We are subject to various claims" in seg

    def test_no_match_returns_none(self):
        text = "This document has no item markers at all."
        item_re = re.compile(r"(?i)item\s+1a[\.\s]")
        next_res = [re.compile(r"(?i)item\s+1b[\.\s]")]
        anchor_re = re.compile(r"(?i)risk\s+factors")
        start, seg = sif.extract_item_section(text, item_re, next_res, anchor_re)
        assert start is None
        assert seg == ""

    def test_anchor_disambiguates_same_item_number_different_meaning(self):
        """10-QのItem2はPart I(MD&A)とPart II(Unregistered Sales)で意味が
        異なる。anchor_reによりMD&A側のみが選ばれること"""
        mda = "Item 2. Management's Discussion and Analysis\nRevenue increased. " + "c" * 200
        boundary = "Item 3. Quantitative Disclosures\n..."
        unregistered = "Item 2. Unregistered Sales of Equity Securities\nNone. " + "d" * 200
        text = mda + boundary + unregistered

        item_re = re.compile(r"(?i)item\s+2[\.\s]")
        next_res = [re.compile(r"(?i)item\s+3[\.\s]"), re.compile(r"(?i)item\s+4[\.\s]")]
        anchor_re = re.compile(r"(?i)management.{0,3}s\s+discussion\s+and\s+analysis")

        start, seg = sif.extract_item_section(text, item_re, next_res, anchor_re)
        assert start == text.index("Item 2. Management's Discussion")
        assert "Revenue increased" in seg
        assert "Unregistered Sales" not in seg

    def test_max_chars_truncates_segment(self):
        real = "ITEM 1A. RISK FACTORS\n" + "z" * 100
        text = real
        item_re = re.compile(r"(?i)item\s+1a[\.\s]")
        next_res = [re.compile(r"(?i)item\s+1b[\.\s]")]
        anchor_re = re.compile(r"(?i)risk\s+factors")
        start, seg = sif.extract_item_section(text, item_re, next_res, anchor_re, max_chars=30)
        assert len(seg) == 30


class TestDetectNoChange:
    def test_returns_none_when_no_change_re_is_none(self):
        assert sif.detect_no_change("anything here", None) is None

    def test_detects_celh_style_pure_no_change(self):
        text = (
            "Except for the risks discussed elsewhere in this Quarterly Report, "
            "during the reporting period covered by this Quarterly Report, "
            "there have been no material changes to our risk factors as set forth in Part I,"
        )
        cfg = sif.ITEM_CONFIGS["risk_factors"]
        assert sif.detect_no_change(text, cfg["no_change_re"]) is True

    def test_detects_sofi_style_no_change_except_below(self):
        text = (
            "There are no material changes from the risk factors set forth in our "
            "2025 Annual Report on Form 10-K except as set forth below."
        )
        cfg = sif.ITEM_CONFIGS["risk_factors"]
        assert sif.detect_no_change(text, cfg["no_change_re"]) is True

    def test_pltr_style_full_restatement_is_not_detected(self):
        """PLTR型（文言自体が存在しない全文再掲）はFalseになる既知の限界"""
        text = (
            "Investing in our Class A common stock involves a high degree of risk. "
            "Risk Factor Summary. Our business is subject to numerous risks..."
        )
        cfg = sif.ITEM_CONFIGS["risk_factors"]
        assert sif.detect_no_change(text, cfg["no_change_re"]) is False


class TestSaveResult:
    def test_saves_period_latest_and_index(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sif, "DATA_DIR", str(tmp_path))
        result = {
            "ticker": "TEST", "item_key": "risk_factors", "period": "2026Q2",
            "source": "10-Q", "changed": False, "excerpt": "x", "excerpt_ja": None,
        }
        sif._save_result(result)

        item_dir = os.path.join(str(tmp_path), "risk_factors", "TEST")
        with open(os.path.join(item_dir, "2026Q2.json"), encoding="utf-8") as f:
            assert json.load(f)["period"] == "2026Q2"
        with open(os.path.join(item_dir, "latest.json"), encoding="utf-8") as f:
            assert json.load(f)["period"] == "2026Q2"
        with open(os.path.join(item_dir, "index.json"), encoding="utf-8") as f:
            assert json.load(f)["periods"] == ["2026Q2"]

    def test_index_merges_across_calls_without_clobbering(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sif, "DATA_DIR", str(tmp_path))
        sif._save_result({"ticker": "TEST", "item_key": "mda", "period": "2025FY"})
        sif._save_result({"ticker": "TEST", "item_key": "mda", "period": "2026Q1"})

        index_path = os.path.join(str(tmp_path), "mda", "TEST", "index.json")
        with open(index_path, encoding="utf-8") as f:
            periods = json.load(f)["periods"]
        assert set(periods) == {"2025FY", "2026Q1"}
        # latest.jsonは最後に保存した呼び出し（2026Q1）の内容
        with open(os.path.join(str(tmp_path), "mda", "TEST", "latest.json"), encoding="utf-8") as f:
            assert json.load(f)["period"] == "2026Q1"


class TestReportDateConversion:
    def test_report_date_to_fy(self):
        assert sif._report_date_to_fy("2025-12-31") == "2025FY"

    def test_report_date_to_fy_invalid_falls_back(self):
        assert sif._report_date_to_fy("not-a-date") == "not-a-dateFY"

    def test_report_date_to_quarter(self):
        assert sif._report_date_to_quarter("2026-06-30") == "2026Q2"
        assert sif._report_date_to_quarter("2026-03-31") == "2026Q1"
