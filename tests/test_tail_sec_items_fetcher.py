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

    def test_restrict_after_re_ignores_trailing_cross_reference_false_positive(self):
        """[[TAIL-SEC-ITEMS-1]] STEP2-Bパイロットで発見した回帰:
        単純な`part\\s+ii\\b`だと文末の証明書等にある「Part II, Item 5.
        Other Information」のようなクロスリファレンスにも一致し、
        `matches[-1]`が本来のPart II区切りより後ろになってしまい抽出が
        失敗する（PLTR 2026-03-31 10-Qで実際に発生）。
        `PART_II_DIVIDER_RE`（"OTHER INFORMATION"が直後に続く場合のみ
        一致）でこれを回避できることを確認する。"""
        part1 = "Item 1. Financial Statements\n" + "a" * 100
        real_divider = "\nPART II - OTHER INFORMATION\n"
        real_body = "Item 1. Legal Proceedings\nWe face claims. " + "b" * 200
        boundary = "Item 1A. Risk Factors\n..."
        trailing_ref = (
            "\nExhibit 31.1 Certification\nas discussed in "
            "Part II, Item 5. Other Information of our prior Quarterly Report.\n"
        )
        text = part1 + real_divider + real_body + boundary + trailing_ref

        item_re = re.compile(r"(?i)item\s+1[\.\s](?!a)")
        next_res = [re.compile(r"(?i)item\s+1a[\.\s]")]
        anchor_re = re.compile(r"(?i)legal\s+proceedings")

        start, seg = sif.extract_item_section(
            text, item_re, next_res, anchor_re,
            restrict_after_re=sif._PART2_RE,
        )
        assert start == text.index("Item 1. Legal Proceedings")
        assert "We face claims" in seg

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
        # report_date未設定の場合は比較不能なため、従来通り最後の呼び出し
        # （2026Q1）の内容がlatest.jsonに残る（フォールバック挙動）
        with open(os.path.join(str(tmp_path), "mda", "TEST", "latest.json"), encoding="utf-8") as f:
            assert json.load(f)["period"] == "2026Q1"

    def test_latest_json_keeps_most_recent_report_date_regardless_of_call_order(
        self, tmp_path, monkeypatch
    ):
        """[[TAIL-SEC-ITEMS-1]] STEP2-Bパイロットで発見した回帰:
        `fetch_quarterly_updates()`はEDGARの返却順（新しい順）で複数
        四半期分を返すため、ループ内でperiodを新しい順（Q2→Q1）に保存
        すると、単純な「最後の呼び出しがlatest」ロジックではQ1（古い方）
        がlatest.jsonに残ってしまっていた（PLTR/SOFI両方で実際に発生）。
        report_dateを比較して新しい方のみ採用することを確認する。"""
        monkeypatch.setattr(sif, "DATA_DIR", str(tmp_path))
        # 新しい順（EDGARの返却順どおり）に保存する
        sif._save_result({
            "ticker": "TEST", "item_key": "risk_factors", "period": "2026Q2",
            "report_date": "2026-06-30",
        })
        sif._save_result({
            "ticker": "TEST", "item_key": "risk_factors", "period": "2026Q1",
            "report_date": "2026-03-31",
        })

        latest_path = os.path.join(str(tmp_path), "risk_factors", "TEST", "latest.json")
        with open(latest_path, encoding="utf-8") as f:
            latest = json.load(f)
        assert latest["period"] == "2026Q2"
        assert latest["report_date"] == "2026-06-30"


class TestTranslateTimeoutConfig:
    """[[TAIL-SEC-ITEMS-1]]: MD&Aのみ翻訳タイムアウトを120秒へ延長
    （STEP2-BパイロットでMD&Aが60秒では複数回タイムアウトする実例を
    確認したため）。他項目・既存Item4は60秒のまま。"""

    def test_mda_uses_120s_timeout(self):
        assert sif.ITEM_CONFIGS["mda"]["translate_timeout"] == 120

    def test_risk_factors_and_legal_proceedings_keep_60s_timeout(self):
        assert sif.ITEM_CONFIGS["risk_factors"]["translate_timeout"] == 60
        assert sif.ITEM_CONFIGS["legal_proceedings"]["translate_timeout"] == 60

    def test_translate_excerpt_default_timeout_is_60s(self, monkeypatch):
        """既存Item4呼び出し（timeout引数省略）が無変更であることを確認"""
        captured = {}

        class _FakeResp:
            def raise_for_status(self):
                pass

            def json(self):
                return {"choices": [{"message": {"content": "ok"}}]}

        def _fake_post(url, headers=None, json=None, timeout=None):
            captured["timeout"] = timeout
            return _FakeResp()

        monkeypatch.setattr(sif._ctrl, "XAI_API_KEY", "dummy")
        monkeypatch.setattr(sif._ctrl.requests, "post", _fake_post)
        sif._ctrl._translate_excerpt("text", "desc")
        assert captured["timeout"] == 60

    def test_translate_excerpt_respects_explicit_timeout(self, monkeypatch):
        captured = {}

        class _FakeResp:
            def raise_for_status(self):
                pass

            def json(self):
                return {"choices": [{"message": {"content": "ok"}}]}

        def _fake_post(url, headers=None, json=None, timeout=None):
            captured["timeout"] = timeout
            return _FakeResp()

        monkeypatch.setattr(sif._ctrl, "XAI_API_KEY", "dummy")
        monkeypatch.setattr(sif._ctrl.requests, "post", _fake_post)
        sif._ctrl._translate_excerpt("text", "desc", timeout=120)
        assert captured["timeout"] == 120


class TestReportDateConversion:
    def test_report_date_to_fy(self):
        assert sif._report_date_to_fy("2025-12-31") == "2025FY"

    def test_report_date_to_fy_invalid_falls_back(self):
        assert sif._report_date_to_fy("not-a-date") == "not-a-dateFY"

    def test_report_date_to_quarter(self):
        assert sif._report_date_to_quarter("2026-06-30") == "2026Q2"
        assert sif._report_date_to_quarter("2026-03-31") == "2026Q1"
