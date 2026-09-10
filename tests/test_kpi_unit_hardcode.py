"""
tests/test_kpi_unit_hardcode.py

[[KPI-UNIT-HARDCODE-USD-1]]の回帰テスト。
- src/tail/xbrl_segment_fetcher.py::_infer_kpi_unit() が比率KPI名
  （「率」「マージン」等を含む）を"ratio"、それ以外を"USD"と正しく
  判定すること
- src/tail/quarterly_review_generator.py::_fmt_kpi_value() が
  unit="ratio"の値を正しくパーセント表示に変換すること
  （unit="USD"の既存フォーマット・unit="%"の既存フォーマットは
  変更していないことも合わせて確認する）

実行方法:
    python -m pytest tests/test_kpi_unit_hardcode.py -v
"""

import sys
import os

_TAIL_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "tail"))
sys.path.insert(0, _TAIL_DIR)

import xbrl_segment_fetcher as xsf  # noqa: E402
import quarterly_review_generator as qrg  # noqa: E402


class TestInferKpiUnit:
    """_infer_kpi_unit(): KPI名から比率/USDを判定する"""

    def test_ratio_keyword_percent_sign_ja(self):
        # 「率」を含む日本語KPI名（PLTR実データで確認済み）
        assert xsf._infer_kpi_unit("貢献利益率（Commercial）") == "ratio"
        assert xsf._infer_kpi_unit("営業利益率") == "ratio"
        assert xsf._infer_kpi_unit("希薄化後EPS成長率") == "ratio"

    def test_ratio_keyword_margin_ja(self):
        # 「マージン」を含む日本語KPI名（SOFI実データで確認済み）
        assert xsf._infer_kpi_unit("純金利マージン（NIM）") == "ratio"

    def test_ratio_keyword_various_tickers(self):
        # 他ティッカーの「率」系KPI名（TSLA/SOUN/CRWV実データ）
        for name in [
            "エネルギー事業粗利益率",
            "自動車売上総利益率",
            "サービス売上成長率",
            "Technology Platform売上成長率",
            "正味貸倒率（NCO）",
            "研究開発費比率",
            "総売上高成長率",
            "米国売上高成長率",
            "米国以外売上高成長率",
            "Hosted Services売上成長率",
            "Licensing売上成長率",
        ]:
            assert xsf._infer_kpi_unit(name) == "ratio", name

    def test_usd_amount_kpi_names_unaffected(self):
        # 金額系KPI名は引き続き"USD"（実データで確認済みの回帰防止）
        for name in [
            "Commercial売上",
            "Government売上",
            "株式報酬費用",
            "GAAP純利益",
            "営業利益",
            "営業キャッシュフロー",
            "研究開発費",
            "現金及び現金等価物残高",
            "エネルギー受注残高",
            "サブスクリプション売上高",
        ]:
            assert xsf._infer_kpi_unit(name) == "USD", name

    def test_english_margin_rate_keywords(self):
        # 英語名でも判定できること（現状tail_kpi_map.jsonに実例はないが、
        # 将来の英語KPI名追加に備えて動作を明示的に固定する）
        assert xsf._infer_kpi_unit("Operating Margin") == "ratio"
        assert xsf._infer_kpi_unit("Growth Rate") == "ratio"
        assert xsf._infer_kpi_unit("Contribution Ratio") == "ratio"


class TestWriteLayer2OutputUnit:
    """_write_layer2_output(): 出力JSONのunitフィールドがKPI名に応じて
    正しく設定されること（実際の書き込みフローを通して確認）"""

    def test_ratio_and_usd_kpis_get_correct_units(self, tmp_path):
        kpi_data = {
            "貢献利益率（Commercial）": [{"quarter": "2026Q2", "value": 0.78, "filed": "2026-08-04"}],
            "Commercial売上": [{"quarter": "2026Q2", "value": 945432000, "filed": "2026-08-04"}],
            "希薄化後EPS成長率": [],  # missing_kpisのケース（値0件でも単位判定は名前ベース）
        }
        result = xsf._write_layer2_output("TESTX", kpi_data, str(tmp_path))
        assert result == "partial"  # 3件中1件missing

        import json
        with open(tmp_path / "TESTX_layer2.json", encoding="utf-8") as f:
            output = json.load(f)

        assert output["kpis"]["貢献利益率（Commercial）"]["unit"] == "ratio"
        assert output["kpis"]["Commercial売上"]["unit"] == "USD"
        assert output["kpis"]["希薄化後EPS成長率"]["unit"] == "ratio"


class TestFmtKpiValueRatio:
    """_fmt_kpi_value(): unit="ratio"の値がパーセント表示になること。
    既存のunit="USD"・unit="%"の挙動は変更していないことも確認する
    （[[TAIL-THESIS-KPIS-EMPTY-ADBE-APGE-1]]等、表示崩れ防止の観点）。
    """

    def test_ratio_unit_formats_as_percent(self):
        assert qrg._fmt_kpi_value(0.78, "ratio") == "78.0%"
        assert qrg._fmt_kpi_value(-0.234456, "ratio") == "-23.4%"

    def test_usd_unit_unchanged_large_amount(self):
        assert qrg._fmt_kpi_value(945432000, "USD") == "$945.4M"
        assert qrg._fmt_kpi_value(1_500_000_000, "USD") == "$1.50B"

    def test_usd_unit_small_value_fallback_unchanged(self):
        # 既存の「保険」分岐（unit=USDでも小さい値はratio扱い）は維持される
        assert qrg._fmt_kpi_value(0.5, "USD") == "50.0%"

    def test_percent_unit_unchanged(self):
        assert qrg._fmt_kpi_value(78.0, "%") == "78.0%"

    def test_none_value(self):
        assert qrg._fmt_kpi_value(None, "ratio") == "N/A"
