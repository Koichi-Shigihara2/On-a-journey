"""
tests/test_xbrl_segment_fetcher_unit_label.py

[[TAIL-KPI-UNIT-MISLABEL-1]]回帰テスト。src/tail/xbrl_segment_fetcher.py
の`_infer_kpi_unit()`/`_write_layer2_output()`が、KPI名の
「率」「マージン」等のキーワードだけでunitを"ratio"と誤判定せず、
tail_kpi_map.jsonの明示的な`unit`キーを優先することを検証する。

背景: 「Technology Platform売上成長率」のようにKPI名は成長率でも、
実際の抽出方式（revenue_tag単体・layer3_field単体、いずれも除算を
伴わない）は生の絶対値（USD）取得に過ぎないケースが複数存在した。
名前ベースの旧ヒューリスティックはこれらを誤って"ratio"と判定し、
表示層（quarterly_review_generator.py::_fmt_kpi_value）で
USD実額×100の意味不明なパーセント表示を生んでいた。

実行方法:
    python -m pytest tests/test_xbrl_segment_fetcher_unit_label.py -v
"""

import json
import os
import sys

_TAIL_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "tail"))
if _TAIL_DIR not in sys.path:
    sys.path.insert(0, _TAIL_DIR)

import xbrl_segment_fetcher as xsf  # noqa: E402


class TestInferKpiUnit:
    def test_explicit_unit_usd_overrides_name_heuristic(self):
        """KPI名に「成長率」を含んでいても、明示的なunit=USDが優先される"""
        cfg = {"kpi_name": "Technology Platform売上成長率", "unit": "USD"}
        assert xsf._infer_kpi_unit("Technology Platform売上成長率", cfg) == "USD"

    def test_explicit_unit_ratio_is_respected(self):
        """明示的なunit=ratioは従来通りratioのまま"""
        cfg = {"kpi_name": "営業利益率", "unit": "ratio"}
        assert xsf._infer_kpi_unit("営業利益率", cfg) == "ratio"

    def test_no_config_falls_back_to_name_heuristic_ratio(self):
        """kpi_config自体がNone（後方互換）の場合は名前ヒューリスティックにフォールバックする"""
        assert xsf._infer_kpi_unit("営業利益率", None) == "ratio"

    def test_no_config_falls_back_to_name_heuristic_usd(self):
        assert xsf._infer_kpi_unit("Commercial売上", None) == "USD"

    def test_config_present_but_no_unit_key_falls_back_to_name_heuristic(self):
        """unitキー自体が存在しない設定（未対応の既存エントリ）は
        名前ヒューリスティックへフォールバックする"""
        cfg = {"kpi_name": "営業利益率"}
        assert xsf._infer_kpi_unit("営業利益率", cfg) == "ratio"


class TestWriteLayer2OutputUnitLookup:
    def test_kpi_list_unit_override_propagates_to_output_file(self, tmp_path):
        """kpi_listのunit明示指定がlayer2.json出力のunitフィールドに反映される"""
        kpi_list = [
            {"kpi_name": "Technology Platform売上成長率", "unit": "USD"},
            {"kpi_name": "営業利益率", "unit": "ratio"},
        ]
        kpi_data = {
            "Technology Platform売上成長率": [{"quarter": "2026Q2", "value": 50512000, "filed": "2026-08-01"}],
            "営業利益率": [{"quarter": "2026Q2", "value": -0.23, "filed": "2026-08-01"}],
        }
        xsf._write_layer2_output("TESTCO", kpi_data, str(tmp_path), kpi_list=kpi_list)
        out = json.loads((tmp_path / "TESTCO_layer2.json").read_text(encoding="utf-8"))
        assert out["kpis"]["Technology Platform売上成長率"]["unit"] == "USD"
        assert out["kpis"]["営業利益率"]["unit"] == "ratio"

    def test_missing_kpi_list_falls_back_to_name_heuristic(self, tmp_path):
        """kpi_list未指定（後方互換呼び出し）でも既存動作を維持する"""
        kpi_data = {"営業利益率": [{"quarter": "2026Q2", "value": -0.23, "filed": "2026-08-01"}]}
        xsf._write_layer2_output("TESTCO2", kpi_data, str(tmp_path))
        out = json.loads((tmp_path / "TESTCO2_layer2.json").read_text(encoding="utf-8"))
        assert out["kpis"]["営業利益率"]["unit"] == "ratio"


class TestTailKpiMapConfigConsistency:
    """config/tail_kpi_map.json自体の棚卸し結果を固定化する回帰テスト。
    将来新規KPIが追加された際、同種のunit未指定・誤指定を検知する。
    """

    @staticmethod
    def _load_config():
        path = os.path.normpath(
            os.path.join(os.path.dirname(__file__), "..", "config", "tail_kpi_map.json")
        )
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    def test_all_ratio_keyword_kpis_have_explicit_unit(self):
        """KPI名に率・マージン等を含む全エントリが明示的なunitキーを持つこと
        （[[TAIL-KPI-UNIT-MISLABEL-1]]の再発防止: 新規KPI追加時に
        unit指定を忘れると本テストが失敗する）"""
        cfg = self._load_config()
        keywords = ("率", "マージン", "Margin", "Rate", "Ratio")
        missing = []
        for ticker, kpis in cfg.items():
            for kpi in kpis:
                name = kpi.get("kpi_name", "")
                if any(kw in name for kw in keywords) and "unit" not in kpi:
                    missing.append(f"{ticker}: {name}")
        assert not missing, f"unit未指定のratio系KPIが存在: {missing}"

    def test_known_usd_mislabeled_kpis_are_now_usd(self):
        """発見済みの誤ラベル18件が正しくUSDへ修正されていることを確認する"""
        cfg = self._load_config()
        expected_usd = {
            ("SOFI", "Technology Platform売上成長率"),
            ("SOFI", "正味貸倒率（NCO）"),
            ("TSLA", "エネルギー事業粗利益率"),
            ("TSLA", "サービス売上成長率"),
            ("TSLA", "自動車売上総利益率"),
            ("SOUN", "Hosted Services売上成長率"),
            ("SOUN", "Licensing売上成長率"),
            ("SOUN", "米国売上成長率"),
            ("SOUN", "総売上高成長率"),
            ("SOUN", "研究開発費比率"),
            ("SOUN", "営業利益率"),
            ("CRWV", "米国売上高成長率"),
            ("CRWV", "米国以外売上高成長率"),
            ("CRWV", "総売上高成長率"),
        }
        for ticker, name in expected_usd:
            kpi = next(k for k in cfg[ticker] if k["kpi_name"] == name)
            assert kpi.get("unit") == "USD", f"{ticker}/{name} は unit=USD であるべき"

    def test_known_genuine_ratio_kpis_remain_ratio(self):
        """真のratio（formula除算・XBRL側で既にパーセント型のタグ）は維持される"""
        cfg = self._load_config()
        expected_ratio = {
            ("PLTR", "貢献利益率（Commercial）"),
            ("PLTR", "営業利益率"),
            # [[TAIL-LAYER3-FORMULA-YOY-UNSUPPORTED-1]]（2026-09-23）:
            # yoy(eps_diluted)によるLayer3実装化に伴い、旧来の生EPS値
            # 単体取得（unit=USD、実質非機能）から真の成長率計算へ変更。
            ("PLTR", "希薄化後EPS成長率"),
            ("SOFI", "純金利マージン（NIM）"),
        }
        for ticker, name in expected_ratio:
            kpi = next(k for k in cfg[ticker] if k["kpi_name"] == name)
            assert kpi.get("unit") == "ratio", f"{ticker}/{name} は unit=ratio であるべき"
