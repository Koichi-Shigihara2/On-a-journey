"""
tests/test_rice_report_txt_none_display.py

[[RICE-ADJ-ASYMMETRIC-ZERO-1]]の回帰テスト（report.txt表示側）。

calculate_rice()がrice/rice_adjをNoneで返すようになったことに伴い、
pipeline.py::_generate_report()の[N. RICE METRICS]セクションが
`rice_bear_d.get('rice', 'N/A')`のようなパターンで文字列化していた箇所は
バグになる: to_dict()は"rice"キー自体を常に持つ（値がNoneでも）ため、
dict.get(key, default)の第2引数（デフォルト値）はキーが存在しない場合
にしか使われず、値がNoneの場合はNoneがそのまま返る。修正前はこの結果
report.txtに「BASE: RICE=None」という生のNone文字列が出力されてしまう。

本テストはtest_pipeline_logic.pyの既存フィクスチャ（_make_pipe/
_minimal_valuation/_minimal_score_data/_minimal_extra）を再利用する。

実行方法:
    python -m pytest tests/test_rice_report_txt_none_display.py -v
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from test_pipeline_logic import (  # noqa: E402
    _make_pipe, _minimal_valuation, _minimal_score_data, _minimal_extra,
)


class TestRiceNoneDisplaysAsNA:
    def test_base_rice_none_shows_na_not_python_none_string(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        val = _minimal_valuation()
        val["rice"]["base"] = {
            "rice": None, "rice_adj": None, "growth_rate": 0.20,
            "rice_na_reason": "cf<=0（投資再生産効率が構造的に測定不能、cf=-0.200）",
        }
        report = pipe._generate_report(
            "TEST", val, _minimal_score_data(), _minimal_extra()
        )
        assert "BASE: RICE=None" not in report
        assert "BASE: RICE=N/A (cf<=0" in report

    def test_bear_and_bull_rice_none_also_show_na(self, tmp_path):
        pipe = _make_pipe(tmp_path)
        val = _minimal_valuation()
        val["rice"]["bear"] = {"rice": None, "rice_adj": None, "growth_rate": 0.10}
        val["rice"]["bull"] = {"rice": None, "rice_adj": None, "growth_rate": 0.30}
        val["rice"]["base"] = {"rice": None, "rice_adj": None, "growth_rate": 0.20}
        report = pipe._generate_report(
            "TEST", val, _minimal_score_data(), _minimal_extra()
        )
        assert "RICE=None" not in report
        assert "BEAR: RICE=N/A" in report
        assert "BULL: RICE=N/A" in report

    def test_base_rice_available_number_still_displays_normally(self, tmp_path):
        """測定可能な場合は既存通り数値がそのまま表示されること（後退互換）"""
        pipe = _make_pipe(tmp_path)
        val = _minimal_valuation()
        val["rice"]["base"] = {"rice": 5.0, "rice_adj": 4.0, "growth_rate": 0.20}
        report = pipe._generate_report(
            "TEST", val, _minimal_score_data(), _minimal_extra()
        )
        assert "BASE: RICE=5.0" in report


class TestMatrixKeyMetricYAndLabelShowReason:
    """[[RICE-ADJ-ASYMMETRIC-ZERO-1]]仕上げ対応（2026-09-19）: MATRIX表示
    （Key_Metric_Y・Label、_compute_matrix_position()経由）もreport.txtと
    同様に「N/A（理由）」形式で理由を表示すること。CIX/ENTG/XOM型
    （旧実装でも負値だった通常ケース）は"OCF赤字"ラベルを踏襲し、
    SPIR型（符号相殺で旧実装が見かけ上プラスだったケース）はcf負である
    旨の理由を表示する。"""

    def test_ocf_deficit_case_shows_na_with_ocf_deficit_label(self, tmp_path):
        """CIX/ENTG/XOM型: rice_na_reason="OCF赤字"の場合、Key_Metric_Y・
        Labelとも「N/A (OCF赤字)」（旧固定ラベルと同一文言）を表示する"""
        pipe = _make_pipe(tmp_path)
        val = _minimal_valuation(upside=-10.0)  # qx=False -> xL="割高"
        val["rice"]["base"] = {
            "rice": None, "rice_adj": None, "growth_rate": 0.20,
            "rice_na_reason": "OCF赤字",
        }
        report = pipe._generate_report(
            "TEST", val, _minimal_score_data(), _minimal_extra()
        )
        assert "Key_Metric_Y: RICE = N/A (OCF赤字)" in report
        assert "Label: 割高×N/A (OCF赤字)" in report

    def test_sign_cancellation_case_shows_na_with_cf_negative_reason(self, tmp_path):
        """SPIR型: rice_na_reason にcf負を示す文言が入っている場合、
        Key_Metric_Y・Labelとも「OCF赤字」ではなくその文言をそのまま表示する
        （"低効率"への誤分類が解消されていることの確認）"""
        pipe = _make_pipe(tmp_path)
        val = _minimal_valuation(upside=-10.0)
        val["rice"]["base"] = {
            "rice": None, "rice_adj": None, "growth_rate": 0.20,
            "rice_na_reason": "cf=-0.070のためCFがマイナスで投資再生産効率が測定不能",
        }
        report = pipe._generate_report(
            "TEST", val, _minimal_score_data(), _minimal_extra()
        )
        assert "N/A (OCF赤字)" not in report
        assert "Key_Metric_Y: RICE = N/A (cf=-0.070のため" in report
        assert "Label: 割高×N/A (cf=-0.070のため" in report
        assert "低効率" not in report
